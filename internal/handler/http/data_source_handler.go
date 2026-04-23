package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/infrastructure/adapters/bigquery"
	"github.com/sentiae/platform-kit/kafka"
	"github.com/sentiae/platform-kit/timetravel"
	_ "github.com/snowflakedb/gosnowflake" // registers "snowflake" driver with database/sql
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DataSourceHandler struct {
	db       *gorm.DB
	pub      kafka.Publisher
	recorder timetravel.Recorder
}

func NewDataSourceHandler(db *gorm.DB, pub kafka.Publisher) *DataSourceHandler {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	return &DataSourceHandler{db: db, pub: pub, recorder: timetravel.NoopRecorder{}}
}

// WithTimeTravelRecorder wires the §13.1 entity-snapshot recorder so
// each DataSource / SemanticField CRUD write surfaces a snapshot row.
func (h *DataSourceHandler) WithTimeTravelRecorder(r timetravel.Recorder) *DataSourceHandler {
	if r != nil {
		h.recorder = r
	}
	return h
}

// emitSource publishes a data_source.* lifecycle event.
func (h *DataSourceHandler) emitSource(r *http.Request, eventType string, ds *domain.DataSource, actor uuid.UUID) {
	if h.pub == nil {
		return
	}
	meta := map[string]any{
		"data_source_id": ds.ID.String(),
		"name":           ds.Name,
		"engine":         string(ds.Engine),
		"status":         string(ds.Status),
		"schema":         ds.Schema,
	}
	if ds.ConnectionID != nil {
		meta["connection_id"] = ds.ConnectionID.String()
	}
	if len(ds.Tables) > 0 {
		meta["tables"] = []string(ds.Tables)
	}
	_ = h.pub.Publish(r.Context(), eventType, kafka.EventData{
		ActorID:        actor.String(),
		ResourceType:   "data_source",
		ResourceID:     ds.ID.String(),
		OrganizationID: ds.OrganizationID.String(),
		Metadata:       meta,
	})
}

// emitField publishes a semantic_field.* event.
func (h *DataSourceHandler) emitField(r *http.Request, eventType string, f *domain.SemanticField, orgID, actor uuid.UUID) {
	if h.pub == nil {
		return
	}
	meta := map[string]any{
		"semantic_field_id": f.ID.String(),
		"data_source_id":    f.DataSourceID.String(),
		"table_name":        f.TableName,
		"column_name":       f.ColumnName,
		"business_name":     f.BusinessName,
		"data_type":         f.DataType,
	}
	if f.Aggregation != "" {
		meta["aggregation"] = f.Aggregation
	}
	if f.Unit != "" {
		meta["unit"] = f.Unit
	}
	if len(f.Tags) > 0 {
		meta["tags"] = []string(f.Tags)
	}
	_ = h.pub.Publish(r.Context(), eventType, kafka.EventData{
		ActorID:        actor.String(),
		ResourceType:   "semantic_field",
		ResourceID:     f.ID.String(),
		OrganizationID: orgID.String(),
		Metadata:       meta,
	})
}

func (h *DataSourceHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data/sources", func(r chi.Router) {
		r.Post("/", h.Create)
		r.Get("/", h.List)
		r.Get("/{id}", h.Get)
		r.Put("/{id}", h.Update)
		r.Delete("/{id}", h.Delete)
		r.Get("/{id}/fields", h.ListFields)
		r.Post("/{id}/fields", h.CreateField)
		r.Put("/{id}/fields/{field_id}", h.UpdateField)
		r.Delete("/{id}/fields/{field_id}", h.DeleteField)
		r.Post("/{id}/sync", h.SyncSchema)
		r.Get("/{id}/tables/{name}/sample", h.GetSample)
		// §12.1 — the spec asks for GET /data-sources/:id/sample?table=X
		// as a flat sibling of the tables-route above. Both resolve to
		// the same handler; the flat route reads `table` from the query
		// string.
		r.Get("/{id}/sample", h.GetSampleByQuery)
	})
}

type createDataSourceRequest struct {
	Name          string     `json:"name"`
	Description   string     `json:"description"`
	Engine        string     `json:"engine"`
	ConnectionID  *uuid.UUID `json:"connection_id,omitempty"`
	ConnectionDSN string     `json:"connection_dsn,omitempty"`
	Schema        string     `json:"schema"`
}

func (h *DataSourceHandler) Create(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	if orgID == "" {
		respondError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Organization required")
		return
	}

	var req createDataSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}

	if req.Schema == "" {
		req.Schema = "public"
	}

	orgUUID, _ := uuid.Parse(orgID)
	userUUID, _ := uuid.Parse(userID)

	ds := &domain.DataSource{
		ID:             uuid.New(),
		OrganizationID: orgUUID,
		Name:           req.Name,
		Description:    req.Description,
		Engine:         domain.DataEngine(req.Engine),
		ConnectionID:   req.ConnectionID,
		ConnectionDSN:  req.ConnectionDSN,
		Schema:         req.Schema,
		Status:         domain.DataSourceStatusPending,
		CreatedBy:      userUUID,
	}

	if err := h.db.Create(ds).Error; err != nil {
		respondInternalError(w, "Failed to create data source")
		return
	}
	h.emitSource(r, "data.source.created", ds, userUUID)
	// §13.1 entity snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "data_source", ds.ID.String(), ds)
	}

	// §12.1 — kick off an async discovery sample so the user sees
	// schema + row previews without having to click "sync". The
	// goroutine swallows errors; operators can always retry with
	// POST /{id}/sync. Only attempt for SQL engines with a DSN
	// configured.
	if ds.Engine.IsSQL() && ds.ConnectionDSN != "" {
		go h.runDiscoverySample(ds.ID, userUUID)
	}

	respondCreated(w, ds)
}

// runDiscoverySample runs the same logic as SyncSchema but untethered
// from a request. It is invoked as a goroutine from Create; errors are
// logged but not surfaced to the caller.
func (h *DataSourceHandler) runDiscoverySample(dsID, actor uuid.UUID) {
	var ds domain.DataSource
	if err := h.db.Where("id = ?", dsID).First(&ds).Error; err != nil {
		return
	}
	// Reject non-SQL engines — BigQuery has its own code path that
	// requires a live *http.Request for header extraction.
	if !ds.Engine.IsSQL() || ds.ConnectionDSN == "" {
		return
	}
	sqlDB, err := sql.Open(driverForEngine(ds.Engine), ds.ConnectionDSN)
	if err != nil {
		return
	}
	defer sqlDB.Close()

	// Flip status to syncing so the UI knows something is happening.
	ds.Status = domain.DataSourceStatusSyncing
	h.db.Save(&ds)

	// Discover tables via information_schema (same approach as
	// SyncSchema; we don't persist semantic fields here — that's the
	// operator's job via /{id}/sync).
	schemaQuery := `SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = ?`
	if ds.Engine == domain.DataEnginePostgres {
		schemaQuery = `SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = $1`
	}
	rows, err := sqlDB.Query(schemaQuery, ds.Schema)
	if err != nil {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(&ds)
		return
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			continue
		}
		tables = append(tables, t)
	}

	sampleLimit := sampleLimitFromEnv()
	if sampleLimit <= 0 {
		return
	}
	sampled := 0
	for _, table := range tables {
		rowsOut, err := h.sampleTable(sqlDB, ds.Schema, table, sampleLimit)
		if err != nil {
			continue
		}
		sample := &domain.DataSourceSample{
			ID:           uuid.New(),
			DataSourceID: ds.ID,
			TableName:    table,
			SampleJSON:   domain.JSONMap{"rows": rowsOut},
			RowCount:     len(rowsOut),
			SampledAt:    time.Now(),
		}
		h.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "data_source_id"}, {Name: "table_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"sample_json", "row_count", "sampled_at", "updated_at"}),
		}).Create(sample)
		sampled++
	}

	// Record tables + lift status to connected if we got any samples.
	tblArr := make(domain.StringArray, 0, len(tables))
	tblArr = append(tblArr, tables...)
	ds.Tables = tblArr
	if sampled > 0 {
		ds.Status = domain.DataSourceStatusConnected
	}
	now := time.Now()
	ds.LastSyncAt = &now
	h.db.Save(&ds)

	if sampled > 0 && h.pub != nil {
		_ = h.pub.Publish(context.Background(), "data.data_source.sampled", kafka.EventData{
			ActorID:        actor.String(),
			ResourceType:   "data_source",
			ResourceID:     ds.ID.String(),
			OrganizationID: ds.OrganizationID.String(),
			Metadata: map[string]any{
				"data_source_id": ds.ID.String(),
				"tables":         tables,
				"sample_rows":    sampled,
				"async":          true,
			},
		})
	}
}

func (h *DataSourceHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid ID")
		return
	}
	var ds domain.DataSource
	if err := h.db.Where("id = ?", id).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}
	respondSuccess(w, ds)
}

func (h *DataSourceHandler) List(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, _ := uuid.Parse(orgID)
	var sources []domain.DataSource
	h.db.Where("organization_id = ?", orgUUID).Order("created_at DESC").Find(&sources)
	respondSuccess(w, map[string]any{"data": sources})
}

func (h *DataSourceHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid ID")
		return
	}
	var ds domain.DataSource
	if err := h.db.Where("id = ?", id).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}
	var updates map[string]any
	json.NewDecoder(r.Body).Decode(&updates)
	h.db.Model(&ds).Updates(updates)
	h.db.Where("id = ?", id).First(&ds)
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emitSource(r, "data.source.updated", &ds, actor)
	// §13.1 entity snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "data_source", ds.ID.String(), ds)
	}
	respondSuccess(w, ds)
}

func (h *DataSourceHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid ID")
		return
	}
	var ds domain.DataSource
	found := h.db.Where("id = ?", id).First(&ds).Error == nil
	h.db.Where("id = ?", id).Delete(&domain.DataSource{})
	if found {
		actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
		h.emitSource(r, "data.source.deleted", &ds, actor)
		// §13.1 tombstone snapshot.
		if h.recorder != nil {
			_ = h.recorder.RecordEntity(r.Context(), "data_source", ds.ID.String(), map[string]any{
				"id":      ds.ID.String(),
				"deleted": true,
			})
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListFields returns semantic fields for a data source.
func (h *DataSourceHandler) ListFields(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid ID")
		return
	}
	var fields []domain.SemanticField
	h.db.Where("data_source_id = ?", id).Order("table_name, column_name").Find(&fields)
	respondSuccess(w, map[string]any{"data": fields})
}

type createFieldRequest struct {
	TableName    string   `json:"table_name"`
	ColumnName   string   `json:"column_name"`
	BusinessName string   `json:"business_name"`
	Description  string   `json:"description"`
	DataType     string   `json:"data_type"`
	Aggregation  string   `json:"aggregation"`
	Unit         string   `json:"unit"`
	Tags         []string `json:"tags"`
	Synonyms     []string `json:"synonyms,omitempty"`
	Aliases      []string `json:"aliases,omitempty"`
	RequiredRole string   `json:"required_role,omitempty"`
}

func (h *DataSourceHandler) CreateField(w http.ResponseWriter, r *http.Request) {
	dsID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid data source ID")
		return
	}
	var req createFieldRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	field := &domain.SemanticField{
		ID:           uuid.New(),
		DataSourceID: dsID,
		TableName:    req.TableName,
		ColumnName:   req.ColumnName,
		BusinessName: req.BusinessName,
		Description:  req.Description,
		DataType:     req.DataType,
		Aggregation:  req.Aggregation,
		Unit:         req.Unit,
		Tags:         domain.StringArray(req.Tags),
		Synonyms:     domain.StringArray(req.Synonyms),
		Aliases:      domain.StringArray(req.Aliases),
		RequiredRole: req.RequiredRole,
	}
	if err := h.db.Create(field).Error; err != nil {
		respondInternalError(w, "Failed to create field")
		return
	}
	// Resolve organization_id by hopping through the data source.
	var ds domain.DataSource
	h.db.Where("id = ?", dsID).First(&ds)
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emitField(r, "data.semantic_field.created", field, ds.OrganizationID, actor)
	respondCreated(w, field)
}

func (h *DataSourceHandler) UpdateField(w http.ResponseWriter, r *http.Request) {
	fieldID, err := uuid.Parse(chi.URLParam(r, "field_id"))
	if err != nil {
		respondBadRequest(w, "Invalid field ID")
		return
	}
	var field domain.SemanticField
	if err := h.db.Where("id = ?", fieldID).First(&field).Error; err != nil {
		respondNotFound(w, "Field not found")
		return
	}
	var updates map[string]any
	json.NewDecoder(r.Body).Decode(&updates)
	h.db.Model(&field).Updates(updates)
	h.db.Where("id = ?", fieldID).First(&field)
	var ds domain.DataSource
	h.db.Where("id = ?", field.DataSourceID).First(&ds)
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emitField(r, "data.semantic_field.updated", &field, ds.OrganizationID, actor)
	respondSuccess(w, field)
}

func (h *DataSourceHandler) DeleteField(w http.ResponseWriter, r *http.Request) {
	fieldID, _ := uuid.Parse(chi.URLParam(r, "field_id"))
	var field domain.SemanticField
	found := h.db.Where("id = ?", fieldID).First(&field).Error == nil
	h.db.Where("id = ?", fieldID).Delete(&domain.SemanticField{})
	if found {
		var ds domain.DataSource
		h.db.Where("id = ?", field.DataSourceID).First(&ds)
		actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
		h.emitField(r, "data.semantic_field.deleted", &field, ds.OrganizationID, actor)
	}
	w.WriteHeader(http.StatusNoContent)
}

// SyncSchema discovers tables/columns from the data source and creates semantic field stubs.
func (h *DataSourceHandler) SyncSchema(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid ID")
		return
	}
	var ds domain.DataSource
	if err := h.db.Where("id = ?", id).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}

	if !ds.Engine.IsSQL() {
		respondBadRequest(w, "Schema sync only supported for SQL databases")
		return
	}

	ds.Status = domain.DataSourceStatusSyncing
	h.db.Save(&ds)

	// Connect to the data source and discover schema via information_schema
	dsn := ds.ConnectionDSN
	if dsn == "" {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(&ds)
		respondBadRequest(w, "No connection DSN configured")
		return
	}

	// BigQuery takes the adapter route: its per-dataset INFORMATION_SCHEMA
	// lives at `<project>.<dataset>.INFORMATION_SCHEMA.COLUMNS` and it is
	// reached through the cloud.google.com/go/bigquery client rather than
	// database/sql.
	if ds.Engine == domain.DataEngineBigQuery {
		h.syncSchemaBigQuery(w, r, &ds)
		return
	}

	sqlDB, err := sql.Open(driverForEngine(ds.Engine), dsn)
	if err != nil {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(&ds)
		respondInternalError(w, "Failed to connect: "+err.Error())
		return
	}
	defer sqlDB.Close()

	// Query information_schema for tables and columns. The placeholder
	// syntax varies by engine — Postgres/pgx uses $1, Snowflake/MySQL/MSSQL
	// use ?. We pass ds.Schema as a bind parameter so each driver renders
	// it correctly.
	schemaQuery := `SELECT table_name, column_name, data_type
		 FROM information_schema.columns
		 WHERE table_schema = ?
		 ORDER BY table_name, ordinal_position`
	if ds.Engine == domain.DataEnginePostgres {
		schemaQuery = `SELECT table_name, column_name, data_type
		 FROM information_schema.columns
		 WHERE table_schema = $1
		 ORDER BY table_name, ordinal_position`
	}
	rows, err := sqlDB.Query(schemaQuery, ds.Schema)
	if err != nil {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(&ds)
		respondInternalError(w, "Schema query failed: "+err.Error())
		return
	}
	defer rows.Close()

	fieldsCreated := 0
	tables := map[string]bool{}
	for rows.Next() {
		var tableName, columnName, dataType string
		if err := rows.Scan(&tableName, &columnName, &dataType); err != nil {
			continue
		}
		tables[tableName] = true

		// Check if field already exists
		var count int64
		h.db.Model(&domain.SemanticField{}).Where(
			"data_source_id = ? AND table_name = ? AND column_name = ?",
			ds.ID, tableName, columnName,
		).Count(&count)

		if count == 0 {
			field := &domain.SemanticField{
				ID:           uuid.New(),
				DataSourceID: ds.ID,
				TableName:    tableName,
				ColumnName:   columnName,
				BusinessName: columnName, // default to column name
				DataType:     dataType,
			}
			h.db.Create(field)
			fieldsCreated++
		}
	}

	// Update data source
	tableList := make(domain.StringArray, 0, len(tables))
	for t := range tables {
		tableList = append(tableList, t)
	}
	ds.Tables = tableList
	ds.Status = domain.DataSourceStatusConnected
	now := time.Now()
	ds.LastSyncAt = &now
	h.db.Save(&ds)

	// Row sampling: pull up to `sampleLimit` rows per table and persist them
	// in data_source_samples. The limit is bounded to avoid pulling large
	// datasets into memory during discovery.
	sampleLimit := sampleLimitFromEnv()
	sampledTables := make([]string, 0, len(tables))
	totalSampleRows := 0
	if sampleLimit > 0 {
		for tableName := range tables {
			rows, err := h.sampleTable(sqlDB, ds.Schema, tableName, sampleLimit)
			if err != nil {
				// Sampling is best-effort — don't fail the whole sync.
				continue
			}
			sample := &domain.DataSourceSample{
				ID:           uuid.New(),
				DataSourceID: ds.ID,
				TableName:    tableName,
				SampleJSON:   domain.JSONMap{"rows": rows},
				RowCount:     len(rows),
				SampledAt:    time.Now(),
			}
			// Upsert on (data_source_id, table_name).
			h.db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "data_source_id"}, {Name: "table_name"}},
				DoUpdates: clause.AssignmentColumns([]string{"sample_json", "row_count", "sampled_at", "updated_at"}),
			}).Create(sample)
			sampledTables = append(sampledTables, tableName)
			totalSampleRows += len(rows)
		}
	}

	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emitSource(r, "data.source.updated", &ds, actor)
	if len(sampledTables) > 0 && h.pub != nil {
		_ = h.pub.Publish(r.Context(), "data.data_source.sampled", kafka.EventData{
			ActorID:        actor.String(),
			ResourceType:   "data_source",
			ResourceID:     ds.ID.String(),
			OrganizationID: ds.OrganizationID.String(),
			Metadata: map[string]any{
				"data_source_id": ds.ID.String(),
				"tables":         sampledTables,
				"sample_rows":    totalSampleRows,
			},
		})
	}

	respondSuccess(w, map[string]any{
		"status":            "connected",
		"tables_found":      len(tables),
		"fields_created":    fieldsCreated,
		"tables_sampled":    len(sampledTables),
		"sample_rows_total": totalSampleRows,
	})
}

// sampleTable runs `SELECT * FROM <schema>.<table> LIMIT <n>` and marshals
// rows into JSON-safe maps. It quotes the schema/table identifiers to
// guard against surprises in lowercase vs. quoted-identifier databases.
func (h *DataSourceHandler) sampleTable(db *sql.DB, schema, table string, limit int) ([]map[string]any, error) {
	q := fmt.Sprintf(`SELECT * FROM %q.%q LIMIT %d`, schema, table, limit)
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, limit)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			v := values[i]
			// Convert []byte to string for JSON friendliness.
			if b, ok := v.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = v
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetSampleByQuery implements GET /data-sources/{id}/sample?table=X.
// Thin wrapper over GetSample so callers can use either URL shape.
func (h *DataSourceHandler) GetSampleByQuery(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid data source ID")
		return
	}
	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		respondBadRequest(w, "table query parameter is required")
		return
	}
	var sample domain.DataSourceSample
	if err := h.db.Where("data_source_id = ? AND table_name = ?", id, tableName).First(&sample).Error; err != nil {
		respondNotFound(w, "No sample captured for this table — run /sync first")
		return
	}
	respondSuccess(w, sample)
}

// GetSample returns the persisted row sample for a given table of a source.
func (h *DataSourceHandler) GetSample(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid data source ID")
		return
	}
	tableName := chi.URLParam(r, "name")
	if tableName == "" {
		respondBadRequest(w, "Table name is required")
		return
	}
	var sample domain.DataSourceSample
	if err := h.db.Where("data_source_id = ? AND table_name = ?", id, tableName).First(&sample).Error; err != nil {
		respondNotFound(w, "No sample captured for this table — run /sync first")
		return
	}
	respondSuccess(w, sample)
}

func sampleLimitFromEnv() int {
	v := os.Getenv("APP_DATA_SAMPLE_LIMIT")
	if v == "" {
		return 100
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 100
	}
	if n > 10000 {
		return 10000
	}
	return n
}

// syncSchemaBigQuery discovers tables + columns for a BigQuery source by
// querying `<project>.<schema>.INFORMATION_SCHEMA.COLUMNS` through the
// google-cloud-go client, then applies the same semantic-field upsert +
// sampling flow that SQL engines get. Schema corresponds to the BigQuery
// dataset name.
func (h *DataSourceHandler) syncSchemaBigQuery(w http.ResponseWriter, r *http.Request, ds *domain.DataSource) {
	ctx := r.Context()
	adapter := bigquery.NewAdapter()

	// Column discovery. ds.Schema is the BigQuery dataset. We pull the
	// project ID directly from the adapter (parsed from DSN) rather than
	// duplicating parsing logic.
	projectID, err := bigqueryProjectFromDSN(ds.ConnectionDSN)
	if err != nil {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(ds)
		respondBadRequest(w, err.Error())
		return
	}
	if ds.Schema == "" || ds.Schema == "public" {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(ds)
		respondBadRequest(w, "BigQuery sync requires ds.Schema to name a dataset (e.g. `analytics`)")
		return
	}

	colSQL := fmt.Sprintf(
		"SELECT table_name, column_name, data_type FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` ORDER BY table_name, ordinal_position",
		projectID, ds.Schema,
	)
	res, err := adapter.QueryWithDSN(ctx, ds.ConnectionDSN, colSQL)
	if err != nil {
		ds.Status = domain.DataSourceStatusError
		h.db.Save(ds)
		respondInternalError(w, "BigQuery schema query failed: "+err.Error())
		return
	}

	fieldsCreated := 0
	tables := map[string]bool{}
	// Map column positions — order is fixed by the SELECT above, but be
	// defensive in case a future BigQuery release reorders the result.
	colIdx := indexOf(res.Columns, "table_name")
	colIdxCol := indexOf(res.Columns, "column_name")
	colIdxType := indexOf(res.Columns, "data_type")
	for _, row := range res.Rows {
		tableName := toString(row, colIdx)
		columnName := toString(row, colIdxCol)
		dataType := toString(row, colIdxType)
		if tableName == "" || columnName == "" {
			continue
		}
		tables[tableName] = true

		var count int64
		h.db.Model(&domain.SemanticField{}).Where(
			"data_source_id = ? AND table_name = ? AND column_name = ?",
			ds.ID, tableName, columnName,
		).Count(&count)
		if count == 0 {
			field := &domain.SemanticField{
				ID:           uuid.New(),
				DataSourceID: ds.ID,
				TableName:    tableName,
				ColumnName:   columnName,
				BusinessName: columnName,
				DataType:     dataType,
			}
			h.db.Create(field)
			fieldsCreated++
		}
	}

	tableList := make(domain.StringArray, 0, len(tables))
	for t := range tables {
		tableList = append(tableList, t)
	}
	ds.Tables = tableList
	ds.Status = domain.DataSourceStatusConnected
	now := time.Now()
	ds.LastSyncAt = &now
	h.db.Save(ds)

	// Sampling is best-effort — small SELECT * ... LIMIT N per table.
	sampleLimit := sampleLimitFromEnv()
	sampledTables := make([]string, 0, len(tables))
	totalSampleRows := 0
	if sampleLimit > 0 {
		for tableName := range tables {
			sampleSQL := fmt.Sprintf("SELECT * FROM `%s.%s.%s` LIMIT %d",
				projectID, ds.Schema, tableName, sampleLimit)
			sres, err := adapter.QueryWithDSN(ctx, ds.ConnectionDSN, sampleSQL)
			if err != nil {
				continue
			}
			rowMaps := make([]map[string]any, 0, len(sres.Rows))
			for _, r := range sres.Rows {
				m := make(map[string]any, len(sres.Columns))
				for i, c := range sres.Columns {
					if i < len(r) {
						m[c] = r[i]
					}
				}
				rowMaps = append(rowMaps, m)
			}
			sample := &domain.DataSourceSample{
				ID:           uuid.New(),
				DataSourceID: ds.ID,
				TableName:    tableName,
				SampleJSON:   domain.JSONMap{"rows": rowMaps},
				RowCount:     len(rowMaps),
				SampledAt:    time.Now(),
			}
			h.db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "data_source_id"}, {Name: "table_name"}},
				DoUpdates: clause.AssignmentColumns([]string{"sample_json", "row_count", "sampled_at", "updated_at"}),
			}).Create(sample)
			sampledTables = append(sampledTables, tableName)
			totalSampleRows += len(rowMaps)
		}
	}

	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emitSource(r, "data.source.updated", ds, actor)
	if len(sampledTables) > 0 && h.pub != nil {
		_ = h.pub.Publish(r.Context(), "data.data_source.sampled", kafka.EventData{
			ActorID:        actor.String(),
			ResourceType:   "data_source",
			ResourceID:     ds.ID.String(),
			OrganizationID: ds.OrganizationID.String(),
			Metadata: map[string]any{
				"data_source_id": ds.ID.String(),
				"tables":         sampledTables,
				"sample_rows":    totalSampleRows,
			},
		})
	}

	respondSuccess(w, map[string]any{
		"status":            "connected",
		"tables_found":      len(tables),
		"fields_created":    fieldsCreated,
		"tables_sampled":    len(sampledTables),
		"sample_rows_total": totalSampleRows,
	})
}

// bigqueryProjectFromDSN extracts the project_id from a `bigquery://<proj>` DSN.
func bigqueryProjectFromDSN(dsn string) (string, error) {
	const prefix = "bigquery://"
	if len(dsn) < len(prefix) || dsn[:len(prefix)] != prefix {
		return "", fmt.Errorf("bigquery DSN must start with bigquery://")
	}
	rest := dsn[len(prefix):]
	// Strip path + query.
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' || rest[i] == '?' {
			rest = rest[:i]
			break
		}
	}
	if rest == "" {
		return "", fmt.Errorf("bigquery DSN missing project_id")
	}
	return rest, nil
}

func indexOf(s []string, target string) int {
	for i, v := range s {
		if v == target {
			return i
		}
	}
	return -1
}

func toString(row []any, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	switch v := row[idx].(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

func driverForEngine(engine domain.DataEngine) string {
	switch engine {
	case domain.DataEnginePostgres:
		return "pgx" // registered by github.com/jackc/pgx/v5/stdlib
	case domain.DataEngineMySQL:
		return "mysql"
	case domain.DataEngineSQLite:
		return "sqlite3"
	case domain.DataEngineMSSQL:
		return "sqlserver"
	case domain.DataEngineSnowflake:
		return "snowflake" // registered by github.com/snowflakedb/gosnowflake
	default:
		return "pgx"
	}
}
