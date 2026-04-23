package http

import (
	"database/sql"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

// schemaCacheTTL controls how long introspection results are memoized per
// data source. Five minutes keeps the semantic catalog responsive without
// hammering large databases.
const schemaCacheTTL = 5 * time.Minute

// SchemaHandler exposes read-only endpoints that describe the structure of a
// connected data source (tables, columns, constraints). Results are cached
// in-process for schemaCacheTTL.
type SchemaHandler struct {
	db    *gorm.DB
	cache *schemaCache
}

// NewSchemaHandler builds a SchemaHandler with an empty cache.
func NewSchemaHandler(db *gorm.DB) *SchemaHandler {
	return &SchemaHandler{db: db, cache: newSchemaCache()}
}

// RegisterRoutes wires the schema introspection endpoints into the router.
// The routes share the `/data/sources/{id}/schema*` prefix for symmetry with
// the existing DataSourceHandler routes.
func (h *SchemaHandler) RegisterRoutes(r chi.Router) {
	r.Get("/data/sources/{id}/schema", h.ListTables)
	r.Get("/data/sources/{id}/schema/{table}", h.DescribeTable)
}

// tableInfo is the payload returned for each discovered table.
type tableInfo struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Type   string `json:"type,omitempty"`
}

// columnInfo describes a single column, including nullability, default, and
// primary/foreign key membership.
type columnInfo struct {
	Name          string  `json:"name"`
	DataType      string  `json:"data_type"`
	Nullable      bool    `json:"nullable"`
	Default       *string `json:"default,omitempty"`
	OrdinalPos    int     `json:"ordinal_position"`
	IsPrimaryKey  bool    `json:"is_primary_key"`
	IsForeignKey  bool    `json:"is_foreign_key"`
	ForeignTable  string  `json:"foreign_table,omitempty"`
	ForeignColumn string  `json:"foreign_column,omitempty"`
}

// tableDescription is returned by the single-table endpoint.
type tableDescription struct {
	Schema  string       `json:"schema"`
	Name    string       `json:"name"`
	Columns []columnInfo `json:"columns"`
}

// ListTables returns the list of tables available in the configured schema
// of the supplied data source.
func (h *SchemaHandler) ListTables(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid data source ID")
		return
	}

	var ds domain.DataSource
	if err := h.db.Where("id = ?", id).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}

	cacheKey := "tables:" + ds.ID.String()
	if cached, ok := h.cache.get(cacheKey); ok {
		respondSuccess(w, cached)
		return
	}

	if !ds.Engine.UsesDatabaseSQL() {
		// REST/GraphQL/Sentiae sources: return stored metadata (tables field)
		payload := map[string]any{
			"engine": ds.Engine,
			"tables": ds.Tables,
			"note":   "Non-SQL source — schema reflects stored metadata rather than live introspection.",
		}
		h.cache.put(cacheKey, payload)
		respondSuccess(w, payload)
		return
	}

	sqlDB, err := sql.Open(driverForEngine(ds.Engine), ds.ConnectionDSN)
	if err != nil {
		respondInternalError(w, "Connection failed: "+err.Error())
		return
	}
	defer sqlDB.Close()

	rows, err := sqlDB.Query(
		`SELECT table_schema, table_name, table_type
		 FROM information_schema.tables
		 WHERE table_schema = $1
		 ORDER BY table_name`, ds.Schema,
	)
	if err != nil {
		respondInternalError(w, "Schema query failed: "+err.Error())
		return
	}
	defer rows.Close()

	tables := make([]tableInfo, 0)
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Schema, &t.Name, &t.Type); err != nil {
			continue
		}
		tables = append(tables, t)
	}

	payload := map[string]any{
		"engine": ds.Engine,
		"schema": ds.Schema,
		"tables": tables,
	}
	h.cache.put(cacheKey, payload)
	respondSuccess(w, payload)
}

// DescribeTable returns columns + PK/FK metadata for a single table.
func (h *SchemaHandler) DescribeTable(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid data source ID")
		return
	}
	table := chi.URLParam(r, "table")
	if table == "" {
		respondBadRequest(w, "Table name required")
		return
	}

	var ds domain.DataSource
	if err := h.db.Where("id = ?", id).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}

	cacheKey := "table:" + ds.ID.String() + ":" + table
	if cached, ok := h.cache.get(cacheKey); ok {
		respondSuccess(w, cached)
		return
	}

	if !ds.Engine.UsesDatabaseSQL() {
		// Return stored semantic fields as the schema surrogate.
		var fields []domain.SemanticField
		h.db.Where("data_source_id = ? AND table_name = ?", ds.ID, table).Find(&fields)
		cols := make([]columnInfo, 0, len(fields))
		for i, f := range fields {
			cols = append(cols, columnInfo{
				Name:       f.ColumnName,
				DataType:   f.DataType,
				Nullable:   true,
				OrdinalPos: i + 1,
			})
		}
		payload := tableDescription{Schema: ds.Schema, Name: table, Columns: cols}
		h.cache.put(cacheKey, payload)
		respondSuccess(w, payload)
		return
	}

	sqlDB, err := sql.Open(driverForEngine(ds.Engine), ds.ConnectionDSN)
	if err != nil {
		respondInternalError(w, "Connection failed: "+err.Error())
		return
	}
	defer sqlDB.Close()

	// Columns
	colRows, err := sqlDB.Query(
		`SELECT column_name, data_type, is_nullable, column_default, ordinal_position
		 FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2
		 ORDER BY ordinal_position`, ds.Schema, table,
	)
	if err != nil {
		respondInternalError(w, "Column query failed: "+err.Error())
		return
	}
	defer colRows.Close()

	cols := make([]columnInfo, 0)
	colIndex := make(map[string]int)
	for colRows.Next() {
		var (
			name       string
			dataType   string
			nullable   string
			defaultVal sql.NullString
			ordinal    int
		)
		if err := colRows.Scan(&name, &dataType, &nullable, &defaultVal, &ordinal); err != nil {
			continue
		}
		ci := columnInfo{
			Name:       name,
			DataType:   dataType,
			Nullable:   strings.EqualFold(nullable, "YES"),
			OrdinalPos: ordinal,
		}
		if defaultVal.Valid {
			d := defaultVal.String
			ci.Default = &d
		}
		colIndex[name] = len(cols)
		cols = append(cols, ci)
	}

	// Primary keys
	pkRows, err := sqlDB.Query(
		`SELECT kcu.column_name
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		 WHERE tc.constraint_type = 'PRIMARY KEY'
		   AND tc.table_schema = $1 AND tc.table_name = $2`, ds.Schema, table,
	)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var col string
			if err := pkRows.Scan(&col); err == nil {
				if idx, ok := colIndex[col]; ok {
					cols[idx].IsPrimaryKey = true
				}
			}
		}
	}

	// Foreign keys
	fkRows, err := sqlDB.Query(
		`SELECT kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		 JOIN information_schema.constraint_column_usage ccu
		   ON tc.constraint_name = ccu.constraint_name
		  AND tc.table_schema = ccu.table_schema
		 WHERE tc.constraint_type = 'FOREIGN KEY'
		   AND tc.table_schema = $1 AND tc.table_name = $2`, ds.Schema, table,
	)
	if err == nil {
		defer fkRows.Close()
		for fkRows.Next() {
			var col, fTable, fCol string
			if err := fkRows.Scan(&col, &fTable, &fCol); err == nil {
				if idx, ok := colIndex[col]; ok {
					cols[idx].IsForeignKey = true
					cols[idx].ForeignTable = fTable
					cols[idx].ForeignColumn = fCol
				}
			}
		}
	}

	payload := tableDescription{Schema: ds.Schema, Name: table, Columns: cols}
	h.cache.put(cacheKey, payload)
	respondSuccess(w, payload)
}

// schemaCache is a tiny in-process cache keyed by arbitrary strings. Entries
// expire after schemaCacheTTL.
type schemaCache struct {
	mu      sync.RWMutex
	entries map[string]schemaCacheEntry
}

type schemaCacheEntry struct {
	value   any
	expires time.Time
}

func newSchemaCache() *schemaCache {
	return &schemaCache{entries: make(map[string]schemaCacheEntry)}
}

func (c *schemaCache) get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.value, true
}

func (c *schemaCache) put(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = schemaCacheEntry{value: value, expires: time.Now().Add(schemaCacheTTL)}
}
