package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/infrastructure/canvasservice"
	"github.com/sentiae/data-service/internal/infrastructure/foundryservice"
	"github.com/sentiae/data-service/internal/usecase"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/gorm"
)

// NLTranslator is the abstract boundary the NL→SQL path uses to reach
// foundry-service. Tests can swap in a fake that returns canned SQL
// without spinning up the real LLM stack.
type NLTranslator interface {
	NLToSQL(ctx context.Context, in foundryservice.NLToSQLInput) (*foundryservice.NLToSQLOutput, error)
}

// defaultAutoSelectConfidenceThreshold is the confidence floor at which
// the NL handler skips the "which data source did you mean?" roundtrip
// and picks the top candidate on the user's behalf. Configurable via
// APP_DATA_NL_AUTO_SELECT_CONFIDENCE.
const defaultAutoSelectConfidenceThreshold = 0.8

// defaultAutoSelectMargin is the confidence gap required between the
// top candidate and its runner-up to auto-pick. When the top two are
// within this margin of each other we return a `needs_selection`
// response so the user can disambiguate.
const defaultAutoSelectMargin = 0.1

type QueryHandler struct {
	db                          *gorm.DB
	pub                         kafka.Publisher
	approvals                   *usecase.WriteApprovalService
	canvasClient                *canvasservice.Client
	history                     *usecase.QueryHistoryService
	translator                  NLTranslator
	selector                    *usecase.DataSourceSelectorUseCase
	autoSelectConfidenceThreshold float64
	autoSelectMargin            float64
}

func NewQueryHandler(db *gorm.DB, pub kafka.Publisher, approvals *usecase.WriteApprovalService, history *usecase.QueryHistoryService) *QueryHandler {
	foundryURL := os.Getenv("FOUNDRY_SERVICE_URL")
	if foundryURL == "" {
		foundryURL = "http://localhost:8085"
	}
	client := foundryservice.NewClient(foundryURL)
	h := &QueryHandler{
		db:                          db,
		pub:                         pub,
		approvals:                   approvals,
		history:                     history,
		translator:                  client,
		selector:                    usecase.NewDataSourceSelector(db, foundryservice.NewSelectorLLMAdapter(client)),
		autoSelectConfidenceThreshold: envFloat("APP_DATA_NL_AUTO_SELECT_CONFIDENCE", defaultAutoSelectConfidenceThreshold),
		autoSelectMargin:            envFloat("APP_DATA_NL_AUTO_SELECT_MARGIN", defaultAutoSelectMargin),
	}
	// §19.1 flow 1G — inline HTTP push to canvas-service when a query
	// is bound to a dashboard node. Kafka remains authoritative.
	if canvasURL := os.Getenv("CANVAS_SERVICE_URL"); canvasURL != "" {
		client := canvasservice.NewClient(canvasURL, 10*time.Second)
		client.ServiceToken = os.Getenv("CANVAS_SERVICE_TOKEN")
		client.ServiceUserID = os.Getenv("SERVICE_USER_ID")
		h.canvasClient = client
	}
	return h
}

// SetTranslator overrides the NL→SQL translator. Tests call this with
// a fake; production callers leave the default foundryservice client
// in place.
func (h *QueryHandler) SetTranslator(t NLTranslator) {
	if t != nil {
		h.translator = t
	}
}

// SetSelector overrides the datasource auto-selector. Tests stage a
// deterministic selector + fake LLM before invoking NaturalLanguageQuery.
func (h *QueryHandler) SetSelector(s *usecase.DataSourceSelectorUseCase) {
	if s != nil {
		h.selector = s
	}
}

// SetAutoSelectThreshold overrides the confidence threshold + margin.
// Tests use this to force the "needs selection" branch deterministically.
func (h *QueryHandler) SetAutoSelectThreshold(threshold, margin float64) {
	if threshold > 0 {
		h.autoSelectConfidenceThreshold = threshold
	}
	if margin > 0 {
		h.autoSelectMargin = margin
	}
}

func (h *QueryHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data/queries", func(r chi.Router) {
		r.Post("/", h.Create)
		r.Get("/", h.List)
		r.Get("/{id}", h.Get)
		r.Put("/{id}", h.Update)
		r.Delete("/{id}", h.Delete)
		r.Post("/{id}/execute", h.Execute)
		r.Get("/{id}/executions", h.ListExecutions)
		r.Post("/{id}/approve", h.ApproveQuery)
	})
	r.Post("/data/nl-query", h.NaturalLanguageQuery)
}

type createQueryRequest struct {
	DataSourceID    uuid.UUID  `json:"data_source_id"`
	CanvasNodeID    *uuid.UUID `json:"canvas_node_id,omitempty"`
	Name            string     `json:"name"`
	Description     string     `json:"description"`
	QueryType       string     `json:"query_type"`
	RawQuery        string     `json:"raw_query"`
	NaturalLanguage string     `json:"natural_language,omitempty"`
	CacheTTLSec     int        `json:"cache_ttl_sec"`
	ReadOnly        bool       `json:"read_only"`
}

func (h *QueryHandler) Create(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	orgUUID, _ := uuid.Parse(orgID)
	userUUID, _ := uuid.Parse(userID)

	var req createQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}

	// Security (Bypass 2 fix): never let the client mark a mutation as
	// read_only. A malicious caller would otherwise POST
	// {"raw_query":"DROP TABLE ...","read_only":true} and skip the
	// write-approval guard entirely at execute time. We authoritatively
	// re-derive ReadOnly from the SQL itself.
	readOnly := req.ReadOnly
	if usecase.IsMutation(req.RawQuery) {
		readOnly = false
	}

	query := &domain.DataQuery{
		ID:              uuid.New(),
		OrganizationID:  orgUUID,
		DataSourceID:    req.DataSourceID,
		CanvasNodeID:    req.CanvasNodeID,
		Name:            req.Name,
		Description:     req.Description,
		QueryType:       domain.QueryType(req.QueryType),
		RawQuery:        req.RawQuery,
		NaturalLanguage: req.NaturalLanguage,
		CacheTTLSec:     req.CacheTTLSec,
		ReadOnly:        readOnly,
		CreatedBy:       userUUID,
	}

	if err := h.db.Create(query).Error; err != nil {
		respondInternalError(w, "Failed to create query")
		return
	}
	// GORM's `default:true` tag on DataQuery.ReadOnly coerces zero
	// values back to true at insert time, which would silently
	// re-open Bypass 2. An explicit UPDATE after Create forces the
	// mutation's stored value to false regardless of the default.
	if !readOnly {
		if err := h.db.Model(query).UpdateColumn("read_only", false).Error; err != nil {
			respondInternalError(w, "Failed to persist read_only flag")
			return
		}
		query.ReadOnly = false
	}
	respondCreated(w, query)
}

func (h *QueryHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var query domain.DataQuery
	if err := h.db.Where("id = ?", id).First(&query).Error; err != nil {
		respondNotFound(w, "Query not found")
		return
	}
	respondSuccess(w, query)
}

func (h *QueryHandler) List(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, _ := uuid.Parse(orgID)
	dsID := r.URL.Query().Get("data_source_id")

	q := h.db.Where("organization_id = ?", orgUUID)
	if dsID != "" {
		q = q.Where("data_source_id = ?", dsID)
	}

	var queries []domain.DataQuery
	q.Order("created_at DESC").Limit(50).Find(&queries)
	respondSuccess(w, map[string]any{"data": queries})
}

func (h *QueryHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var query domain.DataQuery
	if err := h.db.Where("id = ?", id).First(&query).Error; err != nil {
		respondNotFound(w, "Query not found")
		return
	}
	var updates map[string]any
	json.NewDecoder(r.Body).Decode(&updates)
	h.db.Model(&query).Updates(updates)
	h.db.Where("id = ?", id).First(&query)
	respondSuccess(w, query)
}

func (h *QueryHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	h.db.Where("id = ?", id).Delete(&domain.DataQuery{})
	w.WriteHeader(http.StatusNoContent)
}

// Execute runs the query and records the result.
func (h *QueryHandler) Execute(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	userID := r.Header.Get("X-User-ID")
	userUUID, _ := uuid.Parse(userID)

	var query domain.DataQuery
	if err := h.db.Where("id = ?", id).First(&query).Error; err != nil {
		respondNotFound(w, "Query not found")
		return
	}

	// Load data source for connection info
	var ds domain.DataSource
	if err := h.db.Where("id = ?", query.DataSourceID).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}

	// Write-approval guard (Bypass 1 + 3 fix).
	//
	// Previously this used `?approved=true` as a self-asserted flag —
	// any attacker who could reach the endpoint could run mutations
	// with no approval record. The new flow:
	//
	//   * If the query is NOT a mutation → run it, same as before.
	//   * If it IS a mutation and no approval_id is supplied → create a
	//     pending approval and return 202 with approval_required
	//     (unchanged external shape).
	//   * If it IS a mutation AND approval_id is supplied → look the
	//     record up and verify status/ownership/TTL via
	//     WriteApprovalService.ValidateApprovalForExecute. Any failure
	//     (wrong query, not yet approved, already executed, expired) is
	//     mapped to 403/409.
	//   * After a successful run we MarkExecutedByID so the approval
	//     cannot be replayed.
	var consumedApprovalID *uuid.UUID
	if h.approvals != nil && usecase.IsMutation(query.RawQuery) {
		approvalID := parseApprovalID(r)
		if approvalID == nil {
			// No approval_id supplied — treat as an implicit request for
			// approval (legacy shape).
			approval, err := h.approvals.RequestApproval(r.Context(), &query, userUUID)
			if err != nil {
				respondInternalError(w, "Failed to create approval: "+err.Error())
				return
			}
			respondJSON(w, http.StatusAccepted, response{
				Success: true,
				Data: map[string]any{
					"status":      "approval_required",
					"approval_id": approval.ID,
					"query_id":    query.ID,
					"message":     "This query performs a mutation and requires approval before execution.",
				},
			})
			return
		}
		approval, verr := h.approvals.ValidateApprovalForExecute(r.Context(), query.ID, approvalID)
		if verr != nil {
			writeApprovalError(w, verr)
			return
		}
		consumedApprovalID = &approval.ID
	}

	start := time.Now()
	var execResult domain.JSONMap
	var rowCount int
	var execErr string
	var columns []string
	status := domain.QueryExecStatusCompleted

	if ds.Engine.UsesDatabaseSQL() && ds.ConnectionDSN != "" && query.ReadOnly {
		sqlDB, err := sql.Open(driverForEngine(ds.Engine), ds.ConnectionDSN)
		if err != nil {
			execErr = "Connection failed: " + err.Error()
			status = domain.QueryExecStatusFailed
		} else {
			defer sqlDB.Close()
			// §12.1 per-adapter RLS — for Postgres, open a transaction
			// and SET LOCAL tenant identifiers so downstream row
			// policies can read `current_setting('app.current_user_id')`
			// etc. Other engines share a connection without the RLS
			// hook; tenant scoping for Snowflake/BigQuery/ClickHouse
			// needs adapter-specific config so we only emit the
			// Postgres form today.
			rows, err := executeWithRLS(r.Context(), sqlDB, ds.Engine, query.RawQuery, query.OrganizationID.String(), userID)
			if err != nil {
				execErr = "Query failed: " + err.Error()
				status = domain.QueryExecStatusFailed
			} else {
				defer rows.Close()
				cols, _ := rows.Columns()
				columns = cols
				// Capture column types for §12.3 chart auto-selection.
				colMeta := make([]usecase.ColumnMeta, 0, len(cols))
				if types, typesErr := rows.ColumnTypes(); typesErr == nil {
					for _, ct := range types {
						colMeta = append(colMeta, usecase.ColumnMeta{Name: ct.Name(), Type: ct.DatabaseTypeName()})
					}
				} else {
					for _, c := range cols {
						colMeta = append(colMeta, usecase.ColumnMeta{Name: c})
					}
				}
				var resultRows []map[string]any
				for rows.Next() {
					values := make([]any, len(cols))
					ptrs := make([]any, len(cols))
					for i := range values {
						ptrs[i] = &values[i]
					}
					rows.Scan(ptrs...)
					row := make(map[string]any)
					for i, col := range cols {
						row[col] = values[i]
					}
					resultRows = append(resultRows, row)
					rowCount++
				}
				suggestion := usecase.SuggestChart(colMeta, rowCount, nil)
				execResult = domain.JSONMap{
					"columns":         cols,
					"rows":            resultRows,
					"suggested_chart": suggestion,
				}
			}
		}
	} else {
		execResult = domain.JSONMap{"message": "Non-SQL or no DSN configured — query not executed"}
	}

	exec := &domain.QueryExecution{
		ID:         uuid.New(),
		QueryID:    query.ID,
		Status:     status,
		RowCount:   rowCount,
		DurationMS: time.Since(start).Milliseconds(),
		Result:     execResult,
		Error:      execErr,
		ExecutedBy: userUUID,
		ExecutedAt: time.Now(),
	}

	h.db.Create(exec)

	// §12.3 query history — append one row per execution, success or
	// failure. Attribution is carried through the X-User-ID header.
	if h.history != nil {
		dsID := query.DataSourceID
		_ = h.history.RecordQueryHistory(usecase.RecordQueryHistoryInput{
			OrganizationID:  query.OrganizationID,
			UserID:          userUUID,
			DataSourceID:    &dsID,
			NaturalLanguage: query.NaturalLanguage,
			GeneratedSQL:    query.RawQuery,
			RowCount:        exec.RowCount,
			DurationMS:      exec.DurationMS,
			Error:           execErr,
			ExecutedAt:      exec.ExecutedAt,
		})
	}

	// Publish query-executed / query-failed event for downstream canvas/dashboard flows (1G, 2E).
	// Payload shape matches canvas EventDataQueryExecuted consumer (see
	// canvas-service/internal/usecase/event_handlers.go:handleDataQueryExecuted):
	//   canvas_node_id, query_id, row_count, duration_ms, result, status
	if h.pub != nil {
		metadata := map[string]any{
			"query_id":    exec.QueryID.String(),
			"source_id":   ds.ID.String(),
			"status":      string(exec.Status),
			"row_count":   exec.RowCount,
			"duration_ms": exec.DurationMS,
			"result":      execResult,
			"columns":     columns,
		}
		if query.CanvasNodeID != nil {
			metadata["canvas_node_id"] = query.CanvasNodeID.String()
		}
		if execErr != "" {
			metadata["error"] = execErr
		}
		eventType := "data.query.executed"
		if status == domain.QueryExecStatusFailed {
			eventType = "data.query.failed"
		}
		_ = h.pub.Publish(r.Context(), eventType, kafka.EventData{
			ActorID:        userUUID.String(),
			ResourceType:   "query_execution",
			ResourceID:     exec.ID.String(),
			OrganizationID: query.OrganizationID.String(),
			Metadata:       metadata,
		})
	}

	// §19.1 flow 1G — inline HTTP push to canvas-service for dashboard
	// nodes so the cell renders live without Kafka consumer lag.
	if h.canvasClient != nil && query.CanvasNodeID != nil {
		rows, _ := execResult["rows"].([]map[string]any)
		if rows == nil {
			// JSON-rountripped forms may produce []any; best-effort
			// coerce to the richer shape canvas-service expects.
			if raw, ok := execResult["rows"].([]any); ok {
				for _, r := range raw {
					if m, ok := r.(map[string]any); ok {
						rows = append(rows, m)
					}
				}
			}
		}
		h.pushQueryResultToCanvas(r.Context(), *query.CanvasNodeID, rows, columns, exec, execErr)
	}

	// Consume the approval so the same approval_id cannot be replayed.
	// Only flips when the prior execution path actually used an approval.
	if consumedApprovalID != nil && h.approvals != nil && status == domain.QueryExecStatusCompleted {
		_ = h.approvals.MarkExecutedByID(r.Context(), *consumedApprovalID)
	}

	respondSuccess(w, exec)
}

// parseApprovalID extracts the approval_id from either the
// `approval_id` query param or a JSON body of the form
// {"approval_id":"<uuid>"}. Returns nil when absent or malformed so
// callers can distinguish "missing" from "invalid-uuid" via the
// validator sentinel.
func parseApprovalID(r *http.Request) *uuid.UUID {
	if v := r.URL.Query().Get("approval_id"); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			return &id
		}
	}
	if r.Body == nil {
		return nil
	}
	// Peek at the body without destroying it for downstream handlers.
	// Execute does not otherwise read the body, so draining is safe.
	var body struct {
		ApprovalID string `json:"approval_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil
	}
	if body.ApprovalID == "" {
		return nil
	}
	id, err := uuid.Parse(body.ApprovalID)
	if err != nil {
		return nil
	}
	return &id
}

// writeApprovalError maps the typed sentinels from
// WriteApprovalService.ValidateApprovalForExecute to HTTP statuses.
func writeApprovalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecase.ErrApprovalRequired):
		respondBadRequest(w, err.Error())
	case errors.Is(err, usecase.ErrApprovalAlreadyExecuted):
		respondConflict(w, err.Error())
	case errors.Is(err, usecase.ErrApprovalNotFound),
		errors.Is(err, usecase.ErrApprovalNotApproved),
		errors.Is(err, usecase.ErrApprovalMismatch),
		errors.Is(err, usecase.ErrApprovalExpired):
		respondForbidden(w, err.Error())
	default:
		respondInternalError(w, err.Error())
	}
}

// pushQueryResultToCanvas issues a bounded HTTP POST so the dashboard
// cell linked to the query sees results without waiting on Kafka. The
// canvas-service side uses the node id to resolve the owning canvas
// so data-service does not need to carry the canvas id on DataQuery.
func (h *QueryHandler) pushQueryResultToCanvas(ctx context.Context, nodeID uuid.UUID, rows []map[string]any, columns []string, exec *domain.QueryExecution, execErr string) {
	pushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	payload := canvasservice.QueryResultPayload{
		QueryID:    exec.QueryID.String(),
		Rows:       rows,
		Columns:    columns,
		RowCount:   exec.RowCount,
		DurationMS: exec.DurationMS,
		Status:     string(exec.Status),
		ExecutedAt: exec.ExecutedAt,
		Error:      execErr,
	}
	if err := h.canvasClient.ApplyQueryResultByNode(pushCtx, nodeID, payload); err != nil {
		// best-effort — Kafka is authoritative.
		return
	}
}

// ApproveQuery approves a pending write-query and executes it.
func (h *QueryHandler) ApproveQuery(w http.ResponseWriter, r *http.Request) {
	if h.approvals == nil {
		respondInternalError(w, "Approval service is not configured")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid query ID")
		return
	}
	userID := r.Header.Get("X-User-ID")
	userUUID, _ := uuid.Parse(userID)

	approval, err := h.approvals.Approve(r.Context(), id, userUUID)
	if err != nil {
		respondBadRequest(w, err.Error())
		return
	}

	// The caller must now re-POST /execute with the approval_id bound
	// to this approval. The old `?approved=true` self-asserted flag was
	// removed in the Bypass-1 fix — only the approval_id proves the
	// mutation was actually approved.
	respondSuccess(w, map[string]any{
		"status":      "approved",
		"approval_id": approval.ID,
		"query_id":    approval.QueryID,
		"message":     "Approved. POST /queries/{id}/execute?approval_id=<approval_id> to run.",
	})
}

func (h *QueryHandler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var execs []domain.QueryExecution
	h.db.Where("query_id = ?", id).Order("executed_at DESC").Limit(20).Find(&execs)
	respondSuccess(w, map[string]any{"data": execs})
}

type nlQueryRequest struct {
	DataSourceID uuid.UUID `json:"data_source_id"`
	Question     string    `json:"question"`
}

// NaturalLanguageQuery translates a natural language question to SQL
// via foundry's nl_to_sql dispatch op. Write queries are detected and
// routed through the WriteApprovalService before callers are allowed
// to execute them.
//
// Data source selection (§12.3 auto-selector):
//
//   - If `data_source_id` is present in the request we use it directly.
//   - If absent we consult DataSourceSelectorUseCase. When the top
//     candidate's confidence is >= AutoSelectConfidenceThreshold AND
//     the margin to the runner-up is >= AutoSelectMargin we pick it
//     automatically and echo `selected_data_source_id` +
//     `selection_reasoning` in the response.
//   - Otherwise we return `{ needs_selection: true, candidates: [...] }`
//     (HTTP 200) so the UI can prompt the user. This is a normal flow,
//     not an error.
//   - If the org has zero registered data sources we return a 400 with
//     the existing "no data sources configured" shape.
//
// Response shape on success (CS-5 G5.1):
//
//	{
//	  query_id, question, sql, explanation, requires_approval,
//	  approval_id, semantic_context, status,
//	  selected_data_source_id (optional), selection_reasoning (optional)
//	}
//
// Response shape when selection is ambiguous:
//
//	{
//	  needs_selection: true, question, candidates: [...]
//	}
func (h *QueryHandler) NaturalLanguageQuery(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	orgUUID, _ := uuid.Parse(orgID)
	userUUID, _ := uuid.Parse(userID)

	var req nlQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	if req.Question == "" {
		respondBadRequest(w, "question is required")
		return
	}

	var (
		selectionReasoning string
		autoSelected       bool
	)

	// §12.3 — if no data source was pinned, rank candidates and either
	// auto-pick the top one or return a needs_selection prompt.
	if req.DataSourceID == uuid.Nil {
		if h.selector == nil {
			respondBadRequest(w, "data_source_id is required")
			return
		}
		ranks, err := h.selector.SelectForQuestion(r.Context(), orgUUID, userUUID, req.Question)
		if err != nil {
			respondBadRequest(w, err.Error())
			return
		}
		if len(ranks) == 0 {
			respondBadRequest(w, "no data sources configured")
			return
		}
		top := ranks[0]
		threshold := h.autoSelectConfidenceThreshold
		margin := h.autoSelectMargin
		hasRunnerUp := len(ranks) > 1
		runnerUpConf := 0.0
		if hasRunnerUp {
			runnerUpConf = ranks[1].Confidence
		}
		if top.Confidence >= threshold && (!hasRunnerUp || (top.Confidence-runnerUpConf) >= margin) {
			req.DataSourceID = top.DataSourceID
			selectionReasoning = top.Reasoning
			autoSelected = true
		} else {
			respondSuccess(w, map[string]any{
				"needs_selection": true,
				"question":        req.Question,
				"candidates":      ranks,
			})
			return
		}
	}

	// Pull schema context from the DataSource row + its indexed
	// semantic fields. The DataSource.Tables column already enumerates
	// the tables discovered by /sync; we stitch together a schema
	// string that the LLM can ground its output against.
	var ds domain.DataSource
	if err := h.db.Where("id = ?", req.DataSourceID).First(&ds).Error; err != nil {
		respondNotFound(w, "Data source not found")
		return
	}
	var fields []domain.SemanticField
	h.db.Where("data_source_id = ?", req.DataSourceID).Find(&fields)
	schemaStr := buildSchemaContext(ds, fields)

	// §12.2 annotation upgrade — pull org-vocabulary annotations whose
	// business_term / synonyms / aliases match words in the question
	// so the LLM can resolve "MRR" → "Monthly Recurring Revenue".
	vocab := resolveVocabForQuestion(h.db, orgUUID, req.Question)
	vocabStr := buildVocabularyContext(vocab)

	// Call foundry dispatch via the configured translator. On transport
	// failure we still persist the query with a comment-prefixed SQL so
	// the user has something to iterate on.
	out, err := h.translator.NLToSQL(r.Context(), foundryservice.NLToSQLInput{
		Question:     req.Question,
		Schema:       schemaStr,
		Vocabulary:   vocabStr,
		DataSourceID: req.DataSourceID.String(),
		OrgID:        orgUUID.String(),
		UserID:       userUUID.String(),
	})
	var generatedSQL, explanation string
	if err != nil || out == nil {
		generatedSQL = fmt.Sprintf("-- Could not generate SQL: %v\n-- Question: %s", err, req.Question)
	} else {
		generatedSQL = out.SQL
		explanation = out.Explanation
		if generatedSQL == "" {
			generatedSQL = "-- LLM returned empty SQL"
		}
	}

	// Write-query detection — INSERT / UPDATE / DELETE / DDL paths go
	// through WriteApprovalService so no mutation runs without a
	// second-factor approval.
	requiresApproval := usecase.IsMutation(generatedSQL)

	query := &domain.DataQuery{
		ID:              uuid.New(),
		OrganizationID:  orgUUID,
		DataSourceID:    req.DataSourceID,
		Name:            "NL: " + truncate(req.Question, 100),
		QueryType:       domain.QueryTypeNL,
		RawQuery:        generatedSQL,
		NaturalLanguage: req.Question,
		ReadOnly:        !requiresApproval,
		CreatedBy:       userUUID,
	}
	h.db.Create(query)

	var approvalID string
	if requiresApproval && h.approvals != nil {
		approval, approveErr := h.approvals.RequestApproval(r.Context(), query, userUUID)
		if approveErr == nil && approval != nil {
			approvalID = approval.ID.String()
		}
	}

	// §12.3 query history — record the NL translation even if the
	// caller hasn't executed the generated SQL yet, so the "recent
	// questions" UI is populated immediately.
	if h.history != nil {
		dsID := query.DataSourceID
		_ = h.history.RecordQueryHistory(usecase.RecordQueryHistoryInput{
			OrganizationID:  orgUUID,
			UserID:          userUUID,
			DataSourceID:    &dsID,
			NaturalLanguage: req.Question,
			GeneratedSQL:    generatedSQL,
			ExecutedAt:      time.Now(),
		})
	}

	// Emit NL translation event for flow 2E (NL Query → Data → Dashboard).
	if h.pub != nil {
		meta := map[string]any{
			"query_id":          query.ID.String(),
			"question":          req.Question,
			"data_source_id":    req.DataSourceID.String(),
			"requires_approval": requiresApproval,
		}
		if approvalID != "" {
			meta["approval_id"] = approvalID
		}
		if autoSelected {
			meta["auto_selected"] = true
		}
		_ = h.pub.Publish(r.Context(), "data.query.translated", kafka.EventData{
			ActorID:        userUUID.String(),
			ResourceType:   "data_query",
			ResourceID:     query.ID.String(),
			OrganizationID: orgUUID.String(),
			Metadata:       meta,
		})
	}

	resp := map[string]any{
		"query_id":          query.ID,
		"question":          req.Question,
		"sql":               generatedSQL,
		"generated_sql":     generatedSQL, // legacy field for existing callers
		"explanation":       explanation,
		"requires_approval": requiresApproval,
		"semantic_context":  schemaStr + vocabStr,
		"status":            "completed",
	}
	if approvalID != "" {
		resp["approval_id"] = approvalID
	}
	if autoSelected {
		resp["selected_data_source_id"] = req.DataSourceID
		resp["selection_reasoning"] = selectionReasoning
	}
	respondSuccess(w, resp)
}

// buildSchemaContext renders the DataSource + its SemanticField rows
// as a compact schema description for the LLM. Prefers the richer
// semantic-field metadata when present; falls back to the bare table
// list in DataSource.Tables.
func buildSchemaContext(ds domain.DataSource, fields []domain.SemanticField) string {
	out := "Data source: " + ds.Name + " (" + string(ds.Engine) + ", schema=" + ds.Schema + ")\n"
	if len(fields) > 0 {
		perTable := map[string][]domain.SemanticField{}
		var order []string
		for _, f := range fields {
			if _, ok := perTable[f.TableName]; !ok {
				order = append(order, f.TableName)
			}
			perTable[f.TableName] = append(perTable[f.TableName], f)
		}
		out += "Tables:\n"
		for _, t := range order {
			out += "- " + t + ":\n"
			for _, f := range perTable[t] {
				out += "  - " + f.ColumnName
				if f.DataType != "" {
					out += " (" + f.DataType + ")"
				}
				if f.BusinessName != "" && f.BusinessName != f.ColumnName {
					out += " [" + f.BusinessName + "]"
				}
				out += "\n"
			}
		}
		return out
	}
	if len(ds.Tables) > 0 {
		out += "Tables: "
		for i, t := range ds.Tables {
			if i > 0 {
				out += ", "
			}
			out += t
		}
		out += "\n"
	}
	return out
}

// resolveVocabForQuestion picks org-vocabulary rows whose canonical
// term / synonym / alias is mentioned in the question. We tokenize on
// whitespace and hyphens, case-fold, then delegate per-token matching
// to `usecase.ResolveAnnotations`. §12.2.
func resolveVocabForQuestion(db *gorm.DB, orgID uuid.UUID, question string) []domain.OrgVocabulary {
	if db == nil || orgID == uuid.Nil {
		return nil
	}
	seen := map[uuid.UUID]bool{}
	var out []domain.OrgVocabulary
	for _, tok := range tokenizeQuestion(question) {
		rows, err := usecase.ResolveAnnotations(db, orgID, tok)
		if err != nil {
			continue
		}
		for _, r := range rows {
			if seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			out = append(out, r)
		}
	}
	return out
}

func tokenizeQuestion(q string) []string {
	var tokens []string
	current := ""
	flush := func() {
		if current != "" {
			tokens = append(tokens, current)
			current = ""
		}
	}
	for _, r := range q {
		switch {
		case r == ' ' || r == '\t' || r == '\n' || r == ',' || r == '?' || r == '.':
			flush()
		default:
			current += string(r)
		}
	}
	flush()
	// Also emit bigrams — "monthly recurring" can match even when
	// "monthly" and "recurring" alone do not.
	if len(tokens) > 1 {
		for i := 0; i < len(tokens)-1; i++ {
			tokens = append(tokens, tokens[i]+" "+tokens[i+1])
		}
	}
	return tokens
}

func buildVocabularyContext(rows []domain.OrgVocabulary) string {
	if len(rows) == 0 {
		return ""
	}
	out := "\nBusiness vocabulary:\n"
	for _, r := range rows {
		out += "- " + r.BusinessTerm
		if r.DataType != "" {
			out += " [" + r.DataType + "]"
		}
		if r.Unit != "" {
			out += " (" + r.Unit + ")"
		}
		if r.ColumnID != "" {
			out += " column=" + r.ColumnID
		}
		if len(r.Aliases) > 0 {
			out += " aliases=" + joinStrings(r.Aliases, ", ")
		}
		if len(r.Synonyms) > 0 {
			out += " synonyms=" + joinStrings(r.Synonyms, ", ")
		}
		if r.Description != "" {
			out += " — " + r.Description
		}
		out += "\n"
	}
	return out
}

func buildSemanticContext(fields []domain.SemanticField) string {
	if len(fields) == 0 {
		return "No semantic fields defined. Add field mappings to improve NL→SQL translation."
	}
	ctx := "Available fields:\n"
	for _, f := range fields {
		ctx += "- " + f.TableName + "." + f.ColumnName + " → " + f.BusinessName
		if f.Description != "" {
			ctx += " (" + f.Description + ")"
		}
		if f.Aggregation != "" {
			ctx += " [" + f.Aggregation + "]"
		}
		if len(f.Synonyms) > 0 {
			ctx += " synonyms: " + joinStrings(f.Synonyms, ", ")
		}
		if len(f.Aliases) > 0 {
			ctx += " aliases: " + joinStrings(f.Aliases, ", ")
		}
		ctx += "\n"
	}
	return ctx
}

func joinStrings(items domain.StringArray, sep string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// envFloat parses a float env var with a fallback. Used for the NL
// auto-select tuning knobs so ops can tweak thresholds without a
// rebuild.
func envFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return fallback
	}
	return f
}
