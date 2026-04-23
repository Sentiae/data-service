package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/infrastructure/foundryservice"
	pgmigrations "github.com/sentiae/data-service/internal/repository/postgres"
	"github.com/sentiae/data-service/internal/usecase"
	sqlitedriver "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// fakeTranslator is an in-process NLTranslator used by the unit tests
// so we never spin up foundry-service. Each test sets SQL + Err before
// invoking the handler.
type fakeTranslator struct {
	sql         string
	explanation string
	err         error
	lastInput   foundryservice.NLToSQLInput
}

func (f *fakeTranslator) NLToSQL(ctx context.Context, in foundryservice.NLToSQLInput) (*foundryservice.NLToSQLOutput, error) {
	f.lastInput = in
	if f.err != nil {
		return nil, f.err
	}
	return &foundryservice.NLToSQLOutput{SQL: f.sql, Explanation: f.explanation}, nil
}

// newTestQueryHandler stands up a handler on an in-memory SQLite DB so
// the NL→SQL path can persist rows and hit the WriteApprovalService
// without touching Postgres. Uses a hand-rolled schema because the
// pg-flavored AutoMigrate emits `jsonb` and other Postgres idioms
// SQLite rejects.
func newTestQueryHandler(t *testing.T, translator NLTranslator) (*QueryHandler, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlitedriver.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE data_sources (
			id TEXT PRIMARY KEY,
			organization_id TEXT,
			name TEXT, description TEXT,
			engine TEXT,
			connection_id TEXT, connection_dsn TEXT,
			schema TEXT,
			tables TEXT,
			status TEXT,
			last_sync_at DATETIME,
			created_by TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE data_queries (
			id TEXT PRIMARY KEY,
			organization_id TEXT, data_source_id TEXT, canvas_node_id TEXT,
			name TEXT, description TEXT,
			query_type TEXT, raw_query TEXT, natural_language TEXT,
			parameters TEXT, cache_ttl_sec INTEGER, read_only INTEGER,
			created_by TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE query_approvals (
			id TEXT PRIMARY KEY,
			query_id TEXT, organization_id TEXT, requested_by TEXT,
			approved_by TEXT, approved_at DATETIME,
			status TEXT, reason TEXT,
			sql_snapshot TEXT, detected_ops TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE semantic_fields (
			id TEXT PRIMARY KEY,
			data_source_id TEXT, table_name TEXT, column_name TEXT,
			business_name TEXT, description TEXT, data_type TEXT,
			aggregation TEXT, unit TEXT, tags TEXT,
			synonyms TEXT, aliases TEXT, required_role TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE per_org_vocabulary (
			id TEXT PRIMARY KEY,
			organization_id TEXT,
			business_term TEXT, description TEXT,
			data_type TEXT, unit TEXT, column_id TEXT,
			synonyms TEXT, aliases TEXT,
			created_by TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE query_history_entries (
			id TEXT PRIMARY KEY,
			organization_id TEXT, user_id TEXT, data_source_id TEXT,
			natural_language TEXT, generated_sql TEXT,
			row_count INTEGER, duration_ms INTEGER,
			error TEXT, executed_at DATETIME,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE saved_queries (
			id TEXT PRIMARY KEY,
			organization_id TEXT, created_by TEXT,
			name TEXT, natural_language TEXT, generated_sql TEXT,
			tags TEXT, data_source_id TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
	} {
		if err := db.Exec(stmt).Error; err != nil {
			t.Fatalf("exec schema: %v", err)
		}
	}
	approvals := usecase.NewWriteApprovalService(db)
	history := usecase.NewQueryHistoryService(
		pgmigrations.NewQueryHistoryRepo(db),
		pgmigrations.NewSavedQueryRepo(db),
	)
	h := NewQueryHandler(db, nil, approvals, history)
	h.SetTranslator(translator)
	return h, db
}

// seedDataSource persists a minimal DataSource the handler can look up.
func seedDataSource(t *testing.T, db *gorm.DB, orgID uuid.UUID) domain.DataSource {
	t.Helper()
	ds := domain.DataSource{
		ID:             uuid.New(),
		OrganizationID: orgID,
		Name:           "warehouse",
		Engine:         domain.DataEnginePostgres,
		Schema:         "public",
		Tables:         domain.StringArray{"orders", "customers"},
		Status:         domain.DataSourceStatusConnected,
		CreatedBy:      uuid.New(),
	}
	if err := db.Create(&ds).Error; err != nil {
		t.Fatalf("seed data source: %v", err)
	}
	return ds
}

// doNLQuery issues the request the handler expects and returns the
// decoded response body for inspection.
func doNLQuery(t *testing.T, h *QueryHandler, orgID, userID uuid.UUID, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/data/nl-query", bytes.NewReader(raw))
	req.Header.Set("X-Organization-ID", orgID.String())
	req.Header.Set("X-User-ID", userID.String())
	rec := httptest.NewRecorder()
	h.NaturalLanguageQuery(rec, req)

	if rec.Code >= 500 {
		t.Fatalf("server error: %d %s", rec.Code, rec.Body.String())
	}
	if rec.Code == 0 || rec.Body.Len() == 0 {
		return rec, nil
	}
	var decoded struct {
		Success bool           `json:"success"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return rec, decoded.Data
}

func TestNaturalLanguageQuery_ReadOnlyNoApproval(t *testing.T) {
	tr := &fakeTranslator{
		sql:         "SELECT count(*) FROM orders",
		explanation: "Counts all orders.",
	}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	ds := seedDataSource(t, db, orgID)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"data_source_id": ds.ID.String(),
		"question":       "how many orders?",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if data["requires_approval"].(bool) {
		t.Fatalf("expected requires_approval=false for SELECT")
	}
	if data["sql"].(string) != tr.sql {
		t.Errorf("sql mismatch: got %v", data["sql"])
	}
	if data["explanation"].(string) != tr.explanation {
		t.Errorf("explanation mismatch: got %v", data["explanation"])
	}
	// No approval row should exist.
	var count int64
	db.Model(&domain.QueryApproval{}).Count(&count)
	if count != 0 {
		t.Errorf("expected 0 approval rows, got %d", count)
	}
}

func TestNaturalLanguageQuery_WriteRequiresApproval(t *testing.T) {
	tr := &fakeTranslator{
		sql: "UPDATE orders SET status = 'archived' WHERE created_at < now() - interval '1 year'",
	}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	ds := seedDataSource(t, db, orgID)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"data_source_id": ds.ID.String(),
		"question":       "archive old orders",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !data["requires_approval"].(bool) {
		t.Fatalf("expected requires_approval=true for UPDATE")
	}
	if _, ok := data["approval_id"]; !ok {
		t.Errorf("expected approval_id in response")
	}
	// A pending approval row must exist.
	var approvals []domain.QueryApproval
	db.Find(&approvals)
	if len(approvals) != 1 {
		t.Fatalf("expected 1 approval row, got %d", len(approvals))
	}
	if approvals[0].Status != domain.QueryApprovalStatusPending {
		t.Errorf("expected pending, got %s", approvals[0].Status)
	}
	if approvals[0].DetectedOps != "UPDATE" {
		t.Errorf("expected DetectedOps=UPDATE, got %s", approvals[0].DetectedOps)
	}
}

func TestNaturalLanguageQuery_InvalidInput(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	// Missing data_source_id triggers a 400; no row should persist.
	rec, _ := doNLQuery(t, h, orgID, userID, map[string]any{"question": "x"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	var n int64
	db.Model(&domain.DataQuery{}).Count(&n)
	if n != 0 {
		t.Errorf("expected no persisted query, got %d", n)
	}

	// Missing question — also 400.
	rec, _ = doNLQuery(t, h, orgID, userID, map[string]any{"data_source_id": uuid.New().String()})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestNaturalLanguageQuery_LLMError(t *testing.T) {
	tr := &fakeTranslator{err: errors.New("llm down")}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	ds := seedDataSource(t, db, orgID)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"data_source_id": ds.ID.String(),
		"question":       "hello",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	sql, _ := data["sql"].(string)
	if sql == "" || !bytes.Contains([]byte(sql), []byte("Could not generate SQL")) {
		t.Errorf("expected fallback SQL with LLM-error comment, got %q", sql)
	}
	// Fallback SQL starts with `--` so it should NOT require approval.
	if data["requires_approval"].(bool) {
		t.Errorf("comment-only fallback should not require approval")
	}
}
