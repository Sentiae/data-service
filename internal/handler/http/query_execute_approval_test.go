// Approval-enforcement regression tests for /data/queries.
//
// Locks down three confirmed security bypasses in the execute path:
//
//	Bypass 1: `?approved=true` URL flag used to skip the approval
//	          check entirely. Replaced by a real approval_id lookup
//	          via WriteApprovalService.ValidateApprovalForExecute.
//	Bypass 2: Create accepted client-supplied `read_only=true` on a
//	          mutation. Now the server re-derives ReadOnly from SQL.
//	Bypass 3: MarkExecuted existed but was never called, so any
//	          approval could be replayed forever. The handler now
//	          consumes the approval via MarkExecutedByID and the
//	          second call with the same approval_id gets a 409.
package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/usecase"
	"gorm.io/gorm"
)

// routerFor mounts the QueryHandler on a real chi router so URL
// params resolve exactly as they do in production. Using the real
// router path keeps us from hand-crafting chi routing contexts.
func routerFor(h *QueryHandler) chi.Router {
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	return r
}

// doExecute issues POST /data/queries/{id}/execute with optional
// query string + JSON body and returns the recorder for inspection.
func doExecute(t *testing.T, h *QueryHandler, queryID, orgID, userID uuid.UUID, queryStr string, body any) *httptest.ResponseRecorder {
	t.Helper()
	url := fmt.Sprintf("/data/queries/%s/execute", queryID)
	if queryStr != "" {
		url += "?" + queryStr
	}
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(http.MethodPost, url, reader)
	req.Header.Set("X-Organization-ID", orgID.String())
	req.Header.Set("X-User-ID", userID.String())
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	routerFor(h).ServeHTTP(rec, req)
	return rec
}

// seedQuery stores a mutation query directly so tests don't depend on
// the Create path (which we also test separately).
func seedMutationQuery(t *testing.T, db *gorm.DB, orgID uuid.UUID) domain.DataQuery {
	t.Helper()
	q := domain.DataQuery{
		ID:             uuid.New(),
		OrganizationID: orgID,
		DataSourceID:   uuid.New(),
		Name:           "dangerous",
		QueryType:      domain.QueryTypeSQL,
		RawQuery:       "DELETE FROM orders WHERE id = 1",
		ReadOnly:       false,
		CreatedBy:      uuid.New(),
	}
	if err := db.Create(&q).Error; err != nil {
		t.Fatalf("seed query: %v", err)
	}
	// Also seed the DataSource so the Execute handler's lookup succeeds.
	ds := domain.DataSource{
		ID:             q.DataSourceID,
		OrganizationID: orgID,
		Name:           "warehouse",
		Engine:         domain.DataEnginePostgres,
		Schema:         "public",
		Status:         domain.DataSourceStatusConnected,
		CreatedBy:      uuid.New(),
	}
	if err := db.Create(&ds).Error; err != nil {
		t.Fatalf("seed ds: %v", err)
	}
	return q
}

// seedApproval inserts an approval row in the supplied state so tests
// can exercise each validation branch without driving the Approve
// workflow.
func seedApproval(t *testing.T, db *gorm.DB, queryID, orgID uuid.UUID, status domain.QueryApprovalStatus, approvedAt *time.Time) domain.QueryApproval {
	t.Helper()
	approver := uuid.New()
	ap := domain.QueryApproval{
		ID:             uuid.New(),
		QueryID:        queryID,
		OrganizationID: orgID,
		RequestedBy:    uuid.New(),
		Status:         status,
		SQLSnapshot:    "DELETE FROM orders WHERE id = 1",
		DetectedOps:    "DELETE",
	}
	if status == domain.QueryApprovalStatusApproved || status == domain.QueryApprovalStatusExecuted {
		ap.ApprovedBy = &approver
		if approvedAt != nil {
			ap.ApprovedAt = approvedAt
		} else {
			now := time.Now()
			ap.ApprovedAt = &now
		}
	}
	if err := db.Create(&ap).Error; err != nil {
		t.Fatalf("seed approval: %v", err)
	}
	return ap
}

// ---- Bypass 1: execute without/ with bad approval_id ----

// TestExecute_MutationWithoutApprovalID_AllowsLegacyRequestFlow — the
// legacy behaviour (mutation with no approval_id → 202 + pending
// approval record) must still work. This proves we didn't break the
// legit flow when closing Bypass 1.
func TestExecute_MutationWithoutApprovalID_AllowsLegacyRequestFlow(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)

	rec := doExecute(t, h, q.ID, orgID, userID, "", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 approval_required, got %d: %s", rec.Code, rec.Body.String())
	}
	var count int64
	db.Model(&domain.QueryApproval{}).Where("query_id = ? AND status = ?", q.ID, domain.QueryApprovalStatusPending).Count(&count)
	if count != 1 {
		t.Fatalf("expected 1 pending approval, got %d", count)
	}
}

// TestExecute_PendingApprovalIDRejected — Bypass 1: supplying a
// pending (not-yet-approved) approval_id must return 403. Previously
// `?approved=true` let callers skip this entirely.
func TestExecute_PendingApprovalIDRejected(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)
	ap := seedApproval(t, db, q.ID, orgID, domain.QueryApprovalStatusPending, nil)

	rec := doExecute(t, h, q.ID, orgID, userID, "approval_id="+ap.ID.String(), nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestExecute_WrongQueryApprovalRejected — Bypass 1: approval_id that
// belongs to a *different* query must 403 even if it is approved.
func TestExecute_WrongQueryApprovalRejected(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)
	// Approval bound to a DIFFERENT query id.
	other := uuid.New()
	ap := seedApproval(t, db, other, orgID, domain.QueryApprovalStatusApproved, nil)

	rec := doExecute(t, h, q.ID, orgID, userID, "approval_id="+ap.ID.String(), nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-query reuse, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestExecute_UnknownApprovalIDRejected — Bypass 1: a random
// approval_id the attacker made up must 403.
func TestExecute_UnknownApprovalIDRejected(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)
	rec := doExecute(t, h, q.ID, orgID, userID, "approval_id="+uuid.New().String(), nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for unknown approval, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestExecute_LegacyApprovedFlagNoLongerBypasses — Bypass 1 proof:
// the old attack vector (`?approved=true` with no approval_id)
// against a mutation must NOT run the query. It either returns 202
// (pending approval) or a 4xx; it must never return a 200 that
// signals execution.
func TestExecute_LegacyApprovedFlagNoLongerBypasses(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)

	rec := doExecute(t, h, q.ID, orgID, userID, "approved=true", nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("legacy bypass should not succeed, but got 200: %s", rec.Body.String())
	}
	// No execution row should have been recorded.
	var execCount int64
	db.Model(&domain.QueryExecution{}).Where("query_id = ?", q.ID).Count(&execCount)
	if execCount != 0 {
		t.Fatalf("expected 0 executions, got %d", execCount)
	}
}

// ---- Bypass 2: Create cannot be coerced into marking a mutation read-only ----

// TestCreate_MutationForcedReadOnlyFalse — Bypass 2: a client that
// POSTs {"raw_query":"DROP TABLE x","read_only":true} must have
// read_only=false stored server-side. The subsequent Execute then
// flows through the approval guard.
func TestCreate_MutationForcedReadOnlyFalse(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()

	body := map[string]any{
		"data_source_id": uuid.New().String(),
		"name":           "malicious",
		"query_type":     "sql",
		"raw_query":      "DROP TABLE users",
		"read_only":      true, // attacker asserts read-only
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/data/queries/", bytes.NewReader(raw))
	req.Header.Set("X-Organization-ID", orgID.String())
	req.Header.Set("X-User-ID", userID.String())
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	routerFor(h).ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var persisted []domain.DataQuery
	if err := db.Find(&persisted).Error; err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(persisted) != 1 {
		t.Fatalf("expected 1 row, got %d", len(persisted))
	}
	if persisted[0].ReadOnly {
		t.Fatalf("Bypass 2 regression: stored ReadOnly=true for DROP TABLE — server must override to false")
	}
}

// TestCreate_NonMutationRespectsClientReadOnly — Guardrail: a SELECT
// query with `read_only:true` should stay read_only=true; we only
// override for detected mutations.
func TestCreate_NonMutationRespectsClientReadOnly(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()

	body := map[string]any{
		"data_source_id": uuid.New().String(),
		"name":           "safe",
		"query_type":     "sql",
		"raw_query":      "SELECT 1",
		"read_only":      true,
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/data/queries/", bytes.NewReader(raw))
	req.Header.Set("X-Organization-ID", orgID.String())
	req.Header.Set("X-User-ID", userID.String())
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	routerFor(h).ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var persisted []domain.DataQuery
	db.Find(&persisted)
	if len(persisted) != 1 || !persisted[0].ReadOnly {
		t.Fatalf("SELECT should stay read_only=true, got %+v", persisted)
	}
}

// ---- Bypass 3: approvals are consumed and cannot be replayed ----

// TestExecute_ApprovalMarkedExecutedThenReplayIs409 — Bypass 3: the
// first execute with a valid approved approval_id should run; a
// second execute with the same approval_id must return 409 conflict.
func TestExecute_ApprovalMarkedExecutedThenReplayIs409(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)
	ap := seedApproval(t, db, q.ID, orgID, domain.QueryApprovalStatusApproved, nil)

	// First execute — Engine.UsesDatabaseSQL + ConnectionDSN="" means
	// we take the "Non-SQL or no DSN configured" branch, which is
	// still a completed execution (status=completed) so
	// MarkExecutedByID fires.
	rec := doExecute(t, h, q.ID, orgID, userID, "approval_id="+ap.ID.String(), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("first execute: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Approval status should now be executed.
	var reloaded domain.QueryApproval
	if err := db.Where("id = ?", ap.ID).First(&reloaded).Error; err != nil {
		t.Fatalf("reload approval: %v", err)
	}
	if reloaded.Status != domain.QueryApprovalStatusExecuted {
		t.Fatalf("expected status=executed after first run, got %s", reloaded.Status)
	}

	// Second execute — replay attempt — must 409.
	rec2 := doExecute(t, h, q.ID, orgID, userID, "approval_id="+ap.ID.String(), nil)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("replay: expected 409, got %d: %s", rec2.Code, rec2.Body.String())
	}
}

// TestExecute_ApprovalIDInBody — the handler should accept
// approval_id in the JSON body, not only as a query string. Keeps the
// API ergonomic for programmatic callers.
func TestExecute_ApprovalIDInBody(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	orgID := uuid.New()
	userID := uuid.New()
	q := seedMutationQuery(t, db, orgID)
	ap := seedApproval(t, db, q.ID, orgID, domain.QueryApprovalStatusApproved, nil)

	rec := doExecute(t, h, q.ID, orgID, userID, "", map[string]any{"approval_id": ap.ID.String()})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with body approval_id, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ---- Unit tests for the validator itself ----

// TestValidateApprovalForExecute covers each sentinel branch of the
// validator. Keeps the rules asserted at the usecase layer so future
// handlers can rely on them without duplicating logic.
func TestValidateApprovalForExecute(t *testing.T) {
	h, db := newTestQueryHandler(t, &fakeTranslator{})
	_ = h
	svc := usecase.NewWriteApprovalService(db)
	ctx := t.Context()

	orgID := uuid.New()
	queryID := uuid.New()

	// nil approval → ErrApprovalRequired
	if _, err := svc.ValidateApprovalForExecute(ctx, queryID, nil); err != usecase.ErrApprovalRequired {
		t.Fatalf("nil: got %v", err)
	}

	// unknown id → ErrApprovalNotFound
	bogus := uuid.New()
	if _, err := svc.ValidateApprovalForExecute(ctx, queryID, &bogus); err != usecase.ErrApprovalNotFound {
		t.Fatalf("unknown: got %v", err)
	}

	// pending → ErrApprovalNotApproved
	pending := seedApproval(t, db, queryID, orgID, domain.QueryApprovalStatusPending, nil)
	if _, err := svc.ValidateApprovalForExecute(ctx, queryID, &pending.ID); err != usecase.ErrApprovalNotApproved {
		t.Fatalf("pending: got %v", err)
	}

	// wrong queryID → ErrApprovalMismatch
	approved := seedApproval(t, db, queryID, orgID, domain.QueryApprovalStatusApproved, nil)
	wrongQuery := uuid.New()
	if _, err := svc.ValidateApprovalForExecute(ctx, wrongQuery, &approved.ID); err != usecase.ErrApprovalMismatch {
		t.Fatalf("mismatch: got %v", err)
	}

	// approved + within TTL → ok
	if _, err := svc.ValidateApprovalForExecute(ctx, queryID, &approved.ID); err != nil {
		t.Fatalf("approved: got %v", err)
	}

	// executed → ErrApprovalAlreadyExecuted
	old := time.Now()
	executed := seedApproval(t, db, queryID, orgID, domain.QueryApprovalStatusExecuted, &old)
	if _, err := svc.ValidateApprovalForExecute(ctx, queryID, &executed.ID); err != usecase.ErrApprovalAlreadyExecuted {
		t.Fatalf("executed: got %v", err)
	}

	// expired → ErrApprovalExpired (ApprovedAt 48h ago)
	expiredAt := time.Now().Add(-48 * time.Hour)
	expired := seedApproval(t, db, queryID, orgID, domain.QueryApprovalStatusApproved, &expiredAt)
	if _, err := svc.ValidateApprovalForExecute(ctx, queryID, &expired.ID); err != usecase.ErrApprovalExpired {
		t.Fatalf("expired: got %v", err)
	}
}

// TestMarkExecutedByID covers the atomic consume path used to block
// replay (Bypass 3). Two concurrent calls must not both succeed.
func TestMarkExecutedByID(t *testing.T) {
	_, db := newTestQueryHandler(t, &fakeTranslator{})
	svc := usecase.NewWriteApprovalService(db)
	ctx := t.Context()

	orgID := uuid.New()
	queryID := uuid.New()
	ap := seedApproval(t, db, queryID, orgID, domain.QueryApprovalStatusApproved, nil)

	if err := svc.MarkExecutedByID(ctx, ap.ID); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := svc.MarkExecutedByID(ctx, ap.ID); err != usecase.ErrApprovalAlreadyExecuted {
		t.Fatalf("second: expected ErrApprovalAlreadyExecuted, got %v", err)
	}
}

