package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// TestSetRLSContext_RoundTrip ensures concurrent writers don't tear
// the two-field state.
func TestSetRLSContext_RoundTrip(t *testing.T) {
	a := NewAdapter()
	orgID := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	userID := uuid.MustParse("00000000-0000-0000-0000-00000000000b")

	a.SetRLSContext(orgID, userID)
	gotOrg, gotUser := a.rlsSnapshot()
	if gotOrg != orgID {
		t.Errorf("org = %s, want %s", gotOrg, orgID)
	}
	if gotUser != userID {
		t.Errorf("user = %s, want %s", gotUser, userID)
	}
}

// TestApplyRLS_EmitsAlterSession drives applyRLS through the
// database/sql layer with a stub driver so we can assert the exact
// statements issued before the user SELECT.
func TestApplyRLS_EmitsAlterSession(t *testing.T) {
	stub := &stubDriver{}
	sql.Register("snowflake-stub-rls", stub)

	db, err := sql.Open("snowflake-stub-rls", "stub")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	a := NewAdapter()
	orgID := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	userID := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	a.SetRLSContext(orgID, userID)

	if err := a.applyRLS(context.Background(), conn); err != nil {
		t.Fatalf("applyRLS: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.queries) != 2 {
		t.Fatalf("expected 2 ALTER SESSION statements, got %d: %v", len(stub.queries), stub.queries)
	}
	if !strings.Contains(stub.queries[0], "ALTER SESSION SET APP_CURRENT_ORG_ID") ||
		!strings.Contains(stub.queries[0], orgID.String()) {
		t.Errorf("first stmt must stamp org: %q", stub.queries[0])
	}
	if !strings.Contains(stub.queries[1], "ALTER SESSION SET APP_CURRENT_USER_ID") ||
		!strings.Contains(stub.queries[1], userID.String()) {
		t.Errorf("second stmt must stamp user: %q", stub.queries[1])
	}
}

// TestApplyRLS_NoContext skips the alter entirely when context is zero.
func TestApplyRLS_NoContext(t *testing.T) {
	stub := &stubDriver{}
	sql.Register("snowflake-stub-rls-empty", stub)

	db, err := sql.Open("snowflake-stub-rls-empty", "stub")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	a := NewAdapter()
	if err := a.applyRLS(context.Background(), conn); err != nil {
		t.Fatalf("applyRLS: %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.queries) != 0 {
		t.Errorf("expected no statements, got %d", len(stub.queries))
	}
}

// stubDriver is a tiny database/sql driver used to capture executed
// queries without touching a real Snowflake instance.
type stubDriver struct {
	mu      sync.Mutex
	queries []string
}

func (d *stubDriver) Open(name string) (driver.Conn, error) {
	return &stubConn{parent: d}, nil
}

type stubConn struct{ parent *stubDriver }

func (c *stubConn) Prepare(query string) (driver.Stmt, error) {
	c.parent.mu.Lock()
	c.parent.queries = append(c.parent.queries, query)
	c.parent.mu.Unlock()
	return &stubStmt{q: query}, nil
}
func (c *stubConn) Close() error              { return nil }
func (c *stubConn) Begin() (driver.Tx, error) { return &stubTx{}, nil }

// ExecContext lets the driver accept the ALTER SESSION exec directly
// so the pinned-conn path we test is realistic.
func (c *stubConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.parent.mu.Lock()
	c.parent.queries = append(c.parent.queries, query)
	c.parent.mu.Unlock()
	return stubResult{}, nil
}

type stubStmt struct{ q string }

func (s *stubStmt) Close() error                                    { return nil }
func (s *stubStmt) NumInput() int                                   { return 0 }
func (s *stubStmt) Exec(args []driver.Value) (driver.Result, error) { return stubResult{}, nil }
func (s *stubStmt) Query(args []driver.Value) (driver.Rows, error)  { return stubRows{}, nil }

type stubTx struct{}

func (t *stubTx) Commit() error   { return nil }
func (t *stubTx) Rollback() error { return nil }

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) { return 0, nil }
func (stubResult) RowsAffected() (int64, error) { return 0, nil }

type stubRows struct{}

func (stubRows) Columns() []string              { return nil }
func (stubRows) Close() error                   { return nil }
func (stubRows) Next(dest []driver.Value) error { return io.EOF }
