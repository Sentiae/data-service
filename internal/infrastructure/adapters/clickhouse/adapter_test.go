package clickhouse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestParseDSN_ValidBasic(t *testing.T) {
	cfg, err := parseDSN("clickhouse://user:pw@host.example:8123/analytics")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.baseURL != "http://host.example:8123" {
		t.Fatalf("baseURL %q", cfg.baseURL)
	}
	if cfg.user != "user" || cfg.password != "pw" {
		t.Fatalf("creds: %q/%q", cfg.user, cfg.password)
	}
	if cfg.database != "analytics" {
		t.Fatalf("db %q", cfg.database)
	}
}

func TestParseDSN_Secure(t *testing.T) {
	cfg, err := parseDSN("clickhouse://host.example:443/db?secure=true")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.baseURL != "https://host.example:443" {
		t.Fatalf("baseURL %q", cfg.baseURL)
	}
}

func TestParseDSN_PasswordEnv(t *testing.T) {
	t.Setenv("CLICKHOUSE_PW", "from-env")
	cfg, err := parseDSN("clickhouse://u@host:8123/db?password_env=CLICKHOUSE_PW")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.password != "from-env" {
		t.Fatalf("password %q", cfg.password)
	}
}

func TestParseDSN_Rejects(t *testing.T) {
	cases := []string{"", "http://host", "clickhouse:///db"}
	for _, c := range cases {
		if _, err := parseDSN(c); err == nil {
			t.Errorf("expected error for %q", c)
		}
	}
}

func TestParseJSONResult(t *testing.T) {
	body := []byte(`{
		"meta":[{"name":"id","type":"UInt64"},{"name":"name","type":"String"}],
		"data":[{"id":1,"name":"a"},{"id":2,"name":"b"}]
	}`)
	res, err := parseJSONResult(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Columns) != 2 || res.Columns[0] != "id" || res.Columns[1] != "name" {
		t.Fatalf("columns: %v", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows len: %d", len(res.Rows))
	}
	if res.Rows[0][1] != "a" {
		t.Fatalf("row[0][1]: %v", res.Rows[0][1])
	}
}

func TestAdapterExecute_EndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo a valid ClickHouse JSON shape.
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"meta":[{"name":"n","type":"UInt64"}],"data":[{"n":1},{"n":2}]}`)
	}))
	defer srv.Close()

	// Strip scheme since DSN uses clickhouse://, then route HTTP to the test server.
	dsn := "clickhouse://" + srv.Listener.Addr().String() + "/default"
	a := NewAdapter()
	res, err := a.Execute(context.Background(), dsn+" :: SELECT 1")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
}

func TestAdapterExecute_ErrorPassthrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Code: 62. DB::Exception: Syntax error"))
	}))
	defer srv.Close()
	dsn := "clickhouse://" + srv.Listener.Addr().String() + "/default"
	a := NewAdapter()
	if _, err := a.Execute(context.Background(), dsn+" :: SELECT bad"); err == nil {
		t.Fatalf("expected error")
	}
}

// TestAdapterRLS_StampsSessionSettings proves §12.1: when the planner
// has called SetRLSContext, the adapter issues two SET statements on
// a shared session_id BEFORE the user's SELECT. We run a fake
// ClickHouse HTTP server and assert the exact request sequence.
func TestAdapterRLS_StampsSessionSettings(t *testing.T) {
	var mu sync.Mutex
	var received []recordedReq

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, recordedReq{
			Query:     r.URL.Query().Get("query"),
			SessionID: r.URL.Query().Get("session_id"),
		})
		mu.Unlock()

		q := r.URL.Query().Get("query")
		// SET statements return an empty 200 body.
		if strings.HasPrefix(strings.TrimSpace(q), "SET ") {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Otherwise emit the canonical JSON shape.
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"meta":[{"name":"n","type":"UInt64"}],"data":[{"n":1}]}`)
	}))
	defer srv.Close()

	orgID := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	userID := uuid.MustParse("00000000-0000-0000-0000-00000000000b")

	a := NewAdapter()
	a.SetRLSContext(orgID, userID)

	dsn := "clickhouse://" + srv.Listener.Addr().String() + "/default"
	if _, err := a.Execute(context.Background(), dsn+" :: SELECT 1"); err != nil {
		t.Fatalf("execute: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("expected 3 requests (2 SET + 1 SELECT), got %d: %+v", len(received), received)
	}
	sid := received[0].SessionID
	if sid == "" {
		t.Fatal("session_id was empty on the SET org request")
	}
	for _, r := range received {
		if r.SessionID != sid {
			t.Errorf("all requests must share one session_id; got %q vs %q", r.SessionID, sid)
		}
	}
	if !strings.Contains(received[0].Query, "SQL_app_current_org_id") ||
		!strings.Contains(received[0].Query, orgID.String()) {
		t.Errorf("first request must SET org, got %q", received[0].Query)
	}
	if !strings.Contains(received[1].Query, "SQL_app_current_user_id") ||
		!strings.Contains(received[1].Query, userID.String()) {
		t.Errorf("second request must SET user, got %q", received[1].Query)
	}
	if !strings.Contains(strings.ToUpper(received[2].Query), "SELECT 1") {
		t.Errorf("third request must be the SELECT, got %q", received[2].Query)
	}
}

// TestAdapterRLS_NoContext confirms the adapter skips the stamp path
// entirely when SetRLSContext has never been called (local dev, tests,
// non-tenant-scoped data sources).
func TestAdapterRLS_NoContext(t *testing.T) {
	var count int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"meta":[{"name":"n","type":"UInt64"}],"data":[{"n":1}]}`)
	}))
	defer srv.Close()

	dsn := "clickhouse://" + srv.Listener.Addr().String() + "/default"
	a := NewAdapter()
	if _, err := a.Execute(context.Background(), dsn+" :: SELECT 1"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 request when no RLS context is set, got %d", count)
	}
}

// recordedReq captures a single HTTP request for assertion.
type recordedReq struct {
	Query     string
	SessionID string
}
