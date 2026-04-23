package http

import (
	"context"
	"database/sql"
	"reflect"
	"runtime"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sentiae/data-service/internal/domain"
)

// TestExecuteWithRLS_NonPostgresFallsThrough proves that for engines
// other than Postgres we do not attempt to open a transaction or emit
// SET LOCAL — the function simply issues the underlying query. sqlite
// is used as a stand-in because (a) it is already a test dependency and
// (b) it lacks the Postgres-specific `SET LOCAL` grammar.
func TestExecuteWithRLS_NonPostgresFallsThrough(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := executeWithRLS(context.Background(), db, domain.DataEngineSQLite, "SELECT 1", "org", "user")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

// TestStrategyForEngine proves the strategy dispatch table routes
// every engine to the right stamper. Uses runtime.FuncForPC to compare
// function identities since Go doesn't allow == on closures.
func TestStrategyForEngine(t *testing.T) {
	cases := []struct {
		engine   domain.DataEngine
		expected any
	}{
		{domain.DataEnginePostgres, postgresRLSStrategy},
		{domain.DataEngineSnowflake, snowflakeRLSStrategy},
		{domain.DataEngineMySQL, passthroughStrategy},
		{domain.DataEngineSQLite, passthroughStrategy},
		{domain.DataEngineMSSQL, passthroughStrategy},
		{domain.DataEngineBigQuery, passthroughStrategy},
	}
	for _, tc := range cases {
		got := strategyForEngine(tc.engine)
		gotName := runtime.FuncForPC(reflect.ValueOf(got).Pointer()).Name()
		wantName := runtime.FuncForPC(reflect.ValueOf(tc.expected).Pointer()).Name()
		if gotName != wantName {
			t.Errorf("engine %s → %s, want %s", tc.engine, gotName, wantName)
		}
	}
}

func TestSanitizeSetting_StripsBadChars(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"", ""},
		{"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
		{"normal", "normal"},
		{"bad';--", ""},
		{"multi\nline", ""},
		{"  spaces  ", "spaces"},
	}
	for _, tc := range cases {
		got := sanitizeSetting(tc.in)
		if got != tc.out {
			t.Errorf("sanitizeSetting(%q) = %q, want %q", tc.in, got, tc.out)
		}
	}
}
