package usecase

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// --- Fake BigQuery executor --------------------------------------------------

// fakeBQExecutor lets each test describe the adapter's behavior via a
// QueryFn hook and records every invocation for assertion.
type fakeBQExecutor struct {
	mu      sync.Mutex
	calls   []fakeBQCall
	QueryFn func(ctx context.Context, dsn, sql string) (*BigQueryResult, error)
}

type fakeBQCall struct {
	DSN string
	SQL string
}

func (f *fakeBQExecutor) QueryWithDSN(ctx context.Context, dsn, sql string) (*BigQueryResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeBQCall{DSN: dsn, SQL: sql})
	f.mu.Unlock()
	if f.QueryFn != nil {
		return f.QueryFn(ctx, dsn, sql)
	}
	return nil, fmt.Errorf("QueryFn not set")
}

// --- Capturing Kafka publisher ----------------------------------------------

type capturingPublisher struct {
	mu   sync.Mutex
	seen []publishedEvent
}

type publishedEvent struct {
	EventType string
	Data      kafka.EventData
}

func (p *capturingPublisher) Publish(_ context.Context, eventType string, data kafka.EventData) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, publishedEvent{eventType, data})
	return nil
}
func (p *capturingPublisher) PublishBatch(_ context.Context, _ []kafka.Event) error { return nil }
func (p *capturingPublisher) Close() error                                          { return nil }
func (p *capturingPublisher) EnsureTopics(_ context.Context) error                  { return nil }

// --- DB harness --------------------------------------------------------------

// newWorkerDB sets up an in-memory sqlite with just the tables the worker
// reads from (dashboard_alerts, data_queries, data_sources). gorm's
// AutoMigrate on the live domain structs emits `jsonb`/postgres idioms
// sqlite rejects, so we hand-roll a minimal schema instead.
func newWorkerDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE dashboard_alerts (
			id TEXT PRIMARY KEY,
			organization_id TEXT,
			dashboard_id TEXT,
			panel_id TEXT,
			query_id TEXT,
			threshold_type TEXT,
			threshold_value REAL,
			window_seconds INTEGER,
			notify_channel TEXT,
			active INTEGER,
			last_evaluated DATETIME,
			last_triggered DATETIME,
			last_value REAL,
			created_by TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)`,
		`CREATE TABLE data_queries (
			id TEXT PRIMARY KEY,
			organization_id TEXT,
			data_source_id TEXT,
			canvas_node_id TEXT,
			name TEXT,
			description TEXT,
			query_type TEXT,
			raw_query TEXT,
			natural_language TEXT,
			parameters TEXT,
			cache_ttl_sec INTEGER,
			read_only INTEGER,
			created_by TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)`,
		`CREATE TABLE data_sources (
			id TEXT PRIMARY KEY,
			organization_id TEXT,
			name TEXT,
			description TEXT,
			engine TEXT,
			connection_id TEXT,
			connection_dsn TEXT,
			schema TEXT,
			tables TEXT,
			status TEXT,
			last_sync_at DATETIME,
			created_by TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)`,
	} {
		if err := db.Exec(stmt).Error; err != nil {
			t.Fatalf("exec schema: %v", err)
		}
	}
	return db
}

// seedAlert wires up a DashboardAlert + DataQuery + DataSource triple and
// returns the alert (by value) ready for evaluateOne.
func seedAlert(t *testing.T, db *gorm.DB, engine domain.DataEngine, rawSQL, dsn string, threshold float64) *domain.DashboardAlert {
	t.Helper()
	sourceID := uuid.New()
	queryID := uuid.New()
	alertID := uuid.New()
	orgID := uuid.New()
	ds := &domain.DataSource{
		ID:             sourceID,
		OrganizationID: orgID,
		Name:           "test",
		Engine:         engine,
		ConnectionDSN:  dsn,
		Status:         domain.DataSourceStatusConnected,
		CreatedBy:      uuid.New(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := db.Create(ds).Error; err != nil {
		t.Fatalf("seed data source: %v", err)
	}
	q := &domain.DataQuery{
		ID:             queryID,
		OrganizationID: orgID,
		DataSourceID:   sourceID,
		Name:           "alert query",
		QueryType:      domain.QueryTypeSQL,
		RawQuery:       rawSQL,
		ReadOnly:       true,
		CreatedBy:      uuid.New(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := db.Create(q).Error; err != nil {
		t.Fatalf("seed data query: %v", err)
	}
	alert := &domain.DashboardAlert{
		ID:             alertID,
		OrganizationID: orgID,
		DashboardID:    uuid.New(),
		PanelID:        "p1",
		QueryID:        &queryID,
		ThresholdType:  domain.AlertThresholdGT,
		ThresholdValue: threshold,
		WindowSeconds:  300,
		NotifyChannel:  "slack:#ops",
		Active:         true,
		CreatedBy:      uuid.New(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := db.Create(alert).Error; err != nil {
		t.Fatalf("seed alert: %v", err)
	}
	return alert
}

// --- Tests -------------------------------------------------------------------

func TestNewDashboardAlertWorker_Defaults(t *testing.T) {
	w := NewDashboardAlertWorker(nil, nil, 0, nil)
	if w.interval != 60*time.Second {
		t.Errorf("expected default interval 60s, got %s", w.interval)
	}
	if w.pub == nil {
		t.Fatal("expected noop publisher fallback, got nil")
	}
	if w.driverForEngine == nil {
		t.Fatal("expected driverForEngine fallback, got nil")
	}
	if got := w.driverForEngine(domain.DataEnginePostgres); got != "pgx" {
		t.Errorf("default driver mapping: expected 'pgx', got %q", got)
	}
	if w.bqExecutor != nil {
		t.Error("bqExecutor should be nil before SetBigQueryExecutor")
	}
}

func TestSetBigQueryExecutor_Wires(t *testing.T) {
	w := NewDashboardAlertWorker(nil, nil, 0, nil)
	fake := &fakeBQExecutor{}
	w.SetBigQueryExecutor(fake)
	if w.bqExecutor != fake {
		t.Fatalf("executor not wired")
	}
}

func TestEvaluateOne_BigQueryRoutesThroughExecutor(t *testing.T) {
	db := newWorkerDB(t)
	alert := seedAlert(t, db, domain.DataEngineBigQuery, "SELECT COUNT(*) FROM events", "bigquery://proj/ds", 5)

	fake := &fakeBQExecutor{
		QueryFn: func(_ context.Context, dsn, sql string) (*BigQueryResult, error) {
			return &BigQueryResult{
				Columns: []string{"count"},
				Rows:    [][]any{{int64(42)}},
			}, nil
		},
	}
	pub := &capturingPublisher{}

	w := NewDashboardAlertWorker(db, pub, time.Minute, nil)
	w.SetBigQueryExecutor(fake)
	w.evaluateOne(context.Background(), alert)

	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 BigQuery call, got %d", len(fake.calls))
	}
	if fake.calls[0].DSN != "bigquery://proj/ds" {
		t.Errorf("DSN: got %q", fake.calls[0].DSN)
	}
	if fake.calls[0].SQL != "SELECT COUNT(*) FROM events" {
		t.Errorf("SQL: got %q", fake.calls[0].SQL)
	}

	// Threshold GT 5 with observed 42 → breach → one alert event.
	if len(pub.seen) != 1 {
		t.Fatalf("expected 1 breach event, got %d", len(pub.seen))
	}
	if pub.seen[0].EventType != "data.dashboard.alert_triggered" {
		t.Errorf("unexpected event type %q", pub.seen[0].EventType)
	}
	if got, ok := pub.seen[0].Data.Metadata["value"].(float64); !ok || got != 42 {
		t.Errorf("Metadata.value: expected 42, got %v", pub.seen[0].Data.Metadata["value"])
	}
}

func TestEvaluateOne_BigQueryNoRows_NoAlertFired(t *testing.T) {
	db := newWorkerDB(t)
	alert := seedAlert(t, db, domain.DataEngineBigQuery, "SELECT COUNT(*) FROM events", "bigquery://proj/ds", 5)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(nil)

	fake := &fakeBQExecutor{
		QueryFn: func(_ context.Context, _, _ string) (*BigQueryResult, error) {
			return &BigQueryResult{Columns: []string{"count"}, Rows: nil}, nil
		},
	}
	pub := &capturingPublisher{}
	w := NewDashboardAlertWorker(db, pub, time.Minute, nil)
	w.SetBigQueryExecutor(fake)

	// evaluateOne must NOT panic on zero rows.
	w.evaluateOne(context.Background(), alert)

	if len(pub.seen) != 0 {
		t.Fatalf("expected no events on zero-rows, got %d", len(pub.seen))
	}
	if !strings.Contains(logBuf.String(), "alert") {
		t.Errorf("expected the failure to be logged; log output: %q", logBuf.String())
	}
}

func TestEvaluateOne_BigQueryExecutorError_NoAlertFired(t *testing.T) {
	db := newWorkerDB(t)
	alert := seedAlert(t, db, domain.DataEngineBigQuery, "SELECT 1", "bigquery://proj/ds", 0)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(nil)

	fake := &fakeBQExecutor{
		QueryFn: func(_ context.Context, _, _ string) (*BigQueryResult, error) {
			return nil, fmt.Errorf("permission denied")
		},
	}
	pub := &capturingPublisher{}
	w := NewDashboardAlertWorker(db, pub, time.Minute, nil)
	w.SetBigQueryExecutor(fake)

	w.evaluateOne(context.Background(), alert)

	if len(pub.seen) != 0 {
		t.Fatalf("errors must not emit alerts, got %d", len(pub.seen))
	}
	if !strings.Contains(logBuf.String(), "permission denied") {
		t.Errorf("expected error message in log, got %q", logBuf.String())
	}
}

func TestEvaluateOne_BigQueryExecutorNotConfigured(t *testing.T) {
	db := newWorkerDB(t)
	alert := seedAlert(t, db, domain.DataEngineBigQuery, "SELECT 1", "bigquery://proj/ds", 0)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(nil)

	pub := &capturingPublisher{}
	w := NewDashboardAlertWorker(db, pub, time.Minute, nil)
	// Intentionally skip SetBigQueryExecutor.

	w.evaluateOne(context.Background(), alert)

	if len(pub.seen) != 0 {
		t.Fatalf("no alerts when executor unconfigured, got %d", len(pub.seen))
	}
	if !strings.Contains(logBuf.String(), "bigquery executor not configured") {
		t.Errorf("expected 'bigquery executor not configured' in log, got %q", logBuf.String())
	}
}

func TestEvaluateOne_PostgresUsesDatabaseSQLPath(t *testing.T) {
	// Postgres alerts go through sql.Open(driverForEngine, dsn). We don't
	// have a live Postgres so we pass a DSN that fails at connect time and
	// verify the worker surfaces the failure rather than silently falling
	// back to BigQuery or into the noop branch. The BigQuery fake stays
	// wired so a mistaken route (BigQuery used for Postgres engine) would
	// show up as an unexpected fake call — that's a negative assertion
	// for this test.
	db := newWorkerDB(t)
	alert := seedAlert(t, db, domain.DataEnginePostgres, "SELECT 1", "postgres://invalid:5/doesnotexist", 0)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(nil)

	bq := &fakeBQExecutor{
		QueryFn: func(_ context.Context, _, _ string) (*BigQueryResult, error) {
			t.Fatal("BigQueryExecutor must not be called for Postgres alerts")
			return nil, nil
		},
	}
	pub := &capturingPublisher{}
	w := NewDashboardAlertWorker(db, pub, time.Minute, func(e domain.DataEngine) string {
		if e == domain.DataEnginePostgres {
			return "pgx"
		}
		return "pgx"
	})
	w.SetBigQueryExecutor(bq)

	w.evaluateOne(context.Background(), alert)

	if len(bq.calls) != 0 {
		t.Fatalf("BigQuery executor was called for a Postgres engine alert: %d calls", len(bq.calls))
	}
	if len(pub.seen) != 0 {
		t.Fatalf("no alert should fire when sql.Open/Query fails; got %d", len(pub.seen))
	}
	// The failure must surface through the structured log line.
	if !strings.Contains(logBuf.String(), "alert") {
		t.Errorf("expected the Postgres failure to be logged; got %q", logBuf.String())
	}
}

// --- toFloat helper ---------------------------------------------------------

func TestToFloat(t *testing.T) {
	cases := []struct {
		name  string
		in    any
		want  float64
		ok    bool
	}{
		{"nil", nil, 0, false},
		{"float64", float64(1.5), 1.5, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", int(3), 3, true},
		{"int64", int64(4), 4, true},
		{"int32", int32(5), 5, true},
		{"uint64", uint64(6), 6, true},
		{"uint32", uint32(7), 7, true},
		{"bool true", true, 1, true},
		{"bool false", false, 0, true},
		{"[]byte numeric", []byte("3.14"), 3.14, true},
		{"[]byte non-numeric", []byte("abc"), 0, false},
		{"string numeric", "12.5", 12.5, true},
		{"string non-numeric", "nope", 0, false},
		{"unsupported type", struct{}{}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := toFloat(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok: expected %v, got %v", tc.ok, ok)
			}
			if got != tc.want {
				t.Errorf("value: expected %v, got %v", tc.want, got)
			}
		})
	}
}
