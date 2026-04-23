package usecase

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/gorm"
)

// BigQueryExecutor runs a SQL statement against BigQuery using the DSN
// embedded in a DataSource. Implemented by bigquery.Adapter.QueryWithDSN.
// Injected (rather than imported directly) to avoid a usecase→adapters
// compile cycle.
type BigQueryExecutor interface {
	QueryWithDSN(ctx context.Context, dsn, sql string) (*BigQueryResult, error)
}

// BigQueryResult is the minimal (columns, rows) shape the alert worker
// consumes from the BigQuery adapter. The adapter's native type already
// matches this shape, so a tiny local adapter interface keeps the data-
// service package graph acyclic.
type BigQueryResult struct {
	Columns []string
	Rows    [][]any
}

// DashboardAlertWorker periodically evaluates active DashboardAlert rows,
// re-running the associated query and comparing the observed value against
// the configured threshold. Breaches emit `data.dashboard.alert_triggered`
// which ops-service consumes for routing to Slack / email / PagerDuty.
type DashboardAlertWorker struct {
	db       *gorm.DB
	pub      kafka.Publisher
	interval time.Duration

	// driverForEngine maps a DataEngine to a database/sql driver name.
	// Injected to avoid cross-package import cycles with the http handler.
	driverForEngine func(domain.DataEngine) string

	// bqExecutor is optional; when present, BigQuery-engine alerts are
	// routed through this adapter instead of being skipped.
	bqExecutor BigQueryExecutor
}

// NewDashboardAlertWorker constructs a worker with the given evaluation
// interval. Pass 0 to use the default of 60 seconds.
func NewDashboardAlertWorker(db *gorm.DB, pub kafka.Publisher, interval time.Duration, driverForEngine func(domain.DataEngine) string) *DashboardAlertWorker {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	if driverForEngine == nil {
		driverForEngine = func(e domain.DataEngine) string { return "pgx" }
	}
	return &DashboardAlertWorker{
		db:              db,
		pub:             pub,
		interval:        interval,
		driverForEngine: driverForEngine,
	}
}

// SetBigQueryExecutor wires the BigQuery adapter after construction so the
// http server bootstrap doesn't need to know about the adapter package at
// worker-construction time.
func (w *DashboardAlertWorker) SetBigQueryExecutor(exec BigQueryExecutor) {
	w.bqExecutor = exec
}

// Start launches the worker loop. Returns immediately; cancel ctx to stop.
func (w *DashboardAlertWorker) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.evaluateAll(ctx)
			}
		}
	}()
}

// evaluateAll walks every active alert and evaluates it. Runtime errors
// are logged but do not halt the loop.
func (w *DashboardAlertWorker) evaluateAll(ctx context.Context) {
	var alerts []domain.DashboardAlert
	if err := w.db.Where("active = ?", true).Find(&alerts).Error; err != nil {
		log.Printf("dashboard_alert_worker: fetch failed: %v", err)
		return
	}
	for i := range alerts {
		w.evaluateOne(ctx, &alerts[i])
	}
}

// evaluateOne runs the saved query, extracts a single numeric value, and
// emits a Kafka event on breach.
func (w *DashboardAlertWorker) evaluateOne(ctx context.Context, alert *domain.DashboardAlert) {
	value, err := w.observedValue(ctx, alert)
	now := time.Now()
	if err != nil {
		// Record the attempt; don't emit breach on error.
		w.db.Model(alert).Updates(map[string]any{"last_evaluated": now})
		log.Printf("dashboard_alert_worker: alert %s evaluation failed: %v", alert.ID, err)
		return
	}

	breached := alert.Breached(value)
	updates := map[string]any{
		"last_evaluated": now,
		"last_value":     value,
	}
	if breached {
		updates["last_triggered"] = now
	}
	w.db.Model(alert).Updates(updates)

	if !breached {
		return
	}
	_ = w.pub.Publish(ctx, "data.dashboard.alert_triggered", kafka.EventData{
		ActorID:        alert.CreatedBy.String(),
		ResourceType:   "dashboard_alert",
		ResourceID:     alert.ID.String(),
		OrganizationID: alert.OrganizationID.String(),
		Metadata: map[string]any{
			"alert_id":        alert.ID.String(),
			"dashboard_id":    alert.DashboardID.String(),
			"panel_id":        alert.PanelID,
			"threshold_type":  string(alert.ThresholdType),
			"threshold_value": alert.ThresholdValue,
			"value":           value,
			"notify_channel":  alert.NotifyChannel,
			"window_seconds":  alert.WindowSeconds,
		},
	})
}

// observedValue loads the associated DataQuery, executes it, and returns
// the first numeric cell of the first row. This matches the convention
// used by KPI-style panels in the portal dashboard builder.
func (w *DashboardAlertWorker) observedValue(ctx context.Context, alert *domain.DashboardAlert) (float64, error) {
	if alert.QueryID == nil {
		return 0, fmt.Errorf("alert has no query_id — attach a saved query")
	}
	var q domain.DataQuery
	if err := w.db.Where("id = ?", *alert.QueryID).First(&q).Error; err != nil {
		return 0, fmt.Errorf("query not found: %w", err)
	}
	var ds domain.DataSource
	if err := w.db.Where("id = ?", q.DataSourceID).First(&ds).Error; err != nil {
		return 0, fmt.Errorf("data source not found: %w", err)
	}
	if ds.ConnectionDSN == "" {
		return 0, fmt.Errorf("data source has no connection DSN")
	}

	// BigQuery does not speak database/sql — route through the dedicated
	// adapter. When the adapter isn't wired we can't evaluate the alert;
	// log the skip (caller handles this branch) rather than silently
	// succeeding.
	if ds.Engine == domain.DataEngineBigQuery {
		if w.bqExecutor == nil {
			return 0, fmt.Errorf("bigquery executor not configured")
		}
		qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		res, err := w.bqExecutor.QueryWithDSN(qctx, ds.ConnectionDSN, q.RawQuery)
		if err != nil {
			return 0, fmt.Errorf("bigquery query failed: %w", err)
		}
		if res == nil || len(res.Rows) == 0 {
			return 0, fmt.Errorf("query returned no rows")
		}
		for _, v := range res.Rows[0] {
			if f, ok := toFloat(v); ok {
				return f, nil
			}
		}
		return 0, fmt.Errorf("no numeric column in first row")
	}

	if !ds.Engine.UsesDatabaseSQL() {
		return 0, fmt.Errorf("alerts currently only support SQL engines accessible via database/sql (engine=%s)", ds.Engine)
	}

	sqlDB, err := sql.Open(w.driverForEngine(ds.Engine), ds.ConnectionDSN)
	if err != nil {
		return 0, fmt.Errorf("open db: %w", err)
	}
	defer sqlDB.Close()

	// Give queries a reasonable time budget — alerts run on a hot loop.
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := sqlDB.QueryContext(qctx, q.RawQuery)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if !rows.Next() {
		return 0, fmt.Errorf("query returned no rows")
	}

	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return 0, err
	}

	for _, v := range values {
		if f, ok := toFloat(v); ok {
			return f, nil
		}
	}
	return 0, fmt.Errorf("no numeric column in first row")
}

// toFloat coerces a SQL scan result into a float64. Handles int flavors,
// float types, []byte, and string representations.
func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case []byte:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// alertResourceID is unused externally but keeps uuid imported so future
// callers can construct alert URIs without re-introducing the import.
var _ = uuid.Nil
