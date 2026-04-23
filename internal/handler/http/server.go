package http

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sentiae/data-service/internal/domain"
	bqadapter "github.com/sentiae/data-service/internal/infrastructure/adapters/bigquery"
	chadapter "github.com/sentiae/data-service/internal/infrastructure/adapters/clickhouse"
	graphqladapter "github.com/sentiae/data-service/internal/infrastructure/adapters/graphqlsrc"
	restadapter "github.com/sentiae/data-service/internal/infrastructure/adapters/rest"
	"github.com/sentiae/data-service/internal/infrastructure/adapters/sentiae"
	snowflakeadapter "github.com/sentiae/data-service/internal/infrastructure/adapters/snowflake"
	"github.com/sentiae/data-service/internal/infrastructure/permission"
	"github.com/sentiae/data-service/internal/repository/postgres"
	"github.com/sentiae/data-service/internal/usecase"
	"github.com/sentiae/platform-kit/kafka"
	"github.com/sentiae/platform-kit/timetravel"
	"gorm.io/gorm"
)

// Server wraps the HTTP router and owns background workers (e.g. the
// dashboard alert evaluator) so main.go can keep wiring trivial.
type Server struct {
	router       chi.Router
	permChecker  *permission.Checker
	alertWorker  *usecase.DashboardAlertWorker
	embedWorker  *usecase.DashboardEmbedExpiryWorker
	workerCancel context.CancelFunc
}

// Close tears down background workers. Called by main.go on shutdown.
func (s *Server) Close() {
	if s.workerCancel != nil {
		s.workerCancel()
	}
	if s.permChecker != nil {
		_ = s.permChecker.Close()
	}
}

// NewServer wires the HTTP router with all handlers. The publisher is
// injected (may be a NoopPublisher when Kafka is disabled) so handlers can
// emit events uniformly. When pub is nil a NoopPublisher is substituted so
// no handler needs to nil-check at the call site.
func NewServer(db *gorm.DB, pub kafka.Publisher) *Server {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}

	router := chi.NewRouter()

	// §13.1 time-travel recorder. Writer label is "data-service" so
	// cross-service queries can attribute the snapshot back to its
	// owning domain. When the entity_snapshots table is unreachable
	// (e.g. dev with a brand-new DB) the recorder still returns a
	// usable instance; AutoMigrate ensures the table is in place.
	var recorder timetravel.Recorder = timetravel.NoopRecorder{}
	if db != nil {
		if err := timetravel.AutoMigrate(db); err != nil {
			// Downgrade to no-op — the snapshot path is best-effort.
			// Primary writes continue either way.
			_ = err
		} else {
			recorder = timetravel.NewGORMRecorder(db, "data-service", nil)
		}
	}

	// Middleware
	router.Use(middleware.Recoverer)
	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	router.Use(corsMiddleware)

	// Health
	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		respondSuccess(w, map[string]string{"status": "healthy", "service": "data-service"})
	})

	// Permission-service client. Empty endpoint → default-allow checker
	// (suitable for local dev); any real endpoint enforces RBAC.
	permEndpoint := os.Getenv("PERMISSION_SERVICE_GRPC_URL")
	permChecker, err := permission.NewChecker(permEndpoint, db)
	if err != nil {
		// Log but don't crash — fall back to default-allow so the service
		// remains reachable in degraded environments.
		permChecker = &permission.Checker{}
	}

	// API routes
	approvals := usecase.NewWriteApprovalService(db)
	historySvc := usecase.NewQueryHistoryService(
		postgres.NewQueryHistoryRepo(db),
		postgres.NewSavedQueryRepo(db),
	)
	historyHandler := NewQueryHistoryHandler(historySvc).WithTimeTravelRecorder(recorder)
	dsHandler := NewDataSourceHandler(db, pub).WithTimeTravelRecorder(recorder)
	queryHandler := NewQueryHandler(db, pub, approvals, historySvc)
	dashHandler := NewDashboardHandler(db, pub).WithTimeTravelRecorder(recorder)
	dashEmbedHandler := NewDashboardEmbedHandler(db, pub, dashHandler.access).WithTimeTravelRecorder(recorder)
	schemaHandler := NewSchemaHandler(db)
	annotationHandler := NewAnnotationHandler(db).WithTimeTravelRecorder(recorder)
	dashboardYAMLHandler := NewDashboardYAMLHandler(db, pub)

	// Build federated planner with Sentiae adapters wired from env URLs.
	federatedPlanner := usecase.NewFederatedPlanner()
	federatedPlanner.RegisterAdapter("sentiae_vcs", sentiae.NewVCSAdapter(envOr("GIT_SERVICE_URL", "http://localhost:8087")))
	federatedPlanner.RegisterAdapter("sentiae_ops", sentiae.NewOpsAdapter(envOr("OPS_SERVICE_URL", "http://localhost:8083")))
	federatedPlanner.RegisterAdapter("sentiae_canvas", sentiae.NewCanvasAdapter(envOr("CANVAS_SERVICE_URL", "http://localhost:8084")))
	// BigQuery adapter for federated queries. Per-request credentials are
	// read from the sub-query DSN / APP_GCP_SERVICE_ACCOUNT_JSON env.
	federatedPlanner.RegisterAdapter("bigquery", bqadapter.NewAdapter())
	// §12.4 — Snowflake adapter exposes database/sql-backed Query + Execute
	// through the same `<dsn> :: <sql>` DSL as BigQuery/ClickHouse so the
	// federated planner stays engine-agnostic.
	federatedPlanner.RegisterAdapter("snowflake", snowflakeadapter.NewAdapter())
	// §12.4 — REST and GraphQL adapters. Both speak the "endpoint :: query"
	// DSL and decode common response shapes into tabular rows.
	federatedPlanner.RegisterAdapter("rest", restadapter.NewAdapter())
	federatedPlanner.RegisterAdapter("graphql", graphqladapter.NewAdapter())
	// §12.1 — ClickHouse adapter exposes the same `<dsn> :: <sql>` DSL
	// as the other warehouse engines. RLS stamps SQL_app_current_org_id
	// / SQL_app_current_user_id via the session before each query.
	federatedPlanner.RegisterAdapter("clickhouse", chadapter.NewAdapter())
	// Wire the concrete permission checker into the planner so every
	// federated result is filtered by column-level access rules.
	federatedPlanner.SetPermissionChecker(permChecker)

	federatedHandler := NewFederatedQueryHandler(federatedPlanner, db)
	dslHandler := NewDSLHandler(db)
	explainHandler := NewQueryExplainHandler(db)

	// Attach DB to request context so handlers that need direct GORM access
	// (query_explain ChartSuggestion) can grab it without holding a package
	// global. Scoped to /api/v1 so health and metrics stay DB-free.
	injectDB := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(WithDB(r.Context(), db)))
		})
	}

	router.Route("/api/v1", func(r chi.Router) {
		r.Use(injectDB)
		dsHandler.RegisterRoutes(r)
		queryHandler.RegisterRoutes(r)
		historyHandler.RegisterRoutes(r)
		dashHandler.RegisterRoutes(r)
		// §12.5 (C10) embed token rotate lives on a sibling route so
		// the primary DashboardHandler doesn't gain new state.
		dashEmbedHandler.RegisterRoutes(r)
		federatedHandler.RegisterRoutes(r)
		schemaHandler.RegisterRoutes(r)
		annotationHandler.RegisterRoutes(r)
		dashboardYAMLHandler.RegisterRoutes(r)
		// DSL execution endpoint (POST /api/v1/data/dsl/execute) for the
		// foundry flow worker.
		dslHandler.RegisterRoutes(r)
		// §12.3 explain endpoint + chart suggestion GET.
		explainHandler.RegisterRoutes(r)
	})

	// Dashboard alert worker — evaluates active alerts on a fixed interval.
	interval := workerInterval()
	alertWorker := usecase.NewDashboardAlertWorker(db, pub, interval, driverForEngineAdapter)
	// Route BigQuery alerts through the dedicated adapter instead of
	// skipping them. The tiny bqExecutorAdapter wrapper adapts the
	// adapter's native QueryResult type to the BigQueryResult shape the
	// worker expects (keeps usecase→adapters independent).
	alertWorker.SetBigQueryExecutor(bqExecutorAdapter{inner: bqadapter.NewAdapter()})
	ctx, cancel := context.WithCancel(context.Background())
	alertWorker.Start(ctx)

	// §12.5 (C10) hourly sweep that disables expired embed tokens.
	embedWorker := usecase.NewDashboardEmbedExpiryWorker(db, pub, embedExpirySweepInterval())
	embedWorker.Start(ctx)

	return &Server{
		router:       router,
		permChecker:  permChecker,
		alertWorker:  alertWorker,
		embedWorker:  embedWorker,
		workerCancel: cancel,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Organization-ID, X-User-ID, X-Team-IDs")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// workerInterval returns the evaluation interval for the dashboard alert
// worker, configurable via APP_DATA_ALERT_INTERVAL_SECONDS.
func workerInterval() time.Duration {
	v := os.Getenv("APP_DATA_ALERT_INTERVAL_SECONDS")
	if v == "" {
		return 60 * time.Second
	}
	d, err := time.ParseDuration(v + "s")
	if err != nil || d <= 0 {
		return 60 * time.Second
	}
	return d
}

// embedExpirySweepInterval returns how often the DashboardEmbedExpiryWorker
// wakes up. Defaults to 1 hour; override with APP_DATA_EMBED_EXPIRY_SWEEP_SECONDS.
func embedExpirySweepInterval() time.Duration {
	v := os.Getenv("APP_DATA_EMBED_EXPIRY_SWEEP_SECONDS")
	if v == "" {
		return time.Hour
	}
	d, err := time.ParseDuration(v + "s")
	if err != nil || d <= 0 {
		return time.Hour
	}
	return d
}

// driverForEngineAdapter lifts the package-private driverForEngine helper
// so usecase.DashboardAlertWorker can resolve SQL drivers without
// re-importing domain internals.
func driverForEngineAdapter(e domain.DataEngine) string {
	return driverForEngine(e)
}

// bqExecutorAdapter adapts bigquery.Adapter to the usecase-package-level
// BigQueryExecutor interface. We translate the adapter's native
// `*bigquery.QueryResult` into the worker's `BigQueryResult` shape so
// the usecase package stays free of the bigquery import.
type bqExecutorAdapter struct {
	inner *bqadapter.Adapter
}

func (a bqExecutorAdapter) QueryWithDSN(ctx context.Context, dsn, sql string) (*usecase.BigQueryResult, error) {
	res, err := a.inner.QueryWithDSN(ctx, dsn, sql)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return &usecase.BigQueryResult{Columns: res.Columns, Rows: res.Rows}, nil
}
