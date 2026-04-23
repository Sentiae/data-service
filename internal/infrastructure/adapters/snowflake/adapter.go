// Package snowflake provides a SourceAdapter wrapper over Snowflake's
// database/sql driver so the federated planner and data-source handlers
// can treat Snowflake uniformly with other SQL engines.
//
// The driver is the canonical one maintained by Snowflake themselves:
// github.com/snowflakedb/gosnowflake. It registers under the
// "snowflake" driver name the moment any package imports it, which
// data_source_handler.go already does via blank import; depending on
// this adapter doesn't double-register.
//
// DSN format (matches gosnowflake's standard form):
//
//	<user>:<password>@<account>/<database>/<schema>?warehouse=<wh>&role=<r>
//
// Example:
//
//	sentiae:hunter2@xy12345.us-east-1/ANALYTICS/PUBLIC?warehouse=COMPUTE_WH
//
// The adapter's Query/QueryWithDSN methods return the same shape as
// the BigQuery and ClickHouse adapters so the federated planner can
// dispatch engine-agnostically.
//
// §12.1 — Row-Level Security.
// The adapter implements usecase.RLSContextSetter so the federated
// planner can propagate the requesting (org, user) into Snowflake
// session variables before each query runs. Snowflake's
// ALTER SESSION SET exposes session-scoped variables that row-access
// policies consult via `$APP_CURRENT_ORG_ID` / `$APP_CURRENT_USER_ID`.
// Setting is idempotent and cheap — Snowflake does not round-trip to
// storage for session variable writes.
package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	_ "github.com/snowflakedb/gosnowflake" // registers "snowflake" driver

	"github.com/sentiae/data-service/internal/usecase"
)

// Row is the adapter's native row representation.
type Row = map[string]any

// QueryResult mirrors (columns, rows) returned by the BigQuery +
// ClickHouse adapters so call-sites can switch engines with no code
// changes.
type QueryResult struct {
	Columns []string
	Rows    []Row
}

// Adapter speaks Snowflake. Cheap to construct: every Execute call
// builds a fresh *sql.DB from the DSN embedded in the federated
// sub-query string so a single adapter can service multiple data
// sources concurrently.
//
// RLS context is stored on the adapter and consulted before each
// query — the planner stamps it via SetRLSContext before Execute.
type Adapter struct {
	rlsMu  sync.RWMutex
	orgID  uuid.UUID
	userID uuid.UUID
}

// NewAdapter returns a zero-configured adapter.
func NewAdapter() *Adapter { return &Adapter{} }

// Name is the planner-facing identifier. Matches the engine string
// in domain.DataEngineSnowflake so registration stays symmetric with
// the other adapters.
func (a *Adapter) Name() string { return "snowflake" }

// SetRLSContext implements usecase.RLSContextSetter. Called by the
// federated planner before each sub-query dispatch so the adapter
// knows the requesting (org, user).
func (a *Adapter) SetRLSContext(orgID, userID uuid.UUID) {
	a.rlsMu.Lock()
	a.orgID = orgID
	a.userID = userID
	a.rlsMu.Unlock()
}

// rlsSnapshot returns a copy of the current (org, user). Read under
// RLock so concurrent planner dispatches don't torn-read across two
// calls.
func (a *Adapter) rlsSnapshot() (uuid.UUID, uuid.UUID) {
	a.rlsMu.RLock()
	defer a.rlsMu.RUnlock()
	return a.orgID, a.userID
}

// Execute parses `<dsn> :: <sql>` and runs the SQL against the
// Snowflake instance addressed by the DSN. Same shape as the
// BigQuery + ClickHouse adapters so the planner dispatch code can
// stay engine-agnostic.
func (a *Adapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	dsn, sqlText, ok := strings.Cut(query, "::")
	if !ok {
		return nil, fmt.Errorf("snowflake adapter: expected query in the form `<dsn> :: <sql>`")
	}
	res, err := a.QueryWithDSN(ctx, strings.TrimSpace(dsn), strings.TrimSpace(sqlText))
	if err != nil {
		return nil, err
	}
	return &usecase.FederatedQueryResult{Columns: res.Columns, Rows: res.Rows}, nil
}

// Query runs the SQL against a shared DSN read from configuration.
// This is the minimal real implementation the task spec calls for —
// callers that already have an open *sql.DB should use QueryRows
// directly.
func (a *Adapter) Query(ctx context.Context, dsn, sqlText string) ([]Row, error) {
	res, err := a.QueryWithDSN(ctx, dsn, sqlText)
	if err != nil {
		return nil, err
	}
	return res.Rows, nil
}

// QueryWithDSN opens a Snowflake connection using the supplied DSN
// and runs sqlText. Rows are materialised fully before return — this
// adapter targets dashboard + federated-planner workloads where full
// materialisation is the expected shape; streaming support is a
// later pass.
//
// RLS flow:
//  1. Acquire a connection from the pool and pin it via *sql.Conn so
//     the ALTER SESSION SET and the SELECT share the same Snowflake
//     session. Without pinning, the pool could return a session that
//     was never stamped.
//  2. Set APP_CURRENT_ORG_ID / APP_CURRENT_USER_ID via ALTER SESSION.
//  3. Run the caller-supplied SQL on that pinned connection.
func (a *Adapter) QueryWithDSN(ctx context.Context, dsn, sqlText string) (*QueryResult, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("snowflake: empty DSN")
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("snowflake: open: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("snowflake: acquire conn: %w", err)
	}
	defer conn.Close()

	if err := a.applyRLS(ctx, conn); err != nil {
		return nil, fmt.Errorf("snowflake: apply rls: %w", err)
	}

	rows, err := conn.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("snowflake: query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("snowflake: columns: %w", err)
	}
	out := &QueryResult{Columns: cols}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("snowflake: scan: %w", err)
		}
		row := make(Row, len(cols))
		for i, c := range cols {
			row[c] = values[i]
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("snowflake: iterate: %w", err)
	}
	return out, nil
}

// applyRLS stamps the current (org, user) onto the pinned Snowflake
// session. Snowflake session variables are the idiomatic way to pass
// tenant context to row-access policies: policies can reference
// `$APP_CURRENT_ORG_ID` without the caller needing to modify the
// outer SELECT. §12.1.
func (a *Adapter) applyRLS(ctx context.Context, conn *sql.Conn) error {
	orgID, userID := a.rlsSnapshot()
	if orgID == uuid.Nil && userID == uuid.Nil {
		return nil
	}
	// Snowflake identifiers for session variables are uppercase by
	// convention. Values are single-quoted strings; UUID strings have
	// no chars that need escaping but we quote defensively.
	if orgID != uuid.Nil {
		stmt := fmt.Sprintf("ALTER SESSION SET APP_CURRENT_ORG_ID = '%s'", orgID.String())
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("set org: %w", err)
		}
	}
	if userID != uuid.Nil {
		stmt := fmt.Sprintf("ALTER SESSION SET APP_CURRENT_USER_ID = '%s'", userID.String())
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("set user: %w", err)
		}
	}
	return nil
}

// DescribeTable performs a lightweight INFORMATION_SCHEMA lookup so
// the schema introspection endpoint can return column metadata
// without materializing the whole table. Callers that want the full
// schema should hit the generic /schema endpoints instead.
func (a *Adapter) DescribeTable(ctx context.Context, dsn, schema, table string) (*QueryResult, error) {
	if strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" {
		return nil, fmt.Errorf("snowflake: schema and table are required")
	}
	// Snowflake's INFORMATION_SCHEMA.COLUMNS takes UPPER-CASE names by
	// default; we uppercase here so operators don't have to.
	sqlText := fmt.Sprintf(`
		SELECT column_name, data_type, is_nullable, column_default, ordinal_position
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
		ORDER BY ordinal_position
	`, strings.ToUpper(schema), strings.ToUpper(table))
	return a.QueryWithDSN(ctx, dsn, sqlText)
}
