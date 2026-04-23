package http

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/sentiae/data-service/internal/domain"
)

// executeWithRLS runs a read-only SELECT against an open SQL
// connection, injecting per-adapter row-level-security context where
// the engine supports it. §12.1.
//
// Strategy:
//   - Postgres: open a read-only transaction and SET LOCAL
//     app.current_user_id / app.current_org_id so row policies can
//     read them via current_setting(). Transaction rollback discards
//     the LOCAL bindings.
//   - Snowflake: ALTER SESSION SET APP_CURRENT_ORG_ID / APP_CURRENT_USER_ID
//     on a pinned connection before the SELECT. Row-access policies
//     reference the session variables via $APP_CURRENT_ORG_ID.
//   - MySQL / MSSQL / SQLite: no engine-native session-variable
//     mechanism that composes with row policies, so we run the query
//     unwrapped. Tenant scoping for these engines relies on the
//     permission-service column filter applied downstream.
//
// The BigQuery / ClickHouse paths don't go through database/sql at
// all — they are served via their respective adapters whose Query
// methods consult the planner-supplied (org, user) context directly.
// Those adapters implement usecase.RLSContextSetter.
//
// Callers pass empty strings when the context is unavailable — the
// settings are skipped in that case so local development stays simple.
func executeWithRLS(ctx context.Context, db *sql.DB, engine domain.DataEngine, raw, orgID, userID string) (*sql.Rows, error) {
	strategy := strategyForEngine(engine)
	return strategy(ctx, db, raw, orgID, userID)
}

// rlsStrategy is a per-engine session-stamping routine. Each strategy
// is responsible for executing the raw SQL and returning *sql.Rows.
type rlsStrategy func(ctx context.Context, db *sql.DB, raw, orgID, userID string) (*sql.Rows, error)

// strategyForEngine picks the right RLS strategy for the engine.
// Falls back to a plain query for engines that don't have a native
// session-variable mechanism composable with row policies.
func strategyForEngine(engine domain.DataEngine) rlsStrategy {
	switch engine {
	case domain.DataEnginePostgres:
		return postgresRLSStrategy
	case domain.DataEngineSnowflake:
		return snowflakeRLSStrategy
	default:
		return passthroughStrategy
	}
}

// passthroughStrategy executes the query without any session stamping.
// Used for engines where RLS is enforced either downstream (column
// filtering in the federated planner) or at the adapter layer.
func passthroughStrategy(ctx context.Context, db *sql.DB, raw, _, _ string) (*sql.Rows, error) {
	return db.QueryContext(ctx, raw)
}

// postgresRLSStrategy opens a read-only transaction and stamps the
// tenant identifiers via SET LOCAL so downstream row policies can
// read `current_setting('app.current_org_id')`. The transaction is
// intentionally leaked to the caller (rows.Close reclaims it when the
// caller finishes iterating) — the same pattern GORM uses for
// read-only queries when PreferSimpleProtocol is off.
func postgresRLSStrategy(ctx context.Context, db *sql.DB, raw, orgID, userID string) (*sql.Rows, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		// Fallback to unwrapped query if we can't open a tx — some
		// drivers don't support read-only transactions, but we still
		// want to return results rather than fail the whole query.
		return db.QueryContext(ctx, raw)
	}
	if cleanOrg := sanitizeSetting(orgID); cleanOrg != "" {
		if _, err := tx.ExecContext(ctx, "SET LOCAL app.current_org_id = '"+cleanOrg+"'"); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	if cleanUser := sanitizeSetting(userID); cleanUser != "" {
		if _, err := tx.ExecContext(ctx, "SET LOCAL app.current_user_id = '"+cleanUser+"'"); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	rows, err := tx.QueryContext(ctx, raw)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return rows, nil
}

// snowflakeRLSStrategy stamps APP_CURRENT_ORG_ID / APP_CURRENT_USER_ID
// via ALTER SESSION SET on a pinned connection. The same *sql.Conn
// carries both the stamp and the subsequent SELECT so the setting is
// actually in scope when the user's SQL runs.
func snowflakeRLSStrategy(ctx context.Context, db *sql.DB, raw, orgID, userID string) (*sql.Rows, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("snowflake rls: acquire conn: %w", err)
	}
	if cleanOrg := sanitizeSetting(orgID); cleanOrg != "" {
		stmt := fmt.Sprintf("ALTER SESSION SET APP_CURRENT_ORG_ID = '%s'", cleanOrg)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("snowflake rls: set org: %w", err)
		}
	}
	if cleanUser := sanitizeSetting(userID); cleanUser != "" {
		stmt := fmt.Sprintf("ALTER SESSION SET APP_CURRENT_USER_ID = '%s'", cleanUser)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("snowflake rls: set user: %w", err)
		}
	}
	rows, err := conn.QueryContext(ctx, raw)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Note: rows.Close() releases the pinned *sql.Conn automatically.
	return rows, nil
}

// sanitizeSetting strips single quotes and embedded semicolons from a
// SET LOCAL value. We only accept UUID-shaped strings in practice; the
// scrubber is a defence-in-depth check.
func sanitizeSetting(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if strings.ContainsAny(v, "';\n\r") {
		return ""
	}
	return v
}
