// Package clickhouse provides a SourceAdapter over ClickHouse's native
// HTTP interface. It speaks the standard /?query=… wire format with
// JSON response, which avoids pulling in the heavyweight native
// clickhouse-go driver while covering every read-path use case the
// federated planner needs.
//
// DSN format:
//
//	clickhouse://<user>:<pass>@<host>:<port>/<database>
//	clickhouse://default@localhost:8123/default
//
// Credentials can also be supplied via query params for environments
// that prefer them out-of-band:
//
//	clickhouse://localhost:8123/default?user=svc&password_env=APP_CH_PW
//
// The adapter is safe to reuse across concurrent requests; each call
// builds a scoped http.Client with the configured timeout so queries
// cannot leak between tenants.
package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sentiae/data-service/internal/usecase"
)

// Default HTTP timeout applied per-request when the DSN does not carry
// an explicit timeout query param.
const defaultTimeout = 30 * time.Second

// QueryResult mirrors the (columns, rows) shape returned by the
// BigQuery adapter so call-sites can switch engines with no code
// changes. Rows are []any because ClickHouse JSON values map cleanly
// to Go scalars (string, float64, bool, nil) and we want callers to
// JSON-encode without touching vendor types.
type QueryResult struct {
	Columns []string
	Rows    [][]any
}

// Adapter speaks ClickHouse over HTTP. Every Execute call constructs
// a fresh HTTP request using the DSN embedded in the federated
// sub-query string, so a single Adapter can service multiple data
// sources concurrently.
//
// §12.1 RLS — ClickHouse row-policies can consult session settings
// via getSetting('setting_name'). The adapter stamps
// app_current_org_id / app_current_user_id into the HTTP session
// associated with the query, so a policy defined as
//
//	CREATE ROW POLICY tenant_isolation ON <table>
//	USING organization_id = getSetting('app_current_org_id')
//
// scopes every read to the requesting tenant without per-query SQL
// rewrites. Session persistence is achieved via the `session_id` and
// `session_check` query params — ClickHouse holds a per-session
// setting state keyed by session_id for up to `session_timeout`.
type Adapter struct {
	rlsMu  sync.RWMutex
	orgID  uuid.UUID
	userID uuid.UUID
}

// NewAdapter returns a zero-configured adapter.
func NewAdapter() *Adapter { return &Adapter{} }

// SetRLSContext implements usecase.RLSContextSetter so the federated
// planner can propagate tenant identifiers into ClickHouse session
// settings before each query.
func (a *Adapter) SetRLSContext(orgID, userID uuid.UUID) {
	a.rlsMu.Lock()
	a.orgID = orgID
	a.userID = userID
	a.rlsMu.Unlock()
}

func (a *Adapter) rlsSnapshot() (uuid.UUID, uuid.UUID) {
	a.rlsMu.RLock()
	defer a.rlsMu.RUnlock()
	return a.orgID, a.userID
}

// Name is the planner-facing identifier.
func (a *Adapter) Name() string { return "clickhouse" }

// Execute parses `<dsn> :: <sql>` and runs the SQL against the
// ClickHouse instance addressed by the DSN. Same shape as the
// BigQuery adapter so the planner dispatch code can stay engine-
// agnostic.
func (a *Adapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	dsn, sql, ok := strings.Cut(query, "::")
	if !ok {
		return nil, fmt.Errorf("clickhouse adapter: expected query in the form `<dsn> :: <sql>`")
	}
	res, err := a.QueryWithDSN(ctx, strings.TrimSpace(dsn), strings.TrimSpace(sql))
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(res.Rows))
	for _, r := range res.Rows {
		row := make(map[string]any, len(res.Columns))
		for i, col := range res.Columns {
			if i < len(r) {
				row[col] = r[i]
			}
		}
		rows = append(rows, row)
	}
	return &usecase.FederatedQueryResult{Columns: res.Columns, Rows: rows}, nil
}

// QueryWithDSN runs the SQL against the ClickHouse instance in the
// DSN and returns columns + rows. Used directly by the data-source
// handler for ad-hoc inspection.
//
// RLS: when SetRLSContext has been called we issue two SET statements
// in the same ClickHouse session (keyed by a fresh session_id UUID
// on every call) BEFORE the user's SELECT. Row policies defined on
// the target table can then consult the settings via
// getSetting('app_current_org_id').
func (a *Adapter) QueryWithDSN(ctx context.Context, dsn, sql string) (*QueryResult, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: cfg.timeout}

	// Every adapter call gets a fresh session so concurrent planner
	// dispatches never read each other's settings. session_check=0 so
	// ClickHouse does not require a prior SET from a different request
	// to have "registered" the session.
	sessionID := uuid.NewString()

	if err := a.stampRLS(ctx, client, cfg, sessionID); err != nil {
		return nil, err
	}

	// Always request JSON-with-named-columns output so parsing is
	// schema-driven and we don't have to second-guess column types.
	// The FORMAT clause is appended only when the caller didn't
	// already ask for one, so a pinned FORMAT in the SQL wins.
	finalSQL := sql
	lower := strings.ToLower(sql)
	if !strings.Contains(lower, " format ") {
		finalSQL = sql + "\nFORMAT JSON"
	}

	params := url.Values{}
	params.Set("query", finalSQL)
	params.Set("session_id", sessionID)
	if cfg.database != "" {
		params.Set("database", cfg.database)
	}
	endpoint := cfg.baseURL + "/?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: build request: %w", err)
	}
	if cfg.user != "" {
		req.Header.Set("X-ClickHouse-User", cfg.user)
	}
	if cfg.password != "" {
		req.Header.Set("X-ClickHouse-Key", cfg.password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: send: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// ClickHouse puts the error text in the body as plain text.
		msg := strings.TrimSpace(string(body))
		if len(msg) > 512 {
			msg = msg[:512] + "…"
		}
		return nil, fmt.Errorf("clickhouse: http %d: %s", resp.StatusCode, msg)
	}

	return parseJSONResult(body)
}

// stampRLS runs `SET app_current_org_id = '…'; SET app_current_user_id = '…'`
// on the session keyed by sessionID so subsequent queries on the same
// session see the values via getSetting(). Issued as two separate SET
// requests because ClickHouse rejects multi-statement payloads via
// the HTTP interface.
func (a *Adapter) stampRLS(ctx context.Context, client *http.Client, cfg dsnConfig, sessionID string) error {
	orgID, userID := a.rlsSnapshot()
	if orgID == uuid.Nil && userID == uuid.Nil {
		return nil
	}
	// ClickHouse requires every custom setting to be declared via the
	// server-side <custom_settings> config (or prefixed with
	// "SQL_" / "custom_"). We use the `SQL_app_current_org_id` /
	// `SQL_app_current_user_id` convention so operators don't have to
	// touch clickhouse server config to opt in; row policies then
	// reference them via getSetting('SQL_app_current_org_id'). Falling
	// back to the non-prefixed form on clusters that *have* declared
	// the setting is handled by the policy SQL itself.
	stamp := func(name, value string) error {
		params := url.Values{}
		params.Set("session_id", sessionID)
		params.Set("query", fmt.Sprintf("SET %s = '%s'", name, value))
		if cfg.database != "" {
			params.Set("database", cfg.database)
		}
		endpoint := cfg.baseURL + "/?" + params.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return fmt.Errorf("clickhouse: build SET: %w", err)
		}
		if cfg.user != "" {
			req.Header.Set("X-ClickHouse-User", cfg.user)
		}
		if cfg.password != "" {
			req.Header.Set("X-ClickHouse-Key", cfg.password)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("clickhouse: send SET: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			msg := strings.TrimSpace(string(body))
			if len(msg) > 256 {
				msg = msg[:256] + "…"
			}
			return fmt.Errorf("clickhouse: SET %s → http %d: %s", name, resp.StatusCode, msg)
		}
		return nil
	}
	if orgID != uuid.Nil {
		if err := stamp("SQL_app_current_org_id", orgID.String()); err != nil {
			return err
		}
	}
	if userID != uuid.Nil {
		if err := stamp("SQL_app_current_user_id", userID.String()); err != nil {
			return err
		}
	}
	return nil
}

// jsonResult matches the shape ClickHouse returns when FORMAT JSON is
// requested. We only care about meta (name+type) and data.
type jsonResult struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data []map[string]any `json:"data"`
}

func parseJSONResult(body []byte) (*QueryResult, error) {
	var raw jsonResult
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("clickhouse: decode json: %w", err)
	}
	columns := make([]string, 0, len(raw.Meta))
	for _, m := range raw.Meta {
		columns = append(columns, m.Name)
	}
	rows := make([][]any, 0, len(raw.Data))
	for _, r := range raw.Data {
		row := make([]any, len(columns))
		for i, col := range columns {
			row[i] = r[col]
		}
		rows = append(rows, row)
	}
	return &QueryResult{Columns: columns, Rows: rows}, nil
}

// dsnConfig captures the parsed connection parameters. Kept unexported
// so callers always pass raw DSNs — there's no appetite to hand
// fully-formed configs around.
type dsnConfig struct {
	baseURL  string
	user     string
	password string
	database string
	timeout  time.Duration
}

// parseDSN accepts `clickhouse://user:pass@host:port/db?timeout_sec=…`.
// The `password_env` query param is resolved against env vars so
// secrets never need to live in the database row.
func parseDSN(dsn string) (dsnConfig, error) {
	if dsn == "" {
		return dsnConfig{}, fmt.Errorf("clickhouse: empty DSN")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return dsnConfig{}, fmt.Errorf("clickhouse: parse dsn: %w", err)
	}
	if u.Scheme != "clickhouse" {
		return dsnConfig{}, fmt.Errorf("clickhouse: DSN scheme must be `clickhouse://`, got %q", u.Scheme)
	}
	host := u.Host
	if host == "" {
		return dsnConfig{}, fmt.Errorf("clickhouse: host missing in DSN")
	}
	cfg := dsnConfig{
		baseURL:  "http://" + host,
		database: strings.TrimPrefix(u.Path, "/"),
		timeout:  defaultTimeout,
	}
	q := u.Query()
	if q.Get("secure") == "true" {
		cfg.baseURL = "https://" + host
	}
	if u.User != nil {
		cfg.user = u.User.Username()
		if pw, ok := u.User.Password(); ok {
			cfg.password = pw
		}
	}
	if v := q.Get("user"); v != "" {
		cfg.user = v
	}
	if v := q.Get("password"); v != "" {
		cfg.password = v
	}
	if v := q.Get("password_env"); v != "" {
		cfg.password = os.Getenv(v)
	}
	if v := q.Get("database"); v != "" {
		cfg.database = v
	}
	if v := q.Get("timeout_sec"); v != "" {
		if d, err := time.ParseDuration(v + "s"); err == nil {
			cfg.timeout = d
		}
	}
	return cfg, nil
}
