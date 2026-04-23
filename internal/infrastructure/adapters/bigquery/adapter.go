// Package bigquery provides a light SourceAdapter wrapper over the Google
// Cloud BigQuery client so the federated planner and the data-source
// handlers can treat BigQuery uniformly with database/sql engines.
//
// BigQuery does not speak database/sql cleanly, so this adapter exposes a
// `Query(ctx, sql)` method returning (columns, rows) and also implements
// usecase.SourceAdapter (Name + Execute) for the federated planner.
//
// Credentials: either provide a base64-encoded service-account JSON in the
// DataSource.ConnectionDSN (format `bigquery://<project>?credentials_json_b64=<b64>`)
// or rely on Application Default Credentials via APP_GCP_SERVICE_ACCOUNT_JSON /
// GOOGLE_APPLICATION_CREDENTIALS.
package bigquery

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	bq "cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/usecase"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// QueryResult matches the lightweight (columns, rows) shape the task spec
// asks for. It is also convertible to usecase.FederatedQueryResult.
type QueryResult struct {
	Columns []string
	Rows    [][]any
}

// Adapter speaks BigQuery. It is cheap to construct: the *bq.Client is
// built lazily per-call using the credentials embedded in the connection
// DSN, so multiple data sources (different projects/credentials) can share
// one Adapter instance.
//
// §12.1 RLS — BigQuery's first-class tenancy hook is authorized views /
// row-access policies bound to a label or a policy tag. The adapter
// attaches two labels to every job — `app_current_org_id` and
// `app_current_user_id` — so authorized-view definitions can filter on
// them, and audit logs surface the actor for every query. When the
// DSN specifies an `impersonation_email` param, the adapter also
// impersonates that service account so per-org credential boundaries
// are enforced at the Google IAM layer.
type Adapter struct {
	rlsMu  sync.RWMutex
	orgID  uuid.UUID
	userID uuid.UUID
}

// NewAdapter returns a zero-configured adapter. Per-request credentials are
// pulled from the DSN / env at Execute time.
func NewAdapter() *Adapter { return &Adapter{} }

// Name is the planner-facing identifier.
func (a *Adapter) Name() string { return "bigquery" }

// SetRLSContext implements usecase.RLSContextSetter so the federated
// planner can propagate (org, user) into BigQuery job labels.
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

// Execute is used by the federated planner. The sub-query DSL is expected
// to be `<dsn> :: <sql>`, where dsn is a bigquery:// URL. For direct use by
// SyncSchema / query paths, call QueryWithDSN instead.
func (a *Adapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	dsn, sql, ok := strings.Cut(query, "::")
	if !ok {
		return nil, fmt.Errorf("bigquery adapter: expected query in the form `<dsn> :: <sql>`")
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

// QueryWithDSN parses the DSN, constructs a client, runs the SQL, and
// returns columns+rows. The returned rows use []any so callers can JSON-
// encode them without touching the BigQuery value types.
func (a *Adapter) QueryWithDSN(ctx context.Context, dsn, sql string) (*QueryResult, error) {
	projectID, opts, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	client, err := bq.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery: new client: %w", err)
	}
	defer client.Close()

	return a.runQuery(ctx, client, sql)
}

// Query runs a SQL statement against an already-constructed client. Useful
// when a caller wants to reuse a client across multiple statements.
func (a *Adapter) Query(ctx context.Context, client *bq.Client, sql string) (*QueryResult, error) {
	return a.runQuery(ctx, client, sql)
}

// runQuery is a method on Adapter (not a free function) so it can
// read the per-request RLS context the planner set via SetRLSContext.
func (a *Adapter) runQuery(ctx context.Context, client *bq.Client, sql string) (*QueryResult, error) {
	q := client.Query(sql)
	// §12.1 RLS: stamp (org, user) as job labels so authorized views
	// and audit logs can filter by tenant + actor. BigQuery normalizes
	// label keys to lowercase; UUID values are already lowercase.
	orgID, userID := a.rlsSnapshot()
	if orgID != uuid.Nil || userID != uuid.Nil {
		labels := map[string]string{}
		if orgID != uuid.Nil {
			labels["app_current_org_id"] = orgID.String()
		}
		if userID != uuid.Nil {
			labels["app_current_user_id"] = userID.String()
		}
		q.Labels = labels
	}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("bigquery: run query: %w", err)
	}

	out := &QueryResult{Rows: [][]any{}}
	for {
		var row []bq.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("bigquery: iterate rows: %w", err)
		}
		// Populate columns from the schema on the first iteration — the
		// schema is guaranteed to be set after Next returns successfully.
		if len(out.Columns) == 0 && it.Schema != nil {
			for _, f := range it.Schema {
				out.Columns = append(out.Columns, f.Name)
			}
		}
		converted := make([]any, len(row))
		for i, v := range row {
			converted[i] = v
		}
		out.Rows = append(out.Rows, converted)
	}
	// Fall back to schema even if no rows were returned.
	if len(out.Columns) == 0 && it.Schema != nil {
		for _, f := range it.Schema {
			out.Columns = append(out.Columns, f.Name)
		}
	}
	return out, nil
}

// parseDSN accepts URLs of the form:
//
//	bigquery://<project_id>
//	bigquery://<project_id>?credentials_json_b64=<base64 service-account json>
//	bigquery://<project_id>?impersonation_email=<sa@proj.iam.gserviceaccount.com>
//
// If no credentials query param is present, falls back to environment-based
// Application Default Credentials (APP_GCP_SERVICE_ACCOUNT_JSON or the
// standard GOOGLE_APPLICATION_CREDENTIALS file path).
//
// When impersonation_email is present we add option.ImpersonateCredentials
// so the caller's base credentials mint a short-lived token for the
// per-org service account. Combined with BigQuery's dataset-level IAM
// this gives Google-side tenant isolation that the row-policy labels
// complement on the read path.
func parseDSN(dsn string) (string, []option.ClientOption, error) {
	if dsn == "" {
		return "", nil, fmt.Errorf("bigquery: empty DSN")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", nil, fmt.Errorf("bigquery: parse dsn: %w", err)
	}
	if u.Scheme != "bigquery" {
		return "", nil, fmt.Errorf("bigquery: DSN scheme must be `bigquery://`, got %q", u.Scheme)
	}
	project := u.Host
	if project == "" {
		// Allow `bigquery:///?credentials_json_b64=...&project=foo` style too.
		project = u.Query().Get("project")
	}
	if project == "" {
		return "", nil, fmt.Errorf("bigquery: project_id missing in DSN host")
	}

	var opts []option.ClientOption
	if b64 := u.Query().Get("credentials_json_b64"); b64 != "" {
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return "", nil, fmt.Errorf("bigquery: decode credentials: %w", err)
		}
		opts = append(opts, option.WithCredentialsJSON(raw))
	} else if envJSON := os.Getenv("APP_GCP_SERVICE_ACCOUNT_JSON"); envJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(envJSON)))
	}
	// If no opts, Google libraries will use ADC (GOOGLE_APPLICATION_CREDENTIALS
	// file path or metadata server), which is the right default on GCP.

	if impersonate := strings.TrimSpace(u.Query().Get("impersonation_email")); impersonate != "" {
		opts = append(opts, option.ImpersonateCredentials(impersonate))
	}
	return project, opts, nil
}
