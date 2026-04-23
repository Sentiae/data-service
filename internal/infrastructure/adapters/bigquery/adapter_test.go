package bigquery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

// TestParseDSN_Impersonation confirms the DSN `impersonation_email`
// query param produces an ImpersonateCredentials ClientOption. We can't
// introspect the option directly, but the call must succeed without
// error for a DSN that includes both credentials and an impersonation
// target.
func TestParseDSN_Impersonation(t *testing.T) {
	dsn := "bigquery://test-proj?impersonation_email=sa-tenant-a@proj.iam.gserviceaccount.com"
	proj, opts, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if proj != "test-proj" {
		t.Errorf("project = %q, want test-proj", proj)
	}
	// One option expected: the impersonation one.
	if len(opts) != 1 {
		t.Errorf("expected 1 ClientOption, got %d", len(opts))
	}
}

// TestParseDSN_RejectsWrongScheme keeps coverage on the existing shape.
func TestParseDSN_RejectsWrongScheme(t *testing.T) {
	cases := []string{"", "http://host", "postgres://x"}
	for _, dsn := range cases {
		if _, _, err := parseDSN(dsn); err == nil {
			t.Errorf("expected error for %q", dsn)
		}
	}
}

// TestAdapterRLS_AttachesJobLabels exercises §12.1 by pointing the
// BigQuery client at an httptest server that speaks enough of the
// BigQuery REST shape to accept a query insertion + return an empty
// result set. The fake server captures the JobConfigurationQuery
// payload and asserts that it carries the tenant labels.
//
// Because the BigQuery client normalises a POST
//   /projects/<proj>/queries
// into a jobs.query call with a JSON body containing labels, we only
// need to decode the body, record the labels, and return a synthetic
// job-completed response. This exercises the REAL HTTP shape — not a
// no-op mock.
func TestAdapterRLS_AttachesJobLabels(t *testing.T) {
	var mu sync.Mutex
	var gotLabels map[string]string
	var requests []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()

		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		// BigQuery client issues POST /projects/<proj>/queries for
		// jobs.query. The body is a QueryRequest JSON:
		//
		//   {"query":"SELECT 1","labels":{...},"useLegacySql":false,...}
		if strings.HasSuffix(r.URL.Path, "/queries") && r.Method == http.MethodPost {
			var req struct {
				Query  string            `json:"query"`
				Labels map[string]string `json:"labels"`
			}
			_ = json.Unmarshal(body, &req)
			mu.Lock()
			gotLabels = req.Labels
			mu.Unlock()

			// Minimal QueryResponse: complete=true, no rows.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"kind": "bigquery#queryResponse",
				"jobComplete": true,
				"schema": {"fields":[{"name":"n","type":"INT64","mode":"NULLABLE"}]},
				"rows": [],
				"totalRows": "0",
				"jobReference": {"projectId": "test-proj", "jobId": "job-1", "location":"US"}
			}`))
			return
		}

		// Anything else → empty 200 so the client doesn't blow up on
		// ancillary calls (getQueryResults, etc.). Job is already
		// complete above.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	orgID := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	userID := uuid.MustParse("00000000-0000-0000-0000-00000000000b")

	a := NewAdapter()
	a.SetRLSContext(orgID, userID)

	if _, err := a.Query(ctx, client, "SELECT 1"); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if gotLabels == nil {
		t.Fatalf("expected labels on the jobs.query payload, got none. Requests: %v", requests)
	}
	if gotLabels["app_current_org_id"] != orgID.String() {
		t.Errorf("app_current_org_id = %q, want %q", gotLabels["app_current_org_id"], orgID.String())
	}
	if gotLabels["app_current_user_id"] != userID.String() {
		t.Errorf("app_current_user_id = %q, want %q", gotLabels["app_current_user_id"], userID.String())
	}
}

// TestAdapterRLS_NoContext_NoLabels confirms we don't attach labels
// when the planner has not stamped an RLS context, matching the
// behavior for local dev and unit tests.
func TestAdapterRLS_NoContext_NoLabels(t *testing.T) {
	var mu sync.Mutex
	var gotLabels map[string]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		if strings.HasSuffix(r.URL.Path, "/queries") && r.Method == http.MethodPost {
			var req struct {
				Labels map[string]string `json:"labels"`
			}
			_ = json.Unmarshal(body, &req)
			mu.Lock()
			gotLabels = req.Labels
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"kind":"bigquery#queryResponse",
				"jobComplete":true,
				"schema":{"fields":[{"name":"n","type":"INT64","mode":"NULLABLE"}]},
				"rows":[],
				"totalRows":"0",
				"jobReference":{"projectId":"test-proj","jobId":"job-1","location":"US"}
			}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := bq.NewClient(ctx, "test-proj",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	a := NewAdapter()
	if _, err := a.Query(ctx, client, "SELECT 1"); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(gotLabels) != 0 {
		t.Errorf("expected no labels, got %v", gotLabels)
	}
}
