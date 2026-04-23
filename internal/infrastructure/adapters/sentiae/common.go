// Package sentiae provides concrete SourceAdapter implementations for the
// federated-query planner. Each adapter targets a first-party Sentiae
// service (git, ops, canvas) and exposes its data as a tabular result that
// the planner can join against other sources.
//
// The adapters speak the service's HTTP API directly (no shared gRPC stubs)
// so they can evolve independently. Responses are normalized into
// FederatedQueryResult shapes so the planner can treat all sources
// uniformly.
package sentiae

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sentiae/data-service/internal/usecase"
)

// httpClient is the shared client used by every adapter. A 15s timeout keeps
// federated queries from hanging if one downstream service is slow.
var httpClient = &http.Client{Timeout: 15 * time.Second}

// doGET issues a GET request to url with optional query params and decodes
// the JSON body into out. The caller keeps responsibility for the result
// shape; this helper only concerns itself with transport errors.
func doGET(ctx context.Context, url string, params map[string]string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if len(params) > 0 {
		q := req.URL.Query()
		for k, v := range params {
			if v != "" {
				q.Set(k, v)
			}
		}
		req.URL.RawQuery = q.Encode()
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// parseQueryArgs parses a whitespace-separated `key=value` DSL used by the
// federated planner to shape sub-queries against Sentiae services. Example:
//
//	"resource=commits since=2026-04-01 author=rsiegel limit=50"
//
// Unknown keys are preserved in the result so adapters can forward them as
// query parameters.
func parseQueryArgs(raw string) (resource string, params map[string]string) {
	params = make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", params
	}
	for _, tok := range strings.Fields(raw) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			// Treat bare tokens as the resource selector.
			if resource == "" {
				resource = tok
			}
			continue
		}
		if k == "resource" {
			resource = v
			continue
		}
		params[k] = v
	}
	return resource, params
}

// resultFromRecords builds a FederatedQueryResult from a slice of maps,
// deriving the column list from the union of keys (stable order: keys from
// the first row first, then newly-seen keys in subsequent rows).
func resultFromRecords(records []map[string]any) *usecase.FederatedQueryResult {
	result := &usecase.FederatedQueryResult{
		Columns: []string{},
		Rows:    make([]map[string]any, 0, len(records)),
	}
	seen := make(map[string]bool)
	for _, rec := range records {
		for _, k := range sortedKeys(rec, seen) {
			if !seen[k] {
				result.Columns = append(result.Columns, k)
				seen[k] = true
			}
		}
		result.Rows = append(result.Rows, rec)
	}
	return result
}

// sortedKeys returns the keys of m that are not already present in seen, in
// their map-iteration order (Go randomizes this, but that is acceptable for
// column ordering within a single request).
func sortedKeys(m map[string]any, seen map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		if !seen[k] {
			out = append(out, k)
		}
	}
	return out
}
