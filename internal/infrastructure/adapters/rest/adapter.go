// Package rest provides a SourceAdapter that executes simple HTTP
// GET queries against a REST endpoint and returns the body as
// tabular rows. §12.4 — closes the "REST/GraphQL adapters declared
// but not implemented" audit gap.
//
// Query DSL:
//
//	<base_url> :: <method> <path_with_query>
//
// Example:
//
//	https://api.example.com :: GET /orders?status=paid
//
// The adapter performs the HTTP request, decodes the JSON response,
// and unfolds it into a FederatedQueryResult:
//
//   - If the response is a JSON array, each element becomes a row.
//   - If the response is a JSON object with a `data` or `items`
//     field that holds an array, those elements become rows.
//   - If the response is a single JSON object, one row is emitted.
//
// Column set is the union of keys present across all rows.
package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/usecase"
)

// OrgHeaderName is the row-level-security header injected on every
// outbound call when the planner sets an org context. Mirrors the
// convention used by git-service / ops-service. §12.1 (C9).
const OrgHeaderName = "X-Sentiae-Org-Id"

// Adapter implements usecase.SourceAdapter for REST sources.
type Adapter struct {
	client *http.Client
	// Optional default headers applied to every request. The query
	// DSL can override via a `Header: value` prefix — kept simple
	// for MVP.
	defaultHeaders http.Header

	// orgMu guards orgID so concurrent planner dispatches don't race
	// on header injection. The planner sets the org before Execute
	// and reads it inside; we pin both with a single mutex.
	orgMu sync.RWMutex
	orgID uuid.UUID
}

// NewAdapter returns a REST adapter with a 15-second default
// request timeout. Callers can swap the http.Client via
// WithHTTPClient for custom transports.
func NewAdapter() *Adapter {
	return &Adapter{
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// WithHTTPClient replaces the adapter's http.Client. Returns the
// adapter for fluent chaining.
func (a *Adapter) WithHTTPClient(c *http.Client) *Adapter {
	a.client = c
	return a
}

// WithDefaultHeader appends a header applied to every request.
func (a *Adapter) WithDefaultHeader(name, value string) *Adapter {
	if a.defaultHeaders == nil {
		a.defaultHeaders = http.Header{}
	}
	a.defaultHeaders.Add(name, value)
	return a
}

// SetOrgContext satisfies usecase.OrgContextSetter so the federated
// planner can propagate the requesting org down into every outbound
// REST call. Passed through as X-Sentiae-Org-Id. §12.1 (C9).
func (a *Adapter) SetOrgContext(orgID uuid.UUID) {
	a.orgMu.Lock()
	a.orgID = orgID
	a.orgMu.Unlock()
}

// currentOrgHeader returns the active org ID as a header value, or
// "" when none is set.
func (a *Adapter) currentOrgHeader() string {
	a.orgMu.RLock()
	defer a.orgMu.RUnlock()
	if a.orgID == uuid.Nil {
		return ""
	}
	return a.orgID.String()
}

// Name returns the planner-facing identifier.
func (a *Adapter) Name() string { return "rest" }

// Execute parses the query DSL, performs the HTTP request, and
// returns the decoded body as rows/columns.
func (a *Adapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	base, rest, ok := strings.Cut(query, "::")
	if !ok {
		return nil, fmt.Errorf("rest adapter: expected query in form `<base_url> :: <method> <path>`")
	}
	base = strings.TrimSpace(base)
	rest = strings.TrimSpace(rest)
	parts := strings.SplitN(rest, " ", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("rest adapter: expected `METHOD PATH` after `::`")
	}
	method := strings.ToUpper(strings.TrimSpace(parts[0]))
	path := strings.TrimSpace(parts[1])
	url := strings.TrimRight(base, "/") + path

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if a.defaultHeaders != nil {
		for k, vs := range a.defaultHeaders {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}
	req.Header.Set("Accept", "application/json")
	if org := a.currentOrgHeader(); org != "" {
		req.Header.Set(OrgHeaderName, org)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rest request failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("rest status %d: %s", resp.StatusCode, string(body))
	}

	rows, err := decodeRows(body)
	if err != nil {
		return nil, err
	}
	return &usecase.FederatedQueryResult{
		Columns: columnUnion(rows),
		Rows:    rows,
		Sources: []string{"rest"},
	}, nil
}

// decodeRows interprets the response body as tabular data. The
// strategies are ordered by generality: array of objects is the
// happy path; object-with-data-array is the common REST envelope;
// single object is a one-row fallback; anything else becomes a
// single {"value": ...} row so the caller at least sees the payload.
func decodeRows(body []byte) ([]map[string]any, error) {
	var anyDoc any
	if err := json.Unmarshal(body, &anyDoc); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	switch doc := anyDoc.(type) {
	case []any:
		return objectArrayToRows(doc), nil
	case map[string]any:
		for _, key := range []string{"data", "items", "results", "rows"} {
			if v, ok := doc[key]; ok {
				if arr, ok := v.([]any); ok {
					return objectArrayToRows(arr), nil
				}
			}
		}
		return []map[string]any{doc}, nil
	default:
		return []map[string]any{{"value": doc}}, nil
	}
}

func objectArrayToRows(arr []any) []map[string]any {
	rows := make([]map[string]any, 0, len(arr))
	for _, el := range arr {
		if obj, ok := el.(map[string]any); ok {
			rows = append(rows, obj)
			continue
		}
		rows = append(rows, map[string]any{"value": el})
	}
	return rows
}

func columnUnion(rows []map[string]any) []string {
	seen := map[string]struct{}{}
	order := []string{}
	for _, r := range rows {
		for k := range r {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			order = append(order, k)
		}
	}
	return order
}
