// Package graphqlsrc provides a SourceAdapter that executes GraphQL
// queries against a GraphQL endpoint. §12.4 — closes the
// "REST/GraphQL adapters declared but not implemented" audit gap.
//
// Query DSL:
//
//	<endpoint> :: <graphql_query>
//
// Example:
//
//	https://api.example.com/graphql :: query { users { id name } }
//
// The response's `data` field is unfolded into rows:
//
//   - If `data` has exactly one field that holds a list of objects,
//     each element becomes a row (the natural case for top-level
//     query fields like `users`, `orders`, etc.).
//   - Otherwise `data` itself becomes a single row.
//
// Variables can be embedded in the query text as needed; the MVP
// does not carry a separate variables map to keep the query DSL
// single-string.
package graphqlsrc

import (
	"bytes"
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

// OrgHeaderName is the row-level-security header stamped on every
// outbound call when the planner has an org context. §12.1 (C9).
const OrgHeaderName = "X-Sentiae-Org-Id"

// Adapter implements usecase.SourceAdapter for GraphQL sources.
type Adapter struct {
	client         *http.Client
	defaultHeaders http.Header

	orgMu sync.RWMutex
	orgID uuid.UUID
}

// NewAdapter returns a GraphQL adapter with a 15-second default
// request timeout.
func NewAdapter() *Adapter {
	return &Adapter{
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// WithHTTPClient replaces the adapter's http.Client.
func (a *Adapter) WithHTTPClient(c *http.Client) *Adapter {
	a.client = c
	return a
}

// WithDefaultHeader appends a header applied to every request
// (typically Authorization).
func (a *Adapter) WithDefaultHeader(name, value string) *Adapter {
	if a.defaultHeaders == nil {
		a.defaultHeaders = http.Header{}
	}
	a.defaultHeaders.Add(name, value)
	return a
}

// SetOrgContext satisfies usecase.OrgContextSetter so the federated
// planner can propagate the requesting org on every outbound call.
// §12.1 (C9).
func (a *Adapter) SetOrgContext(orgID uuid.UUID) {
	a.orgMu.Lock()
	a.orgID = orgID
	a.orgMu.Unlock()
}

func (a *Adapter) currentOrgHeader() string {
	a.orgMu.RLock()
	defer a.orgMu.RUnlock()
	if a.orgID == uuid.Nil {
		return ""
	}
	return a.orgID.String()
}

// Name returns the planner-facing identifier.
func (a *Adapter) Name() string { return "graphql" }

// Execute parses the query DSL, POSTs the GraphQL body, and returns
// the decoded `data` field as rows.
func (a *Adapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	endpoint, gql, ok := strings.Cut(query, "::")
	if !ok {
		return nil, fmt.Errorf("graphql adapter: expected query in form `<endpoint> :: <graphql>`")
	}
	endpoint = strings.TrimSpace(endpoint)
	gql = strings.TrimSpace(gql)

	bodyMap := map[string]any{"query": gql}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if a.defaultHeaders != nil {
		for k, vs := range a.defaultHeaders {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}
	if org := a.currentOrgHeader(); org != "" {
		req.Header.Set(OrgHeaderName, org)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("graphql status %d: %s", resp.StatusCode, string(respBody))
	}

	var envelope struct {
		Data   map[string]any   `json:"data"`
		Errors []map[string]any `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return nil, fmt.Errorf("decode graphql envelope: %w", err)
	}
	if len(envelope.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", envelope.Errors)
	}

	rows := unfoldData(envelope.Data)
	return &usecase.FederatedQueryResult{
		Columns: columnUnion(rows),
		Rows:    rows,
		Sources: []string{"graphql"},
	}, nil
}

// unfoldData picks the most natural row shape from the `data`
// payload. When data has exactly one field holding a list, that
// list becomes the rows; otherwise data is a single row.
func unfoldData(data map[string]any) []map[string]any {
	if len(data) == 1 {
		for _, v := range data {
			if arr, ok := v.([]any); ok {
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
		}
	}
	return []map[string]any{data}
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
