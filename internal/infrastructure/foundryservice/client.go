// Package foundryservice provides a minimal HTTP client for invoking
// foundry-service's unified dispatch API. data-service uses it to reach
// the nl_to_sql operation (CS-5 G5.1) without embedding foundry's LLM
// router directly.
package foundryservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

// DispatchClient is a thin wrapper over foundry-service's
// POST /api/v1/foundry/dispatch endpoint.
//
// A7.2: when a gRPC channel is wired via WithGRPC the Dispatch call
// prefers FoundryService.Dispatch gRPC; HTTP stays as a rollback
// fallback.
type DispatchClient struct {
	baseURL string
	http    *http.Client
	// Token authenticates service-to-service calls; injected into the
	// Authorization header as a Bearer token when non-empty.
	Token string
	// OrgHeader / UserHeader override the X-Organization-ID /
	// X-User-ID headers foundry uses to derive caller identity.
	OrgHeader  string
	UserHeader string
	grpc       *grpcDispatcher
	preferGRPC bool
}

// NewClient returns a DispatchClient for the given base URL with a
// 30-second default timeout.
func NewClient(baseURL string) *DispatchClient {
	if baseURL == "" {
		baseURL = "http://localhost:8085"
	}
	return &DispatchClient{
		baseURL:    baseURL,
		http:       &http.Client{Timeout: 30 * time.Second},
		preferGRPC: preferGRPCFromEnv(),
	}
}

// WithGRPC attaches a foundry-service gRPC channel (A7.2). Nil conn is
// a no-op so callers can supply a best-effort dial result.
func (c *DispatchClient) WithGRPC(conn *grpc.ClientConn) *DispatchClient {
	if c == nil {
		return c
	}
	c.grpc = newGRPCDispatcher(conn)
	return c
}

// SetPreferGRPC toggles routing post-construction.
func (c *DispatchClient) SetPreferGRPC(v bool) {
	if c == nil {
		return
	}
	c.preferGRPC = v
}

// WithHTTPClient overrides the underlying *http.Client; returns the
// receiver for fluent chaining.
func (c *DispatchClient) WithHTTPClient(h *http.Client) *DispatchClient {
	if h != nil {
		c.http = h
	}
	return c
}

// DispatchRequest mirrors foundry's dispatch.DispatchRequest subset
// that data-service needs to populate.
type DispatchRequest struct {
	Operation      string         `json:"operation"`
	OrganizationID string         `json:"organization_id,omitempty"`
	UserID         string         `json:"user_id,omitempty"`
	Params         map[string]any `json:"params"`
}

// DispatchResult mirrors foundry's dispatch.DispatchResult.
type DispatchResult struct {
	ID         string         `json:"id"`
	Operation  string         `json:"operation"`
	Status     string         `json:"status"`
	Data       map[string]any `json:"data,omitempty"`
	Error      string         `json:"error,omitempty"`
	TokensUsed int            `json:"tokens_used,omitempty"`
	ModelUsed  string         `json:"model_used,omitempty"`
	Provider   string         `json:"provider,omitempty"`
	DurationMS int64          `json:"duration_ms,omitempty"`
}

// envelope matches the {success, data, error} wrapper foundry's HTTP
// layer applies on top of every dispatch result.
type envelope struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Dispatch invokes an operation and returns the unwrapped result. The
// error captures both transport-level failures and foundry-reported
// operation errors.
//
// A7.2: routes to gRPC FoundryService.Dispatch when preferGRPC is set
// and a channel has been wired via WithGRPC. HTTP below is DEPRECATED
// fallback.
func (c *DispatchClient) Dispatch(ctx context.Context, req DispatchRequest) (*DispatchResult, error) {
	if req.Operation == "" {
		return nil, fmt.Errorf("foundry dispatch: operation is required")
	}
	if c.preferGRPC && c.grpc != nil {
		if out, err := c.grpc.dispatch(ctx, req); err == nil {
			return out, nil
		}
	}
	return c.dispatchHTTP(ctx, req)
}

// dispatchHTTP is the DEPRECATED HTTP implementation preserved for A7.2
// rollback.
func (c *DispatchClient) dispatchHTTP(ctx context.Context, req DispatchRequest) (*DispatchResult, error) {
	if req.Operation == "" {
		return nil, fmt.Errorf("foundry dispatch: operation is required")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("foundry dispatch: marshal: %w", err)
	}
	url := c.baseURL + "/api/v1/foundry/dispatch"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("foundry dispatch: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if c.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.OrgHeader != "" {
		httpReq.Header.Set("X-Organization-ID", c.OrgHeader)
	} else if req.OrganizationID != "" {
		httpReq.Header.Set("X-Organization-ID", req.OrganizationID)
	}
	if c.UserHeader != "" {
		httpReq.Header.Set("X-User-ID", c.UserHeader)
	} else if req.UserID != "" {
		httpReq.Header.Set("X-User-ID", req.UserID)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("foundry dispatch: send: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("foundry dispatch: read body: %w", err)
	}
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("foundry dispatch: status %d: %s", resp.StatusCode, string(raw))
	}

	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("foundry dispatch: decode envelope: %w", err)
	}
	if !env.Success || env.Error != nil {
		msg := "operation failed"
		if env.Error != nil {
			msg = env.Error.Message
		}
		return nil, fmt.Errorf("foundry dispatch: %s", msg)
	}
	var result DispatchResult
	if len(env.Data) > 0 {
		if err := json.Unmarshal(env.Data, &result); err != nil {
			return nil, fmt.Errorf("foundry dispatch: decode result: %w", err)
		}
	}
	if result.Status == "error" {
		return &result, fmt.Errorf("foundry dispatch: %s", result.Error)
	}
	return &result, nil
}

// NLToSQLInput bundles the parameters required by the nl_to_sql
// operation. Kept separate so handlers don't have to remember the
// Params-map field names.
type NLToSQLInput struct {
	Question     string
	Schema       string
	Vocabulary   string
	DataSourceID string
	OrgID        string
	UserID       string
}

// NLToSQLOutput captures the decoded response from nl_to_sql.
type NLToSQLOutput struct {
	SQL         string
	Explanation string
	TokensUsed  int
	Model       string
	Provider    string
}

// NLToSQL is a typed helper for the nl_to_sql operation. It packages
// the inputs into a DispatchRequest, calls Dispatch, and extracts the
// `sql` + `explanation` fields from the result data.
func (c *DispatchClient) NLToSQL(ctx context.Context, in NLToSQLInput) (*NLToSQLOutput, error) {
	res, err := c.Dispatch(ctx, DispatchRequest{
		Operation:      "nl_to_sql",
		OrganizationID: in.OrgID,
		UserID:         in.UserID,
		Params: map[string]any{
			"question":       in.Question,
			"schema":         in.Schema,
			"vocabulary":     in.Vocabulary,
			"data_source_id": in.DataSourceID,
		},
	})
	if err != nil {
		return nil, err
	}
	out := &NLToSQLOutput{
		TokensUsed: res.TokensUsed,
		Model:      res.ModelUsed,
		Provider:   res.Provider,
	}
	if res.Data != nil {
		if v, ok := res.Data["sql"].(string); ok {
			out.SQL = v
		}
		if v, ok := res.Data["explanation"].(string); ok {
			out.Explanation = v
		}
	}
	return out, nil
}
