// Package foundryservice is a gRPC client for foundry-service's
// FoundryService.Dispatch RPC. data-service uses it to reach the
// nl_to_sql operation (CS-5 G5.1).
//
// Platform rule: service↔service = gRPC. The REST fallback was removed
// in Foxtrot — callers must attach a gRPC channel via WithGRPC.
package foundryservice

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

// DispatchClient is a thin wrapper over foundry-service's
// FoundryService.Dispatch gRPC RPC.
type DispatchClient struct {
	grpc    *grpcDispatcher
	timeout time.Duration

	// Token / OrgHeader / UserHeader are retained for API compatibility
	// with legacy call-sites; they're no-ops over gRPC.
	Token      string
	OrgHeader  string
	UserHeader string
}

// NewClient returns a DispatchClient. baseURL is ignored and retained
// for DI compatibility.
func NewClient(_ string) *DispatchClient {
	return &DispatchClient{timeout: 30 * time.Second}
}

// WithGRPC attaches a foundry-service gRPC channel.
func (c *DispatchClient) WithGRPC(conn *grpc.ClientConn) *DispatchClient {
	if c == nil {
		return c
	}
	c.grpc = newGRPCDispatcher(conn)
	return c
}

// SetPreferGRPC is a no-op; gRPC is the only transport.
func (c *DispatchClient) SetPreferGRPC(_ bool) {}

// WithHTTPClient is a no-op kept for API compatibility.
func (c *DispatchClient) WithHTTPClient(_ *http.Client) *DispatchClient { return c }

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

// NLToSQL wraps Dispatch(nl_to_sql).
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

// Dispatch invokes an operation and returns the unwrapped result.
func (c *DispatchClient) Dispatch(ctx context.Context, req DispatchRequest) (*DispatchResult, error) {
	if req.Operation == "" {
		return nil, fmt.Errorf("foundry dispatch: operation is required")
	}
	if c == nil || c.grpc == nil {
		return nil, fmt.Errorf("foundry dispatch client not configured")
	}
	if c.OrgHeader != "" && req.OrganizationID == "" {
		req.OrganizationID = c.OrgHeader
	}
	if c.UserHeader != "" && req.UserID == "" {
		req.UserID = c.UserHeader
	}
	return c.grpc.dispatch(ctx, req)
}
