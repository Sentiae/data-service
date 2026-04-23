package foundryservice

import (
	"context"
	"fmt"

	foundryv1 "github.com/sentiae/foundry-service/gen/proto/foundry/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// grpcDispatcher wraps foundry's gRPC FoundryService client. Used by
// data-service's NL-to-SQL flow (A7.2). HTTP Dispatch remains as a
// rollback fallback.
type grpcDispatcher struct {
	client foundryv1.FoundryServiceClient
}

func newGRPCDispatcher(conn *grpc.ClientConn) *grpcDispatcher {
	if conn == nil {
		return nil
	}
	return &grpcDispatcher{client: foundryv1.NewFoundryServiceClient(conn)}
}

// dispatch executes an operation via the foundry gRPC surface, returning
// the same DispatchResult shape the HTTP client produces so upstream
// callers don't have to branch on transport.
func (d *grpcDispatcher) dispatch(ctx context.Context, req DispatchRequest) (*DispatchResult, error) {
	if d == nil || d.client == nil {
		return nil, fmt.Errorf("foundry grpc dispatcher not configured")
	}
	params := req.Params
	if params == nil {
		params = map[string]any{}
	}
	pb, err := structpb.NewStruct(params)
	if err != nil {
		return nil, fmt.Errorf("params -> struct: %w", err)
	}
	resp, err := d.client.Dispatch(ctx, &foundryv1.DispatchRequest{
		Operation:      req.Operation,
		OrganizationId: req.OrganizationID,
		UserId:         req.UserID,
		Params:         pb,
	})
	if err != nil {
		return nil, fmt.Errorf("grpc FoundryService.Dispatch(%s): %w", req.Operation, err)
	}
	out := &DispatchResult{
		ID:         resp.Id,
		Operation:  resp.Operation,
		Status:     resp.Status,
		TokensUsed: int(resp.TokensUsed),
		ModelUsed:  resp.ModelUsed,
		Provider:   resp.Provider,
		DurationMS: resp.DurationMs,
	}
	if resp.Data != nil {
		out.Data = resp.Data.AsMap()
	}
	if resp.Status == "error" {
		return out, fmt.Errorf("foundry dispatch: %s", out.Error)
	}
	return out, nil
}
