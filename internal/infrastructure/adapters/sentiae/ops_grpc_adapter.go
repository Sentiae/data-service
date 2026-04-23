package sentiae

import (
	"context"
	"fmt"
	"os"
	"strconv"

	opsv1 "github.com/sentiae/ops-service/gen/ops/v1"
	"github.com/sentiae/data-service/internal/usecase"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// OpsGRPCAdapter is the A7.2 gRPC companion to OpsAdapter. When a
// gRPC channel is configured, this adapter handles the `deployments`
// resource via OpsDeploymentServiceClient and the architecture-related
// resources (`architecture_map`, `blast_radius`) via
// OpsArchitectureServiceClient.
//
// For resources without a matching gRPC surface (incidents, alerts,
// services) the adapter signals "not supported" via ErrResourceNotGRPC
// so the HTTP OpsAdapter path can take over.
type OpsGRPCAdapter struct {
	deploymentClient   opsv1.OpsDeploymentServiceClient
	architectureClient opsv1.OpsArchitectureServiceClient
}

// ErrResourceNotGRPC is returned when the requested resource has no
// gRPC counterpart yet. Callers should fall back to the HTTP adapter.
var ErrResourceNotGRPC = fmt.Errorf("ops_grpc_adapter: resource not available via gRPC")

// NewOpsGRPCAdapter wraps an already-dialed gRPC channel. nil conn
// yields nil so callers can do a ready-check.
func NewOpsGRPCAdapter(conn *grpc.ClientConn) *OpsGRPCAdapter {
	if conn == nil {
		return nil
	}
	return &OpsGRPCAdapter{
		deploymentClient:   opsv1.NewOpsDeploymentServiceClient(conn),
		architectureClient: opsv1.NewOpsArchitectureServiceClient(conn),
	}
}

// Name is the planner-facing identifier.
func (a *OpsGRPCAdapter) Name() string { return "sentiae_ops_grpc" }

// Execute maps the sub-query DSL onto gRPC calls. A7.2-covered
// resources:
//   - deployments      → OpsDeploymentService.ListDeployments
//   - architecture_map → OpsArchitectureService.GetArchitectureMap
//   - blast_radius     → OpsArchitectureService.GetArchitectureBlastRadius
//
// Other resources return ErrResourceNotGRPC.
//
// TODO(A7.3): add ListIncidents / ListAlerts / ListServices when
// ops-service ships the matching proto surfaces.
func (a *OpsGRPCAdapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	if a == nil {
		return nil, fmt.Errorf("ops_grpc_adapter: not initialized")
	}
	resource, params := parseQueryArgs(query)
	if resource == "" {
		resource = "deployments"
	}

	switch resource {
	case "deployments":
		return a.listDeployments(ctx, params)
	case "architecture_map":
		return a.getArchitectureMap(ctx, params)
	case "blast_radius":
		return a.getBlastRadius(ctx, params)
	default:
		return nil, fmt.Errorf("%w: %s", ErrResourceNotGRPC, resource)
	}
}

func (a *OpsGRPCAdapter) listDeployments(ctx context.Context, params map[string]string) (*usecase.FederatedQueryResult, error) {
	req := &opsv1.ListDeploymentsRequest{
		OrganizationId: params["organization_id"],
	}
	if envID := params["environment_id"]; envID != "" {
		req.EnvironmentId = envID
	}
	if limit := params["page_size"]; limit != "" {
		if v, err := strconv.Atoi(limit); err == nil && v > 0 {
			req.Page = &opsv1.PageRequest{PageSize: int32(v)}
		}
	}
	resp, err := a.deploymentClient.ListDeployments(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc OpsDeploymentService.ListDeployments: %w", err)
	}
	records := make([]map[string]any, 0, len(resp.Items))
	for _, d := range resp.Items {
		rec := map[string]any{
			"id":             d.Id,
			"service_id":     d.ServiceId,
			"environment_id": d.EnvironmentId,
			"status":         d.Status,
			"version":        d.Version,
			"strategy":       d.Strategy,
			"commit_sha":     d.CommitSha,
		}
		if d.CreatedAt != nil {
			rec["created_at"] = d.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00")
		}
		if d.CompletedAt != nil {
			rec["completed_at"] = d.CompletedAt.AsTime().Format("2006-01-02T15:04:05Z07:00")
		}
		records = append(records, rec)
	}
	return resultFromRecords(records), nil
}

func (a *OpsGRPCAdapter) getArchitectureMap(ctx context.Context, params map[string]string) (*usecase.FederatedQueryResult, error) {
	req := &opsv1.GetArchitectureMapRequest{
		OrganizationId: params["organization_id"],
		FeatureId:      params["feature_id"],
		TeamId:         params["team_id"],
		Environment:    params["environment"],
	}
	resp, err := a.architectureClient.GetArchitectureMap(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc OpsArchitectureService.GetArchitectureMap: %w", err)
	}
	m := map[string]any{}
	if resp.Map != nil {
		m = resp.Map.AsMap()
	}
	return resultFromRecords([]map[string]any{m}), nil
}

func (a *OpsGRPCAdapter) getBlastRadius(ctx context.Context, params map[string]string) (*usecase.FederatedQueryResult, error) {
	req := &opsv1.GetArchitectureBlastRadiusRequest{
		OrganizationId: params["organization_id"],
		NodeId:         params["node_id"],
		Direction:      params["direction"],
	}
	if depth := params["max_depth"]; depth != "" {
		if v, err := strconv.Atoi(depth); err == nil && v > 0 {
			req.MaxDepth = int32(v)
		}
	}
	resp, err := a.architectureClient.GetArchitectureBlastRadius(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc OpsArchitectureService.GetArchitectureBlastRadius: %w", err)
	}
	nodes := make([]map[string]any, 0, len(resp.Nodes))
	for _, n := range resp.Nodes {
		node := map[string]any{
			"id":    n.Id,
			"name":  n.Name,
			"kind":  n.Kind,
			"depth": n.Depth,
		}
		if n.Metadata != nil {
			node["metadata"] = n.Metadata.AsMap()
		}
		nodes = append(nodes, node)
	}
	return resultFromRecords([]map[string]any{{
		"root":     resp.Root,
		"severity": resp.Severity,
		"nodes":    nodes,
	}}), nil
}

// OpsAdapterWithGRPC combines a gRPC-preferred adapter with the HTTP
// fallback, implementing usecase.SourceAdapter. Callers wire this
// instead of OpsAdapter when the ops-service gRPC address is known.
type OpsAdapterWithGRPC struct {
	grpc       *OpsGRPCAdapter
	http       *OpsAdapter
	preferGRPC bool
}

// NewOpsAdapterWithGRPC wires the gRPC-preferred variant. conn may be
// nil — in that case the wrapper behaves exactly like OpsAdapter.
func NewOpsAdapterWithGRPC(baseURL string, conn *grpc.ClientConn) *OpsAdapterWithGRPC {
	return &OpsAdapterWithGRPC{
		grpc:       NewOpsGRPCAdapter(conn),
		http:       NewOpsAdapter(baseURL),
		preferGRPC: preferOpsGRPCFromEnv(),
	}
}

// Name returns the planner-facing identifier.
func (a *OpsAdapterWithGRPC) Name() string { return "sentiae_ops" }

// Execute prefers gRPC when the adapter is wired; falls back to HTTP on
// ErrResourceNotGRPC or any transport failure.
func (a *OpsAdapterWithGRPC) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	if a.preferGRPC && a.grpc != nil {
		out, err := a.grpc.Execute(ctx, query)
		if err == nil {
			return out, nil
		}
		// fall through to HTTP for missing-gRPC or transient failures
	}
	return a.http.Execute(ctx, query)
}

func preferOpsGRPCFromEnv() bool {
	v := os.Getenv("APP_PREFER_GRPC")
	if v == "" {
		return true
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return true
	}
	return b
}

// Keep structpb import referenced for future extensions.
var _ = structpb.NewStruct
