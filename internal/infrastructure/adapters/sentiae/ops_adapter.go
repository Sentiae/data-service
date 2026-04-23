package sentiae

import (
	"context"
	"fmt"

	"github.com/sentiae/data-service/internal/usecase"
)

// OpsAdapter queries ops-service for deployments, incidents, alerts, and
// service health. It implements usecase.SourceAdapter.
type OpsAdapter struct {
	BaseURL string // e.g. "http://ops-service:8083"
}

// NewOpsAdapter constructs an OpsAdapter pointed at baseURL.
func NewOpsAdapter(baseURL string) *OpsAdapter {
	return &OpsAdapter{BaseURL: baseURL}
}

// Name returns the planner-facing identifier for this adapter.
func (a *OpsAdapter) Name() string { return "sentiae_ops" }

// Execute maps the sub-query DSL to an ops-service endpoint. Supported
// resources:
//   - deployments → GET /api/v1/ops/deployments
//   - incidents   → GET /api/v1/ops/incidents
//   - alerts      → GET /api/v1/ops/alerts
//   - services    → GET /api/v1/ops/services
func (a *OpsAdapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	resource, params := parseQueryArgs(query)
	if resource == "" {
		resource = "deployments"
	}

	var endpoint string
	switch resource {
	case "deployments":
		endpoint = "/api/v1/ops/deployments"
	case "incidents":
		endpoint = "/api/v1/ops/incidents"
	case "alerts":
		endpoint = "/api/v1/ops/alerts"
	case "services":
		endpoint = "/api/v1/ops/services"
	default:
		return nil, fmt.Errorf("ops_adapter: unsupported resource %q", resource)
	}

	var envelope struct {
		Success bool             `json:"success"`
		Data    []map[string]any `json:"data"`
	}
	if err := doGET(ctx, a.BaseURL+endpoint, params, &envelope); err != nil {
		var direct []map[string]any
		if err2 := doGET(ctx, a.BaseURL+endpoint, params, &direct); err2 == nil {
			return resultFromRecords(direct), nil
		}
		return nil, err
	}
	return resultFromRecords(envelope.Data), nil
}
