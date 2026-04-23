package sentiae

import (
	"context"
	"fmt"

	"github.com/sentiae/data-service/internal/usecase"
)

// CanvasAdapter queries canvas-service for canvases, nodes, and node
// executions. It implements usecase.SourceAdapter.
type CanvasAdapter struct {
	BaseURL string // e.g. "http://canvas-service:8084"
}

// NewCanvasAdapter constructs a CanvasAdapter pointed at baseURL.
func NewCanvasAdapter(baseURL string) *CanvasAdapter {
	return &CanvasAdapter{BaseURL: baseURL}
}

// Name returns the planner-facing identifier for this adapter.
func (a *CanvasAdapter) Name() string { return "sentiae_canvas" }

// Execute resolves the sub-query DSL to a canvas-service endpoint. Supported
// resources:
//   - canvases   → GET /api/v1/canvas/canvases
//   - nodes      → GET /api/v1/canvas/nodes
//   - executions → GET /api/v1/canvas/executions
func (a *CanvasAdapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	resource, params := parseQueryArgs(query)
	if resource == "" {
		resource = "canvases"
	}

	var endpoint string
	switch resource {
	case "canvases":
		endpoint = "/api/v1/canvas/canvases"
	case "nodes":
		endpoint = "/api/v1/canvas/nodes"
	case "executions":
		endpoint = "/api/v1/canvas/executions"
	default:
		return nil, fmt.Errorf("canvas_adapter: unsupported resource %q", resource)
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
