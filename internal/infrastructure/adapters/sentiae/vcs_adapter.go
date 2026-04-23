package sentiae

import (
	"context"
	"fmt"

	"github.com/sentiae/data-service/internal/usecase"
)

// VCSAdapter queries git-service for commits, pull requests, and debug
// sessions. It implements usecase.SourceAdapter so the federated planner can
// register it under the "sentiae_vcs" (or user-chosen) source name.
type VCSAdapter struct {
	BaseURL string // e.g. "http://git-service:8087"
}

// NewVCSAdapter constructs a VCSAdapter pointed at baseURL.
func NewVCSAdapter(baseURL string) *VCSAdapter {
	return &VCSAdapter{BaseURL: baseURL}
}

// Name returns the planner-facing identifier for this adapter.
func (a *VCSAdapter) Name() string { return "sentiae_vcs" }

// Execute parses the sub-query DSL (see parseQueryArgs) and fans out to the
// appropriate git-service endpoint. Supported resources:
//   - commits     → GET /api/v1/git/commits
//   - pulls       → GET /api/v1/git/pull-requests
//   - sessions    → GET /api/v1/git/debug-sessions
func (a *VCSAdapter) Execute(ctx context.Context, query string) (*usecase.FederatedQueryResult, error) {
	resource, params := parseQueryArgs(query)
	if resource == "" {
		resource = "commits"
	}

	var endpoint string
	switch resource {
	case "commits":
		endpoint = "/api/v1/git/commits"
	case "pulls", "pull_requests", "prs":
		endpoint = "/api/v1/git/pull-requests"
	case "sessions", "debug_sessions":
		endpoint = "/api/v1/git/debug-sessions"
	default:
		return nil, fmt.Errorf("vcs_adapter: unsupported resource %q", resource)
	}

	var envelope struct {
		Success bool             `json:"success"`
		Data    []map[string]any `json:"data"`
	}
	if err := doGET(ctx, a.BaseURL+endpoint, params, &envelope); err != nil {
		// Some git-service endpoints return the array directly.
		var direct []map[string]any
		if err2 := doGET(ctx, a.BaseURL+endpoint, params, &direct); err2 == nil {
			return resultFromRecords(direct), nil
		}
		return nil, err
	}
	return resultFromRecords(envelope.Data), nil
}
