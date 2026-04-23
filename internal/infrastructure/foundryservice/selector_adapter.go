package foundryservice

import (
	"context"

	"github.com/sentiae/data-service/internal/usecase"
)

// SelectorLLMAdapter adapts DispatchClient to the selector usecase's
// DataSourceSelectorLLM boundary. Kept in the infrastructure layer so
// the usecase package stays free of HTTP concerns.
type SelectorLLMAdapter struct {
	Client *DispatchClient
}

// NewSelectorLLMAdapter builds an adapter over the given client. A nil
// client is tolerated — the adapter returns a sentinel error so the
// selector falls back to keyword ranking instead of panicking.
func NewSelectorLLMAdapter(client *DispatchClient) *SelectorLLMAdapter {
	return &SelectorLLMAdapter{Client: client}
}

// RankDataSources implements usecase.DataSourceSelectorLLM.
func (a *SelectorLLMAdapter) RankDataSources(ctx context.Context, in usecase.RankDataSourcesInput) ([]usecase.DataSourceRank, error) {
	if a == nil || a.Client == nil {
		return nil, errNoClient
	}
	candidates := make([]map[string]any, 0, len(in.Candidates))
	for _, c := range in.Candidates {
		candidates = append(candidates, map[string]any{
			"id":          c.ID.String(),
			"name":        c.Name,
			"engine":      c.Engine,
			"description": c.Description,
			"schema":      c.Schema,
		})
	}
	out, err := a.Client.SelectDataSource(ctx, SelectDataSourceInput{
		Question:   in.Question,
		OrgID:      in.OrgID,
		UserID:     in.UserID,
		Candidates: candidates,
	})
	if err != nil {
		return nil, err
	}
	return usecase.ParseRanksJSON(out.RanksJSON)
}

// errNoClient is a static sentinel so callers can unambiguously detect
// the "adapter had no HTTP client" branch.
var errNoClient = &selectorError{msg: "foundry client is nil"}

type selectorError struct{ msg string }

func (e *selectorError) Error() string { return e.msg }
