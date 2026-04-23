package foundryservice

import (
	"context"
	"encoding/json"
	"fmt"
)

// SelectDataSourceInput bundles the parameters for the
// `select_data_source` dispatch op. Candidates is the JSON-marshalled
// list of {id, name, engine, schema} records per registered data source
// in the organization.
type SelectDataSourceInput struct {
	Question   string           `json:"question"`
	Candidates []map[string]any `json:"candidates"`
	OrgID      string           `json:"-"`
	UserID     string           `json:"-"`
}

// SelectDataSourceOutput is the raw JSON payload returned by foundry.
// Callers decode it via usecase.ParseRanksJSON.
type SelectDataSourceOutput struct {
	RanksJSON  string
	TokensUsed int
	Model      string
	Provider   string
}

// SelectDataSource calls foundry-service's `select_data_source` op to
// rank candidate data sources by relevance to the question. The op is
// expected to return `{ ranks_json: "[{...}, ...]" }` or a structured
// `ranks` array. This helper normalises the two shapes into one raw
// JSON string the selector usecase already knows how to parse.
func (c *DispatchClient) SelectDataSource(ctx context.Context, in SelectDataSourceInput) (*SelectDataSourceOutput, error) {
	res, err := c.Dispatch(ctx, DispatchRequest{
		Operation:      "select_data_source",
		OrganizationID: in.OrgID,
		UserID:         in.UserID,
		Params: map[string]any{
			"question":   in.Question,
			"candidates": in.Candidates,
		},
	})
	if err != nil {
		return nil, err
	}
	out := &SelectDataSourceOutput{
		TokensUsed: res.TokensUsed,
		Model:      res.ModelUsed,
		Provider:   res.Provider,
	}
	if res.Data == nil {
		return nil, fmt.Errorf("select_data_source: empty response")
	}
	if v, ok := res.Data["ranks_json"].(string); ok && v != "" {
		out.RanksJSON = v
		return out, nil
	}
	if v, ok := res.Data["ranks"]; ok {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("select_data_source: re-marshal ranks: %w", err)
		}
		out.RanksJSON = string(b)
		return out, nil
	}
	return nil, fmt.Errorf("select_data_source: missing ranks in response")
}
