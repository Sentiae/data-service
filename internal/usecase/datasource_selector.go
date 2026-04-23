// §12.3 Natural Language Query datasource auto-selector.
//
// When a user asks a question without pinning a data source, the NL
// handler consults this selector to rank the org's registered data
// sources by relevance to the question. The selector assembles a
// compact schema summary per source (table + column names + business
// annotations if any) and asks foundry-service to rank them via the
// `select_data_source` dispatch op. A keyword-fallback implementation
// keeps unit tests offline and protects the UI when the LLM path is
// temporarily unavailable.
package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

// DataSourceRank is a single scored candidate returned by the selector.
// Confidence is in [0,1]. Higher is better. Reasoning is a short string
// the UI shows when asking the user to disambiguate.
type DataSourceRank struct {
	DataSourceID uuid.UUID `json:"data_source_id"`
	Name         string    `json:"name"`
	Engine       string    `json:"engine"`
	Confidence   float64   `json:"confidence"`
	Reasoning    string    `json:"reasoning,omitempty"`
}

// DataSourceSelectorLLM is the boundary the selector uses to reach
// foundry-service. It is deliberately narrow — tests swap in a fake;
// in production data-service wires the foundryservice.DispatchClient.
type DataSourceSelectorLLM interface {
	RankDataSources(ctx context.Context, in RankDataSourcesInput) ([]DataSourceRank, error)
}

// RankDataSourcesInput bundles the parameters the LLM helper needs.
type RankDataSourcesInput struct {
	Question       string
	OrgID          string
	UserID         string
	Candidates     []DataSourceSchemaSummary
}

// DataSourceSchemaSummary is the compact schema description we feed
// the LLM per candidate. Kept small on purpose — a few kilobytes per
// selector call keeps latency and cost low.
type DataSourceSchemaSummary struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Engine      string    `json:"engine"`
	Description string    `json:"description,omitempty"`
	Schema      string    `json:"schema"`
}

// DataSourceSelectorUseCase is the exported entry point.
type DataSourceSelectorUseCase struct {
	db  *gorm.DB
	llm DataSourceSelectorLLM
}

// NewDataSourceSelector builds a selector against the given DB. llm
// may be nil — in that case every call falls back to the keyword
// matcher, which is correct but low-confidence.
func NewDataSourceSelector(db *gorm.DB, llm DataSourceSelectorLLM) *DataSourceSelectorUseCase {
	return &DataSourceSelectorUseCase{db: db, llm: llm}
}

// SetLLM swaps the LLM boundary. Tests use this to stage responses.
func (u *DataSourceSelectorUseCase) SetLLM(llm DataSourceSelectorLLM) {
	u.llm = llm
}

// SelectForQuestion returns the data sources for the given org ranked
// by relevance to `question`. Callers that pass an empty question get
// an error — the selector refuses to rank without grounding text.
func (u *DataSourceSelectorUseCase) SelectForQuestion(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, question string) ([]DataSourceRank, error) {
	if u.db == nil {
		return nil, errors.New("datasource selector: db is nil")
	}
	if strings.TrimSpace(question) == "" {
		return nil, errors.New("datasource selector: question is required")
	}

	var sources []domain.DataSource
	if err := u.db.WithContext(ctx).
		Where("organization_id = ?", orgID).
		Order("created_at DESC").
		Find(&sources).Error; err != nil {
		return nil, fmt.Errorf("datasource selector: list sources: %w", err)
	}
	if len(sources) == 0 {
		return nil, nil
	}

	summaries := make([]DataSourceSchemaSummary, 0, len(sources))
	for _, ds := range sources {
		var fields []domain.SemanticField
		u.db.WithContext(ctx).
			Where("data_source_id = ?", ds.ID).
			Limit(80).
			Find(&fields)
		summaries = append(summaries, DataSourceSchemaSummary{
			ID:          ds.ID,
			Name:        ds.Name,
			Engine:      string(ds.Engine),
			Description: ds.Description,
			Schema:      buildSchemaContextForSelector(ds, fields),
		})
	}

	// Try LLM first; on any failure fall back to keyword matching so
	// the handler can still make progress.
	if u.llm != nil {
		ranks, err := u.llm.RankDataSources(ctx, RankDataSourcesInput{
			Question:   question,
			OrgID:      orgID.String(),
			UserID:     userID.String(),
			Candidates: summaries,
		})
		if err == nil && len(ranks) > 0 {
			return normalizeRanks(ranks, summaries), nil
		}
	}
	return keywordRankDataSources(question, summaries), nil
}

// buildSchemaContextForSelector is a compact schema renderer: table names,
// column names, optional business names. Mirrors the shape the nl_to_sql
// path uses but keeps it shorter so we can afford to send schemas for
// *every* candidate in a single prompt.
func buildSchemaContextForSelector(ds domain.DataSource, fields []domain.SemanticField) string {
	var b strings.Builder
	if len(fields) > 0 {
		perTable := map[string][]domain.SemanticField{}
		var order []string
		for _, f := range fields {
			if _, ok := perTable[f.TableName]; !ok {
				order = append(order, f.TableName)
			}
			perTable[f.TableName] = append(perTable[f.TableName], f)
		}
		for _, t := range order {
			b.WriteString(t)
			b.WriteString("(")
			for i, f := range perTable[t] {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(f.ColumnName)
				if f.BusinessName != "" && f.BusinessName != f.ColumnName {
					b.WriteString("=")
					b.WriteString(f.BusinessName)
				}
			}
			b.WriteString(")\n")
		}
		return b.String()
	}
	if len(ds.Tables) > 0 {
		for i, t := range ds.Tables {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(t)
		}
	}
	return b.String()
}

// normalizeRanks sorts by descending confidence, clamps to [0,1], and
// drops entries whose DataSourceID is not in the candidate list (an
// LLM may hallucinate ids).
func normalizeRanks(ranks []DataSourceRank, candidates []DataSourceSchemaSummary) []DataSourceRank {
	allowed := map[uuid.UUID]DataSourceSchemaSummary{}
	for _, c := range candidates {
		allowed[c.ID] = c
	}
	out := make([]DataSourceRank, 0, len(ranks))
	for _, r := range ranks {
		c, ok := allowed[r.DataSourceID]
		if !ok {
			continue
		}
		if r.Confidence < 0 {
			r.Confidence = 0
		}
		if r.Confidence > 1 {
			r.Confidence = 1
		}
		if r.Name == "" {
			r.Name = c.Name
		}
		if r.Engine == "" {
			r.Engine = c.Engine
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Confidence > out[j].Confidence
	})
	return out
}

// keywordRankDataSources scores each candidate by counting overlap
// between question tokens and tokens in the candidate's name +
// description + compact schema. This is deterministic, offline, and
// good enough to unblock tests and degraded-LLM situations.
func keywordRankDataSources(question string, candidates []DataSourceSchemaSummary) []DataSourceRank {
	qtoks := tokensForSelector(question)
	qset := map[string]struct{}{}
	for _, t := range qtoks {
		qset[t] = struct{}{}
	}
	out := make([]DataSourceRank, 0, len(candidates))
	for _, c := range candidates {
		haystack := strings.ToLower(c.Name + " " + c.Description + " " + c.Schema)
		htoks := tokensForSelector(haystack)
		hits := 0
		seenHit := map[string]bool{}
		for _, t := range htoks {
			if _, ok := qset[t]; ok && !seenHit[t] {
				hits++
				seenHit[t] = true
			}
		}
		denom := len(qset)
		if denom == 0 {
			denom = 1
		}
		conf := float64(hits) / float64(denom)
		// Keep low-overlap candidates visible but clearly below the
		// auto-pick threshold so the handler still prompts the user.
		if conf == 0 {
			conf = 0.05
		}
		reason := fmt.Sprintf("keyword overlap: %d/%d tokens matched", hits, denom)
		out = append(out, DataSourceRank{
			DataSourceID: c.ID,
			Name:         c.Name,
			Engine:       c.Engine,
			Confidence:   conf,
			Reasoning:    reason,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Confidence > out[j].Confidence
	})
	return out
}

// tokensForSelector lower-cases, splits on non-alnum, drops very short
// tokens and a small stoplist. We keep this tight on purpose — richer
// stemming belongs behind a proper text search index, not in the
// selector fallback.
func tokensForSelector(s string) []string {
	s = strings.ToLower(s)
	var cur []rune
	var out []string
	flush := func() {
		if len(cur) >= 3 {
			out = append(out, string(cur))
		}
		cur = cur[:0]
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			cur = append(cur, r)
		} else {
			flush()
		}
	}
	flush()
	return filterStop(out)
}

var selectorStopwords = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "how": {}, "are": {},
	"was": {}, "what": {}, "when": {}, "where": {}, "which": {},
	"from": {}, "with": {}, "this": {}, "that": {}, "have": {},
	"has": {}, "did": {}, "get": {}, "all": {}, "any": {},
	"our": {}, "out": {}, "top": {}, "per": {},
}

func filterStop(toks []string) []string {
	out := toks[:0]
	for _, t := range toks {
		if _, bad := selectorStopwords[t]; bad {
			continue
		}
		out = append(out, t)
	}
	return out
}

// ParseRanksJSON is a helper tests + the real LLM client share for
// turning an LLM's raw JSON array into typed DataSourceRank entries.
// The LLM is instructed to return `[{data_source_id, confidence,
// reasoning}...]`; we tolerate either `data_source_id` or `id` for
// flexibility.
func ParseRanksJSON(raw string) ([]DataSourceRank, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("empty rank payload")
	}
	// Strip common code-fence wrappers an LLM may add.
	if strings.HasPrefix(raw, "```") {
		raw = strings.TrimPrefix(raw, "```json")
		raw = strings.TrimPrefix(raw, "```")
		raw = strings.TrimSuffix(raw, "```")
		raw = strings.TrimSpace(raw)
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, fmt.Errorf("decode ranks: %w", err)
	}
	out := make([]DataSourceRank, 0, len(arr))
	for _, m := range arr {
		id, _ := m["data_source_id"].(string)
		if id == "" {
			id, _ = m["id"].(string)
		}
		if id == "" {
			continue
		}
		parsed, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		var conf float64
		switch v := m["confidence"].(type) {
		case float64:
			conf = v
		case int:
			conf = float64(v)
		}
		reason, _ := m["reasoning"].(string)
		name, _ := m["name"].(string)
		engine, _ := m["engine"].(string)
		out = append(out, DataSourceRank{
			DataSourceID: parsed,
			Name:         name,
			Engine:       engine,
			Confidence:   conf,
			Reasoning:    reason,
		})
	}
	return out, nil
}
