package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/usecase"
)

// fakeSelectorLLM matches usecase.DataSourceSelectorLLM so handler
// tests can force both "auto-pick" and "needs selection" branches
// without a live foundry.
type fakeSelectorLLM struct {
	ranks []usecase.DataSourceRank
	err   error
}

func (f *fakeSelectorLLM) RankDataSources(ctx context.Context, in usecase.RankDataSourcesInput) ([]usecase.DataSourceRank, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.ranks, nil
}

func TestNaturalLanguageQuery_AutoSelectsHighConfidence(t *testing.T) {
	tr := &fakeTranslator{sql: "SELECT count(*) FROM orders"}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	ds := seedDataSource(t, db, orgID)

	// Stage the selector so the single candidate returns with confidence >= 0.8.
	fake := &fakeSelectorLLM{
		ranks: []usecase.DataSourceRank{
			{DataSourceID: ds.ID, Confidence: 0.95, Reasoning: "matches orders table"},
		},
	}
	sel := usecase.NewDataSourceSelector(db, fake)
	h.SetSelector(sel)
	h.SetAutoSelectThreshold(0.8, 0.1)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"question": "how many orders?",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", rec.Code, rec.Body.String())
	}
	if _, ok := data["needs_selection"]; ok {
		t.Fatalf("expected auto-pick, got needs_selection: %v", data)
	}
	if data["selected_data_source_id"] == nil {
		t.Errorf("expected selected_data_source_id in response")
	}
	if data["selection_reasoning"] != "matches orders table" {
		t.Errorf("unexpected reasoning: %v", data["selection_reasoning"])
	}
	if data["sql"].(string) != tr.sql {
		t.Errorf("expected translated sql, got %v", data["sql"])
	}
}

func TestNaturalLanguageQuery_ReturnsCandidatesWhenAmbiguous(t *testing.T) {
	tr := &fakeTranslator{sql: "SELECT 1"}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	a := seedDataSource(t, db, orgID)
	b := seedDataSource(t, db, orgID)

	fake := &fakeSelectorLLM{
		ranks: []usecase.DataSourceRank{
			{DataSourceID: a.ID, Confidence: 0.6, Reasoning: "kinda matches"},
			{DataSourceID: b.ID, Confidence: 0.55, Reasoning: "also kinda"},
		},
	}
	sel := usecase.NewDataSourceSelector(db, fake)
	h.SetSelector(sel)
	h.SetAutoSelectThreshold(0.8, 0.1)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"question": "pick one",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (not 4xx), got %d", rec.Code)
	}
	needs, ok := data["needs_selection"].(bool)
	if !ok || !needs {
		t.Fatalf("expected needs_selection=true, got %v", data)
	}
	cands, ok := data["candidates"].([]any)
	if !ok {
		t.Fatalf("expected candidates array, got %T", data["candidates"])
	}
	if len(cands) != 2 {
		t.Errorf("expected 2 candidates, got %d", len(cands))
	}
}

func TestNaturalLanguageQuery_AmbiguousWhenTopWithinMargin(t *testing.T) {
	// Top candidate is above the threshold but the runner-up is within
	// `margin` of it — treat as ambiguous.
	tr := &fakeTranslator{sql: "SELECT 1"}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	a := seedDataSource(t, db, orgID)
	b := seedDataSource(t, db, orgID)

	fake := &fakeSelectorLLM{
		ranks: []usecase.DataSourceRank{
			{DataSourceID: a.ID, Confidence: 0.9, Reasoning: "solid match"},
			{DataSourceID: b.ID, Confidence: 0.87, Reasoning: "nearly identical"},
		},
	}
	sel := usecase.NewDataSourceSelector(db, fake)
	h.SetSelector(sel)
	h.SetAutoSelectThreshold(0.8, 0.1)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"question": "anything",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	if needs, _ := data["needs_selection"].(bool); !needs {
		t.Errorf("expected needs_selection=true for tight margin, got %v", data)
	}
}

func TestNaturalLanguageQuery_NoSourcesReturns400(t *testing.T) {
	tr := &fakeTranslator{sql: "SELECT 1"}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()

	// Empty selector output for an org with zero sources.
	sel := usecase.NewDataSourceSelector(db, &fakeSelectorLLM{})
	h.SetSelector(sel)

	rec, _ := doNLQuery(t, h, orgID, userID, map[string]any{
		"question": "does this work?",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when org has no sources, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestNaturalLanguageQuery_ExplicitIDStillWorks(t *testing.T) {
	// The auto-selector must stay out of the way when the caller
	// pins a data source explicitly — this is the legacy path.
	tr := &fakeTranslator{sql: "SELECT count(*) FROM orders"}
	h, db := newTestQueryHandler(t, tr)
	orgID := uuid.New()
	userID := uuid.New()
	ds := seedDataSource(t, db, orgID)

	// Configure a selector that would return a different id if called;
	// the handler must not call it.
	other := uuid.New()
	fake := &fakeSelectorLLM{
		ranks: []usecase.DataSourceRank{{DataSourceID: other, Confidence: 0.99}},
	}
	sel := usecase.NewDataSourceSelector(db, fake)
	h.SetSelector(sel)

	rec, data := doNLQuery(t, h, orgID, userID, map[string]any{
		"data_source_id": ds.ID.String(),
		"question":       "how many orders?",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	// When the caller pins a data source we must NOT emit selector fields.
	if _, ok := data["selected_data_source_id"]; ok {
		t.Errorf("selected_data_source_id should be absent when caller pins ID")
	}
	if _, ok := data["needs_selection"]; ok {
		t.Errorf("needs_selection should be absent when caller pins ID")
	}
	if data["sql"].(string) != tr.sql {
		t.Errorf("sql mismatch: %v", data["sql"])
	}
}
