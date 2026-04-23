// Package http — §12.3 "explain before execute" endpoint.
//
// POST /api/v1/data/queries/explain lets the portal preview what a
// natural-language question would translate to before the user commits
// to running it. The response includes the generated SQL, an estimated
// cost/rows-read figure, the tables that would be touched, and a flag
// indicating whether the query is a mutation (so the UI can warn before
// sending to /execute).
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/usecase"
	"gorm.io/gorm"
)

// QueryExplainHandler wires the explain usecase with a foundry-backed LLM
// and a GORM-backed semantic loader.
type QueryExplainHandler struct {
	uc *usecase.QueryExplainUseCase
}

// NewQueryExplainHandler builds a handler that reuses the same
// NL→SQL translator QueryHandler uses so results match exactly.
func NewQueryExplainHandler(db *gorm.DB) *QueryExplainHandler {
	foundryURL := os.Getenv("FOUNDRY_SERVICE_URL")
	if foundryURL == "" {
		foundryURL = "http://localhost:8085"
	}
	llm := &foundryLLM{url: foundryURL}
	loader := &semanticFieldLoader{db: db}
	return &QueryExplainHandler{
		uc: usecase.NewQueryExplainUseCase(llm, loader),
	}
}

// RegisterRoutes mounts the explain endpoint + the chart suggestion lookup.
func (h *QueryExplainHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data/queries", func(r chi.Router) {
		r.Post("/explain", h.Explain)
		r.Get("/{id}/chart-suggestion", h.ChartSuggestion)
	})
}

type explainRequest struct {
	Question     string    `json:"question"`
	DataSourceID uuid.UUID `json:"dataSourceId"`
}

// Explain returns generated SQL + cost estimates.
func (h *QueryExplainHandler) Explain(w http.ResponseWriter, r *http.Request) {
	var req explainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	if req.Question == "" {
		respondBadRequest(w, "question is required")
		return
	}
	result, err := h.uc.Explain(r.Context(), req.Question, req.DataSourceID)
	if err != nil {
		respondInternalError(w, "Explain failed: "+err.Error())
		return
	}
	respondSuccess(w, result)
}

// ChartSuggestion returns the cached chart recommendation for the most
// recent execution of the given query. If no execution has been run yet,
// returns a neutral "table" suggestion.
func (h *QueryExplainHandler) ChartSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid query id")
		return
	}
	// Re-read the most recent execution to pick up the stored suggestion.
	type execRow struct {
		Result domain.JSONMap
	}
	var exec execRow
	db := dbFromContext(r.Context())
	if db == nil {
		respondSuccess(w, usecase.ChartSuggestion{Chart: usecase.ChartTypeTable, Confidence: 0.3, Reason: "no executions yet"})
		return
	}
	err = db.Table("query_executions").
		Select("result").
		Where("query_id = ?", id).
		Order("executed_at DESC").
		Limit(1).
		Scan(&exec).Error
	if err != nil || exec.Result == nil {
		respondSuccess(w, usecase.ChartSuggestion{Chart: usecase.ChartTypeTable, Confidence: 0.3, Reason: "no executions yet"})
		return
	}
	if v, ok := exec.Result["suggested_chart"]; ok {
		respondSuccess(w, v)
		return
	}
	respondSuccess(w, usecase.ChartSuggestion{Chart: usecase.ChartTypeTable, Confidence: 0.3, Reason: "execution had no suggestion"})
}

type ctxKey int

const dbCtxKey ctxKey = 1

// WithDB attaches a gorm handle to the request context for handlers that
// need raw DB access (chart-suggestion reads the latest execution row).
func WithDB(ctx context.Context, db *gorm.DB) context.Context {
	return context.WithValue(ctx, dbCtxKey, db)
}

func dbFromContext(ctx context.Context) *gorm.DB {
	db, _ := ctx.Value(dbCtxKey).(*gorm.DB)
	return db
}

// foundryLLM mirrors QueryHandler.callLLMForSQL but exposes the
// ExplainLLM interface for the usecase.
type foundryLLM struct {
	url string
}

func (f *foundryLLM) Translate(ctx context.Context, question, semanticContext string) (string, error) {
	prompt := fmt.Sprintf(
		"You are a SQL expert. Given this database schema:\n\n%s\n\nTranslate this question to a PostgreSQL SELECT query:\n\"%s\"\n\nReturn ONLY the SQL query, no explanation.",
		semanticContext, question,
	)
	body, _ := json.Marshal(map[string]any{
		"message":    prompt,
		"model":      "gpt-4o-mini",
		"provider":   "openrouter",
		"max_tokens": 500,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.url+"/api/v1/foundry/completion", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("foundry returned %d: %s", resp.StatusCode, string(raw))
	}
	var parsed struct {
		Data struct {
			Content string `json:"content"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", err
	}
	return parsed.Data.Content, nil
}

// semanticFieldLoader loads the field context using the same shape as
// QueryHandler.NaturalLanguageQuery to keep explain results identical.
type semanticFieldLoader struct {
	db *gorm.DB
}

func (s *semanticFieldLoader) LoadSemanticContext(ctx context.Context, dataSourceID uuid.UUID) (string, error) {
	var fields []domain.SemanticField
	if err := s.db.WithContext(ctx).Where("data_source_id = ?", dataSourceID).Find(&fields).Error; err != nil {
		return "", err
	}
	return buildSemanticContext(fields), nil
}
