package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/usecase"
	"gorm.io/gorm"
)

// FederatedQueryHandler exposes the cross-domain federated query endpoint.
type FederatedQueryHandler struct {
	planner *usecase.FederatedPlanner
	db      *gorm.DB
}

// NewFederatedQueryHandler creates a new FederatedQueryHandler.
func NewFederatedQueryHandler(planner *usecase.FederatedPlanner, db *gorm.DB) *FederatedQueryHandler {
	return &FederatedQueryHandler{planner: planner, db: db}
}

// RegisterRoutes registers the federated query routes.
func (h *FederatedQueryHandler) RegisterRoutes(r chi.Router) {
	r.Post("/federated-query", h.ExecuteFederatedQuery)
	// §12.4 — plan-only endpoint for decomposing NL / SQL into
	// per-source sub-queries without executing them. Used by the
	// portal's query-plan preview panel.
	r.Post("/federated-queries/plan", h.PlanFederatedQuery)
}

// ExecuteFederatedQuery handles POST /api/v1/data/federated-query
func (h *FederatedQueryHandler) ExecuteFederatedQuery(w http.ResponseWriter, r *http.Request) {
	var req usecase.FederatedQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	if len(req.SubQueries) == 0 {
		respondError(w, http.StatusBadRequest, "BAD_REQUEST", "at least one sub_query is required")
		return
	}

	result, err := h.planner.Execute(r.Context(), &req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	respondJSON(w, http.StatusOK, result)
}

// planRequestBody is the wire shape for POST /federated-queries/plan.
// Either nl_query or sql must be populated; sources is optional — when
// absent, the handler loads all data sources owned by the caller's org
// from the catalog so the caller doesn't have to re-send them.
type planRequestBody struct {
	NLQuery string   `json:"nl_query,omitempty"`
	SQL     string   `json:"sql,omitempty"`
	JoinKey string   `json:"join_key,omitempty"`
	Sources []string `json:"sources,omitempty"`
}

// PlanFederatedQuery handles POST /api/v1/data/federated-queries/plan.
// Returns the compiled FederatedQueryRequest so the caller can preview
// the decomposition before submitting it to /federated-query.
func (h *FederatedQueryHandler) PlanFederatedQuery(w http.ResponseWriter, r *http.Request) {
	var body planRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	orgID, _ := uuid.Parse(r.Header.Get("X-Organization-ID"))
	userID, _ := uuid.Parse(r.Header.Get("X-User-ID"))

	// Load the catalog entries the planner should consider. The wire
	// body may narrow by source name; when empty, we return every
	// source the org owns (the planner won't expose columns the
	// requesting user lacks permission on — that's enforced inside
	// Execute).
	var sources []domain.DataSource
	q := h.db.WithContext(r.Context()).Where("organization_id = ?", orgID)
	if len(body.Sources) > 0 {
		q = q.Where("name IN ?", body.Sources)
	}
	if err := q.Find(&sources).Error; err != nil {
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	refs := make([]usecase.DataSourceRef, 0, len(sources))
	for _, s := range sources {
		refs = append(refs, usecase.DataSourceRef{
			Name:   s.Name,
			Engine: string(s.Engine),
			DSN:    s.ConnectionDSN,
			Schema: s.Schema,
			Tables: []string(s.Tables),
		})
	}
	plan, err := h.planner.Plan(r.Context(), usecase.PlanRequest{
		OrganizationID: orgID,
		UserID:         userID,
		NLQuery:        body.NLQuery,
		SQL:            body.SQL,
		Sources:        refs,
		JoinKey:        body.JoinKey,
	})
	if err != nil {
		respondError(w, http.StatusBadRequest, "PLAN_FAILED", err.Error())
		return
	}
	respondJSON(w, http.StatusOK, plan)
}
