package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/repository/postgres"
	"github.com/sentiae/data-service/internal/usecase"
	"github.com/sentiae/platform-kit/timetravel"
)

// QueryHistoryHandler exposes the §12.3 query-history + saved-query
// endpoints. Mounted under /api/v1/data/queries by `Server.NewServer`.
type QueryHistoryHandler struct {
	svc      *usecase.QueryHistoryService
	recorder timetravel.Recorder
}

// NewQueryHistoryHandler wires a handler over the given service.
func NewQueryHistoryHandler(svc *usecase.QueryHistoryService) *QueryHistoryHandler {
	return &QueryHistoryHandler{svc: svc, recorder: timetravel.NoopRecorder{}}
}

// WithTimeTravelRecorder wires the §13.1 entity-snapshot recorder so
// saved-query CRUD writes a snapshot row per change.
func (h *QueryHistoryHandler) WithTimeTravelRecorder(r timetravel.Recorder) *QueryHistoryHandler {
	if r != nil {
		h.recorder = r
	}
	return h
}

// RegisterRoutes mounts:
//
//	GET    /data/queries/history
//	POST   /data/queries/saved
//	GET    /data/queries/saved
//	GET    /data/queries/saved/{id}
//	PATCH  /data/queries/saved/{id}
//	DELETE /data/queries/saved/{id}
func (h *QueryHistoryHandler) RegisterRoutes(r chi.Router) {
	r.Get("/data/queries/history", h.ListHistory)
	r.Route("/data/queries/saved", func(r chi.Router) {
		r.Post("/", h.CreateSaved)
		r.Get("/", h.ListSaved)
		r.Get("/{id}", h.GetSaved)
		r.Patch("/{id}", h.UpdateSaved)
		r.Delete("/{id}", h.DeleteSaved)
	})
}

func (h *QueryHistoryHandler) ListHistory(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(r.Header.Get("X-Organization-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	// Optional user filter: pass ?user_id=<uuid>&. If omitted, falls
	// back to the caller's own X-User-ID unless ?all=true is provided
	// (admins querying org-wide should explicitly request it).
	var userID *uuid.UUID
	if v := r.URL.Query().Get("user_id"); v != "" {
		parsed, err := uuid.Parse(v)
		if err != nil {
			respondBadRequest(w, "Invalid user_id")
			return
		}
		userID = &parsed
	} else if r.URL.Query().Get("all") != "true" {
		callerID, err := uuid.Parse(r.Header.Get("X-User-ID"))
		if err == nil {
			userID = &callerID
		}
	}
	since := parseTimeParam(r.URL.Query().Get("since"))
	until := parseTimeParam(r.URL.Query().Get("until"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	rows, err := h.svc.ListQueryHistory(orgID, userID, since, until, limit)
	if err != nil {
		respondInternalError(w, err.Error())
		return
	}
	respondSuccess(w, map[string]any{"data": rows})
}

type saveQueryRequest struct {
	Name            string     `json:"name"`
	Description     string     `json:"description,omitempty"`
	NaturalLanguage string     `json:"natural_language,omitempty"`
	GeneratedSQL    string     `json:"generated_sql"`
	DataSourceID    *uuid.UUID `json:"data_source_id,omitempty"`
	IsShared        bool       `json:"is_shared,omitempty"`
}

func (h *QueryHistoryHandler) CreateSaved(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(r.Header.Get("X-Organization-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userID, err := uuid.Parse(r.Header.Get("X-User-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-User-ID")
		return
	}
	var req saveQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	if req.Name == "" || req.GeneratedSQL == "" {
		respondBadRequest(w, "name and generated_sql are required")
		return
	}
	row, err := h.svc.SaveQuery(usecase.SaveQueryInput{
		OrganizationID:  orgID,
		UserID:          userID,
		Name:            req.Name,
		Description:     req.Description,
		NaturalLanguage: req.NaturalLanguage,
		GeneratedSQL:    req.GeneratedSQL,
		DataSourceID:    req.DataSourceID,
		IsShared:        req.IsShared,
	})
	if err != nil {
		respondInternalError(w, err.Error())
		return
	}
	// §13.1 snapshot.
	if h.recorder != nil && row != nil {
		_ = h.recorder.RecordEntity(r.Context(), "saved_query", row.ID.String(), row)
	}
	respondCreated(w, row)
}

func (h *QueryHistoryHandler) ListSaved(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(r.Header.Get("X-Organization-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userID, err := uuid.Parse(r.Header.Get("X-User-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-User-ID")
		return
	}
	includeShared := r.URL.Query().Get("include_shared") != "false"
	rows, err := h.svc.ListSavedQueries(orgID, userID, includeShared)
	if err != nil {
		respondInternalError(w, err.Error())
		return
	}
	respondSuccess(w, map[string]any{"data": rows})
}

func (h *QueryHistoryHandler) GetSaved(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(r.Header.Get("X-Organization-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userID, err := uuid.Parse(r.Header.Get("X-User-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-User-ID")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	row, err := h.svc.GetSavedQuery(orgID, userID, id)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			respondNotFound(w, "Saved query not found")
			return
		}
		respondInternalError(w, err.Error())
		return
	}
	respondSuccess(w, row)
}

type updateSavedRequest struct {
	Name            *string `json:"name,omitempty"`
	Description     *string `json:"description,omitempty"`
	NaturalLanguage *string `json:"natural_language,omitempty"`
	GeneratedSQL    *string `json:"generated_sql,omitempty"`
	IsShared        *bool   `json:"is_shared,omitempty"`
}

func (h *QueryHistoryHandler) UpdateSaved(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(r.Header.Get("X-Organization-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userID, err := uuid.Parse(r.Header.Get("X-User-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-User-ID")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	var req updateSavedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	updates := map[string]any{}
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.NaturalLanguage != nil {
		updates["natural_language"] = *req.NaturalLanguage
	}
	if req.GeneratedSQL != nil {
		updates["generated_sql"] = *req.GeneratedSQL
	}
	if req.IsShared != nil {
		updates["is_shared"] = *req.IsShared
	}
	if len(updates) == 0 {
		respondBadRequest(w, "No fields to update")
		return
	}
	row, err := h.svc.UpdateSavedQuery(orgID, userID, id, updates)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			respondNotFound(w, "Saved query not found")
			return
		}
		respondInternalError(w, err.Error())
		return
	}
	// §13.1 snapshot.
	if h.recorder != nil && row != nil {
		_ = h.recorder.RecordEntity(r.Context(), "saved_query", row.ID.String(), row)
	}
	respondSuccess(w, row)
}

func (h *QueryHistoryHandler) DeleteSaved(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(r.Header.Get("X-Organization-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userID, err := uuid.Parse(r.Header.Get("X-User-ID"))
	if err != nil {
		respondBadRequest(w, "Invalid X-User-ID")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	if err := h.svc.DeleteSavedQuery(orgID, userID, id); err != nil {
		respondInternalError(w, err.Error())
		return
	}
	// §13.1 tombstone snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "saved_query", id.String(), map[string]any{
			"id":      id.String(),
			"deleted": true,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseTimeParam accepts RFC3339 or date-only strings.
func parseTimeParam(v string) *time.Time {
	if v == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, v); err == nil {
			return &t
		}
	}
	return nil
}
