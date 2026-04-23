package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/usecase"
	"github.com/sentiae/platform-kit/timetravel"
	"gorm.io/gorm"
)

// AnnotationHandler exposes CRUD for per-org business-term annotations
// (§12.2). Rows live in the per_org_vocabulary table and are keyed by
// column_id — a free-form string callers compose from the
// "<data_source_id>.<table>.<column>" triple. The handler deliberately
// does not validate the column_id shape; any opaque identifier works
// because consumers of this table may have their own schema-inventory
// conventions.
//
// Authorization: rows are organization-scoped via the X-Organization-ID
// header the portal forwards from BFF. We do not gate by role here
// because the schema-editor UI is expected to already check RBAC
// upstream before the portal issues the request.
type AnnotationHandler struct {
	db       *gorm.DB
	recorder timetravel.Recorder
}

// NewAnnotationHandler wires the handler.
func NewAnnotationHandler(db *gorm.DB) *AnnotationHandler {
	return &AnnotationHandler{db: db, recorder: timetravel.NoopRecorder{}}
}

// WithTimeTravelRecorder wires the §13.1 entity-snapshot recorder so
// every OrgVocabulary CRUD write writes a snapshot row. Snapshots are
// keyed by the vocabulary row id so operators can ask "what definition
// was live for <column> on T?".
func (h *AnnotationHandler) WithTimeTravelRecorder(r timetravel.Recorder) *AnnotationHandler {
	if r != nil {
		h.recorder = r
	}
	return h
}

// RegisterRoutes mounts POST/GET/PUT/DELETE /data/annotations.
func (h *AnnotationHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data/annotations", func(r chi.Router) {
		r.Post("/", h.Create)
		r.Get("/", h.List)
		r.Get("/resolve", h.Resolve)
		r.Get("/{id}", h.Get)
		r.Put("/{id}", h.Update)
		r.Delete("/{id}", h.Delete)
	})
}

// annotationRequest is the payload shape accepted by Create + Update.
// §12.2 (annotation upgrade) added Unit/DataType/Format/Aliases to the
// wire format. Missing values are preserved on Update — omit a field to
// keep the existing value instead of blanking it.
type annotationRequest struct {
	ColumnID     string   `json:"column_id"`
	BusinessTerm string   `json:"business_term"`
	Synonyms     []string `json:"synonyms,omitempty"`
	Aliases      []string `json:"aliases,omitempty"`
	Unit         string   `json:"unit,omitempty"`
	DataType     string   `json:"data_type,omitempty"`
	Format       string   `json:"format,omitempty"`
	Description  string   `json:"description,omitempty"`
}

// Create handles POST /data/annotations.
func (h *AnnotationHandler) Create(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userUUID, _ := uuid.Parse(userID)

	var req annotationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	req.ColumnID = strings.TrimSpace(req.ColumnID)
	req.BusinessTerm = strings.TrimSpace(req.BusinessTerm)
	if req.ColumnID == "" || req.BusinessTerm == "" {
		respondBadRequest(w, "column_id and business_term are required")
		return
	}

	row := &domain.OrgVocabulary{
		ID:             uuid.New(),
		OrganizationID: orgUUID,
		ColumnID:       req.ColumnID,
		BusinessTerm:   req.BusinessTerm,
		Synonyms:       domain.StringArray(req.Synonyms),
		Aliases:        domain.StringArray(req.Aliases),
		Unit:           req.Unit,
		DataType:       req.DataType,
		Format:         req.Format,
		Description:    req.Description,
		CreatedBy:      userUUID,
		UpdatedBy:      userUUID,
	}
	if err := h.db.Create(row).Error; err != nil {
		respondInternalError(w, "Failed to create annotation: "+err.Error())
		return
	}
	// §13.1 snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "org_vocabulary", row.ID.String(), row)
	}
	respondCreated(w, row)
}

// List handles GET /data/annotations.
// Optional query params: column_id, business_term (ILIKE).
func (h *AnnotationHandler) List(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	q := h.db.Where("organization_id = ?", orgUUID)
	if colID := r.URL.Query().Get("column_id"); colID != "" {
		q = q.Where("column_id = ?", colID)
	}
	if term := r.URL.Query().Get("business_term"); term != "" {
		q = q.Where("business_term ILIKE ?", "%"+term+"%")
	}
	var rows []domain.OrgVocabulary
	if err := q.Order("updated_at DESC").Limit(200).Find(&rows).Error; err != nil {
		respondInternalError(w, err.Error())
		return
	}
	respondSuccess(w, map[string]any{"data": rows})
}

// Get handles GET /data/annotations/{id}.
func (h *AnnotationHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, _ := uuid.Parse(orgID)
	var row domain.OrgVocabulary
	if err := h.db.Where("id = ? AND organization_id = ?", id, orgUUID).First(&row).Error; err != nil {
		respondNotFound(w, "Annotation not found")
		return
	}
	respondSuccess(w, row)
}

// Update handles PUT /data/annotations/{id}. column_id is immutable
// once created — changing which column an annotation targets is a
// delete+create, not an update.
func (h *AnnotationHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	orgUUID, _ := uuid.Parse(orgID)
	userUUID, _ := uuid.Parse(userID)

	var req annotationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}

	var row domain.OrgVocabulary
	if err := h.db.Where("id = ? AND organization_id = ?", id, orgUUID).First(&row).Error; err != nil {
		respondNotFound(w, "Annotation not found")
		return
	}
	updates := map[string]any{"updated_by": userUUID}
	if req.BusinessTerm != "" {
		updates["business_term"] = strings.TrimSpace(req.BusinessTerm)
	}
	if req.Synonyms != nil {
		updates["synonyms"] = domain.StringArray(req.Synonyms)
	}
	if req.Aliases != nil {
		updates["aliases"] = domain.StringArray(req.Aliases)
	}
	if req.Unit != "" {
		updates["unit"] = req.Unit
	}
	if req.DataType != "" {
		updates["data_type"] = req.DataType
	}
	if req.Format != "" {
		updates["format"] = req.Format
	}
	if req.Description != "" {
		updates["description"] = req.Description
	}
	if err := h.db.Model(&row).Updates(updates).Error; err != nil {
		respondInternalError(w, err.Error())
		return
	}
	h.db.Where("id = ?", id).First(&row)
	// §13.1 snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "org_vocabulary", row.ID.String(), row)
	}
	respondSuccess(w, row)
}

// Resolve handles GET /data/annotations/resolve?term=<term>.
//
// §12.2 (annotation upgrade) — the NL→SQL pipeline calls this to look
// up a user-typed phrase against the org vocabulary. Matches hit both
// the canonical `business_term` and any entry in the `aliases` /
// `synonyms` JSON arrays. A case-insensitive exact match wins; if
// nothing matches exactly we fall back to an ILIKE search so partial
// phrases ("monthly rec") still surface the right row.
func (h *AnnotationHandler) Resolve(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	term := strings.TrimSpace(r.URL.Query().Get("term"))
	if term == "" {
		respondBadRequest(w, "term query parameter is required")
		return
	}
	rows, err := usecase.ResolveAnnotations(h.db, orgUUID, term)
	if err != nil {
		respondInternalError(w, err.Error())
		return
	}
	respondSuccess(w, map[string]any{"data": rows, "term": term})
}

// Delete handles DELETE /data/annotations/{id}.
func (h *AnnotationHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, _ := uuid.Parse(orgID)
	if err := h.db.Where("id = ? AND organization_id = ?", id, orgUUID).Delete(&domain.OrgVocabulary{}).Error; err != nil {
		respondInternalError(w, err.Error())
		return
	}
	// §13.1 tombstone snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "org_vocabulary", id.String(), map[string]any{
			"id":      id.String(),
			"deleted": true,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}
