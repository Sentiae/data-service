package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"github.com/sentiae/platform-kit/timetravel"
	"gorm.io/gorm"
)

type DashboardHandler struct {
	db       *gorm.DB
	pub      kafka.Publisher
	access   *DashboardAccess
	perm     *DashboardPermissionHandler
	alert    *DashboardAlertHandler
	recorder timetravel.Recorder
}

func NewDashboardHandler(db *gorm.DB, pub kafka.Publisher) *DashboardHandler {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	access := NewDashboardAccess(db)
	return &DashboardHandler{
		db:       db,
		pub:      pub,
		access:   access,
		perm:     NewDashboardPermissionHandler(db, pub, access),
		alert:    NewDashboardAlertHandler(db, pub, access),
		recorder: timetravel.NoopRecorder{},
	}
}

// WithTimeTravelRecorder wires the §13.1 entity-snapshot recorder so
// Dashboard CRUD writes snapshot rows.
func (h *DashboardHandler) WithTimeTravelRecorder(r timetravel.Recorder) *DashboardHandler {
	if r != nil {
		h.recorder = r
	}
	return h
}

// recordDashboard writes a dashboard snapshot, swallowing errors.
func (h *DashboardHandler) recordDashboard(r *http.Request, dash *domain.DashboardConfig) {
	if h == nil || h.recorder == nil || dash == nil {
		return
	}
	_ = h.recorder.RecordEntity(r.Context(), "dashboard", dash.ID.String(), dash)
}

func (h *DashboardHandler) emit(r *http.Request, eventType string, dash *domain.DashboardConfig, actor uuid.UUID) {
	if h.pub == nil {
		return
	}
	meta := map[string]any{
		"dashboard_id": dash.ID.String(),
		"version":      dash.Version,
		"name":         dash.Name,
		"description":  dash.Description,
	}
	if dash.CanvasNodeID != nil {
		meta["canvas_node_id"] = dash.CanvasNodeID.String()
	}
	if dash.Panels != nil {
		meta["panels"] = dash.Panels
	}
	if dash.Layout != nil {
		meta["layout"] = dash.Layout
	}
	_ = h.pub.Publish(r.Context(), eventType, kafka.EventData{
		ActorID:        actor.String(),
		ResourceType:   "dashboard_config",
		ResourceID:     dash.ID.String(),
		OrganizationID: dash.OrganizationID.String(),
		Metadata:       meta,
	})
}

func (h *DashboardHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data/dashboards", func(r chi.Router) {
		r.Post("/", h.Create)
		r.Get("/", h.List)
		r.Get("/{id}", h.Get)
		r.Put("/{id}", h.Update)
		r.Delete("/{id}", h.Delete)
		r.Put("/{id}/panels", h.ReplacePanels)
		r.Post("/{id}/share", h.Share)
		r.Post("/{id}/embed-in-canvas", h.EmbedInCanvas)

		// RBAC permission CRUD
		r.Get("/{id}/permissions", h.perm.ListPermissions)
		r.Post("/{id}/permissions", h.perm.CreatePermission)
		r.Delete("/{id}/permissions/{permId}", h.perm.DeletePermission)

		// Alerts
		r.Get("/{id}/alerts", h.alert.List)
		r.Post("/{id}/alerts", h.alert.Create)
		r.Put("/{id}/alerts/{alertId}", h.alert.Update)
		r.Delete("/{id}/alerts/{alertId}", h.alert.Delete)
	})
}

type shareDashboardRequest struct {
	// Recipients are arbitrary principal identifiers (user/team/org ids or emails).
	Recipients []string `json:"recipients"`
	// Permission is "view", "edit", or "admin".
	Permission string `json:"permission"`
	// Public, when true, toggles public-link sharing.
	Public bool `json:"public"`
}

// Share publishes a data.dashboard.shared event so downstream services can
// grant access, notify recipients, and update the canvas view. This keeps
// the legacy free-form Share endpoint (used by the portal) while the
// fine-grained RBAC lives at /permissions.
func (h *DashboardHandler) Share(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireAdmin(w, r, &dash) {
		return
	}
	var req shareDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	if req.Permission == "" {
		req.Permission = "view"
	}

	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	if h.pub != nil {
		meta := map[string]any{
			"dashboard_id": dash.ID.String(),
			"version":      dash.Version,
			"name":         dash.Name,
			"recipients":   req.Recipients,
			"permission":   req.Permission,
			"public":       req.Public,
		}
		if dash.CanvasNodeID != nil {
			meta["canvas_node_id"] = dash.CanvasNodeID.String()
		}
		_ = h.pub.Publish(r.Context(), "data.dashboard.shared", kafka.EventData{
			ActorID:        actor.String(),
			ResourceType:   "dashboard_config",
			ResourceID:     dash.ID.String(),
			OrganizationID: dash.OrganizationID.String(),
			Metadata:       meta,
		})
	}

	respondSuccess(w, map[string]any{
		"status":       "shared",
		"dashboard_id": dash.ID,
		"recipients":   req.Recipients,
		"permission":   req.Permission,
	})
}

type embedInCanvasRequest struct {
	CanvasID uuid.UUID      `json:"canvas_id"`
	NodeID   *uuid.UUID     `json:"node_id,omitempty"`
	Position domain.JSONMap `json:"position,omitempty"`
}

// EmbedInCanvas links the dashboard to a canvas node. When node_id is
// omitted a new UUID is generated so the canvas consumer can create a node
// reference. The event carries full metadata so the canvas consumer can
// render an embedded panel or refresh an existing one.
func (h *DashboardHandler) EmbedInCanvas(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireWrite(w, r, &dash) {
		return
	}
	var req embedInCanvasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	if req.CanvasID == uuid.Nil {
		respondBadRequest(w, "canvas_id is required")
		return
	}
	nodeID := uuid.New()
	if req.NodeID != nil {
		nodeID = *req.NodeID
	}
	// Persist the canvas node pointer on the dashboard so subsequent updates
	// automatically carry the linkage when they publish.
	dash.CanvasNodeID = &nodeID
	if err := h.db.Save(&dash).Error; err != nil {
		respondInternalError(w, "Failed to link dashboard to canvas")
		return
	}

	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	if h.pub != nil {
		meta := map[string]any{
			"dashboard_id": dash.ID.String(),
			"canvas_id":    req.CanvasID.String(),
			"node_id":      nodeID.String(),
			"name":         dash.Name,
			"version":      dash.Version,
			"panels":       dash.Panels,
			"layout":       dash.Layout,
		}
		if req.Position != nil {
			meta["position"] = req.Position
		}
		_ = h.pub.Publish(r.Context(), "data.dashboard.embedded", kafka.EventData{
			ActorID:        actor.String(),
			ResourceType:   "dashboard_config",
			ResourceID:     dash.ID.String(),
			OrganizationID: dash.OrganizationID.String(),
			Metadata:       meta,
		})
	}
	respondSuccess(w, map[string]any{
		"dashboard_id":   dash.ID,
		"canvas_id":      req.CanvasID,
		"canvas_node_id": nodeID,
	})
}

type createDashboardRequest struct {
	CanvasNodeID *uuid.UUID     `json:"canvas_node_id,omitempty"`
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	Panels       domain.JSONMap `json:"panels"`
	Layout       domain.JSONMap `json:"layout"`
}

func (h *DashboardHandler) Create(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	orgUUID, _ := uuid.Parse(orgID)
	userUUID, _ := uuid.Parse(userID)

	var req createDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}

	dash := &domain.DashboardConfig{
		ID:             uuid.New(),
		OrganizationID: orgUUID,
		CanvasNodeID:   req.CanvasNodeID,
		Name:           req.Name,
		Description:    req.Description,
		Version:        1,
		Panels:         req.Panels,
		Layout:         req.Layout,
		CreatedBy:      userUUID,
	}

	if err := h.db.Create(dash).Error; err != nil {
		respondInternalError(w, "Failed to create dashboard")
		return
	}
	h.emit(r, "data.dashboard.created", dash, userUUID)
	h.recordDashboard(r, dash)
	respondCreated(w, dash)
}

func (h *DashboardHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireRead(w, r, &dash) {
		return
	}
	respondSuccess(w, dash)
}

// List returns dashboards the caller can read. Org membership grants
// implicit view, so the baseline filter is organization_id.
func (h *DashboardHandler) List(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, _ := uuid.Parse(orgID)
	userID, _ := uuid.Parse(r.Header.Get("X-User-ID"))

	var dashboards []domain.DashboardConfig
	// Start with all dashboards in the org (implicit view grant).
	h.db.Where("organization_id = ?", orgUUID).Order("created_at DESC").Find(&dashboards)

	// Also include dashboards where the user has an explicit user-scoped
	// permission row across orgs (rare, but valid).
	if userID != uuid.Nil {
		var extra []domain.DashboardConfig
		h.db.Raw(`
			SELECT d.* FROM dashboard_configs d
			JOIN dashboard_permissions p ON p.dashboard_id = d.id
			WHERE p.principal_type = ? AND p.principal_id = ? AND d.organization_id <> ?
		`, domain.DashboardPrincipalUser, userID, orgUUID).Scan(&extra)
		dashboards = append(dashboards, extra...)
	}
	respondSuccess(w, map[string]any{"data": dashboards})
}

func (h *DashboardHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireWrite(w, r, &dash) {
		return
	}
	var updates map[string]any
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		respondBadRequest(w, "Invalid body")
		return
	}
	// Bump version on update
	dash.Version++
	updates["version"] = dash.Version
	h.db.Model(&dash).Updates(updates)
	h.db.Where("id = ?", id).First(&dash)

	// Emit dashboard-updated event for downstream canvas sync.
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emit(r, "data.dashboard.updated", &dash, actor)
	h.recordDashboard(r, &dash)

	respondSuccess(w, dash)
}

type replacePanelsRequest struct {
	Panels domain.JSONMap `json:"panels"`
	Layout domain.JSONMap `json:"layout,omitempty"`
}

// ReplacePanels bulk-replaces a dashboard's panels (and optionally layout).
// This path bypasses field-by-field PATCH semantics and is the primary
// save target for the portal dashboard builder.
func (h *DashboardHandler) ReplacePanels(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireWrite(w, r, &dash) {
		return
	}
	var req replacePanelsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid body")
		return
	}
	dash.Panels = req.Panels
	if req.Layout != nil {
		dash.Layout = req.Layout
	}
	dash.Version++
	if err := h.db.Save(&dash).Error; err != nil {
		respondInternalError(w, "Failed to save panels")
		return
	}
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emit(r, "data.dashboard.updated", &dash, actor)
	h.recordDashboard(r, &dash)
	respondSuccess(w, dash)
}

func (h *DashboardHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, _ := uuid.Parse(chi.URLParam(r, "id"))
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireAdmin(w, r, &dash) {
		return
	}
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	h.emit(r, "data.dashboard.deleted", &dash, actor)
	h.db.Where("id = ?", id).Delete(&domain.DashboardConfig{})
	// Cascade: remove dependent permissions and alerts.
	h.db.Where("dashboard_id = ?", id).Delete(&domain.DashboardPermission{})
	h.db.Where("dashboard_id = ?", id).Delete(&domain.DashboardAlert{})
	// §13.1 tombstone snapshot.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "dashboard", id.String(), map[string]any{
			"id":      id.String(),
			"deleted": true,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}
