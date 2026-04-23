package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/gorm"
)

// DashboardAlertHandler provides CRUD over DashboardAlert rows. Execution
// happens asynchronously in usecase.DashboardAlertWorker.
type DashboardAlertHandler struct {
	db     *gorm.DB
	pub    kafka.Publisher
	access *DashboardAccess
}

func NewDashboardAlertHandler(db *gorm.DB, pub kafka.Publisher, access *DashboardAccess) *DashboardAlertHandler {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	return &DashboardAlertHandler{db: db, pub: pub, access: access}
}

type createAlertRequest struct {
	PanelID        string     `json:"panel_id"`
	QueryID        *uuid.UUID `json:"query_id,omitempty"`
	ThresholdType  string     `json:"threshold_type"`
	ThresholdValue float64    `json:"threshold_value"`
	WindowSeconds  int        `json:"window_seconds"`
	NotifyChannel  string     `json:"notify_channel"`
	Active         *bool      `json:"active,omitempty"`
}

func (h *DashboardAlertHandler) Create(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid dashboard ID")
		return
	}
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireWrite(w, r, &dash) {
		return
	}

	var req createAlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	thresholdType := domain.DashboardAlertThresholdType(req.ThresholdType)
	switch thresholdType {
	case domain.AlertThresholdGT, domain.AlertThresholdLT, domain.AlertThresholdEQ, domain.AlertThresholdNEQ:
	default:
		respondBadRequest(w, "threshold_type must be gt|lt|eq|neq")
		return
	}
	if req.WindowSeconds <= 0 {
		req.WindowSeconds = 300
	}
	active := true
	if req.Active != nil {
		active = *req.Active
	}
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	alert := &domain.DashboardAlert{
		ID:             uuid.New(),
		OrganizationID: dash.OrganizationID,
		DashboardID:    dash.ID,
		PanelID:        req.PanelID,
		QueryID:        req.QueryID,
		ThresholdType:  thresholdType,
		ThresholdValue: req.ThresholdValue,
		WindowSeconds:  req.WindowSeconds,
		NotifyChannel:  req.NotifyChannel,
		Active:         active,
		CreatedBy:      actor,
	}
	if err := h.db.Create(alert).Error; err != nil {
		respondInternalError(w, "Failed to create alert: "+err.Error())
		return
	}
	respondCreated(w, alert)
}

func (h *DashboardAlertHandler) List(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid dashboard ID")
		return
	}
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireRead(w, r, &dash) {
		return
	}
	var alerts []domain.DashboardAlert
	h.db.Where("dashboard_id = ?", id).Order("created_at DESC").Find(&alerts)
	respondSuccess(w, map[string]any{"data": alerts})
}

func (h *DashboardAlertHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid dashboard ID")
		return
	}
	alertID, err := uuid.Parse(chi.URLParam(r, "alertId"))
	if err != nil {
		respondBadRequest(w, "Invalid alert ID")
		return
	}
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireWrite(w, r, &dash) {
		return
	}
	var alert domain.DashboardAlert
	if err := h.db.Where("id = ? AND dashboard_id = ?", alertID, id).First(&alert).Error; err != nil {
		respondNotFound(w, "Alert not found")
		return
	}
	var updates map[string]any
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		respondBadRequest(w, "Invalid body")
		return
	}
	h.db.Model(&alert).Updates(updates)
	h.db.Where("id = ?", alertID).First(&alert)
	respondSuccess(w, alert)
}

func (h *DashboardAlertHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid dashboard ID")
		return
	}
	alertID, err := uuid.Parse(chi.URLParam(r, "alertId"))
	if err != nil {
		respondBadRequest(w, "Invalid alert ID")
		return
	}
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireWrite(w, r, &dash) {
		return
	}
	h.db.Where("id = ? AND dashboard_id = ?", alertID, id).Delete(&domain.DashboardAlert{})
	w.WriteHeader(http.StatusNoContent)
}
