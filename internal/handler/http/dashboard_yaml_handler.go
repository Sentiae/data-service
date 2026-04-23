package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/usecase"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/gorm"
)

// DashboardYAMLHandler exposes §12.5 dashboard-as-code submission.
// POST /data/dashboards/yaml accepts a YAML document (either as the
// raw request body when Content-Type is text/yaml or under a "yaml"
// field in a JSON wrapper). The payload is parsed, checksum-stamped,
// and upserted into dashboards_as_code. The live DashboardConfig row
// is then materialised so the existing dashboard API keeps working.
type DashboardYAMLHandler struct {
	db  *gorm.DB
	pub kafka.Publisher
}

// NewDashboardYAMLHandler wires the handler.
func NewDashboardYAMLHandler(db *gorm.DB, pub kafka.Publisher) *DashboardYAMLHandler {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	return &DashboardYAMLHandler{db: db, pub: pub}
}

// RegisterRoutes mounts the dashboard-as-code routes.
func (h *DashboardYAMLHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data/dashboards/yaml", func(r chi.Router) {
		r.Post("/", h.Submit)
		r.Get("/", h.List)
		r.Get("/{id}", h.Get)
	})
}

// submitRequest supports the JSON-wrapped form of a YAML submission
// so callers that don't set Content-Type can still POST. When the
// body is submitted as raw YAML, decoding drops straight to the
// usecase parser.
type submitRequest struct {
	YAML string `json:"yaml"`
}

// Submit handles POST /data/dashboards/yaml. Parses the YAML, stamps
// a checksum, upserts the dashboards_as_code row, and materialises
// the live DashboardConfig row.
func (h *DashboardYAMLHandler) Submit(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	userID := r.Header.Get("X-User-ID")
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	userUUID, _ := uuid.Parse(userID)

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		respondBadRequest(w, "Failed to read body: "+err.Error())
		return
	}
	defer r.Body.Close()

	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	var yamlBytes []byte
	switch {
	case strings.Contains(contentType, "yaml"),
		strings.Contains(contentType, "text/plain"),
		contentType == "":
		yamlBytes = raw
	default:
		var req submitRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			respondBadRequest(w, "Invalid JSON body: "+err.Error())
			return
		}
		yamlBytes = []byte(req.YAML)
	}

	dash, err := usecase.ParseDashboardYAML(yamlBytes)
	if err != nil {
		respondBadRequest(w, err.Error())
		return
	}
	checksum := usecase.ChecksumYAML(yamlBytes)

	// Upsert on (organization_id, slug). Same checksum → no-op; new
	// checksum → version increments.
	var existing domain.DashboardAsCode
	err = h.db.Where("organization_id = ? AND slug = ?", orgUUID, dash.Slug).
		Order("version DESC").
		First(&existing).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		respondInternalError(w, "lookup failed: "+err.Error())
		return
	}
	if err == nil && existing.Checksum == checksum {
		// Idempotent re-submission.
		respondSuccess(w, existing)
		return
	}

	parsedMap := map[string]any{
		"queries":     dash.Queries,
		"panels":      dash.Panels,
		"tags":        dash.Tags,
		"variables":   dash.Variables,
		"description": dash.Description,
	}

	row := &domain.DashboardAsCode{
		ID:             uuid.New(),
		OrganizationID: orgUUID,
		Slug:           dash.Slug,
		Name:           dash.Name,
		Description:    dash.Description,
		Version:        existing.Version + 1,
		Checksum:       checksum,
		RawYAML:        string(yamlBytes),
		ParsedSpec:     domain.JSONMap(parsedMap),
		CreatedBy:      userUUID,
	}
	if existing.ID != uuid.Nil && existing.DashboardConfigID != nil {
		row.DashboardConfigID = existing.DashboardConfigID
	}
	if err := h.db.Create(row).Error; err != nil {
		respondInternalError(w, "Failed to persist dashboard YAML: "+err.Error())
		return
	}

	// Materialise the live DashboardConfig row. When an existing link
	// exists we update it; otherwise create a fresh one. Keeping this
	// sync makes the YAML path immediately queryable via the legacy
	// GET /data/dashboards endpoints.
	cfg := &domain.DashboardConfig{
		ID:             uuid.Nil,
		OrganizationID: orgUUID,
		Name:           dash.Name,
		Description:    dash.Description,
		Version:        row.Version,
		Panels: domain.JSONMap{
			"panels":  dash.Panels,
			"queries": dash.Queries,
		},
		SourceYAML:   string(yamlBytes),
		SourceDigest: checksum,
	}
	if row.DashboardConfigID != nil {
		cfg.ID = *row.DashboardConfigID
		if err := h.db.Model(&domain.DashboardConfig{}).
			Where("id = ?", cfg.ID).
			Updates(map[string]any{
				"name":          cfg.Name,
				"description":   cfg.Description,
				"version":       cfg.Version,
				"panels":        cfg.Panels,
				"source_yaml":   cfg.SourceYAML,
				"source_digest": cfg.SourceDigest,
			}).Error; err != nil {
			respondInternalError(w, "live dashboard update failed: "+err.Error())
			return
		}
	} else {
		cfg.ID = uuid.New()
		if err := h.db.Create(cfg).Error; err != nil {
			respondInternalError(w, "live dashboard create failed: "+err.Error())
			return
		}
		// Persist the link back onto the as-code row.
		h.db.Model(row).Update("dashboard_config_id", cfg.ID)
		row.DashboardConfigID = &cfg.ID
	}

	if h.pub != nil {
		_ = h.pub.Publish(r.Context(), "data.dashboard.yaml.submitted", kafka.EventData{
			ActorID:        userUUID.String(),
			ResourceType:   "dashboard_as_code",
			ResourceID:     row.ID.String(),
			OrganizationID: orgUUID.String(),
			Metadata: map[string]any{
				"slug":      dash.Slug,
				"version":   row.Version,
				"checksum":  checksum,
				"panels":    len(dash.Panels),
				"queries":   len(dash.Queries),
				"dashboard": row.DashboardConfigID,
			},
		})
	}

	respondCreated(w, row)
}

// List handles GET /data/dashboards/yaml. Returns the latest version
// of every dashboard-as-code row for the caller's org.
func (h *DashboardYAMLHandler) List(w http.ResponseWriter, r *http.Request) {
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		respondBadRequest(w, "Invalid X-Organization-ID")
		return
	}
	// Grab the max version per slug.
	var rows []domain.DashboardAsCode
	err = h.db.Raw(`
		SELECT d.*
		FROM dashboards_as_code d
		INNER JOIN (
		    SELECT slug, MAX(version) AS max_version
		    FROM dashboards_as_code
		    WHERE organization_id = ?
		    GROUP BY slug
		) latest
		ON d.slug = latest.slug AND d.version = latest.max_version
		WHERE d.organization_id = ?
		ORDER BY d.updated_at DESC
		LIMIT 200
	`, orgUUID, orgUUID).Scan(&rows).Error
	if err != nil {
		respondInternalError(w, err.Error())
		return
	}
	respondSuccess(w, map[string]any{"data": rows})
}

// Get handles GET /data/dashboards/yaml/{id}. Returns the raw YAML
// alongside the parsed spec.
func (h *DashboardYAMLHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid id")
		return
	}
	orgID := r.Header.Get("X-Organization-ID")
	orgUUID, _ := uuid.Parse(orgID)
	var row domain.DashboardAsCode
	if err := h.db.Where("id = ? AND organization_id = ?", id, orgUUID).First(&row).Error; err != nil {
		respondNotFound(w, "Dashboard YAML not found")
		return
	}
	respondSuccess(w, row)
}
