package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/gorm"
)

// DashboardAccess evaluates whether a user has the required permission on a
// given dashboard. It combines ownership (creator is admin), explicit
// DashboardPermission rows, and an org-wide implicit view for members of
// the same organization.
type DashboardAccess struct {
	db *gorm.DB
}

func NewDashboardAccess(db *gorm.DB) *DashboardAccess { return &DashboardAccess{db: db} }

// HighestPermission returns the most permissive grant the user holds for
// this dashboard. Returns "" when no grant applies.
func (a *DashboardAccess) HighestPermission(dash *domain.DashboardConfig, userID, orgID uuid.UUID, teamIDs []uuid.UUID) domain.DashboardPermissionLevel {
	if dash == nil {
		return ""
	}
	// Creator always has admin.
	if dash.CreatedBy == userID {
		return domain.DashboardPermAdmin
	}

	// Collect all permissions matching any of the principals.
	var grants []domain.DashboardPermission
	q := a.db.Where("dashboard_id = ?", dash.ID)
	principalExprs := []string{}
	args := []any{}
	// user
	if userID != uuid.Nil {
		principalExprs = append(principalExprs, "(principal_type = ? AND principal_id = ?)")
		args = append(args, domain.DashboardPrincipalUser, userID)
	}
	// org
	if orgID != uuid.Nil {
		principalExprs = append(principalExprs, "(principal_type = ? AND principal_id = ?)")
		args = append(args, domain.DashboardPrincipalOrg, orgID)
	}
	// teams
	for _, tid := range teamIDs {
		principalExprs = append(principalExprs, "(principal_type = ? AND principal_id = ?)")
		args = append(args, domain.DashboardPrincipalTeam, tid)
	}
	if len(principalExprs) > 0 {
		// Build a big OR.
		where := principalExprs[0]
		for i := 1; i < len(principalExprs); i++ {
			where += " OR " + principalExprs[i]
		}
		q = q.Where(where, args...)
	}
	_ = q.Find(&grants).Error

	best := domain.DashboardPermissionLevel("")
	for _, g := range grants {
		if g.Permission.LevelRank() > best.LevelRank() {
			best = g.Permission
		}
	}
	return best
}

// RequireRead ensures the calling user has at least view rights. Writes a
// 403 and returns false when denied; returns true when allowed.
func (a *DashboardAccess) RequireRead(w http.ResponseWriter, r *http.Request, dash *domain.DashboardConfig) bool {
	return a.requireLevel(w, r, dash, domain.DashboardPermView)
}

// RequireWrite ensures edit rights.
func (a *DashboardAccess) RequireWrite(w http.ResponseWriter, r *http.Request, dash *domain.DashboardConfig) bool {
	return a.requireLevel(w, r, dash, domain.DashboardPermEdit)
}

// RequireAdmin ensures admin rights (share/delete).
func (a *DashboardAccess) RequireAdmin(w http.ResponseWriter, r *http.Request, dash *domain.DashboardConfig) bool {
	return a.requireLevel(w, r, dash, domain.DashboardPermAdmin)
}

func (a *DashboardAccess) requireLevel(w http.ResponseWriter, r *http.Request, dash *domain.DashboardConfig, need domain.DashboardPermissionLevel) bool {
	userID, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	orgID, _ := uuid.Parse(r.Header.Get("X-Organization-ID"))
	teamIDs := parseTeamIDs(r.Header.Get("X-Team-IDs"))

	// Org members implicitly have view rights on any dashboard in their org.
	if need == domain.DashboardPermView && orgID != uuid.Nil && dash.OrganizationID == orgID {
		return true
	}

	level := a.HighestPermission(dash, userID, orgID, teamIDs)
	if level.LevelRank() >= need.LevelRank() {
		return true
	}
	respondError(w, http.StatusForbidden, "FORBIDDEN", "Insufficient permission for dashboard")
	return false
}

func parseTeamIDs(header string) []uuid.UUID {
	if header == "" {
		return nil
	}
	out := []uuid.UUID{}
	for _, s := range splitCSV(header) {
		if u, err := uuid.Parse(s); err == nil {
			out = append(out, u)
		}
	}
	return out
}

func splitCSV(s string) []string {
	out := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if i > start {
				out = append(out, trimSpace(s[start:i]))
			}
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, trimSpace(s[start:]))
	}
	return out
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// -------------------------------------------------------------------------
// HTTP handlers for DashboardPermission CRUD.
// -------------------------------------------------------------------------

type DashboardPermissionHandler struct {
	db     *gorm.DB
	pub    kafka.Publisher
	access *DashboardAccess
}

func NewDashboardPermissionHandler(db *gorm.DB, pub kafka.Publisher, access *DashboardAccess) *DashboardPermissionHandler {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	return &DashboardPermissionHandler{db: db, pub: pub, access: access}
}

type sharePermissionRequest struct {
	PrincipalType string    `json:"principal_type"`
	PrincipalID   uuid.UUID `json:"principal_id"`
	Permission    string    `json:"permission"`
}

// CreatePermission grants a new permission on the dashboard.
func (h *DashboardPermissionHandler) CreatePermission(w http.ResponseWriter, r *http.Request) {
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
	if !h.access.RequireAdmin(w, r, &dash) {
		return
	}

	var req sharePermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondBadRequest(w, "Invalid request body")
		return
	}
	principalType := domain.DashboardPrincipalType(req.PrincipalType)
	switch principalType {
	case domain.DashboardPrincipalUser, domain.DashboardPrincipalTeam, domain.DashboardPrincipalOrg:
	default:
		respondBadRequest(w, "principal_type must be 'user', 'team', or 'org'")
		return
	}
	perm := domain.DashboardPermissionLevel(req.Permission)
	if perm.LevelRank() == 0 {
		perm = domain.DashboardPermView
	}
	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	p := &domain.DashboardPermission{
		ID:            uuid.New(),
		DashboardID:   dash.ID,
		PrincipalType: principalType,
		PrincipalID:   req.PrincipalID,
		Permission:    perm,
		GrantedBy:     actor,
	}
	if err := h.db.Create(p).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			respondBadRequest(w, "Permission already exists")
			return
		}
		respondInternalError(w, "Failed to create permission: "+err.Error())
		return
	}

	// Emit shared event.
	_ = h.pub.Publish(r.Context(), "data.dashboard.shared", kafka.EventData{
		ActorID:        actor.String(),
		ResourceType:   "dashboard_config",
		ResourceID:     dash.ID.String(),
		OrganizationID: dash.OrganizationID.String(),
		Metadata: map[string]any{
			"dashboard_id":   dash.ID.String(),
			"permission_id":  p.ID.String(),
			"principal_type": string(principalType),
			"principal_id":   p.PrincipalID.String(),
			"permission":     string(perm),
			"scope":          "rbac",
		},
	})
	respondCreated(w, p)
}

// ListPermissions returns all permission rows for a dashboard.
func (h *DashboardPermissionHandler) ListPermissions(w http.ResponseWriter, r *http.Request) {
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
	var perms []domain.DashboardPermission
	h.db.Where("dashboard_id = ?", id).Order("created_at DESC").Find(&perms)
	respondSuccess(w, map[string]any{"data": perms})
}

// DeletePermission revokes a single permission row.
func (h *DashboardPermissionHandler) DeletePermission(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "Invalid dashboard ID")
		return
	}
	permID, err := uuid.Parse(chi.URLParam(r, "permId"))
	if err != nil {
		respondBadRequest(w, "Invalid permission ID")
		return
	}
	var dash domain.DashboardConfig
	if err := h.db.Where("id = ?", id).First(&dash).Error; err != nil {
		respondNotFound(w, "Dashboard not found")
		return
	}
	if !h.access.RequireAdmin(w, r, &dash) {
		return
	}
	if err := h.db.Where("id = ? AND dashboard_id = ?", permID, id).Delete(&domain.DashboardPermission{}).Error; err != nil {
		respondInternalError(w, "Failed to revoke permission")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
