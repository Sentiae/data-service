package http

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"github.com/sentiae/platform-kit/timetravel"
	"gorm.io/gorm"
)

// DashboardEmbedHandler owns the §12.5 (C10) embed-token rotation
// surface: a token + expiry stamped on DashboardConfig rows so
// operators can refresh the secret without tearing down the dashboard.
type DashboardEmbedHandler struct {
	db       *gorm.DB
	pub      kafka.Publisher
	access   *DashboardAccess
	recorder timetravel.Recorder
}

// NewDashboardEmbedHandler wires the handler.
func NewDashboardEmbedHandler(db *gorm.DB, pub kafka.Publisher, access *DashboardAccess) *DashboardEmbedHandler {
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	return &DashboardEmbedHandler{db: db, pub: pub, access: access, recorder: timetravel.NoopRecorder{}}
}

// WithTimeTravelRecorder wires the §13.1 entity-snapshot recorder so
// embed-token rotations produce a snapshot row with the new token hash.
func (h *DashboardEmbedHandler) WithTimeTravelRecorder(r timetravel.Recorder) *DashboardEmbedHandler {
	if r != nil {
		h.recorder = r
	}
	return h
}

// RegisterRoutes mounts the rotate endpoint. Other embed operations
// (enable/disable, read) already live on the main DashboardHandler.
func (h *DashboardEmbedHandler) RegisterRoutes(r chi.Router) {
	r.Post("/data/dashboards/{id}/embed-token/rotate", h.Rotate)
}

type rotateEmbedTokenResponse struct {
	DashboardID uuid.UUID `json:"dashboard_id"`
	Token       string    `json:"embed_token"`
	ExpiresAt   time.Time `json:"embed_token_expires_at"`
	Enabled     bool      `json:"embed_enabled"`
}

// Rotate replaces EmbedToken with a fresh random secret and stamps a
// new expiry. The old token is immediately invalidated because
// EmbedToken has a uniqueIndex — the update supersedes it in place.
func (h *DashboardEmbedHandler) Rotate(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondBadRequest(w, "invalid dashboard id")
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

	token, err := generateEmbedToken()
	if err != nil {
		respondInternalError(w, "failed to mint embed token")
		return
	}
	ttl := embedTokenTTL()
	expires := time.Now().UTC().Add(ttl)
	updates := map[string]any{
		"embed_token":            token,
		"embed_enabled":          true,
		"embed_token_expires_at": expires,
		"updated_at":             time.Now().UTC(),
	}
	if err := h.db.Model(&dash).Updates(updates).Error; err != nil {
		respondInternalError(w, "failed to persist rotated token")
		return
	}
	dash.EmbedToken = token
	dash.EmbedEnabled = true
	dash.EmbedTokenExpiresAt = &expires

	actor, _ := uuid.Parse(r.Header.Get("X-User-ID"))
	if h.pub != nil {
		_ = h.pub.Publish(r.Context(), "data.dashboard.embed.rotated", kafka.EventData{
			ActorID:        actor.String(),
			ResourceType:   "dashboard_config",
			ResourceID:     dash.ID.String(),
			OrganizationID: dash.OrganizationID.String(),
			Metadata: map[string]any{
				"dashboard_id": dash.ID.String(),
				"expires_at":   expires,
				"ttl_seconds":  int(ttl.Seconds()),
			},
		})
	}
	// §13.1 — embed token rotation is security-sensitive; record the
	// updated dashboard so auditors can reconstruct the enable/disable
	// + expiry timeline. We deliberately avoid recording the token
	// itself; the snapshot payload captures the expiry + enabled flag
	// only.
	if h.recorder != nil {
		_ = h.recorder.RecordEntity(r.Context(), "dashboard_embed", dash.ID.String(), map[string]any{
			"dashboard_id":  dash.ID.String(),
			"enabled":       true,
			"expires_at":    expires,
			"rotated_by":    actor.String(),
			"rotation_time": time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
	respondSuccess(w, rotateEmbedTokenResponse{
		DashboardID: dash.ID,
		Token:       token,
		ExpiresAt:   expires,
		Enabled:     true,
	})
}

// generateEmbedToken returns a 32-byte hex string. Fits in varchar(64).
func generateEmbedToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// embedTokenTTL picks the default lifetime of a rotated token (30
// days) or an override from APP_DATA_EMBED_TOKEN_TTL_SECONDS.
func embedTokenTTL() time.Duration {
	raw := os.Getenv("APP_DATA_EMBED_TOKEN_TTL_SECONDS")
	if raw == "" {
		return 30 * 24 * time.Hour
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 30 * 24 * time.Hour
	}
	return time.Duration(n) * time.Second
}

// decode helper used by unit tests / future body parsing.
var _ = json.NewDecoder
