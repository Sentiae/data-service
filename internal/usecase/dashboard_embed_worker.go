package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
	"gorm.io/gorm"
)

// DashboardEmbedExpiryWorker flips `embed_enabled=false` on
// DashboardConfig rows whose `embed_token_expires_at` has passed.
// §12.5 (C10): the POST /dashboards/{id}/embed-token/rotate handler
// stamps a new expiry when it mints a token; this worker is the
// revocation half of the same loop so expired tokens stop being
// honoured without a manual DB update.
type DashboardEmbedExpiryWorker struct {
	db       *gorm.DB
	pub      kafka.Publisher
	interval time.Duration
	logger   *slog.Logger
}

// NewDashboardEmbedExpiryWorker returns a worker that sweeps at the
// given interval. Pass 0 to use the default of 1 hour.
func NewDashboardEmbedExpiryWorker(db *gorm.DB, pub kafka.Publisher, interval time.Duration) *DashboardEmbedExpiryWorker {
	if interval <= 0 {
		interval = time.Hour
	}
	if pub == nil {
		pub = kafka.NewNoopPublisher()
	}
	return &DashboardEmbedExpiryWorker{
		db:       db,
		pub:      pub,
		interval: interval,
		logger:   slog.Default(),
	}
}

// Start launches the worker loop. Returns immediately; cancel ctx to stop.
func (w *DashboardEmbedExpiryWorker) Start(ctx context.Context) {
	go func() {
		// Run once on startup so restarts catch up tokens that
		// expired while we were down.
		w.tick(ctx)
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.tick(ctx)
			}
		}
	}()
}

// tick disables embeds whose expiry has elapsed. Exposed for tests
// so the sweep can be invoked synchronously.
func (w *DashboardEmbedExpiryWorker) tick(ctx context.Context) {
	now := time.Now().UTC()
	var rows []domain.DashboardConfig
	if err := w.db.WithContext(ctx).
		Where("embed_enabled = ? AND embed_token_expires_at IS NOT NULL AND embed_token_expires_at <= ?", true, now).
		Find(&rows).Error; err != nil {
		w.logger.ErrorContext(ctx, "embed expiry sweep failed", "error", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	ids := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	if err := w.db.WithContext(ctx).
		Model(&domain.DashboardConfig{}).
		Where("id IN ?", ids).
		Updates(map[string]any{
			"embed_enabled": false,
			"updated_at":    now,
		}).Error; err != nil {
		w.logger.ErrorContext(ctx, "embed expiry disable failed", "error", err, "count", len(rows))
		return
	}

	w.logger.InfoContext(ctx, "embed tokens expired", "count", len(rows))
	if w.pub == nil {
		return
	}
	for _, r := range rows {
		_ = w.pub.Publish(ctx, "data.dashboard.embed.expired", kafka.EventData{
			ActorID:        "system",
			ResourceType:   "dashboard_config",
			ResourceID:     r.ID.String(),
			OrganizationID: r.OrganizationID.String(),
			Metadata: map[string]any{
				"dashboard_id": r.ID.String(),
				"expired_at":   now,
			},
		})
	}
}

// SetLogger overrides the default slog logger (for tests / structured
// logging configuration).
func (w *DashboardEmbedExpiryWorker) SetLogger(logger *slog.Logger) {
	if logger != nil {
		w.logger = logger
	}
}
