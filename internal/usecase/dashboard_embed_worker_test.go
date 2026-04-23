package usecase

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// silentLogger routes slog messages to io.Discard so other tests that
// nil-out the std log output don't trip slog's default handler.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newEmbedWorkerDB sets up an in-memory sqlite with just the
// dashboard_configs table; the embed worker never touches anything
// else. Each call gets its own unnamed in-memory handle so parallel
// tests don't collide through a shared cache.
func newEmbedWorkerDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&domain.DashboardConfig{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// TestDashboardEmbedExpiryWorker_DisablesExpiredEmbeds is the §12.5
// (C10) happy path: a dashboard whose EmbedTokenExpiresAt is in the
// past has EmbedEnabled flipped to false on the next sweep.
func TestDashboardEmbedExpiryWorker_DisablesExpiredEmbeds(t *testing.T) {
	db := newEmbedWorkerDB(t)
	pub := &capturingPublisher{}
	worker := NewDashboardEmbedExpiryWorker(db, pub, time.Hour)
	worker.SetLogger(silentLogger())

	past := time.Now().UTC().Add(-10 * time.Minute)
	future := time.Now().UTC().Add(10 * time.Minute)

	expired := &domain.DashboardConfig{
		ID:                  uuid.New(),
		OrganizationID:      uuid.New(),
		Name:                "expired",
		Version:             1,
		EmbedToken:          "expired-token",
		EmbedEnabled:        true,
		EmbedTokenExpiresAt: &past,
		CreatedBy:           uuid.New(),
		Panels:              domain.JSONMap{},
	}
	ok := &domain.DashboardConfig{
		ID:                  uuid.New(),
		OrganizationID:      uuid.New(),
		Name:                "ok",
		Version:             1,
		EmbedToken:          "ok-token",
		EmbedEnabled:        true,
		EmbedTokenExpiresAt: &future,
		CreatedBy:           uuid.New(),
		Panels:              domain.JSONMap{},
	}
	if err := db.Create(expired).Error; err != nil {
		t.Fatalf("seed expired: %v", err)
	}
	if err := db.Create(ok).Error; err != nil {
		t.Fatalf("seed ok: %v", err)
	}

	worker.tick(context.Background())

	// Inspect all rows directly — sqlite's UUID storage + GORM bound
	// parameter coercion mix poorly, so we don't use WHERE id = ?
	// predicates here.
	var all []domain.DashboardConfig
	if err := db.Find(&all).Error; err != nil {
		t.Fatalf("find all: %v", err)
	}
	byName := make(map[string]domain.DashboardConfig, len(all))
	for _, r := range all {
		byName[r.Name] = r
	}
	if byName["expired"].EmbedEnabled {
		t.Fatalf("expected expired dashboard to be disabled, still enabled")
	}
	if !byName["ok"].EmbedEnabled {
		t.Fatalf("expected in-date dashboard to remain enabled, was disabled")
	}

	// One expired dashboard → one published event.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.seen) != 1 {
		t.Fatalf("expected 1 event, got %d", len(pub.seen))
	}
	if pub.seen[0].EventType != "data.dashboard.embed.expired" {
		t.Fatalf("unexpected event type: %s", pub.seen[0].EventType)
	}
}

// TestDashboardEmbedExpiryWorker_Noop covers the quick path where
// nothing has expired — no DB writes, no events.
func TestDashboardEmbedExpiryWorker_Noop(t *testing.T) {
	db := newEmbedWorkerDB(t)
	pub := &capturingPublisher{}
	worker := NewDashboardEmbedExpiryWorker(db, pub, time.Hour)
	worker.SetLogger(silentLogger())

	future := time.Now().UTC().Add(time.Hour)
	row := &domain.DashboardConfig{
		ID:                  uuid.New(),
		OrganizationID:      uuid.New(),
		Name:                "alive",
		Version:             1,
		EmbedToken:          "tok",
		EmbedEnabled:        true,
		EmbedTokenExpiresAt: &future,
		CreatedBy:           uuid.New(),
		Panels:              domain.JSONMap{},
	}
	if err := db.Create(row).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	worker.tick(context.Background())

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.seen) != 0 {
		t.Fatalf("expected no events on quiet tick, got %d", len(pub.seen))
	}
}
