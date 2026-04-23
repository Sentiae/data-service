package usecase

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/repository/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newHistoryTestDB migrates QueryHistoryEntry + SavedQuery in a
// sqlite-in-memory DB. The GORM models carry type:uuid tags that
// sqlite treats as opaque TEXT; the tests fill in UUIDs explicitly.
func newHistoryTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&domain.QueryHistoryEntry{}, &domain.SavedQuery{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func newHistoryService(t *testing.T) (*QueryHistoryService, *gorm.DB) {
	t.Helper()
	db := newHistoryTestDB(t)
	return NewQueryHistoryService(
		postgres.NewQueryHistoryRepo(db),
		postgres.NewSavedQueryRepo(db),
	), db
}

func TestRecordQueryHistory_InsertsRow(t *testing.T) {
	svc, db := newHistoryService(t)
	org := uuid.New()
	user := uuid.New()

	err := svc.RecordQueryHistory(RecordQueryHistoryInput{
		OrganizationID: org,
		UserID:         user,
		GeneratedSQL:   "SELECT 1",
		ExecutedAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var count int64
	db.Model(&domain.QueryHistoryEntry{}).Count(&count)
	if count != 1 {
		t.Errorf("expected 1 history row, got %d", count)
	}
}

func TestListQueryHistory_FiltersByUser(t *testing.T) {
	svc, _ := newHistoryService(t)
	org := uuid.New()
	userA := uuid.New()
	userB := uuid.New()
	base := time.Now()

	for _, u := range []uuid.UUID{userA, userA, userB} {
		_ = svc.RecordQueryHistory(RecordQueryHistoryInput{
			OrganizationID: org,
			UserID:         u,
			GeneratedSQL:   "SELECT 1",
			ExecutedAt:     base,
		})
	}

	got, err := svc.ListQueryHistory(org, &userA, nil, nil, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 rows for userA, got %d", len(got))
	}
	for _, r := range got {
		if r.UserID != userA {
			t.Errorf("leaked row for userB: %+v", r)
		}
	}

	// Org-wide (nil user)
	all, _ := svc.ListQueryHistory(org, nil, nil, nil, 0)
	if len(all) != 3 {
		t.Errorf("expected 3 rows org-wide, got %d", len(all))
	}
}

func TestListQueryHistory_SinceUntil(t *testing.T) {
	svc, _ := newHistoryService(t)
	org := uuid.New()
	user := uuid.New()

	old := time.Now().Add(-24 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)

	_ = svc.RecordQueryHistory(RecordQueryHistoryInput{OrganizationID: org, UserID: user, GeneratedSQL: "a", ExecutedAt: old})
	_ = svc.RecordQueryHistory(RecordQueryHistoryInput{OrganizationID: org, UserID: user, GeneratedSQL: "b", ExecutedAt: recent})

	since := time.Now().Add(-2 * time.Hour)
	got, _ := svc.ListQueryHistory(org, &user, &since, nil, 0)
	if len(got) != 1 {
		t.Errorf("expected 1 recent row, got %d", len(got))
	}
}

func TestSaveQuery_CreateListGetUpdateDelete(t *testing.T) {
	svc, _ := newHistoryService(t)
	org := uuid.New()
	user := uuid.New()

	saved, err := svc.SaveQuery(SaveQueryInput{
		OrganizationID: org,
		UserID:         user,
		Name:           "Top 10 customers",
		GeneratedSQL:   "SELECT * FROM customers ORDER BY revenue DESC LIMIT 10",
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if saved.ID == uuid.Nil {
		t.Errorf("expected id to be set")
	}

	list, err := svc.ListSavedQueries(org, user, false)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 saved query, got %d", len(list))
	}

	got, err := svc.GetSavedQuery(org, user, saved.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "Top 10 customers" {
		t.Errorf("wrong name: %s", got.Name)
	}

	updated, err := svc.UpdateSavedQuery(org, user, saved.ID, map[string]any{"name": "Top 20"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Name != "Top 20" {
		t.Errorf("update did not apply: %+v", updated)
	}

	if err := svc.DeleteSavedQuery(org, user, saved.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := svc.GetSavedQuery(org, user, saved.ID); err == nil {
		t.Errorf("expected ErrNotFound after delete")
	}
}

func TestSaveQuery_SharedVisibility(t *testing.T) {
	svc, _ := newHistoryService(t)
	org := uuid.New()
	owner := uuid.New()
	other := uuid.New()

	_, _ = svc.SaveQuery(SaveQueryInput{
		OrganizationID: org,
		UserID:         owner,
		Name:           "private",
		GeneratedSQL:   "SELECT 1",
		IsShared:       false,
	})
	shared, _ := svc.SaveQuery(SaveQueryInput{
		OrganizationID: org,
		UserID:         owner,
		Name:           "shared",
		GeneratedSQL:   "SELECT 2",
		IsShared:       true,
	})

	// Other user should see only the shared row when include_shared=true.
	list, _ := svc.ListSavedQueries(org, other, true)
	if len(list) != 1 || list[0].ID != shared.ID {
		t.Errorf("expected only shared row visible, got %+v", list)
	}

	// include_shared=false restricts to user's own rows (none).
	list, _ = svc.ListSavedQueries(org, other, false)
	if len(list) != 0 {
		t.Errorf("expected zero rows with include_shared=false, got %d", len(list))
	}
}

func TestSaveQuery_UpdateNotOwnerForbidden(t *testing.T) {
	svc, _ := newHistoryService(t)
	org := uuid.New()
	owner := uuid.New()
	other := uuid.New()

	saved, _ := svc.SaveQuery(SaveQueryInput{
		OrganizationID: org,
		UserID:         owner,
		Name:           "n",
		GeneratedSQL:   "s",
	})

	_, err := svc.UpdateSavedQuery(org, other, saved.ID, map[string]any{"name": "hijacked"})
	if err == nil {
		t.Errorf("expected error when non-owner attempts update")
	}
}
