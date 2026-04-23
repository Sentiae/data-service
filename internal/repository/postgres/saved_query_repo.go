package postgres

import (
	"errors"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

// SavedQueryRepo is the Postgres-backed repository for SavedQuery. §12.3.
type SavedQueryRepo struct {
	db *gorm.DB
}

// ErrNotFound is returned when a saved query is missing or the caller
// has no access to it.
var ErrNotFound = errors.New("not found")

// NewSavedQueryRepo wires a repo.
func NewSavedQueryRepo(db *gorm.DB) *SavedQueryRepo { return &SavedQueryRepo{db: db} }

// Create inserts a new saved query. The ID is assigned by GORM via the
// `default:gen_random_uuid()` column default if the caller leaves it nil.
func (r *SavedQueryRepo) Create(q *domain.SavedQuery) error {
	if q.ID == uuid.Nil {
		q.ID = uuid.New()
	}
	return r.db.Create(q).Error
}

// Get returns a single saved query by id. Enforces org scope +
// (owner or is_shared) visibility via the `userID` filter: pass the
// caller's user id to mimic row-level-security inline.
func (r *SavedQueryRepo) Get(orgID, userID, id uuid.UUID) (*domain.SavedQuery, error) {
	var row domain.SavedQuery
	err := r.db.
		Where("id = ? AND organization_id = ? AND (user_id = ? OR is_shared = TRUE)", id, orgID, userID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// List returns the user's saved queries plus (optionally) shared rows
// from other users in the same org. Ordering is most-recent-first.
func (r *SavedQueryRepo) List(orgID, userID uuid.UUID, includeShared bool) ([]domain.SavedQuery, error) {
	q := r.db.Where("organization_id = ?", orgID)
	if includeShared {
		q = q.Where("user_id = ? OR is_shared = TRUE", userID)
	} else {
		q = q.Where("user_id = ?", userID)
	}
	var rows []domain.SavedQuery
	if err := q.Order("updated_at DESC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// Update applies partial changes to a row. Only the owner may mutate.
// Returns ErrNotFound if the row doesn't exist or the caller isn't the
// owner.
func (r *SavedQueryRepo) Update(orgID, userID, id uuid.UUID, updates map[string]any) (*domain.SavedQuery, error) {
	var row domain.SavedQuery
	err := r.db.Where("id = ? AND organization_id = ? AND user_id = ?", id, orgID, userID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := r.db.Model(&row).Updates(updates).Error; err != nil {
		return nil, err
	}
	// Reload to reflect the new updated_at.
	if err := r.db.Where("id = ?", id).First(&row).Error; err != nil {
		return nil, err
	}
	return &row, nil
}

// Delete removes a row. Only the owner may delete. Silently succeeds
// if the row is already gone (idempotent DELETE).
func (r *SavedQueryRepo) Delete(orgID, userID, id uuid.UUID) error {
	return r.db.Where("id = ? AND organization_id = ? AND user_id = ?", id, orgID, userID).Delete(&domain.SavedQuery{}).Error
}
