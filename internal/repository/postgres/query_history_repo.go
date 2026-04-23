package postgres

import (
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

// QueryHistoryRepo persists QueryHistoryEntry rows. §12.3.
type QueryHistoryRepo struct {
	db *gorm.DB
}

// NewQueryHistoryRepo wires a repository over the given GORM handle.
func NewQueryHistoryRepo(db *gorm.DB) *QueryHistoryRepo { return &QueryHistoryRepo{db: db} }

// Record appends a history row. Callers should populate ExecutedAt; if
// left zero we stamp time.Now() so the caller doesn't have to threads a
// clock helper through the handler.
func (r *QueryHistoryRepo) Record(entry *domain.QueryHistoryEntry) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.ExecutedAt.IsZero() {
		entry.ExecutedAt = time.Now()
	}
	return r.db.Create(entry).Error
}

// ListFilter controls the List query. UserID == nil means org-wide.
type ListHistoryFilter struct {
	OrganizationID uuid.UUID
	UserID         *uuid.UUID
	Since          *time.Time
	Until          *time.Time
	Limit          int
}

// List returns history rows ordered newest-first.
func (r *QueryHistoryRepo) List(filter ListHistoryFilter) ([]domain.QueryHistoryEntry, error) {
	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 100
	}
	q := r.db.Where("organization_id = ?", filter.OrganizationID)
	if filter.UserID != nil {
		q = q.Where("user_id = ?", *filter.UserID)
	}
	if filter.Since != nil {
		q = q.Where("executed_at >= ?", *filter.Since)
	}
	if filter.Until != nil {
		q = q.Where("executed_at <= ?", *filter.Until)
	}
	var rows []domain.QueryHistoryEntry
	if err := q.Order("executed_at DESC").Limit(filter.Limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}
