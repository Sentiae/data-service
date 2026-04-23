package usecase

import (
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/data-service/internal/repository/postgres"
)

// QueryHistoryService is the usecase-layer facade over the query
// history + saved query repositories. §12.3.
//
// The handler injects this service and calls RecordQueryHistory on
// every execution (success + error); the portal reads via
// ListQueryHistory and the saved-query CRUD methods.
type QueryHistoryService struct {
	history *postgres.QueryHistoryRepo
	saved   *postgres.SavedQueryRepo
}

// NewQueryHistoryService wires both repos behind one facade.
func NewQueryHistoryService(history *postgres.QueryHistoryRepo, saved *postgres.SavedQueryRepo) *QueryHistoryService {
	return &QueryHistoryService{history: history, saved: saved}
}

// RecordQueryHistoryInput is the wire-friendly shape handlers send in.
type RecordQueryHistoryInput struct {
	OrganizationID  uuid.UUID
	UserID          uuid.UUID
	DataSourceID    *uuid.UUID
	NaturalLanguage string
	GeneratedSQL    string
	RowCount        int
	DurationMS      int64
	Error           string
	ExecutedAt      time.Time
}

// RecordQueryHistory inserts a new history row. Errors are returned
// rather than swallowed so callers can decide whether to surface a
// warning to the user.
func (s *QueryHistoryService) RecordQueryHistory(input RecordQueryHistoryInput) error {
	if s == nil || s.history == nil {
		return nil
	}
	entry := &domain.QueryHistoryEntry{
		OrganizationID:  input.OrganizationID,
		UserID:          input.UserID,
		DataSourceID:    input.DataSourceID,
		NaturalLanguage: input.NaturalLanguage,
		GeneratedSQL:    input.GeneratedSQL,
		RowCount:        input.RowCount,
		DurationMS:      input.DurationMS,
		Error:           input.Error,
		ExecutedAt:      input.ExecutedAt,
	}
	return s.history.Record(entry)
}

// ListQueryHistory returns recent history rows. Pass userID=nil to
// fetch org-wide (admins / support tooling); otherwise the result is
// scoped to one user.
func (s *QueryHistoryService) ListQueryHistory(orgID uuid.UUID, userID *uuid.UUID, since, until *time.Time, limit int) ([]domain.QueryHistoryEntry, error) {
	if s == nil || s.history == nil {
		return nil, nil
	}
	return s.history.List(postgres.ListHistoryFilter{
		OrganizationID: orgID,
		UserID:         userID,
		Since:          since,
		Until:          until,
		Limit:          limit,
	})
}

// SaveQueryInput is the wire-friendly shape for POST /queries/saved.
type SaveQueryInput struct {
	OrganizationID  uuid.UUID
	UserID          uuid.UUID
	Name            string
	Description     string
	NaturalLanguage string
	GeneratedSQL    string
	DataSourceID    *uuid.UUID
	IsShared        bool
}

// SaveQuery inserts a new SavedQuery row.
func (s *QueryHistoryService) SaveQuery(input SaveQueryInput) (*domain.SavedQuery, error) {
	row := &domain.SavedQuery{
		OrganizationID:  input.OrganizationID,
		UserID:          input.UserID,
		Name:            input.Name,
		Description:     input.Description,
		NaturalLanguage: input.NaturalLanguage,
		GeneratedSQL:    input.GeneratedSQL,
		DataSourceID:    input.DataSourceID,
		IsShared:        input.IsShared,
	}
	if err := s.saved.Create(row); err != nil {
		return nil, err
	}
	return row, nil
}

// ListSavedQueries returns the user's saved queries.
func (s *QueryHistoryService) ListSavedQueries(orgID, userID uuid.UUID, includeShared bool) ([]domain.SavedQuery, error) {
	return s.saved.List(orgID, userID, includeShared)
}

// GetSavedQuery returns one saved query if the caller has access.
func (s *QueryHistoryService) GetSavedQuery(orgID, userID, id uuid.UUID) (*domain.SavedQuery, error) {
	return s.saved.Get(orgID, userID, id)
}

// UpdateSavedQuery applies a partial update. Only the owner may mutate.
func (s *QueryHistoryService) UpdateSavedQuery(orgID, userID, id uuid.UUID, updates map[string]any) (*domain.SavedQuery, error) {
	return s.saved.Update(orgID, userID, id, updates)
}

// DeleteSavedQuery removes a row. Only the owner may delete.
func (s *QueryHistoryService) DeleteSavedQuery(orgID, userID, id uuid.UUID) error {
	return s.saved.Delete(orgID, userID, id)
}
