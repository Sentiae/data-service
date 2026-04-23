package domain

import (
	"time"

	"github.com/google/uuid"
)

// QueryHistoryEntry is one appended audit row for every NL→SQL or
// direct-SQL execution. §12.3 — supports "show me every query I ran
// today" and analytics on the NL→SQL accuracy. Rows are append-only;
// UpdatedAt exists only because GORM insists.
//
// Attribution: both org_id and user_id are stamped so the UI can show
// per-user history and operators can filter org-wide audit trails.
// Duration/row count/error mirror QueryExecution but are kept in a
// dedicated table so NL-only users don't have to join through DataQuery.
type QueryHistoryEntry struct {
	ID               uuid.UUID  `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID   uuid.UUID  `json:"organization_id" gorm:"type:uuid;not null;index:idx_query_hist_org"`
	UserID           uuid.UUID  `json:"user_id" gorm:"type:uuid;not null;index:idx_query_hist_user"`
	DataSourceID     *uuid.UUID `json:"data_source_id,omitempty" gorm:"type:uuid;index"`
	NaturalLanguage  string     `json:"natural_language,omitempty" gorm:"type:text"`
	GeneratedSQL     string     `json:"generated_sql" gorm:"type:text;not null"`
	RowCount         int        `json:"row_count" gorm:"default:0"`
	DurationMS       int64      `json:"duration_ms" gorm:"default:0"`
	Error            string     `json:"error,omitempty" gorm:"type:text"`
	ExecutedAt       time.Time  `json:"executed_at" gorm:"not null;index:idx_query_hist_executed"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// TableName makes the underscore-style table name explicit so
// migrations can reference it deterministically.
func (QueryHistoryEntry) TableName() string { return "query_history_entries" }

// SavedQuery is a user-named, user-shared bookmark of a query. §12.3.
// IsShared controls whether the row is visible to other org members;
// the owner can always see their own rows regardless.
type SavedQuery struct {
	ID              uuid.UUID  `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID  uuid.UUID  `json:"organization_id" gorm:"type:uuid;not null;index:idx_saved_q_org"`
	UserID          uuid.UUID  `json:"user_id" gorm:"type:uuid;not null;index:idx_saved_q_user"`
	Name            string     `json:"name" gorm:"type:varchar(255);not null"`
	Description     string     `json:"description,omitempty" gorm:"type:text"`
	NaturalLanguage string     `json:"natural_language,omitempty" gorm:"type:text"`
	GeneratedSQL    string     `json:"generated_sql" gorm:"type:text;not null"`
	DataSourceID    *uuid.UUID `json:"data_source_id,omitempty" gorm:"type:uuid;index"`
	IsShared        bool       `json:"is_shared" gorm:"not null;default:false;index:idx_saved_q_shared"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// TableName locks the table name for saved queries.
func (SavedQuery) TableName() string { return "saved_queries" }
