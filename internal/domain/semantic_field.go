package domain

import (
	"time"

	"github.com/google/uuid"
)

// SemanticField maps a raw database column to a business-friendly name and description.
// This is the core of the semantic layer — it lets users query in natural language.
type SemanticField struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;primary_key"`
	DataSourceID uuid.UUID `json:"data_source_id" gorm:"type:uuid;not null;index"`
	TableName    string    `json:"table_name" gorm:"type:varchar(255);not null;index:idx_sf_table_col"`
	ColumnName   string    `json:"column_name" gorm:"type:varchar(255);not null;index:idx_sf_table_col"`
	BusinessName string    `json:"business_name" gorm:"type:varchar(255);not null"` // e.g., "Monthly Recurring Revenue"
	Description  string    `json:"description" gorm:"type:text"`                    // e.g., "Sum of active subscription values"
	DataType     string    `json:"data_type" gorm:"type:varchar(50)"`               // e.g., "currency", "count", "percentage"
	Aggregation  string    `json:"aggregation" gorm:"type:varchar(50)"`             // e.g., "sum", "avg", "count", "max"
	Unit         string    `json:"unit,omitempty" gorm:"type:varchar(50)"`          // e.g., "USD", "users", "%"
	// RequiredRole, when non-empty, restricts access to users holding the
	// named role. Evaluated by the PermissionChecker at result time.
	RequiredRole string `json:"required_role,omitempty" gorm:"type:varchar(100)"`
	// Synonyms are alternate terms an NL prompt might use for this field
	// (e.g., "revenue", "earnings" for MRR). Synonyms are suggested by
	// admins and surface directly in the NL→SQL prompt.
	Synonyms StringArray `json:"synonyms,omitempty" gorm:"type:jsonb;serializer:json"`
	// Aliases are alternate fully-qualified column names that resolve to
	// this field (e.g., "subscriptions.amount" ↔ "subs.amt"). Useful
	// when multiple data sources share a semantic concept.
	Aliases   StringArray `json:"aliases,omitempty" gorm:"type:jsonb;serializer:json"`
	Tags      StringArray `json:"tags,omitempty" gorm:"type:jsonb"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}
