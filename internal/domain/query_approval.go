package domain

import (
	"time"

	"github.com/google/uuid"
)

// QueryApprovalStatus represents the lifecycle state of an approval request.
type QueryApprovalStatus string

const (
	QueryApprovalStatusPending  QueryApprovalStatus = "pending"
	QueryApprovalStatusApproved QueryApprovalStatus = "approved"
	QueryApprovalStatusRejected QueryApprovalStatus = "rejected"
	QueryApprovalStatusExecuted QueryApprovalStatus = "executed"
)

// QueryApproval represents a pending approval for a mutation query. Write
// queries (INSERT/UPDATE/DELETE/etc.) are not executed immediately; instead an
// approval record is created and the query only runs after an authorized user
// approves it.
type QueryApproval struct {
	ID             uuid.UUID           `json:"id" gorm:"type:uuid;primary_key"`
	QueryID        uuid.UUID           `json:"query_id" gorm:"type:uuid;not null;index"`
	OrganizationID uuid.UUID           `json:"organization_id" gorm:"type:uuid;not null;index"`
	RequestedBy    uuid.UUID           `json:"requested_by" gorm:"type:uuid;not null"`
	Status         QueryApprovalStatus `json:"status" gorm:"type:varchar(20);not null;default:'pending'"`
	Reason         string              `json:"reason,omitempty" gorm:"type:text"`
	SQLSnapshot    string              `json:"sql_snapshot" gorm:"type:text;not null"`
	DetectedOps    string              `json:"detected_ops,omitempty" gorm:"type:varchar(255)"`
	ApprovedBy     *uuid.UUID          `json:"approved_by,omitempty" gorm:"type:uuid"`
	ApprovedAt     *time.Time          `json:"approved_at,omitempty"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
}
