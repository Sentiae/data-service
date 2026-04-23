package domain

import (
	"time"

	"github.com/google/uuid"
)

// DashboardAlertThresholdType indicates how the observed value is compared
// against the configured threshold. Additional types can be introduced
// without breaking existing rows (e.g. "eq", "neq", "pct_change").
type DashboardAlertThresholdType string

const (
	AlertThresholdGT  DashboardAlertThresholdType = "gt"
	AlertThresholdLT  DashboardAlertThresholdType = "lt"
	AlertThresholdEQ  DashboardAlertThresholdType = "eq"
	AlertThresholdNEQ DashboardAlertThresholdType = "neq"
)

// DashboardAlert watches a specific panel within a dashboard and raises a
// Kafka event (routed by ops-service) whenever the panel's primary metric
// crosses the configured threshold within the evaluation window.
type DashboardAlert struct {
	ID             uuid.UUID `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID uuid.UUID `json:"organization_id" gorm:"type:uuid;not null;index"`
	DashboardID    uuid.UUID `json:"dashboard_id" gorm:"type:uuid;not null;index:idx_da_dashboard"`
	PanelID        string    `json:"panel_id" gorm:"type:varchar(100);not null"`
	// QueryID is optional — when set the worker re-runs that saved query;
	// otherwise it derives the query from the panel definition.
	QueryID        *uuid.UUID                  `json:"query_id,omitempty" gorm:"type:uuid"`
	ThresholdType  DashboardAlertThresholdType `json:"threshold_type" gorm:"type:varchar(10);not null"`
	ThresholdValue float64                     `json:"threshold_value" gorm:"not null"`
	WindowSeconds  int                         `json:"window_seconds" gorm:"not null;default:300"`
	NotifyChannel  string                      `json:"notify_channel" gorm:"type:varchar(255)"` // e.g., "slack:#ops", "email:ops@..."
	Active         bool                        `json:"active" gorm:"not null;default:true"`
	LastEvaluated  *time.Time                  `json:"last_evaluated,omitempty"`
	LastTriggered  *time.Time                  `json:"last_triggered,omitempty"`
	LastValue      *float64                    `json:"last_value,omitempty"`
	CreatedBy      uuid.UUID                   `json:"created_by" gorm:"type:uuid;not null"`
	CreatedAt      time.Time                   `json:"created_at"`
	UpdatedAt      time.Time                   `json:"updated_at"`
}

// Breached returns true when observed crosses threshold per the configured
// comparison type.
func (a *DashboardAlert) Breached(observed float64) bool {
	switch a.ThresholdType {
	case AlertThresholdGT:
		return observed > a.ThresholdValue
	case AlertThresholdLT:
		return observed < a.ThresholdValue
	case AlertThresholdEQ:
		return observed == a.ThresholdValue
	case AlertThresholdNEQ:
		return observed != a.ThresholdValue
	}
	return false
}
