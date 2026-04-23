package domain

import (
	"time"

	"github.com/google/uuid"
)

// DashboardPrincipalType identifies the kind of principal a permission grant
// applies to: an individual user, a team, or a whole organization.
type DashboardPrincipalType string

const (
	DashboardPrincipalUser DashboardPrincipalType = "user"
	DashboardPrincipalTeam DashboardPrincipalType = "team"
	DashboardPrincipalOrg  DashboardPrincipalType = "org"
)

// DashboardPermissionLevel describes the capabilities granted by a share.
type DashboardPermissionLevel string

const (
	DashboardPermView  DashboardPermissionLevel = "view"
	DashboardPermEdit  DashboardPermissionLevel = "edit"
	DashboardPermAdmin DashboardPermissionLevel = "admin"
)

// DashboardPermission is an RBAC grant on a dashboard. Multiple rows can
// exist for a single dashboard/principal combination — the effective
// permission is the highest-ranked level.
type DashboardPermission struct {
	ID            uuid.UUID                `json:"id" gorm:"type:uuid;primary_key"`
	DashboardID   uuid.UUID                `json:"dashboard_id" gorm:"type:uuid;not null;index:idx_dp_dashboard"`
	PrincipalType DashboardPrincipalType   `json:"principal_type" gorm:"type:varchar(20);not null;index:idx_dp_principal"`
	PrincipalID   uuid.UUID                `json:"principal_id" gorm:"type:uuid;not null;index:idx_dp_principal"`
	Permission    DashboardPermissionLevel `json:"permission" gorm:"type:varchar(20);not null;default:'view'"`
	GrantedBy     uuid.UUID                `json:"granted_by" gorm:"type:uuid;not null"`
	CreatedAt     time.Time                `json:"created_at"`
	UpdatedAt     time.Time                `json:"updated_at"`
}

// LevelRank orders permission levels so callers can compare grants.
func (p DashboardPermissionLevel) LevelRank() int {
	switch p {
	case DashboardPermAdmin:
		return 3
	case DashboardPermEdit:
		return 2
	case DashboardPermView:
		return 1
	}
	return 0
}

// AllowsRead returns true when the grant permits reading the dashboard.
func (p DashboardPermissionLevel) AllowsRead() bool { return p.LevelRank() >= 1 }

// AllowsWrite returns true when the grant permits editing the dashboard.
func (p DashboardPermissionLevel) AllowsWrite() bool { return p.LevelRank() >= 2 }

// AllowsAdmin returns true when the grant permits re-sharing / deletion.
func (p DashboardPermissionLevel) AllowsAdmin() bool { return p.LevelRank() >= 3 }
