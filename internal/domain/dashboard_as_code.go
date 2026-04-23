package domain

import (
	"time"

	"github.com/google/uuid"
)

// DashboardAsCode is the versioned, checksum-stamped record of a
// dashboard whose source of truth is a YAML file (§12.5 dashboards-as-
// code). The live-rendered dashboard still lives in DashboardConfig
// (so panels, alerts, and permissions keep their existing shape). This
// table is the audit + provenance layer: every POST /dashboards/yaml
// materialises a row here, then upserts the corresponding
// DashboardConfig row with SourceYAML populated.
//
// Storing the parsed spec alongside the raw YAML lets the portal
// "View YAML" without re-parsing and lets a reviewer diff two
// versions without re-loading the file.
type DashboardAsCode struct {
	ID             uuid.UUID `json:"id" gorm:"type:uuid;primaryKey;default:gen_random_uuid()"`
	OrganizationID uuid.UUID `json:"organization_id" gorm:"type:uuid;not null;index:idx_dash_as_code_org"`

	// Slug is a stable, human-friendly identifier from the YAML
	// top-level `slug` key. Combined with organization_id it forms a
	// unique index so operators can re-POST the same file and watch
	// the version increment.
	Slug string `json:"slug" gorm:"type:varchar(255);not null"`

	// Name + Description come from the YAML top-level metadata.
	Name        string `json:"name" gorm:"type:varchar(255);not null"`
	Description string `json:"description" gorm:"type:text"`

	// Version increments on every successful POST where the checksum
	// differs from the last persisted row. POSTs with the same
	// checksum are no-ops (we return the existing row unchanged).
	Version int `json:"version" gorm:"not null;default:1"`

	// Checksum is the SHA-256 of the raw YAML bytes. Used to detect
	// no-op re-submissions.
	Checksum string `json:"checksum" gorm:"type:varchar(64);not null"`

	// RawYAML holds the exact bytes the operator submitted. Stored as
	// text so round-tripping (import → export) is lossless.
	RawYAML string `json:"raw_yaml" gorm:"type:text;not null"`

	// ParsedSpec is the JSON-encoded parsed spec (queries + panels).
	// Kept next to the raw YAML so the portal can render without
	// re-parsing.
	ParsedSpec JSONMap `json:"parsed_spec" gorm:"type:jsonb;not null"`

	// DashboardConfigID is the live DashboardConfig row this YAML
	// materialises to. Populated after the first successful publish.
	DashboardConfigID *uuid.UUID `json:"dashboard_config_id,omitempty" gorm:"type:uuid;index"`

	CreatedBy uuid.UUID `json:"created_by" gorm:"type:uuid;not null"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName pins the table to the documented name so direct-SQL
// migrations and dashboards-as-code CLI tools can both reference it
// unambiguously.
func (DashboardAsCode) TableName() string {
	return "dashboards_as_code"
}
