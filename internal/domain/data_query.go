package domain

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DataQuery represents a saved or ad-hoc query against a data source.
type DataQuery struct {
	ID              uuid.UUID  `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID  uuid.UUID  `json:"organization_id" gorm:"type:uuid;not null;index"`
	DataSourceID    uuid.UUID  `json:"data_source_id" gorm:"type:uuid;not null;index"`
	CanvasNodeID    *uuid.UUID `json:"canvas_node_id,omitempty" gorm:"type:uuid;index"`
	Name            string     `json:"name" gorm:"type:varchar(255);not null"`
	Description     string     `json:"description" gorm:"type:text"`
	QueryType       QueryType  `json:"query_type" gorm:"type:varchar(20);not null;default:'sql'"`
	RawQuery        string     `json:"raw_query" gorm:"type:text;not null"`         // SQL or API template
	NaturalLanguage string     `json:"natural_language,omitempty" gorm:"type:text"` // Original NL prompt
	Parameters      JSONMap    `json:"parameters,omitempty" gorm:"type:jsonb"`
	CacheTTLSec     int        `json:"cache_ttl_sec" gorm:"not null;default:0"`
	ReadOnly        bool       `json:"read_only" gorm:"not null;default:true"`
	CreatedBy       uuid.UUID  `json:"created_by" gorm:"type:uuid;not null"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type QueryType string

const (
	QueryTypeSQL     QueryType = "sql"
	QueryTypeNL      QueryType = "nl" // natural language → translated to SQL
	QueryTypeREST    QueryType = "rest"
	QueryTypeGraphQL QueryType = "graphql"
)

// QueryExecution records the result of running a query.
type QueryExecution struct {
	ID         uuid.UUID       `json:"id" gorm:"type:uuid;primary_key"`
	QueryID    uuid.UUID       `json:"query_id" gorm:"type:uuid;not null;index"`
	Status     QueryExecStatus `json:"status" gorm:"type:varchar(20);not null"`
	RowCount   int             `json:"row_count" gorm:"default:0"`
	DurationMS int64           `json:"duration_ms" gorm:"default:0"`
	Result     JSONMap         `json:"result,omitempty" gorm:"type:jsonb"`
	Error      string          `json:"error,omitempty" gorm:"type:text"`
	ExecutedBy uuid.UUID       `json:"executed_by" gorm:"type:uuid;not null"`
	ExecutedAt time.Time       `json:"executed_at" gorm:"not null"`
}

type QueryExecStatus string

const (
	QueryExecStatusRunning   QueryExecStatus = "running"
	QueryExecStatusCompleted QueryExecStatus = "completed"
	QueryExecStatusFailed    QueryExecStatus = "failed"
)

// JSONMap is a generic JSON map for GORM JSONB storage.
type JSONMap map[string]any

func (j JSONMap) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

func (j *JSONMap) Scan(value any) error {
	if value == nil {
		*j = nil
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(b, j)
}

// DashboardConfig stores a version-controlled dashboard definition.
type DashboardConfig struct {
	ID             uuid.UUID  `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID uuid.UUID  `json:"organization_id" gorm:"type:uuid;not null;index"`
	CanvasNodeID   *uuid.UUID `json:"canvas_node_id,omitempty" gorm:"type:uuid;index"`
	Name           string     `json:"name" gorm:"type:varchar(255);not null"`
	Description    string     `json:"description" gorm:"type:text"`
	Version        int        `json:"version" gorm:"not null;default:1"`
	Panels         JSONMap    `json:"panels" gorm:"type:jsonb;not null"`
	Layout         JSONMap    `json:"layout" gorm:"type:jsonb"`

	// §12.5 dashboard-as-code: when SourceYAML is set, this row is the
	// materialisation of a YAML file checked into a repo. SourcePath
	// names the file (e.g. "dashboards/sla.yaml"); SourceRepoID points
	// at the owning git-service repository. The YAML itself is
	// persisted verbatim so round-tripping (import → export) is
	// lossless even when the portal mutates Panels/Layout.
	SourceYAML   string     `json:"source_yaml,omitempty" gorm:"type:text"`
	SourcePath   string     `json:"source_path,omitempty" gorm:"type:varchar(500)"`
	SourceRepoID *uuid.UUID `json:"source_repo_id,omitempty" gorm:"type:uuid;index"`
	SourceDigest string     `json:"source_digest,omitempty" gorm:"type:varchar(64)"`

	// §12.5 dashboard embeds: EmbedToken + EmbedEnabled mirror the
	// Roadmap share-token pattern (§4.10 of work-service). A rotated
	// token is the caller's proof of access; EmbedEnabled=false
	// revokes without erasing the column.
	EmbedToken   string `json:"embed_token,omitempty" gorm:"type:varchar(64);uniqueIndex"`
	EmbedEnabled bool   `json:"embed_enabled" gorm:"not null;default:false"`
	// §12.5 (C10) embed token rotation: EmbedTokenExpiresAt stamps
	// when a rotated token should stop being honoured. The
	// DashboardEmbedExpiryWorker flips EmbedEnabled=false once this
	// passes; the column remains so operators can inspect history.
	// Type omitted so each dialect (postgres → timestamptz, sqlite →
	// datetime) picks the right storage.
	EmbedTokenExpiresAt *time.Time `json:"embed_token_expires_at,omitempty"`

	CreatedBy uuid.UUID `json:"created_by" gorm:"type:uuid;not null"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
