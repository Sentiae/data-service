package domain

import (
	"time"

	"github.com/google/uuid"
)

// DataSourceSample stores a set of sampled rows pulled from a source table
// during schema discovery. Samples power the "preview" views in the portal
// and feed the NL→SQL prompt with realistic example values.
type DataSourceSample struct {
	ID           uuid.UUID `json:"id" gorm:"type:uuid;primary_key"`
	DataSourceID uuid.UUID `json:"data_source_id" gorm:"type:uuid;not null;index:idx_dss_src_tbl"`
	TableName    string    `json:"table_name" gorm:"type:varchar(255);not null;index:idx_dss_src_tbl"`
	// SampleJSON is the serialized rows array: [{col: val, ...}, ...].
	SampleJSON JSONMap   `json:"sample" gorm:"type:jsonb"`
	RowCount   int       `json:"row_count" gorm:"default:0"`
	SampledAt  time.Time `json:"sampled_at" gorm:"not null"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
