package domain

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DataSource represents a connected data source (database, API, file).
type DataSource struct {
	ID             uuid.UUID        `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID uuid.UUID        `json:"organization_id" gorm:"type:uuid;not null;index"`
	Name           string           `json:"name" gorm:"type:varchar(255);not null"`
	Description    string           `json:"description" gorm:"type:text"`
	Engine         DataEngine       `json:"engine" gorm:"type:varchar(50);not null"`
	ConnectionID   *uuid.UUID       `json:"connection_id,omitempty" gorm:"type:uuid"` // ref to ops-service DatabaseConnection
	ConnectionDSN  string           `json:"connection_dsn,omitempty" gorm:"type:varchar(1000)"`
	Schema         string           `json:"schema" gorm:"type:varchar(100);default:'public'"`
	Tables         StringArray      `json:"tables,omitempty" gorm:"type:jsonb"`
	Status         DataSourceStatus `json:"status" gorm:"type:varchar(20);not null;default:'pending'"`
	LastSyncAt     *time.Time       `json:"last_sync_at,omitempty"`
	CreatedBy      uuid.UUID        `json:"created_by" gorm:"type:uuid;not null"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

type DataEngine string

const (
	DataEnginePostgres DataEngine = "postgres"
	DataEngineMySQL    DataEngine = "mysql"
	DataEngineSQLite   DataEngine = "sqlite"
	DataEngineMSSQL    DataEngine = "mssql"
	DataEngineREST     DataEngine = "rest_api"
	DataEngineGraphQL  DataEngine = "graphql_api"
	// Analytical warehouses (§12.1 gap-closure). Both speak SQL but
	// with enough dialect divergence that callers pick the right
	// adapter to get native performance (result streaming, JSON column
	// handling, Unicode identifiers).
	DataEngineSnowflake DataEngine = "snowflake"
	DataEngineBigQuery  DataEngine = "bigquery"
	// Cross-domain engines for querying other Sentiae services
	DataEngineVCS    DataEngine = "sentiae_vcs"    // Query git-service (commits, PRs, sessions)
	DataEngineOps    DataEngine = "sentiae_ops"    // Query ops-service (deployments, incidents, metrics)
	DataEngineCanvas DataEngine = "sentiae_canvas" // Query canvas-service (nodes, executions)
)

func (e DataEngine) IsValid() bool {
	switch e {
	case DataEnginePostgres, DataEngineMySQL, DataEngineSQLite,
		DataEngineMSSQL, DataEngineREST, DataEngineGraphQL,
		DataEngineSnowflake, DataEngineBigQuery,
		DataEngineVCS, DataEngineOps, DataEngineCanvas:
		return true
	}
	return false
}

func (e DataEngine) IsSQL() bool {
	switch e {
	case DataEnginePostgres, DataEngineMySQL, DataEngineSQLite,
		DataEngineMSSQL, DataEngineSnowflake, DataEngineBigQuery:
		return true
	}
	return false
}

// UsesDatabaseSQL reports whether the engine can be reached via Go's
// database/sql package (i.e. there is a registered driver + DSN). BigQuery
// is SQL semantically but is accessed through the google-cloud-go client,
// so it returns false here. Callers that need to branch between
// "sql.Open(driver, dsn)" and adapter-based access should use this
// narrower predicate rather than IsSQL.
func (e DataEngine) UsesDatabaseSQL() bool {
	switch e {
	case DataEnginePostgres, DataEngineMySQL, DataEngineSQLite,
		DataEngineMSSQL, DataEngineSnowflake:
		return true
	}
	return false
}

type DataSourceStatus string

const (
	DataSourceStatusPending   DataSourceStatus = "pending"
	DataSourceStatusConnected DataSourceStatus = "connected"
	DataSourceStatusError     DataSourceStatus = "error"
	DataSourceStatusSyncing   DataSourceStatus = "syncing"
)

// StringArray is a JSON-serializable string slice.
type StringArray []string

func (s StringArray) Value() (driver.Value, error) {
	if s == nil {
		return nil, nil
	}
	return json.Marshal(s)
}

func (s *StringArray) Scan(value any) error {
	if value == nil {
		*s = nil
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(b, s)
}
