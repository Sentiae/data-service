package postgres

import (
	"fmt"

	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

func AutoMigrate(db *gorm.DB) error {
	models := []any{
		&domain.DataSource{},
		&domain.SemanticField{},
		&domain.DataQuery{},
		&domain.QueryExecution{},
		&domain.DashboardConfig{},
		&domain.QueryApproval{},
		&domain.DataSourceSample{},
		&domain.DashboardPermission{},
		&domain.DashboardAlert{},
		&domain.OrgVocabulary{},
		&domain.DashboardAsCode{},
		&domain.QueryHistoryEntry{},
		&domain.SavedQuery{},
	}

	if err := db.AutoMigrate(models...); err != nil {
		return fmt.Errorf("auto-migration failed: %w", err)
	}

	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_data_sources_org ON data_sources (organization_id)",
		"CREATE INDEX IF NOT EXISTS idx_semantic_fields_ds ON semantic_fields (data_source_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_fields_table_col ON semantic_fields (data_source_id, table_name, column_name)",
		"CREATE INDEX IF NOT EXISTS idx_data_queries_org ON data_queries (organization_id)",
		"CREATE INDEX IF NOT EXISTS idx_data_queries_ds ON data_queries (data_source_id)",
		"CREATE INDEX IF NOT EXISTS idx_query_executions_query ON query_executions (query_id, executed_at DESC)",
		"CREATE INDEX IF NOT EXISTS idx_dashboard_configs_org ON dashboard_configs (organization_id)",
		"CREATE INDEX IF NOT EXISTS idx_query_approvals_query ON query_approvals (query_id)",
		"CREATE INDEX IF NOT EXISTS idx_query_approvals_org ON query_approvals (organization_id)",
		"CREATE INDEX IF NOT EXISTS idx_query_approvals_status ON query_approvals (status)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_data_source_samples_unique ON data_source_samples (data_source_id, table_name)",
		"CREATE INDEX IF NOT EXISTS idx_dashboard_perms_dash ON dashboard_permissions (dashboard_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_dashboard_perms_unique ON dashboard_permissions (dashboard_id, principal_type, principal_id, permission)",
		"CREATE INDEX IF NOT EXISTS idx_dashboard_alerts_dash ON dashboard_alerts (dashboard_id)",
		"CREATE INDEX IF NOT EXISTS idx_dashboard_alerts_active ON dashboard_alerts (active)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_org_vocab_unique ON per_org_vocabulary (organization_id, column_id, business_term)",
		"CREATE INDEX IF NOT EXISTS idx_dash_as_code_org ON dashboards_as_code (organization_id)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_dash_as_code_slug ON dashboards_as_code (organization_id, slug)",
	}

	for _, idx := range indexes {
		if err := db.Exec(idx).Error; err != nil {
			return fmt.Errorf("index creation failed: %w", err)
		}
	}

	return nil
}
