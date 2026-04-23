// Package permission provides a concrete PermissionChecker implementation
// that calls permission-service over gRPC to evaluate both row-level
// (data source) access and column-level (semantic field) access for a user.
package permission

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	permissionv1 "github.com/sentiae/permission-service/gen/permission/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/gorm"
)

// Checker speaks to permission-service over gRPC and enforces both row-level
// (data source `can_read`) and column-level (SemanticField.RequiredRole) access.
type Checker struct {
	conn    *grpc.ClientConn
	client  permissionv1.PermissionServiceClient
	db      *gorm.DB
	timeout time.Duration

	// requiredRoleCache caches SemanticField.RequiredRole lookups to avoid a
	// round-trip to the database for every column filter.
	requiredRoleCache sync.Map // key: "sourceID|table|column" → string
}

// NewChecker dials permission-service and returns a Checker wired to the
// caller's GORM handle (used only to resolve SemanticField.RequiredRole).
//
// If endpoint is empty, dial is skipped and all checks default-allow — this
// keeps local development frictionless while production deployments set
// PERMISSION_SERVICE_URL explicitly.
func NewChecker(endpoint string, db *gorm.DB) (*Checker, error) {
	c := &Checker{db: db, timeout: 3 * time.Second}
	if endpoint == "" {
		return c, nil
	}
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("permission checker: dial %s: %w", endpoint, err)
	}
	c.conn = conn
	c.client = permissionv1.NewPermissionServiceClient(conn)
	return c, nil
}

// Close releases the gRPC connection.
func (c *Checker) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// CanReadSource returns true when the user has `can_read` on the data_source
// in permission-service. Default-allow when no gRPC client is configured.
func (c *Checker) CanReadSource(ctx context.Context, userID, sourceID uuid.UUID) bool {
	if c.client == nil {
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := c.client.CheckPermission(cctx, &permissionv1.CheckPermissionRequest{
		ResourceType: "data_source",
		ResourceId:   sourceID.String(),
		Permission:   "can_read",
		SubjectType:  "user",
		SubjectId:    userID.String(),
	})
	if err != nil {
		// Fail closed on explicit errors from the permission backend.
		return false
	}
	return resp.GetAllowed()
}

// CanAccessColumn implements usecase.PermissionChecker. A column is only
// filtered when a matching SemanticField has RequiredRole set and the user
// does not hold that role on the org / source. Unknown columns are allowed.
//
// The `source` argument is the federated source name (e.g. "sentiae_ops");
// for dynamic data sources we look up the SemanticField by (source, table,
// column) and fall back to allow-on-unknown.
func (c *Checker) CanAccessColumn(ctx context.Context, userID uuid.UUID, source, table, column string) bool {
	if column == "" {
		return true
	}
	requiredRole := c.requiredRoleFor(source, table, column)
	if requiredRole == "" {
		return true
	}
	if c.client == nil {
		return true
	}
	// Check membership via the permission-service: resource_type=role,
	// resource_id=<role_name>, permission=can_assume, subject=user:<id>.
	// This mirrors how ops-service expresses role gating.
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.client.CheckPermission(cctx, &permissionv1.CheckPermissionRequest{
		ResourceType: "role",
		ResourceId:   requiredRole,
		Permission:   "can_assume",
		SubjectType:  "user",
		SubjectId:    userID.String(),
	})
	if err != nil {
		return false
	}
	return resp.GetAllowed()
}

// FilterSemanticFields drops fields whose RequiredRole the user does not
// hold. Used by the NL→SQL prompt and the /fields endpoint to avoid leaking
// restricted columns into the LLM context.
func (c *Checker) FilterSemanticFields(ctx context.Context, userID uuid.UUID, fields []domain.SemanticField) []domain.SemanticField {
	if len(fields) == 0 {
		return fields
	}
	out := make([]domain.SemanticField, 0, len(fields))
	for _, f := range fields {
		if f.RequiredRole == "" || c.roleAllowed(ctx, userID, f.RequiredRole) {
			out = append(out, f)
		}
	}
	return out
}

// FilterResultColumns removes columns whose SemanticField.RequiredRole the
// user does not hold, mutating both the Columns slice and every row map.
func (c *Checker) FilterResultColumns(ctx context.Context, userID uuid.UUID, sourceID uuid.UUID, columns []string, rows []map[string]any) ([]string, []map[string]any) {
	if len(columns) == 0 {
		return columns, rows
	}
	// Resolve required roles for every column once up front.
	allowed := make(map[string]bool, len(columns))
	for _, col := range columns {
		table, column := splitTableColumn(col)
		role := c.requiredRoleForSourceID(sourceID, table, column)
		if role == "" || c.roleAllowed(ctx, userID, role) {
			allowed[col] = true
		}
	}

	newCols := make([]string, 0, len(columns))
	for _, col := range columns {
		if allowed[col] {
			newCols = append(newCols, col)
		}
	}
	newRows := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		nr := make(map[string]any, len(newCols))
		for k, v := range row {
			if allowed[k] {
				nr[k] = v
			}
		}
		newRows = append(newRows, nr)
	}
	return newCols, newRows
}

func (c *Checker) roleAllowed(ctx context.Context, userID uuid.UUID, role string) bool {
	if c.client == nil {
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.client.CheckPermission(cctx, &permissionv1.CheckPermissionRequest{
		ResourceType: "role",
		ResourceId:   role,
		Permission:   "can_assume",
		SubjectType:  "user",
		SubjectId:    userID.String(),
	})
	if err != nil {
		return false
	}
	return resp.GetAllowed()
}

// requiredRoleFor resolves the RequiredRole for a federated-source column by
// table+column name alone (we have no source UUID in the federated plan).
func (c *Checker) requiredRoleFor(source, table, column string) string {
	if c.db == nil {
		return ""
	}
	key := source + "|" + table + "|" + column
	if v, ok := c.requiredRoleCache.Load(key); ok {
		return v.(string)
	}
	var f domain.SemanticField
	q := c.db.Where("column_name = ?", column)
	if table != "" {
		q = q.Where("table_name = ?", table)
	}
	if err := q.First(&f).Error; err != nil {
		c.requiredRoleCache.Store(key, "")
		return ""
	}
	c.requiredRoleCache.Store(key, f.RequiredRole)
	return f.RequiredRole
}

// requiredRoleForSourceID resolves RequiredRole scoped to a specific data source.
func (c *Checker) requiredRoleForSourceID(sourceID uuid.UUID, table, column string) string {
	if c.db == nil || column == "" {
		return ""
	}
	key := sourceID.String() + "|" + table + "|" + column
	if v, ok := c.requiredRoleCache.Load(key); ok {
		return v.(string)
	}
	var f domain.SemanticField
	q := c.db.Where("data_source_id = ? AND column_name = ?", sourceID, column)
	if table != "" {
		q = q.Where("table_name = ?", table)
	}
	if err := q.First(&f).Error; err != nil {
		c.requiredRoleCache.Store(key, "")
		return ""
	}
	c.requiredRoleCache.Store(key, f.RequiredRole)
	return f.RequiredRole
}

func splitTableColumn(col string) (string, string) {
	parts := strings.SplitN(col, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", col
}
