package grpc

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	datav1 "github.com/sentiae/data-service/gen/data/v1"
	"github.com/sentiae/data-service/internal/domain"
)

// parseUUID turns a wire string into a uuid.UUID with a consistent
// InvalidArgument error. Empty strings are rejected.
func parseUUID(s, field string) (uuid.UUID, error) {
	if s == "" {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s is required", field)
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s: invalid uuid", field)
	}
	return id, nil
}

// optionalUUID parses a uuid string but returns nil + nil when empty.
func optionalUUID(s string) (*uuid.UUID, error) {
	if s == "" {
		return nil, nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid uuid: %s", s)
	}
	return &id, nil
}

// toTs wraps *time.Time in a *timestamppb.Timestamp, preserving nil.
func toTs(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func toTsPtr(t *time.Time) *timestamppb.Timestamp {
	if t == nil || t.IsZero() {
		return nil
	}
	return timestamppb.New(*t)
}

// structFromAny marshals an arbitrary value through JSON into a
// google.protobuf.Struct. Returns nil on marshal errors.
func structFromAny(v any) *structpb.Struct {
	if v == nil {
		return nil
	}
	buf, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return s
}

// structToMap decodes a *structpb.Struct into a map[string]any.
func structToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

// dataSourceToPB maps a domain DataSource onto the wire shape.
func dataSourceToPB(ds *domain.DataSource) *datav1.DataSource {
	if ds == nil {
		return nil
	}
	out := &datav1.DataSource{
		Id:             ds.ID.String(),
		OrganizationId: ds.OrganizationID.String(),
		Name:           ds.Name,
		Description:    ds.Description,
		Engine:         string(ds.Engine),
		ConnectionDsn:  ds.ConnectionDSN,
		Schema:         ds.Schema,
		Tables:         []string(ds.Tables),
		Status:         string(ds.Status),
		LastSyncAt:     toTsPtr(ds.LastSyncAt),
		CreatedAt:      toTs(ds.CreatedAt),
		UpdatedAt:      toTs(ds.UpdatedAt),
	}
	if ds.ConnectionID != nil {
		out.ConnectionId = ds.ConnectionID.String()
	}
	return out
}

// semanticFieldToPB maps a domain SemanticField onto the wire shape.
func semanticFieldToPB(f *domain.SemanticField) *datav1.SemanticField {
	if f == nil {
		return nil
	}
	return &datav1.SemanticField{
		Id:           f.ID.String(),
		DataSourceId: f.DataSourceID.String(),
		TableName:    f.TableName,
		ColumnName:   f.ColumnName,
		BusinessName: f.BusinessName,
		Description:  f.Description,
		DataType:     f.DataType,
		Aggregation:  f.Aggregation,
		Unit:         f.Unit,
		Tags:         []string(f.Tags),
		Synonyms:     []string(f.Synonyms),
		CreatedAt:    toTs(f.CreatedAt),
		UpdatedAt:    toTs(f.UpdatedAt),
	}
}

// dataQueryToPB maps a domain DataQuery onto the wire shape.
func dataQueryToPB(q *domain.DataQuery) *datav1.DataQuery {
	if q == nil {
		return nil
	}
	out := &datav1.DataQuery{
		Id:              q.ID.String(),
		OrganizationId:  q.OrganizationID.String(),
		DataSourceId:    q.DataSourceID.String(),
		Name:            q.Name,
		Description:     q.Description,
		QueryType:       string(q.QueryType),
		RawQuery:        q.RawQuery,
		NaturalLanguage: q.NaturalLanguage,
		CacheTtlSec:     int32(q.CacheTTLSec),
		ReadOnly:        q.ReadOnly,
		CreatedAt:       toTs(q.CreatedAt),
		UpdatedAt:       toTs(q.UpdatedAt),
	}
	if q.CanvasNodeID != nil {
		out.CanvasNodeId = q.CanvasNodeID.String()
	}
	return out
}

// queryExecutionToPB maps a domain QueryExecution onto the wire shape.
func queryExecutionToPB(e *domain.QueryExecution) *datav1.QueryExecution {
	if e == nil {
		return nil
	}
	out := &datav1.QueryExecution{
		Id:         e.ID.String(),
		QueryId:    e.QueryID.String(),
		Status:     string(e.Status),
		RowCount:   int32(e.RowCount),
		DurationMs: e.DurationMS,
		Error:      e.Error,
		ExecutedAt: toTs(e.ExecutedAt),
	}
	if len(e.Result) > 0 {
		out.Result = structFromAny(e.Result)
	}
	return out
}

// dashboardConfigToPB maps a domain DashboardConfig onto the wire shape.
func dashboardConfigToPB(d *domain.DashboardConfig) *datav1.DashboardConfig {
	if d == nil {
		return nil
	}
	out := &datav1.DashboardConfig{
		Id:             d.ID.String(),
		OrganizationId: d.OrganizationID.String(),
		Name:           d.Name,
		Description:    d.Description,
		Version:        int32(d.Version),
		CreatedAt:      toTs(d.CreatedAt),
		UpdatedAt:      toTs(d.UpdatedAt),
	}
	if d.CanvasNodeID != nil {
		out.CanvasNodeId = d.CanvasNodeID.String()
	}
	if len(d.Panels) > 0 {
		out.Panels = structFromAny(d.Panels)
	}
	if len(d.Layout) > 0 {
		out.Layout = structFromAny(d.Layout)
	}
	return out
}

// dashboardPermissionToPB maps a domain DashboardPermission onto the
// wire shape.
func dashboardPermissionToPB(p *domain.DashboardPermission) *datav1.DashboardPermission {
	if p == nil {
		return nil
	}
	return &datav1.DashboardPermission{
		Id:            p.ID.String(),
		DashboardId:   p.DashboardID.String(),
		PrincipalType: string(p.PrincipalType),
		PrincipalId:   p.PrincipalID.String(),
		Permission:    string(p.Permission),
		GrantedBy:     p.GrantedBy.String(),
		CreatedAt:     toTs(p.CreatedAt),
	}
}

// vocabularyEntryToPB maps a domain OrgVocabulary onto the wire shape.
func vocabularyEntryToPB(v *domain.OrgVocabulary) *datav1.VocabularyEntry {
	if v == nil {
		return nil
	}
	return &datav1.VocabularyEntry{
		Id:             v.ID.String(),
		OrganizationId: v.OrganizationID.String(),
		ColumnId:       v.ColumnID,
		BusinessTerm:   v.BusinessTerm,
		Synonyms:       []string(v.Synonyms),
		Aliases:        []string(v.Aliases),
		Unit:           v.Unit,
		DataType:       v.DataType,
		Format:         v.Format,
		Description:    v.Description,
		CreatedAt:      toTs(v.CreatedAt),
		UpdatedAt:      toTs(v.UpdatedAt),
	}
}

// parseRFC3339 parses an RFC3339 timestamp string. Returns (zero, false)
// when the input is empty or malformed; the bridge calls fall back to
// "no expiry" in that case.
func parseRFC3339(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
