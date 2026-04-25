package grpc

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	datav1 "github.com/sentiae/data-service/gen/data/v1"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/kafka"
)

// DataSourceServiceServer wraps the data-source + semantic-field CRUD
// surface plus the SyncSchema deep-path bridge.
type DataSourceServiceServer struct {
	datav1.UnimplementedDataSourceServiceServer
	baseServer
}

// NewDataSourceServiceServer constructs the handler.
func NewDataSourceServiceServer(deps Deps) *DataSourceServiceServer {
	return &DataSourceServiceServer{baseServer: baseServer{deps: deps}}
}

// CreateDataSource mirrors POST /api/v1/data/sources.
func (s *DataSourceServiceServer) CreateDataSource(ctx context.Context, req *datav1.CreateDataSourceRequest) (*datav1.CreateDataSourceResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	actor, _ := optionalUUID(req.GetActorId())
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetEngine() == "" {
		return nil, status.Error(codes.InvalidArgument, "engine is required")
	}
	connID, err := optionalUUID(req.GetConnectionId())
	if err != nil {
		return nil, err
	}

	schema := req.GetSchema()
	if schema == "" {
		schema = "public"
	}
	var actorID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	ds := &domain.DataSource{
		ID:             uuid.New(),
		OrganizationID: orgID,
		Name:           req.GetName(),
		Description:    req.GetDescription(),
		Engine:         domain.DataEngine(req.GetEngine()),
		ConnectionID:   connID,
		ConnectionDSN:  req.GetConnectionDsn(),
		Schema:         schema,
		Status:         domain.DataSourceStatusPending,
		CreatedBy:      actorID,
	}
	if err := s.deps.DB.Create(ds).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "create data source: %v", err)
	}
	s.emitSource(ctx, "data.source.created", ds, actorID)
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "data_source", ds.ID.String(), ds)
	}
	return &datav1.CreateDataSourceResponse{DataSource: dataSourceToPB(ds)}, nil
}

// GetDataSource mirrors GET /api/v1/data/sources/{id}.
func (s *DataSourceServiceServer) GetDataSource(ctx context.Context, req *datav1.GetDataSourceRequest) (*datav1.GetDataSourceResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var ds domain.DataSource
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&ds).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "data source not found")
		}
		return nil, status.Errorf(codes.Internal, "get data source: %v", err)
	}
	return &datav1.GetDataSourceResponse{DataSource: dataSourceToPB(&ds)}, nil
}

// UpdateDataSource mirrors PUT /api/v1/data/sources/{id}.
func (s *DataSourceServiceServer) UpdateDataSource(ctx context.Context, req *datav1.UpdateDataSourceRequest) (*datav1.UpdateDataSourceResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var ds domain.DataSource
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&ds).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "data source not found")
		}
		return nil, status.Errorf(codes.Internal, "lookup data source: %v", err)
	}
	updates := map[string]any{}
	if req.GetUpdateName() {
		updates["name"] = req.GetName()
	}
	if req.GetUpdateDescription() {
		updates["description"] = req.GetDescription()
	}
	if req.GetUpdateSchema() {
		updates["schema"] = req.GetSchema()
	}
	if len(updates) > 0 {
		if err := s.deps.DB.WithContext(ctx).Model(&ds).Updates(updates).Error; err != nil {
			return nil, status.Errorf(codes.Internal, "update data source: %v", err)
		}
		s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&ds)
	}
	actor, _ := optionalUUID(req.GetActorId())
	var actorID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	s.emitSource(ctx, "data.source.updated", &ds, actorID)
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "data_source", ds.ID.String(), ds)
	}
	return &datav1.UpdateDataSourceResponse{DataSource: dataSourceToPB(&ds)}, nil
}

// DeleteDataSource mirrors DELETE /api/v1/data/sources/{id}.
func (s *DataSourceServiceServer) DeleteDataSource(ctx context.Context, req *datav1.DeleteDataSourceRequest) (*datav1.DeleteDataSourceResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	res := s.deps.DB.WithContext(ctx).Where("id = ?", id).Delete(&domain.DataSource{})
	if res.Error != nil {
		return nil, status.Errorf(codes.Internal, "delete data source: %v", res.Error)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "data_source", id.String(), map[string]any{"id": id.String(), "deleted": true})
	}
	return &datav1.DeleteDataSourceResponse{Deleted: res.RowsAffected > 0}, nil
}

// ListDataSources mirrors GET /api/v1/data/sources?organization_id=…
func (s *DataSourceServiceServer) ListDataSources(ctx context.Context, req *datav1.ListDataSourcesRequest) (*datav1.ListDataSourcesResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	var rows []domain.DataSource
	if err := s.deps.DB.WithContext(ctx).Where("organization_id = ?", orgID).Order("created_at DESC").Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list data sources: %v", err)
	}
	out := make([]*datav1.DataSource, 0, len(rows))
	for i := range rows {
		out = append(out, dataSourceToPB(&rows[i]))
	}
	return &datav1.ListDataSourcesResponse{Items: out}, nil
}

// SyncSchema mirrors POST /api/v1/data/sources/{id}/sync. Bridges through
// the existing HTTP handler because the schema-discovery + sampling
// pipeline is too heavy to duplicate today. Returns the post-sync row
// state so callers can refresh their UI without a follow-up Get.
func (s *DataSourceServiceServer) SyncSchema(ctx context.Context, req *datav1.SyncSchemaRequest) (*datav1.SyncSchemaResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	headers := map[string]string{
		"X-User-ID": req.GetActorId(),
	}
	code, body, err := s.dispatchHTTP("POST", "/api/v1/data/sources/"+id.String()+"/sync", headers, nil)
	if err != nil {
		return nil, err
	}
	if err := statusFromHTTP(code, body); err != nil {
		return nil, err
	}
	// Re-read the row so the response carries the updated state.
	var ds domain.DataSource
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&ds).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "post-sync lookup: %v", err)
	}
	return &datav1.SyncSchemaResponse{DataSource: dataSourceToPB(&ds)}, nil
}

// --- Semantic Fields --------------------------------------------------------

// ListSemanticFields mirrors GET /api/v1/data/sources/{id}/fields.
func (s *DataSourceServiceServer) ListSemanticFields(ctx context.Context, req *datav1.ListSemanticFieldsRequest) (*datav1.ListSemanticFieldsResponse, error) {
	dsID, err := parseUUID(req.GetDataSourceId(), "data_source_id")
	if err != nil {
		return nil, err
	}
	var rows []domain.SemanticField
	if err := s.deps.DB.WithContext(ctx).Where("data_source_id = ?", dsID).Order("table_name, column_name").Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list semantic fields: %v", err)
	}
	out := make([]*datav1.SemanticField, 0, len(rows))
	for i := range rows {
		out = append(out, semanticFieldToPB(&rows[i]))
	}
	return &datav1.ListSemanticFieldsResponse{Items: out}, nil
}

// CreateSemanticField mirrors POST /api/v1/data/sources/{id}/fields.
func (s *DataSourceServiceServer) CreateSemanticField(ctx context.Context, req *datav1.CreateSemanticFieldRequest) (*datav1.CreateSemanticFieldResponse, error) {
	dsID, err := parseUUID(req.GetDataSourceId(), "data_source_id")
	if err != nil {
		return nil, err
	}
	if req.GetTableName() == "" || req.GetColumnName() == "" || req.GetBusinessName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name, column_name, business_name are required")
	}
	field := &domain.SemanticField{
		ID:           uuid.New(),
		DataSourceID: dsID,
		TableName:    req.GetTableName(),
		ColumnName:   req.GetColumnName(),
		BusinessName: req.GetBusinessName(),
		Description:  req.GetDescription(),
		DataType:     req.GetDataType(),
		Aggregation:  req.GetAggregation(),
		Unit:         req.GetUnit(),
		Tags:         domain.StringArray(req.GetTags()),
	}
	if err := s.deps.DB.WithContext(ctx).Create(field).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "create semantic field: %v", err)
	}
	actor, _ := optionalUUID(req.GetActorId())
	orgID, _ := optionalUUID(req.GetOrganizationId())
	var actorID, organizationID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	if orgID != nil {
		organizationID = *orgID
	}
	s.emitField(ctx, "data.semantic_field.created", field, organizationID, actorID)
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "semantic_field", field.ID.String(), field)
	}
	return &datav1.CreateSemanticFieldResponse{Field: semanticFieldToPB(field)}, nil
}

// UpdateSemanticField mirrors PUT /api/v1/data/sources/_/fields/{id}.
func (s *DataSourceServiceServer) UpdateSemanticField(ctx context.Context, req *datav1.UpdateSemanticFieldRequest) (*datav1.UpdateSemanticFieldResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var field domain.SemanticField
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&field).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "semantic field not found")
		}
		return nil, status.Errorf(codes.Internal, "lookup semantic field: %v", err)
	}
	updates := map[string]any{}
	if req.GetUpdateBusinessName() {
		updates["business_name"] = req.GetBusinessName()
	}
	if req.GetUpdateDescription() {
		updates["description"] = req.GetDescription()
	}
	if req.GetUpdateDataType() {
		updates["data_type"] = req.GetDataType()
	}
	if req.GetUpdateAggregation() {
		updates["aggregation"] = req.GetAggregation()
	}
	if req.GetUpdateUnit() {
		updates["unit"] = req.GetUnit()
	}
	if req.GetUpdateTags() {
		updates["tags"] = domain.StringArray(req.GetTags())
	}
	if req.GetUpdateSynonyms() {
		updates["synonyms"] = domain.StringArray(req.GetSynonyms())
	}
	if len(updates) > 0 {
		if err := s.deps.DB.WithContext(ctx).Model(&field).Updates(updates).Error; err != nil {
			return nil, status.Errorf(codes.Internal, "update semantic field: %v", err)
		}
		s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&field)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "semantic_field", field.ID.String(), field)
	}
	return &datav1.UpdateSemanticFieldResponse{Field: semanticFieldToPB(&field)}, nil
}

// DeleteSemanticField mirrors DELETE /api/v1/data/sources/_/fields/{id}.
func (s *DataSourceServiceServer) DeleteSemanticField(ctx context.Context, req *datav1.DeleteSemanticFieldRequest) (*datav1.DeleteSemanticFieldResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	res := s.deps.DB.WithContext(ctx).Where("id = ?", id).Delete(&domain.SemanticField{})
	if res.Error != nil {
		return nil, status.Errorf(codes.Internal, "delete semantic field: %v", res.Error)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "semantic_field", id.String(), map[string]any{"id": id.String(), "deleted": true})
	}
	return &datav1.DeleteSemanticFieldResponse{Deleted: res.RowsAffected > 0}, nil
}

// emitSource publishes a data_source.* lifecycle event mirroring the
// HTTP handler's emitSource.
func (s *DataSourceServiceServer) emitSource(ctx context.Context, eventType string, ds *domain.DataSource, actor uuid.UUID) {
	if s.deps.Pub == nil {
		return
	}
	meta := map[string]any{
		"data_source_id": ds.ID.String(),
		"name":           ds.Name,
		"engine":         string(ds.Engine),
		"status":         string(ds.Status),
		"schema":         ds.Schema,
	}
	if ds.ConnectionID != nil {
		meta["connection_id"] = ds.ConnectionID.String()
	}
	if len(ds.Tables) > 0 {
		meta["tables"] = []string(ds.Tables)
	}
	_ = s.deps.Pub.Publish(ctx, eventType, kafka.EventData{
		ActorID:        actor.String(),
		ResourceType:   "data_source",
		ResourceID:     ds.ID.String(),
		OrganizationID: ds.OrganizationID.String(),
		Metadata:       meta,
	})
}

// emitField publishes a semantic_field.* event.
func (s *DataSourceServiceServer) emitField(ctx context.Context, eventType string, f *domain.SemanticField, orgID, actor uuid.UUID) {
	if s.deps.Pub == nil {
		return
	}
	meta := map[string]any{
		"semantic_field_id": f.ID.String(),
		"data_source_id":    f.DataSourceID.String(),
		"table_name":        f.TableName,
		"column_name":       f.ColumnName,
		"business_name":     f.BusinessName,
		"data_type":         f.DataType,
	}
	if f.Aggregation != "" {
		meta["aggregation"] = f.Aggregation
	}
	if f.Unit != "" {
		meta["unit"] = f.Unit
	}
	if len(f.Tags) > 0 {
		meta["tags"] = []string(f.Tags)
	}
	_ = s.deps.Pub.Publish(ctx, eventType, kafka.EventData{
		ActorID:        actor.String(),
		ResourceType:   "semantic_field",
		ResourceID:     f.ID.String(),
		OrganizationID: orgID.String(),
		Metadata:       meta,
	})
}

// Compile-time check that decodeEnvelope is used somewhere in the
// package (it is — query_service uses it).
var _ = json.Marshal
