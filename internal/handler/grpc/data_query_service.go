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
)

// DataQueryServiceServer wraps DataQuery CRUD + Execute + NL bridge.
type DataQueryServiceServer struct {
	datav1.UnimplementedDataQueryServiceServer
	baseServer
}

func NewDataQueryServiceServer(deps Deps) *DataQueryServiceServer {
	return &DataQueryServiceServer{baseServer: baseServer{deps: deps}}
}

// CreateDataQuery mirrors POST /api/v1/data/queries.
func (s *DataQueryServiceServer) CreateDataQuery(ctx context.Context, req *datav1.CreateDataQueryRequest) (*datav1.CreateDataQueryResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	dsID, err := parseUUID(req.GetDataSourceId(), "data_source_id")
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetRawQuery() == "" {
		return nil, status.Error(codes.InvalidArgument, "raw_query is required")
	}
	canvasID, err := optionalUUID(req.GetCanvasNodeId())
	if err != nil {
		return nil, err
	}
	actor, _ := optionalUUID(req.GetActorId())
	var actorID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	queryType := req.GetQueryType()
	if queryType == "" {
		queryType = string(domain.QueryTypeSQL)
	}
	q := &domain.DataQuery{
		ID:              uuid.New(),
		OrganizationID:  orgID,
		DataSourceID:    dsID,
		CanvasNodeID:    canvasID,
		Name:            req.GetName(),
		Description:     req.GetDescription(),
		QueryType:       domain.QueryType(queryType),
		RawQuery:        req.GetRawQuery(),
		NaturalLanguage: req.GetNaturalLanguage(),
		CacheTTLSec:     int(req.GetCacheTtlSec()),
		ReadOnly:        req.GetReadOnly(),
		CreatedBy:       actorID,
	}
	if err := s.deps.DB.WithContext(ctx).Create(q).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "create data query: %v", err)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "data_query", q.ID.String(), q)
	}
	return &datav1.CreateDataQueryResponse{Query: dataQueryToPB(q)}, nil
}

func (s *DataQueryServiceServer) GetDataQuery(ctx context.Context, req *datav1.GetDataQueryRequest) (*datav1.GetDataQueryResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var q domain.DataQuery
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&q).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "data query not found")
		}
		return nil, status.Errorf(codes.Internal, "get data query: %v", err)
	}
	return &datav1.GetDataQueryResponse{Query: dataQueryToPB(&q)}, nil
}

func (s *DataQueryServiceServer) UpdateDataQuery(ctx context.Context, req *datav1.UpdateDataQueryRequest) (*datav1.UpdateDataQueryResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var q domain.DataQuery
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&q).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "data query not found")
		}
		return nil, status.Errorf(codes.Internal, "lookup data query: %v", err)
	}
	updates := map[string]any{}
	if req.GetUpdateName() {
		updates["name"] = req.GetName()
	}
	if req.GetUpdateDescription() {
		updates["description"] = req.GetDescription()
	}
	if req.GetUpdateRawQuery() {
		updates["raw_query"] = req.GetRawQuery()
	}
	if req.GetUpdateCacheTtlSec() {
		updates["cache_ttl_sec"] = req.GetCacheTtlSec()
	}
	if req.GetUpdateReadOnly() {
		updates["read_only"] = req.GetReadOnly()
	}
	if len(updates) > 0 {
		if err := s.deps.DB.WithContext(ctx).Model(&q).Updates(updates).Error; err != nil {
			return nil, status.Errorf(codes.Internal, "update data query: %v", err)
		}
		s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&q)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "data_query", q.ID.String(), q)
	}
	return &datav1.UpdateDataQueryResponse{Query: dataQueryToPB(&q)}, nil
}

func (s *DataQueryServiceServer) DeleteDataQuery(ctx context.Context, req *datav1.DeleteDataQueryRequest) (*datav1.DeleteDataQueryResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	res := s.deps.DB.WithContext(ctx).Where("id = ?", id).Delete(&domain.DataQuery{})
	if res.Error != nil {
		return nil, status.Errorf(codes.Internal, "delete data query: %v", res.Error)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "data_query", id.String(), map[string]any{"id": id.String(), "deleted": true})
	}
	return &datav1.DeleteDataQueryResponse{Deleted: res.RowsAffected > 0}, nil
}

func (s *DataQueryServiceServer) ListDataQueries(ctx context.Context, req *datav1.ListDataQueriesRequest) (*datav1.ListDataQueriesResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	q := s.deps.DB.WithContext(ctx).Where("organization_id = ?", orgID)
	if dsStr := req.GetDataSourceId(); dsStr != "" {
		dsID, err := parseUUID(dsStr, "data_source_id")
		if err != nil {
			return nil, err
		}
		q = q.Where("data_source_id = ?", dsID)
	}
	var rows []domain.DataQuery
	if err := q.Order("updated_at DESC").Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list data queries: %v", err)
	}
	out := make([]*datav1.DataQuery, 0, len(rows))
	for i := range rows {
		out = append(out, dataQueryToPB(&rows[i]))
	}
	return &datav1.ListDataQueriesResponse{Items: out}, nil
}

// ListQueryExecutions mirrors GET /api/v1/data/queries/{id}/executions.
func (s *DataQueryServiceServer) ListQueryExecutions(ctx context.Context, req *datav1.ListQueryExecutionsRequest) (*datav1.ListQueryExecutionsResponse, error) {
	queryID, err := parseUUID(req.GetQueryId(), "query_id")
	if err != nil {
		return nil, err
	}
	var rows []domain.QueryExecution
	if err := s.deps.DB.WithContext(ctx).Where("query_id = ?", queryID).Order("executed_at DESC").Limit(100).Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list query executions: %v", err)
	}
	out := make([]*datav1.QueryExecution, 0, len(rows))
	for i := range rows {
		out = append(out, queryExecutionToPB(&rows[i]))
	}
	return &datav1.ListQueryExecutionsResponse{Items: out}, nil
}

// ExecuteDataQuery bridges to the HTTP handler for the mutation-gating,
// approval, sampling, and result-recording pipeline.
func (s *DataQueryServiceServer) ExecuteDataQuery(ctx context.Context, req *datav1.ExecuteDataQueryRequest) (*datav1.ExecuteDataQueryResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	headers := map[string]string{
		"X-User-ID": req.GetActorId(),
	}
	code, body, err := s.dispatchHTTP("POST", "/api/v1/data/queries/"+id.String()+"/execute", headers, nil)
	if err != nil {
		return nil, err
	}
	if err := statusFromHTTP(code, body); err != nil {
		return nil, err
	}
	data, err := decodeEnvelope(body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode execute response: %v", err)
	}
	var exec domain.QueryExecution
	if len(data) > 0 {
		if err := json.Unmarshal(data, &exec); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal execution: %v", err)
		}
	}
	return &datav1.ExecuteDataQueryResponse{Execution: queryExecutionToPB(&exec)}, nil
}

// NaturalLanguageQuery bridges to the HTTP handler for NL→SQL.
func (s *DataQueryServiceServer) NaturalLanguageQuery(ctx context.Context, req *datav1.NaturalLanguageQueryRequest) (*datav1.NaturalLanguageQueryResponse, error) {
	if req.GetQuestion() == "" {
		return nil, status.Error(codes.InvalidArgument, "question is required")
	}
	body := map[string]any{"question": req.GetQuestion()}
	if dsStr := req.GetDataSourceId(); dsStr != "" {
		body["data_source_id"] = dsStr
	}
	headers := map[string]string{
		"X-Organization-ID": req.GetOrganizationId(),
		"X-User-ID":         req.GetActorId(),
	}
	code, payload, err := s.dispatchHTTP("POST", "/api/v1/data/nl-query", headers, body)
	if err != nil {
		return nil, err
	}
	if err := statusFromHTTP(code, payload); err != nil {
		return nil, err
	}
	data, err := decodeEnvelope(payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode nl response: %v", err)
	}
	// The HTTP handler returns the NL result either at the envelope's
	// data field or as the raw payload. We model the BFF-shaped fields
	// onto the proto.
	type httpCandidate struct {
		DataSourceID string  `json:"data_source_id"`
		Name         string  `json:"name"`
		Engine       string  `json:"engine"`
		Confidence   float64 `json:"confidence"`
		Reasoning    string  `json:"reasoning,omitempty"`
	}
	type httpResult struct {
		QueryID              string          `json:"query_id"`
		Question             string          `json:"question"`
		GeneratedSQL         string          `json:"generated_sql"`
		SemanticContext      string          `json:"semantic_context"`
		Status               string          `json:"status"`
		NeedsSelection       bool            `json:"needs_selection,omitempty"`
		SelectedDataSourceID string          `json:"selected_data_source_id,omitempty"`
		SelectionReasoning   string          `json:"selection_reasoning,omitempty"`
		Candidates           []httpCandidate `json:"candidates,omitempty"`
	}
	var raw httpResult
	if len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			// Try the raw body as a fallback.
			if err2 := json.Unmarshal(payload, &raw); err2 != nil {
				return nil, status.Errorf(codes.Internal, "unmarshal nl result: %v", err)
			}
		}
	}
	out := &datav1.NLQueryResult{
		QueryId:              raw.QueryID,
		Question:             raw.Question,
		GeneratedSql:         raw.GeneratedSQL,
		SemanticContext:      raw.SemanticContext,
		Status:               raw.Status,
		NeedsSelection:       raw.NeedsSelection,
		SelectedDataSourceId: raw.SelectedDataSourceID,
		SelectionReasoning:   raw.SelectionReasoning,
	}
	for _, c := range raw.Candidates {
		out.Candidates = append(out.Candidates, &datav1.NLQueryCandidate{
			DataSourceId: c.DataSourceID,
			Name:         c.Name,
			Engine:       c.Engine,
			Confidence:   c.Confidence,
			Reasoning:    c.Reasoning,
		})
	}
	return &datav1.NaturalLanguageQueryResponse{Result: out}, nil
}
