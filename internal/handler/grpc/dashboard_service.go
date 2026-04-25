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

// DashboardServiceServer wraps DashboardConfig CRUD + permissions +
// embed-token rotation + public embed view (the last two via the
// HTTP-handler bridge).
type DashboardServiceServer struct {
	datav1.UnimplementedDashboardServiceServer
	baseServer
}

func NewDashboardServiceServer(deps Deps) *DashboardServiceServer {
	return &DashboardServiceServer{baseServer: baseServer{deps: deps}}
}

// CreateDashboard mirrors POST /api/v1/data/dashboards.
func (s *DashboardServiceServer) CreateDashboard(ctx context.Context, req *datav1.CreateDashboardRequest) (*datav1.CreateDashboardResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
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
	d := &domain.DashboardConfig{
		ID:             uuid.New(),
		OrganizationID: orgID,
		CanvasNodeID:   canvasID,
		Name:           req.GetName(),
		Description:    req.GetDescription(),
		Version:        1,
		Panels:         domain.JSONMap(structToMap(req.GetPanels())),
		Layout:         domain.JSONMap(structToMap(req.GetLayout())),
		CreatedBy:      actorID,
	}
	if d.Panels == nil {
		d.Panels = domain.JSONMap{}
	}
	if err := s.deps.DB.WithContext(ctx).Create(d).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "create dashboard: %v", err)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "dashboard_config", d.ID.String(), d)
	}
	return &datav1.CreateDashboardResponse{Dashboard: dashboardConfigToPB(d)}, nil
}

func (s *DashboardServiceServer) GetDashboard(ctx context.Context, req *datav1.GetDashboardRequest) (*datav1.GetDashboardResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var d domain.DashboardConfig
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&d).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "dashboard not found")
		}
		return nil, status.Errorf(codes.Internal, "get dashboard: %v", err)
	}
	return &datav1.GetDashboardResponse{Dashboard: dashboardConfigToPB(&d)}, nil
}

func (s *DashboardServiceServer) UpdateDashboard(ctx context.Context, req *datav1.UpdateDashboardRequest) (*datav1.UpdateDashboardResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	var d domain.DashboardConfig
	if err := s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&d).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "dashboard not found")
		}
		return nil, status.Errorf(codes.Internal, "lookup dashboard: %v", err)
	}
	updates := map[string]any{}
	if req.GetUpdateName() {
		updates["name"] = req.GetName()
	}
	if req.GetUpdateDescription() {
		updates["description"] = req.GetDescription()
	}
	if req.GetUpdatePanels() {
		updates["panels"] = domain.JSONMap(structToMap(req.GetPanels()))
	}
	if req.GetUpdateLayout() {
		updates["layout"] = domain.JSONMap(structToMap(req.GetLayout()))
	}
	if len(updates) > 0 {
		if err := s.deps.DB.WithContext(ctx).Model(&d).Updates(updates).Error; err != nil {
			return nil, status.Errorf(codes.Internal, "update dashboard: %v", err)
		}
		s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&d)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "dashboard_config", d.ID.String(), d)
	}
	return &datav1.UpdateDashboardResponse{Dashboard: dashboardConfigToPB(&d)}, nil
}

func (s *DashboardServiceServer) DeleteDashboard(ctx context.Context, req *datav1.DeleteDashboardRequest) (*datav1.DeleteDashboardResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	res := s.deps.DB.WithContext(ctx).Where("id = ?", id).Delete(&domain.DashboardConfig{})
	if res.Error != nil {
		return nil, status.Errorf(codes.Internal, "delete dashboard: %v", res.Error)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "dashboard_config", id.String(), map[string]any{"id": id.String(), "deleted": true})
	}
	return &datav1.DeleteDashboardResponse{Deleted: res.RowsAffected > 0}, nil
}

func (s *DashboardServiceServer) ListDashboards(ctx context.Context, req *datav1.ListDashboardsRequest) (*datav1.ListDashboardsResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	var rows []domain.DashboardConfig
	if err := s.deps.DB.WithContext(ctx).Where("organization_id = ?", orgID).Order("updated_at DESC").Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list dashboards: %v", err)
	}
	out := make([]*datav1.DashboardConfig, 0, len(rows))
	for i := range rows {
		out = append(out, dashboardConfigToPB(&rows[i]))
	}
	return &datav1.ListDashboardsResponse{Items: out}, nil
}

// --- Permissions -----------------------------------------------------------

func (s *DashboardServiceServer) ListDashboardPermissions(ctx context.Context, req *datav1.ListDashboardPermissionsRequest) (*datav1.ListDashboardPermissionsResponse, error) {
	dashID, err := parseUUID(req.GetDashboardId(), "dashboard_id")
	if err != nil {
		return nil, err
	}
	var rows []domain.DashboardPermission
	if err := s.deps.DB.WithContext(ctx).Where("dashboard_id = ?", dashID).Order("created_at DESC").Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list permissions: %v", err)
	}
	out := make([]*datav1.DashboardPermission, 0, len(rows))
	for i := range rows {
		out = append(out, dashboardPermissionToPB(&rows[i]))
	}
	return &datav1.ListDashboardPermissionsResponse{Items: out}, nil
}

func (s *DashboardServiceServer) CreateDashboardPermission(ctx context.Context, req *datav1.CreateDashboardPermissionRequest) (*datav1.CreateDashboardPermissionResponse, error) {
	dashID, err := parseUUID(req.GetDashboardId(), "dashboard_id")
	if err != nil {
		return nil, err
	}
	principalID, err := parseUUID(req.GetPrincipalId(), "principal_id")
	if err != nil {
		return nil, err
	}
	if req.GetPrincipalType() == "" || req.GetPermission() == "" {
		return nil, status.Error(codes.InvalidArgument, "principal_type and permission are required")
	}
	actor, _ := optionalUUID(req.GetActorId())
	var actorID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	p := &domain.DashboardPermission{
		ID:            uuid.New(),
		DashboardID:   dashID,
		PrincipalType: domain.DashboardPrincipalType(req.GetPrincipalType()),
		PrincipalID:   principalID,
		Permission:    domain.DashboardPermissionLevel(req.GetPermission()),
		GrantedBy:     actorID,
	}
	if err := s.deps.DB.WithContext(ctx).Create(p).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "create permission: %v", err)
	}
	return &datav1.CreateDashboardPermissionResponse{Permission: dashboardPermissionToPB(p)}, nil
}

func (s *DashboardServiceServer) DeleteDashboardPermission(ctx context.Context, req *datav1.DeleteDashboardPermissionRequest) (*datav1.DeleteDashboardPermissionResponse, error) {
	dashID, err := parseUUID(req.GetDashboardId(), "dashboard_id")
	if err != nil {
		return nil, err
	}
	permID, err := parseUUID(req.GetPermissionId(), "permission_id")
	if err != nil {
		return nil, err
	}
	res := s.deps.DB.WithContext(ctx).Where("id = ? AND dashboard_id = ?", permID, dashID).Delete(&domain.DashboardPermission{})
	if res.Error != nil {
		return nil, status.Errorf(codes.Internal, "delete permission: %v", res.Error)
	}
	return &datav1.DeleteDashboardPermissionResponse{Deleted: res.RowsAffected > 0}, nil
}

// --- Embed token + view (HTTP bridge) --------------------------------------

func (s *DashboardServiceServer) RotateDashboardEmbedToken(ctx context.Context, req *datav1.RotateDashboardEmbedTokenRequest) (*datav1.RotateDashboardEmbedTokenResponse, error) {
	dashID, err := parseUUID(req.GetDashboardId(), "dashboard_id")
	if err != nil {
		return nil, err
	}
	headers := map[string]string{"X-User-ID": req.GetActorId()}
	code, body, err := s.dispatchHTTP("POST", "/api/v1/data/dashboards/"+dashID.String()+"/embed-token/rotate", headers, nil)
	if err != nil {
		return nil, err
	}
	if err := statusFromHTTP(code, body); err != nil {
		return nil, err
	}
	data, err := decodeEnvelope(body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode embed token response: %v", err)
	}
	type httpToken struct {
		DashboardID string `json:"dashboard_id"`
		Token       string `json:"embed_token"`
		ExpiresAt   string `json:"embed_token_expires_at,omitempty"`
		Enabled     bool   `json:"embed_enabled"`
	}
	var raw httpToken
	if len(data) > 0 {
		_ = json.Unmarshal(data, &raw)
	}
	out := &datav1.DashboardEmbedToken{
		DashboardId: raw.DashboardID,
		EmbedToken:  raw.Token,
		Enabled:     raw.Enabled,
	}
	if raw.ExpiresAt != "" {
		// Best-effort parse; the wire layer accepts nil when missing.
		if t, ok := parseRFC3339(raw.ExpiresAt); ok {
			out.ExpiresAt = toTs(t)
		}
	}
	return &datav1.RotateDashboardEmbedTokenResponse{Token: out}, nil
}

func (s *DashboardServiceServer) GetDashboardByEmbedToken(ctx context.Context, req *datav1.GetDashboardByEmbedTokenRequest) (*datav1.GetDashboardByEmbedTokenResponse, error) {
	if req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}
	code, body, err := s.dispatchHTTP("GET", "/api/v1/data/dashboards/embed/"+req.GetToken(), nil, nil)
	if err != nil {
		return nil, err
	}
	if code == 404 {
		return &datav1.GetDashboardByEmbedTokenResponse{}, nil
	}
	if err := statusFromHTTP(code, body); err != nil {
		return nil, err
	}
	data, err := decodeEnvelope(body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode embed view: %v", err)
	}
	var view domain.DashboardConfig
	if len(data) > 0 {
		_ = json.Unmarshal(data, &view)
	}
	out := &datav1.DashboardEmbedView{
		Id:             view.ID.String(),
		OrganizationId: view.OrganizationID.String(),
		Name:           view.Name,
		Description:    view.Description,
		Version:        int32(view.Version),
	}
	if len(view.Panels) > 0 {
		out.Panels = structFromAny(view.Panels)
	}
	if len(view.Layout) > 0 {
		out.Layout = structFromAny(view.Layout)
	}
	return &datav1.GetDashboardByEmbedTokenResponse{Dashboard: out}, nil
}
