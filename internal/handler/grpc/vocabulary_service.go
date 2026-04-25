package grpc

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	datav1 "github.com/sentiae/data-service/gen/data/v1"
	"github.com/sentiae/data-service/internal/domain"
)

// VocabularyServiceServer wraps the per-org business-term annotation
// CRUD surface.
type VocabularyServiceServer struct {
	datav1.UnimplementedVocabularyServiceServer
	baseServer
}

func NewVocabularyServiceServer(deps Deps) *VocabularyServiceServer {
	return &VocabularyServiceServer{baseServer: baseServer{deps: deps}}
}

func (s *VocabularyServiceServer) ListVocabulary(ctx context.Context, req *datav1.ListVocabularyRequest) (*datav1.ListVocabularyResponse, error) {
	q := s.deps.DB.WithContext(ctx).Model(&domain.OrgVocabulary{})
	if orgStr := req.GetOrganizationId(); orgStr != "" {
		orgID, err := parseUUID(orgStr, "organization_id")
		if err != nil {
			return nil, err
		}
		q = q.Where("organization_id = ?", orgID)
	}
	if colID := req.GetColumnId(); colID != "" {
		q = q.Where("column_id = ?", colID)
	}
	if term := req.GetBusinessTerm(); term != "" {
		q = q.Where("business_term ILIKE ?", "%"+term+"%")
	}
	var rows []domain.OrgVocabulary
	if err := q.Order("updated_at DESC").Limit(200).Find(&rows).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "list vocabulary: %v", err)
	}
	out := make([]*datav1.VocabularyEntry, 0, len(rows))
	for i := range rows {
		out = append(out, vocabularyEntryToPB(&rows[i]))
	}
	return &datav1.ListVocabularyResponse{Items: out}, nil
}

func (s *VocabularyServiceServer) CreateVocabulary(ctx context.Context, req *datav1.CreateVocabularyRequest) (*datav1.CreateVocabularyResponse, error) {
	orgID, err := parseUUID(req.GetOrganizationId(), "organization_id")
	if err != nil {
		return nil, err
	}
	colID := strings.TrimSpace(req.GetColumnId())
	term := strings.TrimSpace(req.GetBusinessTerm())
	if colID == "" || term == "" {
		return nil, status.Error(codes.InvalidArgument, "column_id and business_term are required")
	}
	actor, _ := optionalUUID(req.GetActorId())
	var actorID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	row := &domain.OrgVocabulary{
		ID:             uuid.New(),
		OrganizationID: orgID,
		ColumnID:       colID,
		BusinessTerm:   term,
		Synonyms:       domain.StringArray(req.GetSynonyms()),
		Aliases:        domain.StringArray(req.GetAliases()),
		Unit:           req.GetUnit(),
		DataType:       req.GetDataType(),
		Format:         req.GetFormat(),
		Description:    req.GetDescription(),
		CreatedBy:      actorID,
		UpdatedBy:      actorID,
	}
	if err := s.deps.DB.WithContext(ctx).Create(row).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "create vocabulary: %v", err)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "org_vocabulary", row.ID.String(), row)
	}
	return &datav1.CreateVocabularyResponse{Entry: vocabularyEntryToPB(row)}, nil
}

func (s *VocabularyServiceServer) UpdateVocabulary(ctx context.Context, req *datav1.UpdateVocabularyRequest) (*datav1.UpdateVocabularyResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	q := s.deps.DB.WithContext(ctx).Where("id = ?", id)
	if orgStr := req.GetOrganizationId(); orgStr != "" {
		orgID, err := parseUUID(orgStr, "organization_id")
		if err != nil {
			return nil, err
		}
		q = q.Where("organization_id = ?", orgID)
	}
	var row domain.OrgVocabulary
	if err := q.First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "vocabulary entry not found")
		}
		return nil, status.Errorf(codes.Internal, "lookup vocabulary: %v", err)
	}
	actor, _ := optionalUUID(req.GetActorId())
	var actorID uuid.UUID
	if actor != nil {
		actorID = *actor
	}
	updates := map[string]any{"updated_by": actorID}
	if req.GetUpdateBusinessTerm() {
		updates["business_term"] = strings.TrimSpace(req.GetBusinessTerm())
	}
	if req.GetUpdateSynonyms() {
		updates["synonyms"] = domain.StringArray(req.GetSynonyms())
	}
	if req.GetUpdateAliases() {
		updates["aliases"] = domain.StringArray(req.GetAliases())
	}
	if req.GetUpdateUnit() {
		updates["unit"] = req.GetUnit()
	}
	if req.GetUpdateDataType() {
		updates["data_type"] = req.GetDataType()
	}
	if req.GetUpdateFormat() {
		updates["format"] = req.GetFormat()
	}
	if req.GetUpdateDescription() {
		updates["description"] = req.GetDescription()
	}
	if err := s.deps.DB.WithContext(ctx).Model(&row).Updates(updates).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "update vocabulary: %v", err)
	}
	s.deps.DB.WithContext(ctx).Where("id = ?", id).First(&row)
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "org_vocabulary", row.ID.String(), row)
	}
	return &datav1.UpdateVocabularyResponse{Entry: vocabularyEntryToPB(&row)}, nil
}

func (s *VocabularyServiceServer) DeleteVocabulary(ctx context.Context, req *datav1.DeleteVocabularyRequest) (*datav1.DeleteVocabularyResponse, error) {
	id, err := parseUUID(req.GetId(), "id")
	if err != nil {
		return nil, err
	}
	q := s.deps.DB.WithContext(ctx).Where("id = ?", id)
	if orgStr := req.GetOrganizationId(); orgStr != "" {
		orgID, err := parseUUID(orgStr, "organization_id")
		if err != nil {
			return nil, err
		}
		q = q.Where("organization_id = ?", orgID)
	}
	res := q.Delete(&domain.OrgVocabulary{})
	if res.Error != nil {
		return nil, status.Errorf(codes.Internal, "delete vocabulary: %v", res.Error)
	}
	if s.deps.Recorder != nil {
		_ = s.deps.Recorder.RecordEntity(ctx, "org_vocabulary", id.String(), map[string]any{"id": id.String(), "deleted": true})
	}
	return &datav1.DeleteVocabularyResponse{Deleted: res.RowsAffected > 0}, nil
}
