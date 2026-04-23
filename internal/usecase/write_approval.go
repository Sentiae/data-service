// Package usecase hosts application services that orchestrate domain logic.
//
// write_approval implements the write-approval guard: mutations must be
// approved by an authorized user before the query engine will execute them.
package usecase

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

// mutationRe detects SQL statements that mutate data. The pattern is
// deliberately conservative — it only matches the first keyword after any
// leading comments or whitespace.
var mutationRe = regexp.MustCompile(`(?is)^\s*(?:--[^\n]*\n|/\*.*?\*/|\s)*?\b(insert|update|delete|truncate|drop|alter|create|grant|revoke|merge|replace)\b`)

// IsMutation returns true when the supplied SQL looks like a write/DDL
// statement. It is used as the gate for the write-approval workflow.
func IsMutation(sql string) bool {
	if sql == "" {
		return false
	}
	return mutationRe.MatchString(sql)
}

// DetectedMutation returns the keyword that triggered the mutation detection
// (uppercased), or an empty string if the SQL is read-only.
func DetectedMutation(sql string) string {
	m := mutationRe.FindStringSubmatch(sql)
	if len(m) < 2 {
		return ""
	}
	return strings.ToUpper(m[1])
}

// Sentinel errors returned by ValidateApprovalForExecute so the handler
// layer can map each failure mode to the right HTTP status without
// string matching.
var (
	// ErrApprovalRequired — caller supplied no approval_id for a mutation query.
	ErrApprovalRequired = errors.New("approval_id is required to execute a mutation query")
	// ErrApprovalNotFound — approval_id does not exist.
	ErrApprovalNotFound = errors.New("approval record not found")
	// ErrApprovalNotApproved — approval exists but is pending, rejected, or in some other non-approved state.
	ErrApprovalNotApproved = errors.New("approval is not in approved state")
	// ErrApprovalMismatch — approval exists but belongs to a different query.
	ErrApprovalMismatch = errors.New("approval does not belong to this query")
	// ErrApprovalAlreadyExecuted — approval was already consumed by a prior execution.
	ErrApprovalAlreadyExecuted = errors.New("approval has already been executed")
	// ErrApprovalExpired — approval expired (future: if TTL is added).
	ErrApprovalExpired = errors.New("approval has expired")
)

// approvalTTL caps how long an approval remains valid after being granted.
// Tuned conservatively — a mutation approved two days ago should not
// silently run today.
const approvalTTL = 24 * time.Hour

// WriteApprovalService manages the approval lifecycle for mutation queries.
type WriteApprovalService struct {
	db *gorm.DB
}

// NewWriteApprovalService wires the service to its persistence layer.
func NewWriteApprovalService(db *gorm.DB) *WriteApprovalService {
	return &WriteApprovalService{db: db}
}

// RequestApproval creates a pending approval record for the supplied query.
// If an open approval already exists for this query, it is returned instead
// to avoid creating duplicate pending requests.
func (s *WriteApprovalService) RequestApproval(ctx context.Context, query *domain.DataQuery, requestedBy uuid.UUID) (*domain.QueryApproval, error) {
	if query == nil {
		return nil, errors.New("query is required")
	}

	var existing domain.QueryApproval
	err := s.db.WithContext(ctx).
		Where("query_id = ? AND status = ?", query.ID, domain.QueryApprovalStatusPending).
		First(&existing).Error
	if err == nil {
		return &existing, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("lookup pending approval: %w", err)
	}

	approval := &domain.QueryApproval{
		ID:             uuid.New(),
		QueryID:        query.ID,
		OrganizationID: query.OrganizationID,
		RequestedBy:    requestedBy,
		Status:         domain.QueryApprovalStatusPending,
		SQLSnapshot:    query.RawQuery,
		DetectedOps:    DetectedMutation(query.RawQuery),
	}
	if err := s.db.WithContext(ctx).Create(approval).Error; err != nil {
		return nil, fmt.Errorf("create approval: %w", err)
	}
	return approval, nil
}

// Approve marks the pending approval for queryID as approved by approver.
func (s *WriteApprovalService) Approve(ctx context.Context, queryID, approver uuid.UUID) (*domain.QueryApproval, error) {
	var approval domain.QueryApproval
	err := s.db.WithContext(ctx).
		Where("query_id = ? AND status = ?", queryID, domain.QueryApprovalStatusPending).
		First(&approval).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("no pending approval found for this query")
		}
		return nil, fmt.Errorf("lookup approval: %w", err)
	}

	now := time.Now()
	approval.Status = domain.QueryApprovalStatusApproved
	approval.ApprovedBy = &approver
	approval.ApprovedAt = &now
	if err := s.db.WithContext(ctx).Save(&approval).Error; err != nil {
		return nil, fmt.Errorf("save approval: %w", err)
	}
	return &approval, nil
}

// Reject marks the pending approval for queryID as rejected.
func (s *WriteApprovalService) Reject(ctx context.Context, queryID, rejector uuid.UUID, reason string) (*domain.QueryApproval, error) {
	var approval domain.QueryApproval
	err := s.db.WithContext(ctx).
		Where("query_id = ? AND status = ?", queryID, domain.QueryApprovalStatusPending).
		First(&approval).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("no pending approval found for this query")
		}
		return nil, fmt.Errorf("lookup approval: %w", err)
	}

	approval.Status = domain.QueryApprovalStatusRejected
	approval.Reason = reason
	approval.ApprovedBy = &rejector
	if err := s.db.WithContext(ctx).Save(&approval).Error; err != nil {
		return nil, fmt.Errorf("save approval: %w", err)
	}
	return &approval, nil
}

// Get returns an approval by its primary ID. Used by the execute-time
// enforcement path so the handler can verify a caller-supplied
// approval_id before running a mutation.
func (s *WriteApprovalService) Get(ctx context.Context, approvalID uuid.UUID) (*domain.QueryApproval, error) {
	var approval domain.QueryApproval
	if err := s.db.WithContext(ctx).Where("id = ?", approvalID).First(&approval).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrApprovalNotFound
		}
		return nil, fmt.Errorf("lookup approval: %w", err)
	}
	return &approval, nil
}

// ValidateApprovalForExecute is the single source of truth for the
// "can this execute call proceed?" question on mutation queries. It is
// intentionally separated from the HTTP handler so every future
// execute path (CLI, worker, cron) goes through the same checks.
//
// Rules enforced, in order:
//
//  1. approvalID must be non-nil
//  2. approval row must exist
//  3. approval.QueryID must match queryID (no cross-query reuse)
//  4. approval.Status must be Approved (not Pending / Rejected / Executed)
//  5. approval.ApprovedAt must be within approvalTTL
//
// Each failure returns a typed sentinel the handler maps to an HTTP
// status code (403 for auth, 409 for already-executed, 400 for
// missing, etc.).
func (s *WriteApprovalService) ValidateApprovalForExecute(ctx context.Context, queryID uuid.UUID, approvalID *uuid.UUID) (*domain.QueryApproval, error) {
	if approvalID == nil || *approvalID == uuid.Nil {
		return nil, ErrApprovalRequired
	}
	approval, err := s.Get(ctx, *approvalID)
	if err != nil {
		return nil, err
	}
	if approval.QueryID != queryID {
		return nil, ErrApprovalMismatch
	}
	switch approval.Status {
	case domain.QueryApprovalStatusApproved:
		// fall through to TTL check
	case domain.QueryApprovalStatusExecuted:
		return nil, ErrApprovalAlreadyExecuted
	default:
		return nil, ErrApprovalNotApproved
	}
	if approval.ApprovedAt == nil {
		return nil, ErrApprovalNotApproved
	}
	if time.Since(*approval.ApprovedAt) > approvalTTL {
		return nil, ErrApprovalExpired
	}
	return approval, nil
}

// MarkExecuted flips every approved row for queryID to executed. Kept
// for callers that only have the queryID handy. Prefer
// MarkExecutedByID so a specific approval is consumed atomically.
func (s *WriteApprovalService) MarkExecuted(ctx context.Context, queryID uuid.UUID) error {
	return s.db.WithContext(ctx).
		Model(&domain.QueryApproval{}).
		Where("query_id = ? AND status = ?", queryID, domain.QueryApprovalStatusApproved).
		Update("status", domain.QueryApprovalStatusExecuted).Error
}

// MarkExecutedByID atomically consumes the specific approval identified
// by approvalID. It uses a conditional UPDATE (status=Approved) so two
// concurrent execute calls with the same approval_id cannot both
// succeed — the second call sees RowsAffected=0 and gets
// ErrApprovalAlreadyExecuted bubbled up.
func (s *WriteApprovalService) MarkExecutedByID(ctx context.Context, approvalID uuid.UUID) error {
	res := s.db.WithContext(ctx).
		Model(&domain.QueryApproval{}).
		Where("id = ? AND status = ?", approvalID, domain.QueryApprovalStatusApproved).
		Update("status", domain.QueryApprovalStatusExecuted)
	if res.Error != nil {
		return fmt.Errorf("mark executed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrApprovalAlreadyExecuted
	}
	return nil
}
