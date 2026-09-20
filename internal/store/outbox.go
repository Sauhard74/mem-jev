package store

import (
	"context"
	"errors"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrInvalidLeaseRequest = errors.New("invalid outbox lease request")
	ErrStaleLease          = errors.New("stale outbox lease")
	ErrOutboxJobNotFound   = errors.New("outbox job not found")
)

type OutboxState string

const (
	OutboxStatePending    OutboxState = "pending"
	OutboxStateLeased     OutboxState = "leased"
	OutboxStateCompleted  OutboxState = "completed"
	OutboxStateDeadLetter OutboxState = "dead_letter"
)

type OutboxJobType string

const (
	OutboxJobSynthesizeTrace   OutboxJobType = "synthesize_trace"
	OutboxJobSynthesizeOutcome OutboxJobType = "synthesize_outcome"
)

type ClaimOutboxRequest struct {
	WorkerID      string
	JobTypes      []OutboxJobType
	Limit         int
	LeaseDuration time.Duration
}

type OutboxLease struct {
	TenantID       domain.TenantID
	WorkflowID     string
	TraceID        domain.TraceID
	OutcomeID      domain.OutcomeID
	JobType        OutboxJobType
	ContentHash    string
	AttemptCount   int
	FencingToken   uint64
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

type ReleaseOutboxRequest struct {
	TenantID     domain.TenantID
	WorkflowID   string
	WorkerID     string
	FencingToken uint64
	RetryDelay   time.Duration
	ErrorCode    string
	DeadLetter   bool
}

type CompleteOutboxRequest struct {
	TenantID     domain.TenantID
	WorkflowID   string
	WorkerID     string
	FencingToken uint64
}

type OutboxRepository interface {
	ClaimOutbox(context.Context, ClaimOutboxRequest) ([]OutboxLease, error)
	ReleaseOutbox(context.Context, ReleaseOutboxRequest) error
	CompleteOutbox(context.Context, CompleteOutboxRequest) error
}

func ValidateClaimOutbox(request ClaimOutboxRequest) error {
	if request.WorkerID == "" || request.Limit < 1 || request.Limit > 100 ||
		request.LeaseDuration < time.Second || request.LeaseDuration > 15*time.Minute || len(request.JobTypes) == 0 {
		return ErrInvalidLeaseRequest
	}
	seen := make(map[OutboxJobType]struct{}, len(request.JobTypes))
	for _, jobType := range request.JobTypes {
		if jobType != OutboxJobSynthesizeTrace && jobType != OutboxJobSynthesizeOutcome {
			return ErrInvalidLeaseRequest
		}
		if _, exists := seen[jobType]; exists {
			return ErrInvalidLeaseRequest
		}
		seen[jobType] = struct{}{}
	}
	return nil
}

func ValidateReleaseOutbox(request ReleaseOutboxRequest) error {
	if request.TenantID == "" || request.WorkflowID == "" || request.WorkerID == "" || request.FencingToken == 0 ||
		request.ErrorCode == "" || request.RetryDelay < 0 || request.RetryDelay > 24*time.Hour ||
		(request.DeadLetter && request.RetryDelay != 0) {
		return ErrInvalidLeaseRequest
	}
	return nil
}

func ValidateCompleteOutbox(request CompleteOutboxRequest) error {
	if request.TenantID == "" || request.WorkflowID == "" || request.WorkerID == "" || request.FencingToken == 0 {
		return ErrInvalidLeaseRequest
	}
	return nil
}
