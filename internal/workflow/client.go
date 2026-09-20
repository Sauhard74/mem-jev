package workflow

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/store"
)

var ErrInvalidStartRequest = errors.New("invalid workflow start request")

var lowercaseSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

type StartDisposition string

const (
	StartDispositionStarted  StartDisposition = "started"
	StartDispositionExisting StartDisposition = "existing"
)

type SynthesisInput struct {
	SchemaVersion string              `json:"schema_version"`
	TenantID      domain.TenantID     `json:"tenant_id"`
	TraceID       domain.TraceID      `json:"trace_id"`
	OutcomeID     domain.OutcomeID    `json:"outcome_id,omitempty"`
	JobType       store.OutboxJobType `json:"job_type"`
	ContentHash   string              `json:"content_hash"`
}

type StartRequest struct {
	WorkflowID string
	Input      SynthesisInput
}

type StartReceipt struct {
	WorkflowID  string
	RunID       string
	Disposition StartDisposition
}

type Starter interface {
	Start(context.Context, StartRequest) (StartReceipt, error)
}

type ProviderError struct {
	Code      string
	Retryable bool
	Err       error
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("workflow provider %s: %v", e.Code, e.Err)
}

func (e *ProviderError) Unwrap() error { return e.Err }

type TemporalConnectionConfig struct {
	Address   string
	Namespace string
	APIKey    string
	TLS       *tls.Config
}

type TemporalStarterConfig struct {
	TaskQueue                string
	WorkflowName             string
	WorkflowExecutionTimeout time.Duration
	WorkflowTaskTimeout      time.Duration
}

func RequestFromLease(lease store.OutboxLease) (StartRequest, error) {
	request := StartRequest{
		WorkflowID: lease.WorkflowID,
		Input: SynthesisInput{
			SchemaVersion: "synthesis-input.v1", TenantID: lease.TenantID, TraceID: lease.TraceID,
			OutcomeID: lease.OutcomeID, JobType: lease.JobType, ContentHash: lease.ContentHash,
		},
	}
	if err := ValidateStartRequest(request); err != nil {
		return StartRequest{}, err
	}
	return request, nil
}

func ValidateStartRequest(request StartRequest) error {
	if request.WorkflowID == "" || request.Input.SchemaVersion != "synthesis-input.v1" || request.Input.TenantID == "" ||
		request.Input.TraceID == "" || !lowercaseSHA256.MatchString(request.Input.ContentHash) {
		return ErrInvalidStartRequest
	}
	var expected string
	switch request.Input.JobType {
	case store.OutboxJobSynthesizeTrace:
		if request.Input.OutcomeID != "" {
			return ErrInvalidStartRequest
		}
		expected = store.WorkflowID(request.Input.TenantID, request.Input.TraceID)
	case store.OutboxJobSynthesizeOutcome:
		if request.Input.OutcomeID == "" {
			return ErrInvalidStartRequest
		}
		expected = store.OutcomeWorkflowID(request.Input.TenantID, request.Input.TraceID, request.Input.OutcomeID)
	default:
		return ErrInvalidStartRequest
	}
	if request.WorkflowID != expected {
		return ErrInvalidStartRequest
	}
	return nil
}
