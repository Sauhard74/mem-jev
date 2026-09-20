package selection

import (
	"context"
	"errors"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrIdempotencyConflict  = errors.New("selection idempotency identity was reused with different content")
	ErrRetrievalRunConflict = errors.New("retrieval run already has a different selection")
	ErrSelectionNotFound    = errors.New("selection was not found")
	ErrSelectionExpired     = errors.New("selection idempotency identity has expired")
)

type Disposition string

const (
	DispositionCommitted Disposition = "committed"
	DispositionDuplicate Disposition = "duplicate"
)

type CommitRequest struct {
	TenantID                domain.TenantID
	IdempotencyIdentityHash string
	Draft                   Draft
}

type Receipt struct {
	Record      Record
	Disposition Disposition
}

type Repository interface {
	Commit(context.Context, CommitRequest) (Receipt, error)
	FindByInjectionID(context.Context, domain.TenantID, string) (Record, error)
}

type RetentionPolicy struct {
	TTL time.Duration
}

func (policy RetentionPolicy) Validate() error {
	if policy.TTL < time.Minute || policy.TTL > 30*24*time.Hour {
		return ErrInvalidSelection
	}
	return nil
}

func ValidateCommitRequest(request CommitRequest) error {
	if request.TenantID == "" || request.Draft.TenantID != request.TenantID || !sha256Pattern.MatchString(request.IdempotencyIdentityHash) || ValidateDraft(request.Draft) != nil {
		return ErrInvalidSelection
	}
	return nil
}
