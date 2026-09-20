package memory

import (
	"context"
	"sync"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/selection"
)

type SelectionRepository struct {
	mu            sync.RWMutex
	policy        selection.RetentionPolicy
	now           func() time.Time
	byIdempotency map[selectionKey]selection.Record
	byInjection   map[selectionKey]selection.Record
	byRun         map[selectionKey]selection.Record
}

type selectionKey struct {
	tenantID domain.TenantID
	identity string
}

func NewSelectionRepository(policy selection.RetentionPolicy) (*SelectionRepository, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &SelectionRepository{
		policy: policy, now: time.Now,
		byIdempotency: make(map[selectionKey]selection.Record), byInjection: make(map[selectionKey]selection.Record), byRun: make(map[selectionKey]selection.Record),
	}, nil
}

func (r *SelectionRepository) Commit(ctx context.Context, request selection.CommitRequest) (selection.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return selection.Receipt{}, err
	}
	if r == nil || selection.ValidateCommitRequest(request) != nil {
		return selection.Receipt{}, selection.ErrInvalidSelection
	}
	now := r.now().UTC()
	idempotencyKey := selectionKey{tenantID: request.TenantID, identity: request.IdempotencyIdentityHash}
	runKey := selectionKey{tenantID: request.TenantID, identity: request.Draft.RetrievalRunID}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, found := r.byIdempotency[idempotencyKey]; found {
		if !now.Before(existing.ExpiresAt) {
			return selection.Receipt{}, selection.ErrSelectionExpired
		} else if existing.DraftHash != request.Draft.ContentHash {
			return selection.Receipt{}, selection.ErrIdempotencyConflict
		} else {
			return selection.Receipt{Record: selection.CloneRecord(existing), Disposition: selection.DispositionDuplicate}, nil
		}
	}
	if existing, found := r.byRun[runKey]; found {
		if !now.Before(existing.ExpiresAt) {
			return selection.Receipt{}, selection.ErrSelectionExpired
		} else if existing.DraftHash != request.Draft.ContentHash || existing.IdempotencyIdentityHash != request.IdempotencyIdentityHash {
			return selection.Receipt{}, selection.ErrRetrievalRunConflict
		} else {
			return selection.Receipt{Record: selection.CloneRecord(existing), Disposition: selection.DispositionDuplicate}, nil
		}
	}
	record, err := selection.NewRecord(request.Draft, request.IdempotencyIdentityHash, now, now.Add(r.policy.TTL))
	if err != nil {
		return selection.Receipt{}, err
	}
	injectionKey := selectionKey{tenantID: request.TenantID, identity: record.InjectionID}
	r.byIdempotency[idempotencyKey] = selection.CloneRecord(record)
	r.byInjection[injectionKey] = selection.CloneRecord(record)
	r.byRun[runKey] = selection.CloneRecord(record)
	return selection.Receipt{Record: selection.CloneRecord(record), Disposition: selection.DispositionCommitted}, nil
}

func (r *SelectionRepository) FindByInjectionID(ctx context.Context, tenantID domain.TenantID, injectionID string) (selection.Record, error) {
	if err := ctx.Err(); err != nil {
		return selection.Record{}, err
	}
	if r == nil || tenantID == "" || injectionID == "" {
		return selection.Record{}, selection.ErrInvalidSelection
	}
	key := selectionKey{tenantID: tenantID, identity: injectionID}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, found := r.byInjection[key]
	if !found {
		return selection.Record{}, selection.ErrSelectionNotFound
	}
	if !r.now().UTC().Before(record.ExpiresAt) {
		return selection.Record{}, selection.ErrSelectionNotFound
	}
	if selection.ValidateRecord(record) != nil {
		return selection.Record{}, selection.ErrInvalidSelection
	}
	return selection.CloneRecord(record), nil
}
