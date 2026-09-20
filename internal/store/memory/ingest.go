package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/store"
)

var errInjectedFailure = errors.New("injected commit failure")

type failurePoint uint8

const (
	failureNone failurePoint = iota
	failureAfterEvents
)

type receiptRecord struct {
	receipt store.IngestReceipt
}

type IngestRepository struct {
	mu       sync.RWMutex
	failure  failurePoint
	receipts map[string]receiptRecord
	traces   map[string]string
	events   map[string]struct{}
	archives map[string]struct{}
	outbox   map[string]struct{}
	now      func() time.Time
}

func NewIngestRepository() *IngestRepository {
	return newIngestRepository(failureNone)
}

func newIngestRepository(failure failurePoint) *IngestRepository {
	return &IngestRepository{
		failure:  failure,
		receipts: make(map[string]receiptRecord),
		traces:   make(map[string]string),
		events:   make(map[string]struct{}),
		archives: make(map[string]struct{}),
		outbox:   make(map[string]struct{}),
		now:      time.Now,
	}
}

func (r *IngestRepository) Commit(ctx context.Context, request store.CommitIngestRequest) (store.IngestReceipt, error) {
	if err := ctx.Err(); err != nil {
		return store.IngestReceipt{}, err
	}
	if err := store.ValidateCommitRequest(request); err != nil {
		return store.IngestReceipt{}, err
	}
	key := string(request.TenantID) + "\x00" + request.IdempotencyKeyHash

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return store.IngestReceipt{}, err
	}
	if existing, ok := r.receipts[key]; ok {
		if existing.receipt.ContentHash != request.Batch.Hash {
			return store.IngestReceipt{}, store.ErrIdempotencyConflict
		}
		receipt := existing.receipt
		receipt.Disposition = store.DispositionDuplicate
		return receipt, nil
	}
	traceKey := tenantKey(request.TenantID, string(request.Batch.Trace.ID))
	if contentHash, exists := r.traces[traceKey]; exists && contentHash != request.Batch.Hash {
		return store.IngestReceipt{}, store.ErrTraceConflict
	}

	receipt := store.IngestReceipt{
		ID:          store.ReceiptID(request.TenantID, request.IdempotencyKeyHash),
		TenantID:    request.TenantID,
		TraceID:     request.Batch.Trace.ID,
		ContentHash: request.Batch.Hash,
		WorkflowID:  store.WorkflowID(request.TenantID, request.Batch.Trace.ID),
		Disposition: store.DispositionAccepted,
		CreatedAt:   r.now().UTC(),
	}
	if r.failure == failureAfterEvents {
		return store.IngestReceipt{}, errInjectedFailure
	}

	r.receipts[key] = receiptRecord{receipt: receipt}
	r.traces[traceKey] = request.Batch.Hash
	for _, event := range request.Batch.Events {
		r.events[tenantKey(request.TenantID, string(event.ID))] = struct{}{}
	}
	r.archives[tenantKey(request.TenantID, string(request.Archive.Key))] = struct{}{}
	r.outbox[tenantKey(request.TenantID, receipt.WorkflowID)] = struct{}{}
	return receipt, nil
}

func (r *IngestRepository) Counts(ctx context.Context) (store.AggregateCounts, error) {
	if err := ctx.Err(); err != nil {
		return store.AggregateCounts{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return store.AggregateCounts{
		Receipts:   len(r.receipts),
		Traces:     len(r.traces),
		Events:     len(r.events),
		Archives:   len(r.archives),
		OutboxJobs: len(r.outbox),
	}, nil
}

func tenantKey(tenantID domain.TenantID, value string) string {
	return string(tenantID) + "\x00" + value
}
