package storetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/store"
)

type Repository interface {
	store.IngestRepository
	Counts(context.Context) (store.AggregateCounts, error)
}

type Factory func(*testing.T) Repository

func RunIngestContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("accepted and duplicate", func(t *testing.T) {
		repository := factory(t)
		request := ValidCommitRequest(t)
		accepted, err := repository.Commit(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if accepted.Disposition != store.DispositionAccepted {
			t.Fatalf("disposition = %q", accepted.Disposition)
		}
		duplicate, err := repository.Commit(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if duplicate.ID != accepted.ID || duplicate.Disposition != store.DispositionDuplicate {
			t.Fatalf("accepted=%#v duplicate=%#v", accepted, duplicate)
		}
		AssertCounts(t, repository, store.AggregateCounts{Receipts: 1, Traces: 1, Events: 2, Archives: 1, OutboxJobs: 1})
	})

	t.Run("conflicting duplicate", func(t *testing.T) {
		repository := factory(t)
		request := ValidCommitRequest(t)
		if _, err := repository.Commit(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		conflict := request
		conflict.Batch.CanonicalJSON = []byte(`{"different":true}`)
		sum := sha256.Sum256(conflict.Batch.CanonicalJSON)
		conflict.Batch.Hash = hex.EncodeToString(sum[:])
		key, err := archive.KeyFor(conflict.TenantID, conflict.Batch.SchemaVersion, conflict.Batch.Hash)
		if err != nil {
			t.Fatal(err)
		}
		conflict.Archive = archive.Object{Key: key, Hash: conflict.Batch.Hash, Size: int64(len(conflict.Batch.CanonicalJSON))}
		if _, err = repository.Commit(context.Background(), conflict); !errors.Is(err, store.ErrIdempotencyConflict) {
			t.Fatalf("error = %v, want %v", err, store.ErrIdempotencyConflict)
		}
		AssertCounts(t, repository, store.AggregateCounts{Receipts: 1, Traces: 1, Events: 2, Archives: 1, OutboxJobs: 1})
	})

	t.Run("same content with a new idempotency key reuses aggregate", func(t *testing.T) {
		repository := factory(t)
		request := ValidCommitRequest(t)
		if _, err := repository.Commit(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		request.IdempotencyKeyHash = strings.Repeat("c", 64)
		second, err := repository.Commit(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if second.Disposition != store.DispositionAccepted {
			t.Fatalf("disposition = %q, want accepted for a new key", second.Disposition)
		}
		AssertCounts(t, repository, store.AggregateCounts{Receipts: 2, Traces: 1, Events: 2, Archives: 1, OutboxJobs: 1})
	})

	t.Run("same trace identity with changed content conflicts", func(t *testing.T) {
		repository := factory(t)
		request := ValidCommitRequest(t)
		if _, err := repository.Commit(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		changed := request
		changed.IdempotencyKeyHash = strings.Repeat("d", 64)
		changed.Batch.CanonicalJSON = []byte(`{"canonical":"changed"}`)
		sum := sha256.Sum256(changed.Batch.CanonicalJSON)
		changed.Batch.Hash = hex.EncodeToString(sum[:])
		key, err := archive.KeyFor(changed.TenantID, changed.Batch.SchemaVersion, changed.Batch.Hash)
		if err != nil {
			t.Fatal(err)
		}
		changed.Archive = archive.Object{Key: key, Hash: changed.Batch.Hash, Size: int64(len(changed.Batch.CanonicalJSON))}
		if _, err = repository.Commit(context.Background(), changed); !errors.Is(err, store.ErrTraceConflict) {
			t.Fatalf("error = %v, want %v", err, store.ErrTraceConflict)
		}
		AssertCounts(t, repository, store.AggregateCounts{Receipts: 1, Traces: 1, Events: 2, Archives: 1, OutboxJobs: 1})
	})

	t.Run("concurrent identical commit", func(t *testing.T) {
		repository := factory(t)
		request := ValidCommitRequest(t)
		const callers = 16
		start := make(chan struct{})
		receipts := make(chan store.IngestReceipt, callers)
		errorsSeen := make(chan error, callers)
		var wait sync.WaitGroup
		for range callers {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				receipt, err := repository.Commit(context.Background(), request)
				if err != nil {
					errorsSeen <- err
					return
				}
				receipts <- receipt
			}()
		}
		close(start)
		wait.Wait()
		close(receipts)
		close(errorsSeen)
		for err := range errorsSeen {
			t.Errorf("concurrent commit: %v", err)
		}
		var receiptID domain.ReceiptID
		for receipt := range receipts {
			if receiptID == "" {
				receiptID = receipt.ID
			}
			if receipt.ID != receiptID {
				t.Errorf("receipt ID = %q, want %q", receipt.ID, receiptID)
			}
		}
		AssertCounts(t, repository, store.AggregateCounts{Receipts: 1, Traces: 1, Events: 2, Archives: 1, OutboxJobs: 1})
	})

	t.Run("rejects invariant mismatches", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*store.CommitIngestRequest)
		}{
			{name: "tenant", mutate: func(request *store.CommitIngestRequest) { request.Batch.TenantID = "other" }},
			{name: "archive hash", mutate: func(request *store.CommitIngestRequest) {
				request.Archive.Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}},
			{name: "archive size", mutate: func(request *store.CommitIngestRequest) { request.Archive.Size++ }},
			{name: "empty events", mutate: func(request *store.CommitIngestRequest) { request.Batch.Events = nil }},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				repository := factory(t)
				request := ValidCommitRequest(t)
				tt.mutate(&request)
				if _, err := repository.Commit(context.Background(), request); !errors.Is(err, store.ErrInvalidCommit) {
					t.Fatalf("error = %v, want %v", err, store.ErrInvalidCommit)
				}
				AssertCounts(t, repository, store.AggregateCounts{})
			})
		}
	})

	t.Run("canceled before commit", func(t *testing.T) {
		repository := factory(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := repository.Commit(ctx, ValidCommitRequest(t)); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want %v", err, context.Canceled)
		}
		AssertCounts(t, repository, store.AggregateCounts{})
	})
}

func ValidCommitRequest(t *testing.T) store.CommitIngestRequest {
	t.Helper()
	tenantID := domain.TenantID("tenant_a")
	body := []byte(`{"canonical":"batch"}`)
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	key, err := archive.KeyFor(tenantID, "canonical.v1", hash)
	if err != nil {
		t.Fatal(err)
	}
	return store.CommitIngestRequest{
		TenantID:           tenantID,
		IdempotencyKeyHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Batch: domain.CanonicalBatch{
			SchemaVersion: "canonical.v1",
			TenantID:      tenantID,
			Trace: domain.CanonicalTrace{
				ID:            domain.TraceID("tr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ClientTraceID: "client-trace",
				Harness:       "test",
			},
			Events: []domain.CanonicalEvent{
				{ID: domain.EventID("ev_1111111111111111111111111111111111111111111111111111111111111111"), Position: 0, ClientEventID: "one", OccurredAt: "2026-09-20T00:00:00Z", Kind: "TRACE_EVENT_KIND_TOOL_CALL", ToolName: "read"},
				{ID: domain.EventID("ev_2222222222222222222222222222222222222222222222222222222222222222"), Position: 1, ClientEventID: "two", OccurredAt: "2026-09-20T00:00:01Z", Kind: "TRACE_EVENT_KIND_TOOL_RESULT", ToolName: "read"},
			},
			Hash:          hash,
			CanonicalJSON: body,
		},
		Archive: archive.Object{Key: key, Hash: hash, Size: int64(len(body))},
	}
}

func AssertCounts(t *testing.T, repository Repository, want store.AggregateCounts) {
	t.Helper()
	got, err := repository.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}
