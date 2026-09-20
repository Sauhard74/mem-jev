package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestSelectionRepositoryCommitsBeforeReturningAndReplaysByteIdentically(t *testing.T) {
	repository, err := NewSelectionRepository(selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC)
	repository.now = func() time.Time { return now }
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	request := selection.CommitRequest{TenantID: draft.TenantID, IdempotencyIdentityHash: hash64('d'), Draft: draft}
	first, err := repository.Commit(context.Background(), request)
	if err != nil || first.Disposition != selection.DispositionCommitted || selection.ValidateRecord(first.Record) != nil {
		t.Fatalf("receipt=%#v error=%v", first, err)
	}
	stored, err := repository.FindByInjectionID(context.Background(), draft.TenantID, first.Record.InjectionID)
	if err != nil || string(stored.CanonicalJSON) != string(first.Record.CanonicalJSON) {
		t.Fatalf("stored=%#v error=%v", stored, err)
	}
	second, err := repository.Commit(context.Background(), request)
	if err != nil || second.Disposition != selection.DispositionDuplicate || string(second.Record.CanonicalJSON) != string(first.Record.CanonicalJSON) {
		t.Fatalf("duplicate=%#v error=%v", second, err)
	}
}

func TestSelectionRepositoryConvergesConcurrentDuplicatesAndRejectsConflicts(t *testing.T) {
	repository, err := NewSelectionRepository(selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	repository.now = func() time.Time { return time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC) }
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	request := selection.CommitRequest{TenantID: draft.TenantID, IdempotencyIdentityHash: hash64('e'), Draft: draft}
	var committed atomic.Int32
	var failures atomic.Int32
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			receipt, commitErr := repository.Commit(context.Background(), request)
			if commitErr != nil {
				failures.Add(1)
				return
			}
			if receipt.Disposition == selection.DispositionCommitted {
				committed.Add(1)
			}
		}()
	}
	wait.Wait()
	if failures.Load() != 0 || committed.Load() != 1 {
		t.Fatalf("failures=%d committed=%d", failures.Load(), committed.Load())
	}
	changed := storetest.SelectionDraft(t, "tenant_a", 'f')
	_, err = repository.Commit(context.Background(), selection.CommitRequest{TenantID: changed.TenantID, IdempotencyIdentityHash: request.IdempotencyIdentityHash, Draft: changed})
	if !errors.Is(err, selection.ErrIdempotencyConflict) {
		t.Fatalf("error=%v", err)
	}
}

func TestSelectionRepositoryEnforcesTenantAndExpiry(t *testing.T) {
	repository, err := NewSelectionRepository(selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC)
	repository.now = func() time.Time { return now }
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	receipt, err := repository.Commit(context.Background(), selection.CommitRequest{TenantID: draft.TenantID, IdempotencyIdentityHash: hash64('1'), Draft: draft})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repository.FindByInjectionID(context.Background(), domain.TenantID("tenant_b"), receipt.Record.InjectionID); !errors.Is(err, selection.ErrSelectionNotFound) {
		t.Fatalf("foreign lookup error=%v", err)
	}
	now = now.Add(time.Hour)
	if _, err = repository.FindByInjectionID(context.Background(), draft.TenantID, receipt.Record.InjectionID); !errors.Is(err, selection.ErrSelectionNotFound) {
		t.Fatalf("expired lookup error=%v", err)
	}
	_, err = repository.Commit(context.Background(), selection.CommitRequest{TenantID: draft.TenantID, IdempotencyIdentityHash: hash64('1'), Draft: draft})
	if !errors.Is(err, selection.ErrSelectionExpired) {
		t.Fatalf("expired replay error=%v", err)
	}
}

func hash64(value byte) string {
	result := make([]byte, 64)
	for index := range result {
		result[index] = value
	}
	return string(result)
}
