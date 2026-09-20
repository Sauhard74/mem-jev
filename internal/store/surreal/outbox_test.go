//go:build integration

package surreal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestOutboxCompetingClaimersProduceOneLease(t *testing.T) {
	db := projectionDatabase(t)
	seedOutboxJobs(t, db, 1)
	repository := NewOutboxRepository(db)
	request := claimRequest("worker-placeholder")
	const workers = 12
	start := make(chan struct{})
	var claimed atomic.Int64
	errorsSeen := make(chan error, workers)
	var wait sync.WaitGroup
	for number := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			local := request
			local.WorkerID = fmt.Sprintf("worker-%d", number)
			leases, err := repository.ClaimOutbox(context.Background(), local)
			if err == nil {
				claimed.Add(int64(len(leases)))
			}
			errorsSeen <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("ClaimOutbox() error = %v", err)
		}
	}
	if got := claimed.Load(); got != 1 {
		t.Fatalf("claimed = %d, want 1", got)
	}
}

func TestOutboxLeaseExpiryFencesStaleWorker(t *testing.T) {
	db := projectionDatabase(t)
	seedOutboxJobs(t, db, 1)
	repository := NewOutboxRepository(db)
	first := mustClaimOne(t, repository, claimRequest("worker-a"))
	if first.FencingToken != 1 || first.AttemptCount != 1 || first.LeaseExpiresAt.IsZero() {
		t.Fatalf("first lease = %#v", first)
	}
	_, err := surrealdb.Query[any](context.Background(), db, `UPDATE outbox_job SET lease_expires_at = time::now() - 1s`, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = repository.CompleteOutbox(context.Background(), store.CompleteOutboxRequest{
		TenantID: first.TenantID, WorkflowID: first.WorkflowID, WorkerID: first.LeaseOwner, FencingToken: first.FencingToken,
	})
	if !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("expired completion error = %v", err)
	}
	second := mustClaimOne(t, repository, claimRequest("worker-b"))
	if second.FencingToken != 2 || second.AttemptCount != 2 {
		t.Fatalf("second lease = %#v", second)
	}
	err = repository.CompleteOutbox(context.Background(), store.CompleteOutboxRequest{
		TenantID: first.TenantID, WorkflowID: first.WorkflowID, WorkerID: first.LeaseOwner, FencingToken: first.FencingToken,
	})
	if !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("stale completion error = %v", err)
	}
}

func TestOutboxRetryCompletionAndDeadLetterAreIdempotent(t *testing.T) {
	db := projectionDatabase(t)
	seedOutboxJobs(t, db, 2)
	repository := NewOutboxRepository(db)
	claim := claimRequest("worker-a")
	claim.Limit = 1
	first := mustClaimOne(t, repository, claim)
	retry := store.ReleaseOutboxRequest{
		TenantID: first.TenantID, WorkflowID: first.WorkflowID, WorkerID: first.LeaseOwner,
		FencingToken: first.FencingToken, ErrorCode: "provider_unavailable",
	}
	if err := repository.ReleaseOutbox(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReleaseOutbox(context.Background(), retry); err != nil {
		t.Fatalf("duplicate retry = %v", err)
	}
	second := mustClaimOne(t, repository, claim)
	complete := store.CompleteOutboxRequest{
		TenantID: second.TenantID, WorkflowID: second.WorkflowID, WorkerID: second.LeaseOwner, FencingToken: second.FencingToken,
	}
	if err := repository.CompleteOutbox(context.Background(), complete); err != nil {
		t.Fatal(err)
	}
	if err := repository.CompleteOutbox(context.Background(), complete); err != nil {
		t.Fatalf("duplicate completion = %v", err)
	}
	third := mustClaimOne(t, repository, claim)
	deadLetter := store.ReleaseOutboxRequest{
		TenantID: third.TenantID, WorkflowID: third.WorkflowID, WorkerID: third.LeaseOwner,
		FencingToken: third.FencingToken, ErrorCode: "invalid_payload", DeadLetter: true,
	}
	if err := repository.ReleaseOutbox(context.Background(), deadLetter); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReleaseOutbox(context.Background(), deadLetter); err != nil {
		t.Fatalf("duplicate dead letter = %v", err)
	}
	leases, err := repository.ClaimOutbox(context.Background(), claim)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 0 {
		t.Fatalf("terminal jobs were reclaimed: %#v", leases)
	}
}

func TestOutboxTransitionIsTenantScoped(t *testing.T) {
	db := projectionDatabase(t)
	seedOutboxJobs(t, db, 1)
	repository := NewOutboxRepository(db)
	lease := mustClaimOne(t, repository, claimRequest("worker-a"))
	err := repository.CompleteOutbox(context.Background(), store.CompleteOutboxRequest{
		TenantID: "tenant-b", WorkflowID: lease.WorkflowID, WorkerID: lease.LeaseOwner, FencingToken: lease.FencingToken,
	})
	if !errors.Is(err, store.ErrOutboxJobNotFound) {
		t.Fatalf("error = %v, want %v", err, store.ErrOutboxJobNotFound)
	}
}

func projectionDatabase(t *testing.T) *surrealdb.DB {
	t.Helper()
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedOutboxJobs(t *testing.T, db *surrealdb.DB, count int) {
	t.Helper()
	for number := 1; number <= count; number++ {
		_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY outbox_job CONTENT $record`, map[string]any{
			"record": map[string]any{
				"tenant_id": "tenant-a", "workflow_id": fmt.Sprintf("workflow-%02d", number),
				"trace_id": fmt.Sprintf("trace-%02d", number), "outcome_id": fmt.Sprintf("outcome-%02d", number),
				"job_type": "synthesize_outcome", "state": "pending", "attempt_count": 0, "lease_generation": 0,
				"available_at": time.Now().UTC().Add(-time.Minute), "created_at": time.Now().UTC().Add(-time.Minute),
				"schema_version": "outcome.v1", "content_hash": fmt.Sprintf("%064x", number),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func claimRequest(workerID string) store.ClaimOutboxRequest {
	return store.ClaimOutboxRequest{
		WorkerID: workerID, JobTypes: []store.OutboxJobType{store.OutboxJobSynthesizeOutcome},
		Limit: 10, LeaseDuration: time.Minute,
	}
}

func mustClaimOne(t *testing.T, repository *OutboxRepository, request store.ClaimOutboxRequest) store.OutboxLease {
	t.Helper()
	leases, err := repository.ClaimOutbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one", leases)
	}
	return leases[0]
}
