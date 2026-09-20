//go:build integration

package surreal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/maintenance"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"github.com/sauhard74/mem-jev/internal/testinfra"
)

func TestMaintenanceJobLeaseRecoveryAndFencing(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	repository := NewMaintenanceRepository(db)
	createdAt := time.Now().UTC().Add(-time.Second)
	spec, err := maintenance.NewSpec("tenant_a", maintenance.ProjectionRebuild, strings.Repeat("a", 64), []byte(`{"epoch":42}`), createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Enqueue(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if err := repository.Enqueue(context.Background(), spec); err != nil {
		t.Fatalf("duplicate enqueue: %v", err)
	}
	first, err := repository.Claim(context.Background(), maintenance.ClaimRequest{WorkerID: "worker_a", Kinds: []maintenance.Kind{maintenance.ProjectionRebuild}, Limit: 1, LeaseDuration: time.Second})
	if err != nil || len(first) != 1 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if leases, err := repository.Claim(context.Background(), maintenance.ClaimRequest{WorkerID: "worker_b", Kinds: []maintenance.Kind{maintenance.ProjectionRebuild}, Limit: 1, LeaseDuration: time.Second}); err != nil || len(leases) != 0 {
		t.Fatalf("live lease stolen: %#v %v", leases, err)
	}
	time.Sleep(1100 * time.Millisecond)
	recovered, err := repository.Claim(context.Background(), maintenance.ClaimRequest{WorkerID: "worker_b", Kinds: []maintenance.Kind{maintenance.ProjectionRebuild}, Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(recovered) != 1 || recovered[0].FencingToken <= first[0].FencingToken || recovered[0].AttemptCount != 2 {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	stale := maintenance.FinishRequest{TenantID: first[0].TenantID, JobID: first[0].ID, WorkerID: first[0].LeaseOwner, FencingToken: first[0].FencingToken}
	if err := repository.Complete(context.Background(), stale); !errors.Is(err, maintenance.ErrStaleLease) {
		t.Fatalf("stale completion error=%v", err)
	}
	current := maintenance.FinishRequest{TenantID: recovered[0].TenantID, JobID: recovered[0].ID, WorkerID: recovered[0].LeaseOwner, FencingToken: recovered[0].FencingToken}
	if err := repository.Complete(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if leases, err := repository.Claim(context.Background(), maintenance.ClaimRequest{WorkerID: "worker_c", Kinds: []maintenance.Kind{maintenance.ProjectionRebuild}, Limit: 1, LeaseDuration: time.Minute}); err != nil || len(leases) != 0 {
		t.Fatalf("completed job reclaimed: %#v %v", leases, err)
	}
}

func TestRebuildActivationPublicationIsImmutableAndIdempotent(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := rebuild.BuildDerivedSnapshot(rebuild.DerivedInput{TenantID: "tenant_a", ProjectionEpoch: 12})
	if err != nil {
		t.Fatal(err)
	}
	permit, err := rebuild.CompareDerivedSnapshots(context.Background(), snapshot, snapshot, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	repository := NewRebuildRepository(db)
	if err := repository.PublishActivation(context.Background(), snapshot, snapshot, permit); err != nil {
		t.Fatal(err)
	}
	if err := repository.PublishActivation(context.Background(), snapshot, snapshot, permit); err != nil {
		t.Fatalf("duplicate activation: %v", err)
	}
}

func TestRebuildActivationPublicationIsConcurrentIdempotent(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := rebuild.BuildDerivedSnapshot(rebuild.DerivedInput{TenantID: "tenant_a", ProjectionEpoch: 13})
	if err != nil {
		t.Fatal(err)
	}
	permit, err := rebuild.CompareDerivedSnapshots(context.Background(), snapshot, snapshot, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	repository := NewRebuildRepository(db)
	const publishers = 8
	start := make(chan struct{})
	errorsByPublisher := make(chan error, publishers)
	var ready sync.WaitGroup
	ready.Add(publishers)
	for range publishers {
		go func() {
			ready.Done()
			<-start
			errorsByPublisher <- repository.PublishActivation(context.Background(), snapshot, snapshot, permit)
		}()
	}
	ready.Wait()
	close(start)
	for range publishers {
		if err := <-errorsByPublisher; err != nil {
			t.Errorf("concurrent activation: %v", err)
		}
	}
}

func TestTransientExpiryStatementsExecuteAtomically(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := NewMaintenanceRepository(db).ExpireTransient(context.Background(), "tenant_a", time.Now().UTC(), 100); err != nil {
		t.Fatal(err)
	}
}
