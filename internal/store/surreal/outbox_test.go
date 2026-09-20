//go:build integration

package surreal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
	memworkflow "github.com/sauhard74/mem-jev/internal/workflow"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestStageArtifactRepositoryIsImmutableAndTenantScoped(t *testing.T) {
	db := projectionDatabase(t)
	repository := NewStageArtifactRepository(db)
	artifact := memworkflow.StageArtifact{
		TenantID: "tenant-a", WorkflowID: "workflow-a", Stage: memworkflow.StageLoad,
		InputHash: strings.Repeat("a", 64), Payload: []byte(`{"ok":true}`),
		Status: memworkflow.StageStatusReady, CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	hash, err := memworkflow.StageArtifactHash(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifact.ContentHash = hash
	if err := repository.PutStageArtifact(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutStageArtifact(context.Background(), artifact); err != nil {
		t.Fatalf("duplicate write = %v", err)
	}
	got, err := repository.GetStageArtifact(context.Background(), artifact.TenantID, artifact.WorkflowID, artifact.Stage)
	if err != nil || got.ContentHash != artifact.ContentHash || string(got.Payload) != string(artifact.Payload) {
		t.Fatalf("artifact=%#v error=%v", got, err)
	}
	conflict := artifact
	conflict.ContentHash = strings.Repeat("c", 64)
	if err := repository.PutStageArtifact(context.Background(), conflict); !errors.Is(err, memworkflow.ErrStageArtifactConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	if _, err := repository.GetStageArtifact(context.Background(), "tenant-b", artifact.WorkflowID, artifact.Stage); !errors.Is(err, memworkflow.ErrStageArtifactNotFound) {
		t.Fatalf("cross-tenant read error = %v", err)
	}
}

func TestTenantToolRegistryResolvesStoredContractAndAlias(t *testing.T) {
	db := projectionDatabase(t)
	manifest, err := toolcontract.Canonicalize(toolcontract.Manifest{
		SchemaVersion: "tool-contract.v1", ToolID: "writer", Version: "1.0.0", Aliases: []string{"write-file"},
		Inputs:     []toolcontract.FieldSpec{{Name: "path", Type: "string", Required: true, Sanitizer: toolcontract.SanitizerRelativePath}},
		Writes:     []toolcontract.ResourceSpec{{Name: "file", Type: "file", Namespace: "workspace", Field: "path"}},
		SideEffect: toolcontract.SideEffectWrite, Risk: toolcontract.RiskLow,
		Idempotency:         toolcontract.IdempotencySpec{Mode: toolcontract.IdempotencyGuaranteed},
		Retry:               toolcontract.RetrySpec{Mode: toolcontract.RetryOnDeclaredTransient, MaximumAttempts: 2},
		SuccessPredicates:   []toolcontract.PredicateSpec{{ID: "goal", Field: "path", Operator: "exists"}},
		VerificationMethods: []toolcontract.VerificationMethod{{ID: "ci", EvidenceClass: string(domain.EvidenceClassGoalPredicate)}},
		Compatibility:       []toolcontract.CompatibilityRange{{MinimumInclusive: "1.0.0", MaximumExclusive: "2.0.0"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = surrealdb.Query[any](context.Background(), db, `CREATE ONLY tool_contract CONTENT $contract;
		CREATE ONLY tool_contract_version CONTENT $version`, map[string]any{
		"contract": map[string]any{
			"tenant_id": "tenant-a", "tool_id": manifest.ToolID, "created_at": time.Now().UTC(),
			"schema_version": "tool-contract.v1", "content_hash": manifest.ContentHash,
		},
		"version": map[string]any{
			"tenant_id": "tenant-a", "contract_version_id": manifest.ID, "tool_id": manifest.ToolID,
			"tool_version": manifest.Version, "manifest": manifest, "created_at": time.Now().UTC(),
			"schema_version": "tool-contract.v1", "content_hash": manifest.ContentHash,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewTenantRegistryProvider(db).ForTenant("tenant-a")
	for _, name := range []string{"writer", "write-file"} {
		resolved, err := registry.Resolve(context.Background(), toolcontract.Query{Name: name, Version: "1.0.0"})
		if err != nil || resolved.Opaque || resolved.Manifest.ID != manifest.ID {
			t.Fatalf("Resolve(%q) = %#v, %v", name, resolved, err)
		}
	}
	versioned, ok := registry.(memworkflow.VersionedRegistry)
	if !ok {
		t.Fatal("registry does not expose snapshot provenance")
	}
	if !strings.HasPrefix(versioned.SnapshotVersion(), "registry_") {
		t.Fatalf("registry snapshot version = %q", versioned.SnapshotVersion())
	}
	crossTenant := NewTenantRegistryProvider(db).ForTenant("tenant-b")
	resolved, err := crossTenant.Resolve(context.Background(), toolcontract.Query{Name: "writer", Version: "1.0.0"})
	if err != nil || !resolved.Opaque {
		t.Fatalf("cross-tenant resolve = %#v, %v", resolved, err)
	}
}

func TestWorkflowSourceLoaderReconstructsTenantScopedTraceAndOutcome(t *testing.T) {
	db := projectionDatabase(t)
	archives := archive.NewMemoryStore()
	exitCode := int32(0)
	batch, err := canonical.Build("tenant_a", &memjevv1.IngestTraceRequest{
		ClientTraceId: "source-loader-trace", Harness: "integration", Task: "verify source loading",
		Events: []*memjevv1.TraceEvent{{
			ClientEventId: "event-1", OccurredAt: timestamppb.New(time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)),
			Kind: memjevv1.EventKind_EVENT_KIND_EXECUTE, ToolName: "verify", ToolVersion: "1.0.0",
			Result: &memjevv1.ToolResult{State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS, ExitCode: &exitCode},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	object, err := archives.PutCanonical(context.Background(), archive.PutRequest{
		TenantID: batch.TenantID, SchemaVersion: batch.SchemaVersion, Hash: batch.Hash, Body: batch.CanonicalJSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	ingestRequest := store.CommitIngestRequest{
		TenantID: batch.TenantID, IdempotencyKeyHash: strings.Repeat("9", 64), Batch: batch, Archive: object,
	}
	if _, err := NewIngestRepository(db).Commit(context.Background(), ingestRequest); err != nil {
		t.Fatal(err)
	}
	canonicalOutcome, err := evidence.Canonicalize(batch.TenantID, &memjevv1.RecordOutcomeRequest{
		TraceId: string(batch.Trace.ID), ExecutionId: "execution-1",
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "evidence-1", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
			Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, PredicateId: "goal", VerifierId: "ci",
			ObservedAt: timestamppb.New(time.Date(2026, 9, 20, 0, 0, 1, 0, time.UTC)),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcomeRequest := store.CommitOutcomeRequest{
		TenantID: batch.TenantID, IdempotencyKeyHash: strings.Repeat("8", 64), Outcome: canonicalOutcome,
		Evaluation: evidence.Result{State: domain.OutcomeStateVerifiedSuccess, PromotionEligible: true, PolicyVersion: "outcome-policy.v1"},
	}
	if _, err := NewOutcomeRepository(db).CommitOutcome(context.Background(), outcomeRequest); err != nil {
		t.Fatal(err)
	}
	loader := NewWorkflowSourceLoader(db, rebuild.NewLoader(archives, rebuild.Config{MaximumBytes: 1 << 20, AcceptedSchemas: []string{"canonical.v1"}}))
	loaded, err := loader.Load(context.Background(), memworkflow.SynthesisInput{
		SchemaVersion: "synthesis-input.v1", TenantID: outcomeRequest.TenantID, TraceID: outcomeRequest.Outcome.TraceID,
		OutcomeID: outcomeRequest.Outcome.ID, JobType: store.OutboxJobSynthesizeOutcome, ContentHash: outcomeRequest.Outcome.Hash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Batch.Hash != ingestRequest.Batch.Hash || loaded.Outcome == nil || loaded.Outcome.ID != outcomeRequest.Outcome.ID ||
		loaded.Outcome.ContentHash != outcomeRequest.Outcome.Hash || len(loaded.Outcome.Evidence) != 1 {
		t.Fatalf("loaded = %#v", loaded)
	}
	_, err = loader.Load(context.Background(), memworkflow.SynthesisInput{
		SchemaVersion: "synthesis-input.v1", TenantID: "tenant-b", TraceID: outcomeRequest.Outcome.TraceID,
		OutcomeID: outcomeRequest.Outcome.ID, JobType: store.OutboxJobSynthesizeOutcome, ContentHash: outcomeRequest.Outcome.Hash,
	})
	var stageErr *memworkflow.StageError
	if !errors.As(err, &stageErr) || stageErr.Code != "trace_not_found" {
		t.Fatalf("cross-tenant error = %v", err)
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
