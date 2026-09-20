package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/store"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/synthesis"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

type staticSourceLoader struct{ source LoadedSource }

func (l staticSourceLoader) Load(context.Context, SynthesisInput) (LoadedSource, error) {
	return l.source, nil
}

type staticRegistryProvider struct{ registry toolcontract.Registry }

func (p staticRegistryProvider) ForTenant(domain.TenantID) toolcontract.Registry { return p.registry }

type memoryStageArtifacts struct {
	mu    sync.RWMutex
	items map[string]StageArtifact
}

func newMemoryStageArtifacts() *memoryStageArtifacts {
	return &memoryStageArtifacts{items: make(map[string]StageArtifact)}
}

func (r *memoryStageArtifacts) PutStageArtifact(_ context.Context, artifact StageArtifact) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := string(artifact.TenantID) + "\x00" + artifact.WorkflowID + "\x00" + string(artifact.Stage)
	if existing, found := r.items[key]; found {
		if existing.ContentHash != artifact.ContentHash || existing.InputHash != artifact.InputHash {
			return ErrStageArtifactConflict
		}
		return nil
	}
	r.items[key] = artifact
	return nil
}

func (r *memoryStageArtifacts) GetStageArtifact(_ context.Context, tenantID domain.TenantID, workflowID string, stage Stage) (StageArtifact, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	artifact, found := r.items[string(tenantID)+"\x00"+workflowID+"\x00"+string(stage)]
	if !found {
		return StageArtifact{}, ErrStageArtifactNotFound
	}
	return artifact, nil
}

func TestProcessorBuildsAndPublishesDeterministicProjection(t *testing.T) {
	processor, projections, request := testProcessor(t, true)
	prior := ""
	publishInputHash := ""
	var result StageResult
	var err error
	for _, stage := range []Stage{StageLoad, StageEvaluate, StageBuildGraph, StageSynthesize, StagePublish} {
		stageRequest := StageRequest{SchemaVersion: "stage-request.v1", Input: request.Input, Stage: stage, PriorHash: prior}
		switch stage {
		case StageLoad:
			result, err = processor.Load(context.Background(), stageRequest)
		case StageEvaluate:
			result, err = processor.Evaluate(context.Background(), stageRequest)
		case StageBuildGraph:
			result, err = processor.BuildGraph(context.Background(), stageRequest)
		case StageSynthesize:
			result, err = processor.Synthesize(context.Background(), stageRequest)
		case StagePublish:
			publishInputHash = prior
			result, err = processor.Publish(context.Background(), stageRequest)
		}
		if err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
		prior = result.ArtifactHash
	}
	counts, err := projections.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts != (projection.Counts{Families: 1, Versions: 1, Steps: 1, Manifests: 1, EvidenceLinks: 1}) {
		t.Fatalf("counts = %#v", counts)
	}
	duplicate, err := processor.Publish(context.Background(), StageRequest{SchemaVersion: "stage-request.v1", Input: request.Input, Stage: StagePublish, PriorHash: publishInputHash})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate != result {
		t.Fatalf("duplicate=%#v first=%#v", duplicate, result)
	}
}

func TestProcessorTerminatesTraceOnlyJobAfterArchiveValidation(t *testing.T) {
	processor, _, request := testProcessor(t, false)
	loaded, err := processor.Load(context.Background(), StageRequest{SchemaVersion: "stage-request.v1", Input: request.Input, Stage: StageLoad})
	if err != nil {
		t.Fatal(err)
	}
	evaluated, err := processor.Evaluate(context.Background(), StageRequest{SchemaVersion: "stage-request.v1", Input: request.Input, Stage: StageEvaluate, PriorHash: loaded.ArtifactHash})
	if err != nil {
		t.Fatal(err)
	}
	if evaluated.Status != StageStatusTerminal || evaluated.Code != "awaiting_outcome" {
		t.Fatalf("result = %#v", evaluated)
	}
}

func TestProcessorRejectsIncorrectStageChain(t *testing.T) {
	processor, _, request := testProcessor(t, true)
	_, err := processor.Evaluate(context.Background(), StageRequest{
		SchemaVersion: "stage-request.v1", Input: request.Input, Stage: StageEvaluate, PriorHash: strings.Repeat("f", 64),
	})
	if !errors.Is(err, ErrStageArtifactNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func testProcessor(t *testing.T, withOutcome bool) (*Processor, *storememory.ProjectionRepository, StartRequest) {
	t.Helper()
	tenantID := domain.TenantID("tenant-a")
	traceID := domain.TraceID("tr_" + strings.Repeat("a", 64))
	outcomeID := domain.OutcomeID("out_" + strings.Repeat("b", 64))
	eventID := domain.EventID("ev_" + strings.Repeat("d", 64))
	batch := domain.CanonicalBatch{
		SchemaVersion: "v1", TenantID: tenantID, Hash: strings.Repeat("c", 64),
		Trace: domain.CanonicalTrace{ID: traceID, Harness: "test", HarnessVersion: "1", Task: "write the file"},
		Events: []domain.CanonicalEvent{{
			ID: eventID, Position: 0, Kind: "tool", ToolName: "writer", ToolVersion: "1.0.0",
			Fields: []domain.CanonicalField{{Name: "path", Value: "output.txt"}}, Result: &domain.CanonicalResult{State: "TOOL_RESULT_STATE_SUCCESS"},
		}},
	}
	source := LoadedSource{SchemaVersion: "loaded-source.v1", Batch: batch, ArchiveHash: batch.Hash}
	jobType := store.OutboxJobSynthesizeTrace
	if withOutcome {
		jobType = store.OutboxJobSynthesizeOutcome
		source.Outcome = &StoredOutcome{
			ID: outcomeID, ContentHash: strings.Repeat("c", 64), State: domain.OutcomeStateVerifiedSuccess, PromotionEligible: true, PolicyVersion: "outcome-policy.v1",
			Evidence: []domain.OutcomeEvidence{{
				ID: domain.EvidenceID("oe_" + strings.Repeat("e", 64)), Class: domain.EvidenceClassGoalPredicate,
				Verdict: domain.EvidenceVerdictSatisfied, PredicateID: "goal", VerifierID: "ci", ObservedAt: "2026-09-20T00:00:00Z",
			}},
		}
	}
	registry := toolcontract.NewMemoryRegistry()
	manifest := toolcontract.Manifest{
		SchemaVersion: "tool-contract.v1", ToolID: "writer", Version: "1.0.0",
		Inputs:     []toolcontract.FieldSpec{{Name: "path", Type: "string", Required: true, Sanitizer: toolcontract.SanitizerRelativePath}},
		Writes:     []toolcontract.ResourceSpec{{Name: "workspace_file", Type: "file", Namespace: "workspace", Field: "path"}},
		SideEffect: toolcontract.SideEffectWrite, Risk: toolcontract.RiskLow,
		Idempotency:         toolcontract.IdempotencySpec{Mode: toolcontract.IdempotencyGuaranteed},
		Retry:               toolcontract.RetrySpec{Mode: toolcontract.RetryOnDeclaredTransient, MaximumAttempts: 2},
		SuccessPredicates:   []toolcontract.PredicateSpec{{ID: "goal", Field: "path", Operator: "exists"}},
		VerificationMethods: []toolcontract.VerificationMethod{{ID: "ci", EvidenceClass: string(domain.EvidenceClassGoalPredicate)}},
		Compatibility:       []toolcontract.CompatibilityRange{{MinimumInclusive: "1.0.0", MaximumExclusive: "2.0.0"}},
	}
	if err := registry.Register(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	projections := storememory.NewProjectionRepository()
	processor, err := NewProcessor(
		staticSourceLoader{source: source}, staticRegistryProvider{registry: registry}, newMemoryStageArtifacts(), projections,
		ProcessorConfig{ArtifactRetention: 24 * time.Hour, Versions: synthesis.Versions{
			Sanitizer: "sanitizer.v1", Registry: "registry.v1", Policy: "outcome-policy.v1", GraphBuilder: "graph.v1", Synthesizer: "synth.v1",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	processor.now = func() time.Time { return time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC) }
	input := SynthesisInput{SchemaVersion: "synthesis-input.v1", TenantID: tenantID, TraceID: traceID, OutcomeID: outcomeID, JobType: jobType, ContentHash: strings.Repeat("c", 64)}
	workflowID := store.WorkflowID(tenantID, traceID)
	if withOutcome {
		workflowID = store.OutcomeWorkflowID(tenantID, traceID, outcomeID)
	} else {
		input.OutcomeID = ""
	}
	return processor, projections, StartRequest{WorkflowID: workflowID, Input: input}
}

var _ StageArtifactRepository = (*memoryStageArtifacts)(nil)
