package synthesis_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/synthesis"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

func TestBuilderCreatesOnlyDeclaredCausalEdges(t *testing.T) {
	registry := registryWithContracts(t)
	batch := graphBatch()
	batch.Events = batch.Events[:3]
	graph, err := synthesis.NewBuilder(registry).Build(context.Background(), synthesis.BuildRequest{
		Batch:                batch,
		ControlDependencies:  []synthesis.DeclaredDependency{{Prerequisite: batch.Events[1].ID, Dependent: batch.Events[2].ID}},
		VerifierDependencies: []synthesis.DeclaredDependency{{Prerequisite: batch.Events[0].ID, Dependent: batch.Events[2].ID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []synthesis.Edge{
		{From: batch.Events[0].ID, To: batch.Events[1].ID, Type: synthesis.EdgeResourceFlow, ResourceID: graph.Nodes[0].Writes[0].ID},
		{From: batch.Events[0].ID, To: batch.Events[2].ID, Type: synthesis.EdgeVerifier},
		{From: batch.Events[1].ID, To: batch.Events[2].ID, Type: synthesis.EdgeControl},
	}
	if !slices.Equal(graph.Edges, want) {
		t.Fatalf("edges = %#v; want %#v", graph.Edges, want)
	}
	if !graph.AutoPromotable || graph.Hash == "" {
		t.Fatalf("graph = %#v", graph)
	}
}

func TestBuilderDoesNotTreatTemporalAdjacencyAsCausality(t *testing.T) {
	batch := graphBatch()
	batch.Events = batch.Events[2:]
	batch.Events[0].Position = 0
	batch.Events[1].Position = 1
	graph, err := synthesis.NewBuilder(toolcontract.NewMemoryRegistry()).Build(context.Background(), synthesis.BuildRequest{Batch: batch})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 0 || graph.AutoPromotable {
		t.Fatalf("graph = %#v; adjacency or opaque tools created causality", graph)
	}
	for _, node := range graph.Nodes {
		if !node.Opaque || len(node.Reads) != 0 || len(node.Writes) != 0 {
			t.Fatalf("opaque node inferred resources: %#v", node)
		}
	}
}

func TestBuilderRejectsDeclaredCycle(t *testing.T) {
	batch := graphBatch()
	_, err := synthesis.NewBuilder(registryWithContracts(t)).Build(context.Background(), synthesis.BuildRequest{
		Batch: batch,
		ControlDependencies: []synthesis.DeclaredDependency{
			{Prerequisite: batch.Events[0].ID, Dependent: batch.Events[1].ID},
			{Prerequisite: batch.Events[1].ID, Dependent: batch.Events[0].ID},
		},
	})
	if !errors.Is(err, synthesis.ErrCausalCycle) {
		t.Fatalf("Build() error = %v; want causal cycle", err)
	}
}

func TestBuilderDoesNotLinkIncompatibleResourceTypes(t *testing.T) {
	registry := toolcontract.NewMemoryRegistry()
	manifests := []toolcontract.Manifest{
		contract("writer", []toolcontract.ResourceSpec{{Name: "workspace", Type: "repository", Namespace: "resource", Field: "resource_id"}}, nil),
		contract("reader", nil, []toolcontract.ResourceSpec{{Name: "workspace", Type: "database", Namespace: "resource", Field: "resource_id"}}),
	}
	for _, source := range manifests {
		manifest, err := toolcontract.Canonicalize(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(context.Background(), manifest); err != nil {
			t.Fatal(err)
		}
	}
	batch := graphBatch()
	batch.Events = batch.Events[:2]
	graph, err := synthesis.NewBuilder(registry).Build(context.Background(), synthesis.BuildRequest{Batch: batch})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 0 {
		t.Fatalf("incompatible resources linked: %#v", graph.Edges)
	}
}

func TestBuilderOutputIsStableAcrossDependencyOrder(t *testing.T) {
	batch := graphBatch()
	dependencies := []synthesis.DeclaredDependency{
		{Prerequisite: batch.Events[0].ID, Dependent: batch.Events[2].ID},
		{Prerequisite: batch.Events[1].ID, Dependent: batch.Events[2].ID},
	}
	first, err := synthesis.NewBuilder(registryWithContracts(t)).Build(context.Background(), synthesis.BuildRequest{Batch: batch, ControlDependencies: dependencies})
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(dependencies)
	second, err := synthesis.NewBuilder(registryWithContracts(t)).Build(context.Background(), synthesis.BuildRequest{Batch: batch, ControlDependencies: dependencies})
	if err != nil {
		t.Fatal(err)
	}
	if first.Hash != second.Hash || !slices.Equal(first.Edges, second.Edges) {
		t.Fatalf("graphs differ: %#v %#v", first, second)
	}
}

func registryWithContracts(t *testing.T) *toolcontract.MemoryRegistry {
	t.Helper()
	registry := toolcontract.NewMemoryRegistry()
	contracts := []toolcontract.Manifest{
		contract("writer", []toolcontract.ResourceSpec{{Name: "workspace", Type: "repository", Namespace: "repo", Field: "resource_id"}}, nil),
		contract("reader", nil, []toolcontract.ResourceSpec{{Name: "workspace", Type: "repository", Namespace: "repo", Field: "resource_id"}}),
		contract("verify", nil, nil),
	}
	for _, source := range contracts {
		manifest, err := toolcontract.Canonicalize(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(context.Background(), manifest); err != nil {
			t.Fatal(err)
		}
	}
	return registry
}

func contract(name string, writes, reads []toolcontract.ResourceSpec) toolcontract.Manifest {
	return toolcontract.Manifest{
		SchemaVersion: "tool-contract.v1", ToolID: name, Version: "1.0.0",
		Inputs:  []toolcontract.FieldSpec{{Name: "resource_id", Type: "string", Required: len(reads) > 0, Sanitizer: toolcontract.SanitizerToken}},
		Outputs: []toolcontract.FieldSpec{{Name: "result", Type: "string", Sanitizer: toolcontract.SanitizerText}},
		Reads:   reads, Writes: writes, SideEffect: toolcontract.SideEffectWrite, Risk: toolcontract.RiskLow,
		Idempotency:         toolcontract.IdempotencySpec{Mode: toolcontract.IdempotencyGuaranteed},
		Retry:               toolcontract.RetrySpec{Mode: toolcontract.RetryNever, MaximumAttempts: 1},
		SuccessPredicates:   []toolcontract.PredicateSpec{{ID: "success", Field: "result", Operator: "equals", Value: "ok"}},
		VerificationMethods: []toolcontract.VerificationMethod{{ID: "success", EvidenceClass: "tool_postcondition"}},
		Compatibility:       []toolcontract.CompatibilityRange{{MinimumInclusive: "1.0.0", MaximumExclusive: "2.0.0"}},
	}
}

func graphBatch() domain.CanonicalBatch {
	return domain.CanonicalBatch{
		SchemaVersion: "canonical.v1", TenantID: "tenant_a",
		Trace: domain.CanonicalTrace{ID: domain.TraceID("tr_trace")},
		Events: []domain.CanonicalEvent{
			{ID: "ev_write", Position: 0, ToolName: "writer", ToolVersion: "1.0.0", Fields: []domain.CanonicalField{{Name: "resource_id", Value: "repo-1"}}, Result: &domain.CanonicalResult{State: "TOOL_RESULT_STATE_SUCCESS", Evidence: []domain.CanonicalField{{Name: "result", Value: "ok"}}}},
			{ID: "ev_read", Position: 1, ToolName: "reader", ToolVersion: "1.0.0", Fields: []domain.CanonicalField{{Name: "resource_id", Value: "repo-1"}}, Result: &domain.CanonicalResult{State: "TOOL_RESULT_STATE_SUCCESS", Evidence: []domain.CanonicalField{{Name: "result", Value: "ok"}}}},
			{ID: "ev_verify", Position: 2, ToolName: "verify", ToolVersion: "1.0.0", Result: &domain.CanonicalResult{State: "TOOL_RESULT_STATE_SUCCESS", Evidence: []domain.CanonicalField{{Name: "result", Value: "ok"}}}},
			{ID: "ev_unknown", Position: 3, ToolName: "custom", ToolVersion: "9", Result: &domain.CanonicalResult{State: "TOOL_RESULT_STATE_SUCCESS"}},
		},
	}
}
