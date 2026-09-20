package composition_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/memory"
)

func TestBuildCompatibilityGraphUsesOnlyTypedContracts(t *testing.T) {
	identity := hash('a')
	source := candidate(t, "tenant_a", "pv_source", "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v2", ProducedEffects: []string{"file.created"}, SuccessPredicateIDs: []string{"file.exists"}}})
	target := candidate(t, "tenant_a", "pv_target", "candidate", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read", PredicateIDs: []string{"file.exists"}}}, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{
		TenantID: "tenant_a", ProjectionEpoch: 7, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1",
		Matrix:     matrix(t, []composition.SchemaCompatibility{{ResourceType: "file", Namespace: "workspace", ProvidedSchema: "file.v2", RequiredSchema: "file.v1"}}),
		Candidates: []composition.Candidate{target, source},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("edges = %#v", graph.Edges)
	}
	edge := graph.Edges[0]
	if edge.SourceVersionID != "pv_source" || edge.TargetVersionID != "pv_target" || len(edge.SourceProvisionIDs) != 1 || len(edge.SatisfiedRequirementIDs) != 1 || edge.ProjectionEpoch != 7 {
		t.Fatalf("edge = %#v", edge)
	}
	if edge.SourceInterfaceHash != source.Interface.ContentHash || edge.TargetInterfaceHash != target.Interface.ContentHash {
		t.Fatalf("interface hashes were not pinned: %#v", edge)
	}
}

func TestBuildCompatibilityGraphRejectsUnsafeInputs(t *testing.T) {
	identity := hash('b')
	validSource := candidate(t, "tenant_a", "pv_source", "active", nil, []domain.ProcedureProvision{{ResourceType: "record", Namespace: "crm", IdentityHash: identity, SchemaVersion: "record.v1"}})
	validTarget := candidate(t, "tenant_a", "pv_target", "active", []domain.ProcedureRequirement{{ResourceType: "record", Namespace: "crm", IdentityHash: identity, SchemaVersion: "record.v1", AccessMode: "read"}}, nil)
	base := composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 2, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{validSource, validTarget}}

	foreign := validTarget
	foreign.TenantID = "tenant_b"
	quarantined := validTarget
	quarantined.Lifecycle = "quarantined"
	superseded := validTarget
	superseded.Lifecycle = "superseded"
	wildcard := validTarget
	wildcard.Interface.Requirements = append([]domain.ProcedureRequirement(nil), validTarget.Interface.Requirements...)
	wildcard.Interface.Requirements[0].IdentityHash = "*"
	for name, candidates := range map[string][]composition.Candidate{
		"cross tenant": {validSource, foreign}, "quarantined": {validSource, quarantined},
		"superseded": {validSource, superseded}, "wildcard": {validSource, wildcard},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			request.Candidates = candidates
			if _, err := composition.BuildCompatibilityGraph(request); !errors.Is(err, composition.ErrInvalidCompatibilityInput) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestBuildCompatibilityGraphDoesNotGuessCompatibility(t *testing.T) {
	identity := hash('c')
	source := candidate(t, "tenant_a", "pv_source", "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v2", ProducedEffects: []string{"target.ready"}}})
	tests := map[string]domain.ProcedureRequirement{
		"schema mismatch":    {ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read"},
		"identity mismatch":  {ResourceType: "file", Namespace: "workspace", IdentityHash: hash('d'), SchemaVersion: "file.v2", AccessMode: "read"},
		"namespace mismatch": {ResourceType: "file", Namespace: "other", IdentityHash: identity, SchemaVersion: "file.v2", AccessMode: "read"},
	}
	for name, requirement := range tests {
		t.Run(name, func(t *testing.T) {
			target := candidate(t, "tenant_a", "pv_target", "active", []domain.ProcedureRequirement{requirement}, nil)
			graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 1, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{source, target}})
			if err != nil || len(graph.Edges) != 0 {
				t.Fatalf("graph=%#v error=%v", graph, err)
			}
		})
	}
	noProvision := candidate(t, "tenant_a", "pv_effect_only", "active", nil, nil)
	target := candidate(t, "tenant_a", "pv_target", "active", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v2", AccessMode: "read"}}, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 1, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{noProvision, target}})
	if err != nil || len(graph.Edges) != 0 {
		t.Fatalf("effect-only candidate created edge: graph=%#v error=%v", graph, err)
	}
}

func TestBuildCompatibilityGraphRequiresPredicateGuarantees(t *testing.T) {
	identity := hash('9')
	source := candidate(t, "tenant_a", "pv_source", "active", nil, []domain.ProcedureProvision{{ResourceType: "workspace", Namespace: "repo", IdentityHash: identity, SchemaVersion: "workspace.v1", SuccessPredicateIDs: []string{"workspace.exists"}}})
	target := candidate(t, "tenant_a", "pv_target", "active", []domain.ProcedureRequirement{{ResourceType: "workspace", Namespace: "repo", IdentityHash: identity, SchemaVersion: "workspace.v1", AccessMode: "read", PredicateIDs: []string{"workspace.clean"}}}, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 1, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{source, target}})
	if err != nil || len(graph.Edges) != 0 {
		t.Fatalf("unmet predicate created an edge: graph=%#v error=%v", graph, err)
	}
}

func TestCompatibilityGraphIsStableAndRepositoryIsImmutable(t *testing.T) {
	identity := hash('e')
	source := candidate(t, "tenant_a", "pv_source", "active", nil, []domain.ProcedureProvision{
		{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1"},
		{ResourceType: "token", Namespace: "auth", IdentityHash: hash('f'), SchemaVersion: "token.v1"},
	})
	target := candidate(t, "tenant_a", "pv_target", "candidate", []domain.ProcedureRequirement{
		{ResourceType: "token", Namespace: "auth", IdentityHash: hash('f'), SchemaVersion: "token.v1", AccessMode: "read"},
		{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read"},
	}, nil)
	request := composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 9, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{source, target}}
	first, err := composition.BuildCompatibilityGraph(request)
	if err != nil {
		t.Fatal(err)
	}
	for seed := int64(0); seed < 20; seed++ {
		shuffled := request
		shuffled.Candidates = append([]composition.Candidate(nil), request.Candidates...)
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffled.Candidates), func(i, j int) {
			shuffled.Candidates[i], shuffled.Candidates[j] = shuffled.Candidates[j], shuffled.Candidates[i]
		})
		got, buildErr := composition.BuildCompatibilityGraph(shuffled)
		if buildErr != nil || string(got.CanonicalJSON) != string(first.CanonicalJSON) {
			t.Fatalf("seed %d changed graph: error=%v\n%s\n%s", seed, buildErr, got.CanonicalJSON, first.CanonicalJSON)
		}
	}
	repository := memory.NewCompositionRepository()
	if err = repository.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err = repository.Publish(context.Background(), first); err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	edges, err := repository.Edges(context.Background(), "tenant_a", 9, "planner_1", "policy_1")
	if err != nil || len(edges) != 1 || len(edges[0].SatisfiedRequirementIDs) != 2 {
		t.Fatalf("edges=%#v error=%v", edges, err)
	}
	otherTarget := candidate(t, "tenant_a", "pv_other", "candidate", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read"}}, nil)
	conflict, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 9, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{source, otherTarget}})
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.Publish(context.Background(), conflict); !errors.Is(err, composition.ErrCompositionConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestCompatibilityMatrixRejectsDuplicateAndWildcardRules(t *testing.T) {
	rule := composition.SchemaCompatibility{ResourceType: "file", Namespace: "workspace", ProvidedSchema: "file.v2", RequiredSchema: "file.v1"}
	for name, rules := range map[string][]composition.SchemaCompatibility{
		"duplicate": {rule, rule},
		"wildcard":  {{ResourceType: "file", Namespace: "*", ProvidedSchema: "file.v2", RequiredSchema: "file.v1"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := composition.BuildCompatibilityMatrix(rules); !errors.Is(err, composition.ErrInvalidCompatibilityInput) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCompatibilityGraphEnforcesManifestBoundsAndCollisionSafeIdentities(t *testing.T) {
	identity := hash('8')
	source := candidate(t, "tenant_a", "pv_source", "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1"}})
	target := candidate(t, "tenant_a", "pv_target", "active", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read"}}, nil)
	limits := composition.CompatibilityLimits{MaximumCandidates: 1, MaximumInterfaceEntries: 8, MaximumTotalInterfaceEntries: 8, MaximumProvidersPerResource: 8, MaximumEdges: 8, MaximumMatchComparisons: 8}
	bounded, err := composition.BuildCompatibilityMatrixWithLimits(nil, limits)
	if err != nil {
		t.Fatal(err)
	}
	request := composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 1, PlannerManifestID: "planner_1", PolicyManifestID: "policy_1", Matrix: bounded, Candidates: []composition.Candidate{source, target}}
	if _, err = composition.BuildCompatibilityGraph(request); !errors.Is(err, composition.ErrInvalidCompatibilityInput) {
		t.Fatalf("candidate bound error = %v", err)
	}

	colliding := candidate(t, "tenant_a", "pv_collision", "active", nil, []domain.ProcedureProvision{{ResourceType: "file\x00workspace", Namespace: "scope", IdentityHash: identity, SchemaVersion: "file.v1"}})
	request.Matrix = matrix(t, nil)
	request.Candidates = []composition.Candidate{colliding, target}
	if _, err = composition.BuildCompatibilityGraph(request); !errors.Is(err, composition.ErrInvalidCompatibilityInput) {
		t.Fatalf("NUL identity error = %v", err)
	}

	secondSource := candidate(t, "tenant_a", "pv_source_2", "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1"}})
	workLimits := composition.CompatibilityLimits{MaximumCandidates: 3, MaximumInterfaceEntries: 8, MaximumTotalInterfaceEntries: 8, MaximumProvidersPerResource: 2, MaximumEdges: 8, MaximumMatchComparisons: 1}
	workBounded, err := composition.BuildCompatibilityMatrixWithLimits(nil, workLimits)
	if err != nil {
		t.Fatal(err)
	}
	request.Matrix, request.Candidates = workBounded, []composition.Candidate{source, secondSource, target}
	if _, err = composition.BuildCompatibilityGraph(request); !errors.Is(err, composition.ErrInvalidCompatibilityInput) {
		t.Fatalf("comparison budget error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = composition.BuildCompatibilityGraphContext(cancelled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled build error = %v", err)
	}
}

func candidate(t *testing.T, tenant, version, lifecycle string, requirements []domain.ProcedureRequirement, provisions []domain.ProcedureProvision) composition.Candidate {
	t.Helper()
	procedureInterface, err := retrieval.NewProcedureInterface(requirements, provisions)
	if err != nil {
		t.Fatal(err)
	}
	return composition.Candidate{TenantID: domain.TenantID(tenant), ProcedureVersionID: version, Lifecycle: lifecycle, Interface: procedureInterface}
}

func matrix(t *testing.T, rules []composition.SchemaCompatibility) composition.CompatibilityMatrix {
	t.Helper()
	result, err := composition.BuildCompatibilityMatrix(rules)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func hash(value byte) string {
	result := make([]byte, 64)
	for index := range result {
		result[index] = value
	}
	return string(result)
}
