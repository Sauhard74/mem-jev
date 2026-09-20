package composition_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestPlannerInsertsTypedBridgeAndReturnsStablePartialPlan(t *testing.T) {
	identity := hash('7')
	bridge := candidate(t, "tenant_a", "pv_bridge", "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", SuccessPredicateIDs: []string{"workspace.ready"}}})
	primary := candidate(t, "tenant_a", "pv_primary", "active", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read", PredicateIDs: []string{"workspace.ready"}}}, nil)
	bridgePlanner, primaryPlanner := plannerCandidate(bridge, false, nil, 700), plannerCandidate(primary, true, []string{"output.ready"}, 900)
	bridge, primary = bindCandidate(t, bridge, bridgePlanner), bindCandidate(t, primary, primaryPlanner)
	manifest := plannerManifest(t, func(spec *composition.PlannerManifestSpec) { spec.Version = "planner_1" })
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 3, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{bridge, primary}})
	if err != nil {
		t.Fatal(err)
	}
	request := composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 3, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"output.ready"}, Candidates: []composition.PlannerCandidate{
		bridgePlanner, primaryPlanner,
	}}
	plan, err := composition.BuildPlan(manifest, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes) != 2 || plan.Nodes[0].VersionID != "pv_bridge" || !plan.Nodes[0].Bridge || plan.Nodes[1].VersionID != "pv_primary" || len(plan.Gaps) != 0 || !plan.Complete {
		t.Fatalf("plan = %#v", plan)
	}

	request.Graph, err = composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 3, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{primary}})
	if err != nil {
		t.Fatal(err)
	}
	request.Candidates = []composition.PlannerCandidate{primaryPlanner}
	partial, err := composition.BuildPlan(manifest, request)
	if err != nil || partial.Complete || len(partial.Gaps) != 1 || partial.Gaps[0].Code != "unsatisfied_requirement" {
		t.Fatalf("partial=%#v error=%v", partial, err)
	}
}

func TestPlannerRejectsCyclesAndHonorsCostLimits(t *testing.T) {
	leftIdentity, rightIdentity := hash('5'), hash('6')
	left := candidate(t, "tenant_a", "pv_left", "active",
		[]domain.ProcedureRequirement{{ResourceType: "token", Namespace: "right", IdentityHash: rightIdentity, SchemaVersion: "v1", AccessMode: "read"}},
		[]domain.ProcedureProvision{{ResourceType: "token", Namespace: "left", IdentityHash: leftIdentity, SchemaVersion: "v1"}})
	right := candidate(t, "tenant_a", "pv_right", "active",
		[]domain.ProcedureRequirement{{ResourceType: "token", Namespace: "left", IdentityHash: leftIdentity, SchemaVersion: "v1", AccessMode: "read"}},
		[]domain.ProcedureProvision{{ResourceType: "token", Namespace: "right", IdentityHash: rightIdentity, SchemaVersion: "v1"}})
	leftPlanner, rightPlanner := plannerCandidate(left, true, []string{"done"}, 900), plannerCandidate(right, false, nil, 900)
	left, right = bindCandidate(t, left, leftPlanner), bindCandidate(t, right, rightPlanner)
	manifest := plannerManifest(t, func(spec *composition.PlannerManifestSpec) { spec.MaximumTotalToolCost = 5 })
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 4, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{left, right}})
	if err != nil || len(graph.Edges) != 2 {
		t.Fatalf("graph=%#v error=%v", graph, err)
	}
	request := composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 4, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"done"}, Candidates: []composition.PlannerCandidate{
		leftPlanner, rightPlanner,
	}}
	plan, err := composition.BuildPlan(manifest, request)
	if err != nil || len(plan.Nodes) != 1 || plan.Complete {
		t.Fatalf("cycle/cost plan=%#v error=%v", plan, err)
	}
}

func TestPlannerSkipsOverBudgetSeedWhenAFeasibleSeedExists(t *testing.T) {
	expensive := candidate(t, "tenant_a", "pv_a_expensive", "active", nil, nil)
	feasible := candidate(t, "tenant_a", "pv_b_feasible", "active", nil, nil)
	expensivePlanner := plannerCandidate(expensive, true, []string{"done"}, 900)
	expensivePlanner.ToolCost = 50
	expensivePlanner.PlanningFactsHash, _ = composition.BuildPlanningFactsHash(composition.PlanningFacts{ProcedureVersionID: expensivePlanner.VersionID, InterfaceHash: expensivePlanner.Interface.ContentHash, Lifecycle: expensivePlanner.Lifecycle, PolicyManifestID: expensivePlanner.PolicyManifestID, GoalPredicateIDs: expensivePlanner.GoalPredicateIDs, EvidenceStrength: expensivePlanner.EvidenceStrength, ObservedEndToEnd: expensivePlanner.ObservedEndToEnd, RiskCost: expensivePlanner.RiskCost, ToolCost: expensivePlanner.ToolCost})
	feasiblePlanner := plannerCandidate(feasible, true, []string{"done"}, 800)
	expensive, feasible = bindCandidate(t, expensive, expensivePlanner), bindCandidate(t, feasible, feasiblePlanner)
	manifest := plannerManifest(t, func(spec *composition.PlannerManifestSpec) { spec.MaximumTotalToolCost = 10 })
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 11, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{expensive, feasible}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := composition.BuildPlan(manifest, composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 11, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"done"}, Candidates: []composition.PlannerCandidate{expensivePlanner, feasiblePlanner}})
	if err != nil || len(plan.Nodes) != 1 || plan.Nodes[0].VersionID != feasiblePlanner.VersionID || !plan.Complete {
		t.Fatalf("plan=%#v error=%v", plan, err)
	}
}

func TestPlannerManifestIsContentAddressedAndValidated(t *testing.T) {
	manifest := plannerManifest(t, nil)
	if composition.ValidatePlannerManifest(manifest) != nil || manifest.ID[:5] != "pman_" || len(manifest.ID) != 69 {
		t.Fatalf("manifest=%#v", manifest)
	}
	rebuilt, err := composition.NewPlannerManifest(manifest.PlannerManifestSpec)
	if err != nil || rebuilt.ID != manifest.ID || !bytes.Equal(rebuilt.CanonicalJSON, manifest.CanonicalJSON) {
		t.Fatalf("rebuilt=%#v error=%v", rebuilt, err)
	}
	invalid := manifest.PlannerManifestSpec
	invalid.MaximumExpansions = 1
	if _, err = composition.NewPlannerManifest(invalid); err == nil {
		t.Fatal("manifest accepted an expansion budget below beam width")
	}
	invalid = manifest.PlannerManifestSpec
	invalid.MaximumTotalCandidateGoals = invalid.MaximumGoalPredicates - 1
	if _, err = composition.NewPlannerManifest(invalid); err == nil {
		t.Fatal("manifest accepted aggregate candidate goals below the per-candidate limit")
	}
}

func TestPlannerRejectsCandidateGoalListsBeyondManifestBound(t *testing.T) {
	value := candidate(t, "tenant_a", "pv_seed", "active", nil, nil)
	plannerValue := plannerCandidate(value, true, nil, 500)
	for index := 0; index < 101; index++ {
		plannerValue.GoalPredicateIDs = append(plannerValue.GoalPredicateIDs, fmt.Sprintf("goal.%03d", index))
	}
	plannerValue.PlanningFactsHash, _ = composition.BuildPlanningFactsHash(composition.PlanningFacts{ProcedureVersionID: plannerValue.VersionID, InterfaceHash: plannerValue.Interface.ContentHash, Lifecycle: plannerValue.Lifecycle, PolicyManifestID: plannerValue.PolicyManifestID, GoalPredicateIDs: plannerValue.GoalPredicateIDs, EvidenceStrength: plannerValue.EvidenceStrength, ObservedEndToEnd: plannerValue.ObservedEndToEnd, RiskCost: plannerValue.RiskCost, ToolCost: plannerValue.ToolCost})
	value = bindCandidate(t, value, plannerValue)
	manifest := plannerManifest(t, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 12, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{value}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = composition.BuildPlan(manifest, composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 12, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"done"}, Candidates: []composition.PlannerCandidate{plannerValue}})
	if !errors.Is(err, composition.ErrInvalidPlanRequest) {
		t.Fatalf("error=%v", err)
	}
}

func TestPlannerBindsCandidateFactsAndMissingGoalsIntoPlanIdentity(t *testing.T) {
	value := candidate(t, "tenant_a", "pv_seed", "active", nil, nil)
	plannerValue := plannerCandidate(value, true, nil, 500)
	value = bindCandidate(t, value, plannerValue)
	manifest := plannerManifest(t, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 5, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{value}})
	if err != nil {
		t.Fatal(err)
	}
	request := composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 5, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"goal.a"}, Candidates: []composition.PlannerCandidate{plannerValue}}
	first, err := composition.BuildPlan(manifest, request)
	if err != nil || first.Complete || len(first.Gaps) != 1 || first.Gaps[0].Code != "uncovered_goal" || first.Gaps[0].GoalPredicateID != "goal.a" {
		t.Fatalf("first=%#v error=%v", first, err)
	}
	request.GoalPredicateIDs = []string{"goal.b"}
	second, err := composition.BuildPlan(manifest, request)
	if err != nil || first.ID == second.ID || second.Gaps[0].GoalPredicateID != "goal.b" {
		t.Fatalf("second=%#v error=%v", second, err)
	}
	tampered := plannerValue
	tampered.EvidenceStrength++
	request.Candidates = []composition.PlannerCandidate{tampered}
	if _, err = composition.BuildPlan(manifest, request); err == nil {
		t.Fatal("unbound candidate scoring facts were accepted")
	}
}

func TestPlannerBindsGraphSnapshotAndExternalSatisfactionIntoPlanIdentity(t *testing.T) {
	identity := hash('3')
	bridge := candidate(t, "tenant_a", "pv_bridge", "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "v1"}})
	seed := candidate(t, "tenant_a", "pv_seed", "active", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "v1", AccessMode: "read"}}, nil)
	extra := candidate(t, "tenant_a", "pv_unselected", "active", nil, nil)
	bridgePlanner := plannerCandidate(bridge, false, []string{"goal.bridge"}, 700)
	seedPlanner := plannerCandidate(seed, true, []string{"goal.seed"}, 900)
	extraPlanner := plannerCandidate(extra, false, nil, 100)
	bridge, seed, extra = bindCandidate(t, bridge, bridgePlanner), bindCandidate(t, seed, seedPlanner), bindCandidate(t, extra, extraPlanner)
	manifest := plannerManifest(t, nil)
	compatMatrix := matrix(t, nil)
	baseGraph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 8, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: compatMatrix, Candidates: []composition.Candidate{bridge, seed}})
	if err != nil {
		t.Fatal(err)
	}
	base := composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 8, PolicyManifestID: "policy_1", Graph: baseGraph, GoalPredicateIDs: []string{"goal.bridge", "goal.seed"}, Candidates: []composition.PlannerCandidate{bridgePlanner, seedPlanner}}
	first, err := composition.BuildPlan(manifest, base)
	if err != nil || len(first.Nodes) != 2 || !first.Complete {
		t.Fatalf("first=%#v error=%v", first, err)
	}

	changedGraph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 8, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: compatMatrix, Candidates: []composition.Candidate{bridge, seed, extra}})
	if err != nil {
		t.Fatal(err)
	}
	changed := base
	changed.Graph = changedGraph
	changed.Candidates = []composition.PlannerCandidate{bridgePlanner, seedPlanner, extraPlanner}
	second, err := composition.BuildPlan(manifest, changed)
	if err != nil || first.ID == second.ID || first.CandidateSetHash == second.CandidateSetHash || first.CompatibilityGraphID == second.CompatibilityGraphID {
		t.Fatalf("snapshot was not bound: first=%#v second=%#v error=%v", first, second, err)
	}

	externallySatisfied := base
	externallySatisfied.SatisfiedRequirementIDs = []string{seed.Interface.Requirements[0].ID}
	third, err := composition.BuildPlan(manifest, externallySatisfied)
	if err != nil || len(third.Nodes) != 2 || first.ID == third.ID || !slices.Equal(third.ExternallySatisfiedRequirementIDs, externallySatisfied.SatisfiedRequirementIDs) {
		t.Fatalf("external satisfaction was not bound: first=%#v third=%#v error=%v", first, third, err)
	}
}

func TestPlannerReportsDeterministicWorkExhaustion(t *testing.T) {
	identity := hash('4')
	target := candidate(t, "tenant_a", "pv_target", "active", []domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "v1", AccessMode: "read"}}, nil)
	targetPlanner := plannerCandidate(target, true, []string{"done"}, 900)
	target = bindCandidate(t, target, targetPlanner)
	graphCandidates := []composition.Candidate{target}
	plannerCandidates := []composition.PlannerCandidate{targetPlanner}
	for index := 0; index < 20; index++ {
		version := fmt.Sprintf("pv_source_%02d", index)
		source := candidate(t, "tenant_a", version, "active", nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "v1"}})
		plannerSource := plannerCandidate(source, false, nil, 800)
		source = bindCandidate(t, source, plannerSource)
		graphCandidates = append(graphCandidates, source)
		plannerCandidates = append(plannerCandidates, plannerSource)
	}
	manifest := plannerManifest(t, func(spec *composition.PlannerManifestSpec) {
		spec.MaximumExpansions = 16
		spec.MaximumWorkUnits = 90
	})
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 6, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: graphCandidates})
	if err != nil || len(graph.Edges) != 20 {
		t.Fatalf("graph edges=%d error=%v", len(graph.Edges), err)
	}
	plan, err := composition.BuildPlan(manifest, composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 6, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"done"}, Candidates: plannerCandidates})
	if err != nil || !slices.Contains(plan.LimitCodes, "work_budget_exhausted") || composition.ValidatePlan(plan) != nil {
		t.Fatalf("plan=%#v error=%v", plan, err)
	}
}

func TestPlannerIsPermutationInvariantAndUsesCanonicalTieBreak(t *testing.T) {
	left := candidate(t, "tenant_a", "pv_a", "active", nil, nil)
	right := candidate(t, "tenant_a", "pv_b", "active", nil, nil)
	leftPlanner, rightPlanner := plannerCandidate(left, true, []string{"done"}, 500), plannerCandidate(right, true, []string{"done"}, 500)
	left, right = bindCandidate(t, left, leftPlanner), bindCandidate(t, right, rightPlanner)
	manifest := plannerManifest(t, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 1, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{left, right}})
	if err != nil {
		t.Fatal(err)
	}
	base := composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 1, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"done"}, Candidates: []composition.PlannerCandidate{
		rightPlanner, leftPlanner,
	}}
	want, err := composition.BuildPlan(manifest, base)
	if err != nil || len(want.Nodes) != 1 {
		t.Fatalf("plan=%#v error=%v", want, err)
	}
	for seed := int64(0); seed < 20; seed++ {
		request := base
		request.Candidates = slices.Clone(base.Candidates)
		rand.New(rand.NewSource(seed)).Shuffle(len(request.Candidates), func(i, j int) {
			request.Candidates[i], request.Candidates[j] = request.Candidates[j], request.Candidates[i]
		})
		got, planErr := composition.BuildPlan(manifest, request)
		if planErr != nil || string(got.CanonicalJSON) != string(want.CanonicalJSON) {
			t.Fatalf("seed=%d plan=%#v error=%v", seed, got, planErr)
		}
	}
}

func plannerManifest(t *testing.T, mutate func(*composition.PlannerManifestSpec)) composition.PlannerManifest {
	t.Helper()
	spec := composition.PlannerManifestSpec{Version: "planner_1", BeamWidth: 16, MaximumCandidates: 100, MaximumGoalPredicates: 100, MaximumTotalCandidateGoals: 1_000, MaximumParallelEntries: 1_000, MaximumTotalParallelEntries: 10_000, MaximumSatisfiedRequirements: 1_000, MaximumDepth: 4, MaximumProcedures: 4, MaximumBridges: 2, MaximumTotalToolCost: 100, MaximumExpansions: 1000, MaximumWorkUnits: 100_000, NoveltyPenalty: 20, UnobservedEdgePenalty: 10}
	if mutate != nil {
		mutate(&spec)
	}
	manifest, err := composition.NewPlannerManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func plannerCandidate(value composition.Candidate, seed bool, goals []string, evidence int64) composition.PlannerCandidate {
	result := composition.PlannerCandidate{VersionID: value.ProcedureVersionID, Interface: value.Interface, Lifecycle: value.Lifecycle, Seed: seed, Eligible: true, PolicyManifestID: "policy_1", GoalPredicateIDs: goals, EvidenceStrength: evidence, ObservedEndToEnd: true, RiskCost: 1, ToolCost: 5}
	result.PlanningFactsHash, _ = composition.BuildPlanningFactsHash(composition.PlanningFacts{ProcedureVersionID: result.VersionID, InterfaceHash: result.Interface.ContentHash, Lifecycle: result.Lifecycle, PolicyManifestID: result.PolicyManifestID, GoalPredicateIDs: result.GoalPredicateIDs, EvidenceStrength: result.EvidenceStrength, ObservedEndToEnd: result.ObservedEndToEnd, RiskCost: result.RiskCost, ToolCost: result.ToolCost})
	return result
}

func bindCandidate(t *testing.T, candidate composition.Candidate, planner composition.PlannerCandidate) composition.Candidate {
	t.Helper()
	if planner.PlanningFactsHash == "" {
		t.Fatal("planner facts hash missing")
	}
	candidate.PlanningFactsHash = planner.PlanningFactsHash
	return candidate
}
