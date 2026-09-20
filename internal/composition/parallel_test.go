package composition_test

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"testing"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestParallelScheduleAllowsReadReadAndRejectsResourceAndPolicyConflicts(t *testing.T) {
	shared := "res_" + hash('2')
	tests := []struct {
		name  string
		left  composition.ParallelismFacts
		right composition.ParallelismFacts
		want  int
	}{
		{name: "read read", left: composition.ParallelismFacts{ReadResourceIDs: []string{shared}}, right: composition.ParallelismFacts{ReadResourceIDs: []string{shared}}, want: 2},
		{name: "write read", left: composition.ParallelismFacts{WriteResourceIDs: []string{shared}}, right: composition.ParallelismFacts{ReadResourceIDs: []string{shared}}, want: 3},
		{name: "write write", left: composition.ParallelismFacts{WriteResourceIDs: []string{shared}}, right: composition.ParallelismFacts{WriteResourceIDs: []string{shared}}, want: 3},
		{name: "exclusive read", left: composition.ParallelismFacts{ExclusiveResourceIDs: []string{shared}}, right: composition.ParallelismFacts{ReadResourceIDs: []string{shared}}, want: 3},
		{name: "compensation", left: composition.ParallelismFacts{CompensationBoundary: true}, want: 3},
		{name: "policy mutex", left: composition.ParallelismFacts{PolicyMutexKeys: []string{"tenant.billing"}}, right: composition.ParallelismFacts{PolicyMutexKeys: []string{"tenant.billing"}}, want: 3},
		{name: "policy serial", left: composition.ParallelismFacts{PolicySerial: true}, want: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest, plan, graph, candidates := forkPlan(t, test.left, test.right)
			schedule, err := composition.DeriveParallelSchedule(context.Background(), manifest, plan, graph, candidates)
			if err != nil || len(schedule.Groups) != test.want || composition.ValidateParallelSchedule(schedule) != nil {
				t.Fatalf("schedule=%#v error=%v", schedule, err)
			}
			if test.want == 2 && !slices.Equal(schedule.Groups[0].NodeVersionIDs, []string{"pv_left", "pv_right"}) {
				t.Fatalf("parallel group=%v", schedule.Groups[0].NodeVersionIDs)
			}
		})
	}
}

func TestParallelScheduleExcludesIndirectDependenciesAndIsPermutationInvariant(t *testing.T) {
	manifest, plan, graph, candidates := chainPlan(t)
	want, err := composition.DeriveParallelSchedule(context.Background(), manifest, plan, graph, candidates)
	if err != nil || len(want.Groups) != 3 {
		t.Fatalf("schedule=%#v error=%v", want, err)
	}
	for seed := int64(0); seed < 20; seed++ {
		shuffled := slices.Clone(candidates)
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		got, scheduleErr := composition.DeriveParallelSchedule(context.Background(), manifest, plan, graph, shuffled)
		if scheduleErr != nil || string(got.CanonicalJSON) != string(want.CanonicalJSON) {
			t.Fatalf("seed=%d schedule=%#v error=%v", seed, got, scheduleErr)
		}
	}
}

func TestParallelScheduleRejectsPartialPlans(t *testing.T) {
	value := candidate(t, "tenant_a", "pv_seed", "active", nil, nil)
	plannerValue := plannerCandidate(value, true, nil, 500)
	value = bindCandidate(t, value, plannerValue)
	manifest := plannerManifest(t, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 23, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{value}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := composition.BuildPlan(manifest, composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 23, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"uncovered"}, Candidates: []composition.PlannerCandidate{plannerValue}})
	if err != nil || plan.Complete {
		t.Fatalf("plan=%#v error=%v", plan, err)
	}
	_, err = composition.DeriveParallelSchedule(context.Background(), manifest, plan, graph, []composition.PlannerCandidate{plannerValue})
	if !errors.Is(err, composition.ErrInvalidParallelRequest) {
		t.Fatalf("error=%v", err)
	}
}

func forkPlan(t *testing.T, leftFacts, rightFacts composition.ParallelismFacts) (composition.PlannerManifest, composition.Plan, composition.CompatibilityGraph, []composition.PlannerCandidate) {
	t.Helper()
	leftIdentity, rightIdentity := hash('a'), hash('b')
	left := candidate(t, "tenant_a", "pv_left", "active", nil, []domain.ProcedureProvision{{ResourceType: "token", Namespace: "left", IdentityHash: leftIdentity, SchemaVersion: "v1"}})
	right := candidate(t, "tenant_a", "pv_right", "active", nil, []domain.ProcedureProvision{{ResourceType: "token", Namespace: "right", IdentityHash: rightIdentity, SchemaVersion: "v1"}})
	root := candidate(t, "tenant_a", "pv_root", "active", []domain.ProcedureRequirement{
		{ResourceType: "token", Namespace: "left", IdentityHash: leftIdentity, SchemaVersion: "v1", AccessMode: "read"},
		{ResourceType: "token", Namespace: "right", IdentityHash: rightIdentity, SchemaVersion: "v1", AccessMode: "read"},
	}, nil)
	leftPlanner := parallelCandidate(t, plannerCandidate(left, false, []string{"goal.left"}, 800), leftFacts)
	rightPlanner := parallelCandidate(t, plannerCandidate(right, false, []string{"goal.right"}, 800), rightFacts)
	rootPlanner := plannerCandidate(root, true, []string{"goal.root"}, 900)
	left, right, root = bindCandidate(t, left, leftPlanner), bindCandidate(t, right, rightPlanner), bindCandidate(t, root, rootPlanner)
	manifest := plannerManifest(t, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 21, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{left, right, root}})
	if err != nil {
		t.Fatal(err)
	}
	candidates := []composition.PlannerCandidate{leftPlanner, rightPlanner, rootPlanner}
	plan, err := composition.BuildPlan(manifest, composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 21, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"goal.left", "goal.right", "goal.root"}, Candidates: candidates})
	if err != nil || len(plan.Nodes) != 3 {
		t.Fatalf("plan=%#v error=%v", plan, err)
	}
	return manifest, plan, graph, candidates
}

func chainPlan(t *testing.T) (composition.PlannerManifest, composition.Plan, composition.CompatibilityGraph, []composition.PlannerCandidate) {
	t.Helper()
	firstIdentity, secondIdentity := hash('c'), hash('d')
	first := candidate(t, "tenant_a", "pv_first", "active", nil, []domain.ProcedureProvision{{ResourceType: "token", Namespace: "first", IdentityHash: firstIdentity, SchemaVersion: "v1"}})
	second := candidate(t, "tenant_a", "pv_second", "active", []domain.ProcedureRequirement{{ResourceType: "token", Namespace: "first", IdentityHash: firstIdentity, SchemaVersion: "v1", AccessMode: "read"}}, []domain.ProcedureProvision{{ResourceType: "token", Namespace: "second", IdentityHash: secondIdentity, SchemaVersion: "v1"}})
	third := candidate(t, "tenant_a", "pv_third", "active", []domain.ProcedureRequirement{{ResourceType: "token", Namespace: "second", IdentityHash: secondIdentity, SchemaVersion: "v1", AccessMode: "read"}}, nil)
	firstPlanner := plannerCandidate(first, false, []string{"goal.first"}, 700)
	secondPlanner := plannerCandidate(second, false, []string{"goal.second"}, 800)
	thirdPlanner := plannerCandidate(third, true, []string{"goal.third"}, 900)
	first, second, third = bindCandidate(t, first, firstPlanner), bindCandidate(t, second, secondPlanner), bindCandidate(t, third, thirdPlanner)
	manifest := plannerManifest(t, nil)
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 22, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_1", Matrix: matrix(t, nil), Candidates: []composition.Candidate{first, second, third}})
	if err != nil {
		t.Fatal(err)
	}
	candidates := []composition.PlannerCandidate{firstPlanner, secondPlanner, thirdPlanner}
	plan, err := composition.BuildPlan(manifest, composition.PlanRequest{TenantID: "tenant_a", ProjectionEpoch: 22, PolicyManifestID: "policy_1", Graph: graph, GoalPredicateIDs: []string{"goal.first", "goal.second", "goal.third"}, Candidates: candidates})
	if err != nil || len(plan.Nodes) != 3 {
		t.Fatalf("plan=%#v error=%v", plan, err)
	}
	return manifest, plan, graph, candidates
}

func parallelCandidate(t *testing.T, candidate composition.PlannerCandidate, facts composition.ParallelismFacts) composition.PlannerCandidate {
	t.Helper()
	candidate.Parallelism = facts
	var err error
	candidate.PlanningFactsHash, err = composition.BuildPlanningFactsHash(composition.PlanningFacts{ProcedureVersionID: candidate.VersionID, InterfaceHash: candidate.Interface.ContentHash, Lifecycle: candidate.Lifecycle, PolicyManifestID: candidate.PolicyManifestID, GoalPredicateIDs: candidate.GoalPredicateIDs, Parallelism: candidate.Parallelism, EvidenceStrength: candidate.EvidenceStrength, ObservedEndToEnd: candidate.ObservedEndToEnd, RiskCost: candidate.RiskCost, ToolCost: candidate.ToolCost})
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}
