package storetest

import (
	"context"
	"strings"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/selection"
)

type TestingT interface {
	Helper()
	Fatal(args ...any)
}

func SelectionDraft(t TestingT, tenant domain.TenantID, queryByte byte) selection.Draft {
	t.Helper()
	procedureInterface, err := retrieval.NewProcedureInterface(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := composition.NewPlannerManifest(composition.PlannerManifestSpec{
		Version: "planner.selection.v1", BeamWidth: 4, MaximumCandidates: 10, MaximumGoalPredicates: 10, MaximumTotalCandidateGoals: 100,
		MaximumParallelEntries: 100, MaximumTotalParallelEntries: 1_000, MaximumSatisfiedRequirements: 100,
		MaximumDepth: 4, MaximumProcedures: 4, MaximumBridges: 2, MaximumTotalToolCost: 100, MaximumExpansions: 100, MaximumWorkUnits: 10_000,
		NoveltyPenalty: 10, UnobservedEdgePenalty: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	plannerCandidate := composition.PlannerCandidate{VersionID: "pv_selection", Interface: procedureInterface, Lifecycle: "active", Seed: true, Eligible: true, PolicyManifestID: "policy_selection", GoalPredicateIDs: []string{"goal.done"}, EvidenceStrength: 100, ObservedEndToEnd: true, RiskCost: 1, ToolCost: 1}
	plannerCandidate.PlanningFactsHash, err = composition.BuildPlanningFactsHash(composition.PlanningFacts{ProcedureVersionID: plannerCandidate.VersionID, InterfaceHash: procedureInterface.ContentHash, Lifecycle: plannerCandidate.Lifecycle, PolicyManifestID: plannerCandidate.PolicyManifestID, GoalPredicateIDs: plannerCandidate.GoalPredicateIDs, EvidenceStrength: plannerCandidate.EvidenceStrength, ObservedEndToEnd: plannerCandidate.ObservedEndToEnd, RiskCost: plannerCandidate.RiskCost, ToolCost: plannerCandidate.ToolCost})
	if err != nil {
		t.Fatal(err)
	}
	matrix, err := composition.BuildCompatibilityMatrix(nil)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: tenant, ProjectionEpoch: 1, PlannerManifestID: manifest.ID, PolicyManifestID: "policy_selection", Matrix: matrix, Candidates: []composition.Candidate{{TenantID: tenant, ProcedureVersionID: plannerCandidate.VersionID, Lifecycle: plannerCandidate.Lifecycle, Interface: procedureInterface, PlanningFactsHash: plannerCandidate.PlanningFactsHash}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := composition.BuildPlan(manifest, composition.PlanRequest{TenantID: tenant, ProjectionEpoch: 1, PolicyManifestID: "policy_selection", Graph: graph, GoalPredicateIDs: []string{"goal.done"}, Candidates: []composition.PlannerCandidate{plannerCandidate}})
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := composition.DeriveParallelSchedule(context.Background(), manifest, plan, graph, []composition.PlannerCandidate{plannerCandidate})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := selection.NewDraft(selection.Draft{
		TenantID: tenant, QueryHash: strings.Repeat(string(queryByte), 64), RequestContextHash: strings.Repeat("b", 64), RetrievalRunID: "rr_selection_" + string(queryByte),
		ProjectionEpoch: 1, DocumentSetHash: strings.Repeat("c", 64), ServingConfigID: "serving_selection", PolicyManifestID: "policy_selection",
		RankerManifestID: "ranker_selection", PlannerManifestID: manifest.ID, LifecyclePolicyManifestID: "lifecycle_selection", NoveltyClass: selection.NoveltyExact,
		Plan: plan, ParallelSchedule: &schedule,
	})
	if err != nil {
		t.Fatal(err)
	}
	return draft
}
