package planning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/selection"
)

type DeterministicIssuer struct {
	repository selection.Repository
	manifest   composition.PlannerManifest
	matrix     composition.CompatibilityMatrix
}

func NewDeterministicIssuer(repository selection.Repository) (*DeterministicIssuer, error) {
	manifest, err := composition.NewPlannerManifest(composition.PlannerManifestSpec{
		Version: "retrieval-plan.v1", BeamWidth: 32, MaximumCandidates: 100, MaximumGoalPredicates: 128,
		MaximumTotalCandidateGoals: 12_800, MaximumParallelEntries: 1_024, MaximumTotalParallelEntries: 102_400,
		MaximumSatisfiedRequirements: 4_096, MaximumDepth: 8, MaximumProcedures: 8, MaximumBridges: 4,
		MaximumTotalToolCost: 4_096, MaximumExpansions: 10_000, MaximumWorkUnits: 1_000_000,
		NoveltyPenalty: 100, UnobservedEdgePenalty: 100,
	})
	if err != nil {
		return nil, err
	}
	matrix, err := composition.BuildCompatibilityMatrix(nil)
	if err != nil {
		return nil, err
	}
	if repository == nil {
		return nil, retrieval.ErrPlanUnavailable
	}
	return &DeterministicIssuer{repository: repository, manifest: manifest, matrix: matrix}, nil
}

func (i *DeterministicIssuer) FindByRetrievalRunID(ctx context.Context, tenantID domain.TenantID, runID string) (retrieval.PlanArtifact, error) {
	if i == nil || i.repository == nil {
		return retrieval.PlanArtifact{}, retrieval.ErrPlanUnavailable
	}
	record, err := i.repository.FindByRetrievalRunID(ctx, tenantID, runID)
	if err != nil {
		if errors.Is(err, selection.ErrSelectionNotFound) {
			return retrieval.PlanArtifact{}, retrieval.ErrPlanNotFound
		}
		return retrieval.PlanArtifact{}, err
	}
	return artifact(record), nil
}

func (i *DeterministicIssuer) Issue(ctx context.Context, run retrieval.Run, documents []retrieval.Document) (retrieval.PlanArtifact, error) {
	if i == nil || i.repository == nil || retrieval.ValidateRun(run) != nil || run.Disposition != retrieval.RunSelected || len(run.SelectedVersionIDs) == 0 {
		return retrieval.PlanArtifact{}, retrieval.ErrPlanUnavailable
	}
	byID := make(map[string]retrieval.Document, len(documents))
	for _, document := range documents {
		if retrieval.ValidateDocument(document) != nil || document.TenantID != run.TenantID {
			return retrieval.PlanArtifact{}, retrieval.ErrPlanUnavailable
		}
		byID[document.ProcedureVersionID] = document
	}
	primary, ok := byID[run.SelectedVersionIDs[0]]
	if !ok || primary.Interface == nil {
		return retrieval.PlanArtifact{}, retrieval.ErrPlanUnavailable
	}
	goalSum := sha256.Sum256([]byte(primary.ProcedureVersionID + "\x00" + run.QueryHash))
	goalID := "goal_" + hex.EncodeToString(goalSum[:])
	plannerCandidates := make([]composition.PlannerCandidate, 0, len(run.SelectedVersionIDs))
	graphCandidates := make([]composition.Candidate, 0, len(run.SelectedVersionIDs))
	for index, versionID := range run.SelectedVersionIDs {
		document, found := byID[versionID]
		if !found || document.Interface == nil {
			return retrieval.PlanArtifact{}, retrieval.ErrPlanUnavailable
		}
		goals := []string(nil)
		if index == 0 {
			goals = []string{goalID}
		}
		candidate := composition.PlannerCandidate{
			VersionID: document.ProcedureVersionID, Interface: *document.Interface, Lifecycle: document.Lifecycle,
			Seed: index == 0, Eligible: true, PolicyManifestID: run.Snapshot.PolicyManifestID, GoalPredicateIDs: goals,
			Parallelism: parallelismFacts(*document.Interface), EvidenceStrength: int64(document.VerificationStrength), ObservedEndToEnd: document.ObservedEndToEnd,
			RiskCost: riskCost(document.RiskClass), ToolCost: uint32(max(1, len(document.Tools))),
		}
		var factsErr error
		candidate.PlanningFactsHash, factsErr = composition.BuildPlanningFactsHash(composition.PlanningFacts{
			ProcedureVersionID: candidate.VersionID, InterfaceHash: candidate.Interface.ContentHash, Lifecycle: candidate.Lifecycle,
			PolicyManifestID: candidate.PolicyManifestID, GoalPredicateIDs: candidate.GoalPredicateIDs, Parallelism: candidate.Parallelism,
			EvidenceStrength: candidate.EvidenceStrength, ObservedEndToEnd: candidate.ObservedEndToEnd, RiskCost: candidate.RiskCost, ToolCost: candidate.ToolCost,
		})
		if factsErr != nil {
			return retrieval.PlanArtifact{}, fmt.Errorf("build planning facts: %w", factsErr)
		}
		plannerCandidates = append(plannerCandidates, candidate)
		graphCandidates = append(graphCandidates, composition.Candidate{TenantID: run.TenantID, ProcedureVersionID: candidate.VersionID, Lifecycle: candidate.Lifecycle, Interface: candidate.Interface, PlanningFactsHash: candidate.PlanningFactsHash})
	}
	graph, err := composition.BuildCompatibilityGraphContext(ctx, composition.GraphRequest{
		TenantID: run.TenantID, ProjectionEpoch: run.Snapshot.ProjectionEpoch, PlannerManifestID: i.manifest.ID,
		PolicyManifestID: run.Snapshot.PolicyManifestID, Matrix: i.matrix, Candidates: graphCandidates,
	})
	if err != nil {
		return retrieval.PlanArtifact{}, fmt.Errorf("build compatibility graph: %w", err)
	}
	plan, err := composition.PlanProcedures(ctx, i.manifest, composition.PlanRequest{
		TenantID: run.TenantID, ProjectionEpoch: run.Snapshot.ProjectionEpoch, PolicyManifestID: run.Snapshot.PolicyManifestID,
		Graph: graph, GoalPredicateIDs: []string{goalID}, Candidates: plannerCandidates,
	})
	if err != nil {
		return retrieval.PlanArtifact{}, fmt.Errorf("build executable plan: %w", err)
	}
	var schedule *composition.ParallelSchedule
	novelty := selection.NoveltyPartial
	if plan.Complete {
		derived, scheduleErr := composition.DeriveParallelSchedule(ctx, i.manifest, plan, graph, plannerCandidates)
		if scheduleErr != nil {
			return retrieval.PlanArtifact{}, fmt.Errorf("derive parallel schedule: %w", scheduleErr)
		}
		schedule = &derived
		novelty = selection.NoveltyKnownShape
		hasBridge := false
		for _, node := range plan.Nodes {
			hasBridge = hasBridge || node.Bridge
		}
		if hasBridge {
			novelty = selection.NoveltyBridged
		} else if primary.ObservedEndToEnd {
			novelty = selection.NoveltyExact
		}
	}
	draft, err := selection.NewDraft(selection.Draft{
		TenantID: run.TenantID, QueryHash: run.QueryHash, RequestContextHash: run.RequestContextHash, RetrievalRunID: run.ID,
		ProjectionEpoch: run.Snapshot.ProjectionEpoch, DocumentSetHash: run.Snapshot.DocumentSetHash, ServingConfigID: run.Snapshot.ServingConfigID,
		PolicyManifestID: run.Snapshot.PolicyManifestID, RankerManifestID: run.Snapshot.RankerManifestID, PlannerManifestID: i.manifest.ID,
		LifecyclePolicyManifestID: run.Snapshot.PolicyManifestID, NoveltyClass: novelty, Plan: plan, ParallelSchedule: schedule,
	})
	if err != nil {
		return retrieval.PlanArtifact{}, fmt.Errorf("build selection draft: %w", err)
	}
	identity := sha256.Sum256([]byte(string(run.TenantID) + "\x00" + run.ID))
	receipt, err := i.repository.Commit(ctx, selection.CommitRequest{TenantID: run.TenantID, IdempotencyIdentityHash: hex.EncodeToString(identity[:]), Draft: draft})
	if err != nil {
		return retrieval.PlanArtifact{}, err
	}
	return artifact(receipt.Record), nil
}

func parallelismFacts(value domain.ProcedureInterface) composition.ParallelismFacts {
	reads := make([]string, 0, len(value.Requirements))
	writes := make([]string, 0, len(value.Provisions))
	for _, requirement := range value.Requirements {
		reads = append(reads, "res_"+requirement.IdentityHash)
	}
	for _, provision := range value.Provisions {
		writes = append(writes, "res_"+provision.IdentityHash)
	}
	return composition.ParallelismFacts{ReadResourceIDs: sortedUnique(reads), WriteResourceIDs: sortedUnique(writes)}
}

func sortedUnique(values []string) []string {
	sort.Strings(values)
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func artifact(record selection.Record) retrieval.PlanArtifact {
	result := retrieval.PlanArtifact{
		InjectionID: record.InjectionID, TaskExecutionID: selection.TaskExecutionID(record.InjectionID), SelectionHash: record.ContentHash, PlanHash: record.Plan.ContentHash,
		CompatibilityGraphID: record.Plan.CompatibilityGraphID, CompatibilityGraphHash: record.Plan.CompatibilityGraphHash,
		CompatibilityMatrixHash: record.Plan.CompatibilityMatrixHash, CandidateSetHash: record.Plan.CandidateSetHash,
		PlannerManifestID: record.PlannerManifestID, PolicyManifestID: record.PolicyManifestID, RankerManifestID: record.RankerManifestID,
		ServingConfigID: record.ServingConfigID, DocumentSetHash: record.DocumentSetHash, TenantID: record.TenantID,
		RetrievalRunID: record.RetrievalRunID, QueryHash: record.QueryHash, RequestContextHash: record.RequestContextHash,
		ProjectionEpoch: record.ProjectionEpoch, NoveltyClass: string(record.NoveltyClass), Complete: record.Plan.Complete,
		LimitCodes: append([]string(nil), record.Plan.LimitCodes...),
	}
	for _, node := range record.Plan.Nodes {
		result.Nodes = append(result.Nodes, retrieval.PlanNode{Ordinal: node.Ordinal, VersionID: node.VersionID, InterfaceHash: node.InterfaceHash, Bridge: node.Bridge})
	}
	for _, dependency := range record.Plan.Dependencies {
		result.Dependencies = append(result.Dependencies, retrieval.PlanDependency{CompatibilityEdgeID: dependency.CompatibilityEdgeID, SourceVersionID: dependency.SourceVersionID, TargetVersionID: dependency.TargetVersionID, SourceProvisionIDs: append([]string(nil), dependency.SourceProvisionIDs...), SatisfiedRequirementIDs: append([]string(nil), dependency.SatisfiedRequirementIDs...)})
	}
	if record.ParallelSchedule != nil {
		for _, group := range record.ParallelSchedule.Groups {
			result.ParallelGroups = append(result.ParallelGroups, retrieval.ParallelGroup{Ordinal: group.Ordinal, NodeVersionIDs: append([]string(nil), group.NodeVersionIDs...)})
		}
	}
	for _, gap := range record.Plan.Gaps {
		result.Gaps = append(result.Gaps, retrieval.PlanGap{VersionID: gap.VersionID, RequirementID: gap.RequirementID, GoalPredicateID: gap.GoalPredicateID, Code: gap.Code})
	}
	return result
}

func riskCost(value string) uint32 {
	switch value {
	case "critical":
		return 4
	case "high":
		return 3
	case "medium":
		return 2
	default:
		return 1
	}
}
