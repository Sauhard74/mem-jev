package composition

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var ErrInvalidParallelRequest = errors.New("invalid parallel schedule request")

const maximumParallelPlanEdges = 32 * 31 / 2

type ParallelGroup struct {
	SchemaVersion  string   `json:"schema_version"`
	ID             string   `json:"id,omitempty"`
	Ordinal        uint32   `json:"ordinal"`
	PlanID         string   `json:"plan_id"`
	NodeVersionIDs []string `json:"node_version_ids"`
	ContentHash    string   `json:"content_hash,omitempty"`
	CanonicalJSON  []byte   `json:"-"`
}

type ParallelSchedule struct {
	SchemaVersion          string          `json:"schema_version"`
	ID                     string          `json:"id,omitempty"`
	TenantID               domain.TenantID `json:"tenant_id"`
	ProjectionEpoch        uint64          `json:"projection_epoch"`
	PlanID                 string          `json:"plan_id"`
	PlanHash               string          `json:"plan_hash"`
	CompatibilityGraphID   string          `json:"compatibility_graph_id"`
	CompatibilityGraphHash string          `json:"compatibility_graph_hash"`
	PlannerManifestID      string          `json:"planner_manifest_id"`
	PolicyManifestID       string          `json:"policy_manifest_id"`
	Groups                 []ParallelGroup `json:"groups"`
	ContentHash            string          `json:"content_hash,omitempty"`
	CanonicalJSON          []byte          `json:"-"`
}

func DeriveParallelSchedule(ctx context.Context, manifest PlannerManifest, plan Plan, graph CompatibilityGraph, candidates []PlannerCandidate) (ParallelSchedule, error) {
	if err := ctx.Err(); err != nil {
		return ParallelSchedule{}, err
	}
	budget := &workBudget{maximum: manifest.MaximumWorkUnits}
	factsByVersion, selectedEdges, err := validateParallelInputs(ctx, manifest, plan, graph, candidates, budget)
	if err != nil {
		return ParallelSchedule{}, err
	}
	predecessors := make(map[string][]string, len(plan.Nodes))
	position := make(map[string]int, len(plan.Nodes))
	for index, node := range plan.Nodes {
		position[node.VersionID] = index
	}
	for _, edge := range selectedEdges {
		if position[edge.SourceVersionID] >= position[edge.TargetVersionID] {
			return ParallelSchedule{}, ErrInvalidParallelRequest
		}
		predecessors[edge.TargetVersionID] = append(predecessors[edge.TargetVersionID], edge.SourceVersionID)
	}

	groups := make([][]string, 0, len(plan.Nodes))
	groupByVersion := make(map[string]int, len(plan.Nodes))
	for index, node := range plan.Nodes {
		if index%32 == 0 {
			if err = ctx.Err(); err != nil {
				return ParallelSchedule{}, err
			}
		}
		minimumGroup := 0
		for _, predecessor := range predecessors[node.VersionID] {
			if candidate := groupByVersion[predecessor] + 1; candidate > minimumGroup {
				minimumGroup = candidate
			}
		}
		assigned := false
		for groupIndex := minimumGroup; groupIndex < len(groups); groupIndex++ {
			accepts, acceptsErr := groupAccepts(ctx, groups[groupIndex], node.VersionID, factsByVersion, budget)
			if acceptsErr != nil {
				return ParallelSchedule{}, acceptsErr
			}
			if accepts {
				groups[groupIndex] = append(groups[groupIndex], node.VersionID)
				groupByVersion[node.VersionID] = groupIndex
				assigned = true
				break
			}
		}
		if !assigned {
			groupByVersion[node.VersionID] = len(groups)
			groups = append(groups, []string{node.VersionID})
		}
	}

	schedule := ParallelSchedule{
		SchemaVersion: "parallel-schedule.v1", TenantID: plan.TenantID, ProjectionEpoch: plan.ProjectionEpoch, PlanID: plan.ID, PlanHash: plan.ContentHash,
		CompatibilityGraphID: graph.ID, CompatibilityGraphHash: graph.ContentHash,
		PlannerManifestID: manifest.ID, PolicyManifestID: plan.PolicyManifestID,
	}
	for index, versions := range groups {
		group := ParallelGroup{SchemaVersion: "parallel-group.v1", Ordinal: uint32(index), PlanID: plan.ID, NodeVersionIDs: append([]string(nil), versions...)}
		if err = canonicalizeParallelGroup(&group); err != nil {
			return ParallelSchedule{}, err
		}
		schedule.Groups = append(schedule.Groups, group)
	}
	if err = canonicalizeParallelSchedule(&schedule); err != nil {
		return ParallelSchedule{}, err
	}
	return schedule, nil
}

func validateParallelInputs(ctx context.Context, manifest PlannerManifest, plan Plan, graph CompatibilityGraph, candidates []PlannerCandidate, budget *workBudget) (map[string]ParallelismFacts, []CompatibilityEdge, error) {
	if ValidatePlannerManifest(manifest) != nil || len(plan.Nodes) > int(manifest.MaximumProcedures) || len(plan.RequestedGoalPredicateIDs) > int(manifest.MaximumGoalPredicates) || len(plan.ExternallySatisfiedRequirementIDs) > int(manifest.MaximumSatisfiedRequirements) || len(plan.SatisfiedRequirementIDs) > int(manifest.MaximumSatisfiedRequirements)+32*1_024 || len(plan.ProducedResourceIDs) > 32*1_024 || len(plan.CoveredGoalPredicateIDs) > int(manifest.MaximumGoalPredicates) || len(plan.SeedVersionIDs) > int(manifest.MaximumCandidates) || len(plan.CompatibilityEdgeIDs) > maximumParallelPlanEdges || len(plan.Gaps) != 0 || len(plan.CanonicalJSON) > 16<<20 || ValidatePlan(plan) != nil || !plan.Complete ||
		plan.PlannerManifestID != manifest.ID || plan.CompatibilityGraphID != graph.ID || plan.CompatibilityGraphHash != graph.ContentHash ||
		plan.CandidateSetHash != graph.CandidateSetHash || plan.CompatibilityMatrixHash != graph.MatrixHash || plan.TenantID != graph.TenantID || plan.ProjectionEpoch != graph.ProjectionEpoch || graph.PlannerManifestID != manifest.ID || plan.PolicyManifestID != graph.PolicyManifestID || len(candidates) == 0 || len(candidates) > int(manifest.MaximumCandidates) {
		return nil, nil, ErrInvalidParallelRequest
	}
	charge := func(units uint64) error {
		if !budget.consume(units) {
			return errWorkLimit
		}
		return nil
	}
	if err := validateCompatibilityGraph(ctx, graph, charge); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errWorkLimit) {
			return nil, nil, err
		}
		return nil, nil, ErrInvalidParallelRequest
	}
	ordered := append([]PlannerCandidate(nil), candidates...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].VersionID < ordered[j].VersionID })
	factsByVersion := make(map[string]ParallelismFacts, len(ordered))
	graphCandidates := make([]Candidate, 0, len(ordered))
	var totalEntries, totalGoals uint64
	for index := range ordered {
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		candidate := &ordered[index]
		entries := parallelEntryCount(candidate.Parallelism)
		goalsCount := uint64(len(candidate.GoalPredicateIDs))
		if entries > uint64(manifest.MaximumParallelEntries) || totalEntries+entries > uint64(manifest.MaximumTotalParallelEntries) || goalsCount > uint64(manifest.MaximumGoalPredicates) || totalGoals+goalsCount > uint64(manifest.MaximumTotalCandidateGoals) {
			return nil, nil, ErrInvalidParallelRequest
		}
		totalEntries += entries
		totalGoals += goalsCount
		if !budget.consume((entries+goalsCount)*2 + 1) {
			return nil, nil, errWorkLimit
		}
		goals, ok := normalizedIDs(candidate.GoalPredicateIDs, true)
		parallelism, parallelOK := normalizedParallelism(candidate.Parallelism)
		factsHash, hashErr := BuildPlanningFactsHash(PlanningFacts{ProcedureVersionID: candidate.VersionID, InterfaceHash: candidate.Interface.ContentHash, Lifecycle: candidate.Lifecycle, PolicyManifestID: candidate.PolicyManifestID, GoalPredicateIDs: goals, Parallelism: parallelism, EvidenceStrength: candidate.EvidenceStrength, ObservedEndToEnd: candidate.ObservedEndToEnd, RiskCost: candidate.RiskCost, ToolCost: candidate.ToolCost})
		if !ok || !parallelOK || hashErr != nil || factsHash != candidate.PlanningFactsHash || candidate.PolicyManifestID != plan.PolicyManifestID {
			return nil, nil, ErrInvalidParallelRequest
		}
		if _, duplicate := factsByVersion[candidate.VersionID]; duplicate {
			return nil, nil, ErrInvalidParallelRequest
		}
		factsByVersion[candidate.VersionID] = parallelism
		graphCandidates = append(graphCandidates, Candidate{TenantID: plan.TenantID, ProcedureVersionID: candidate.VersionID, Lifecycle: candidate.Lifecycle, Interface: candidate.Interface, PlanningFactsHash: candidate.PlanningFactsHash})
	}
	candidateSetHash, err := buildCandidateSetHash(graphCandidates)
	if err != nil || candidateSetHash != graph.CandidateSetHash {
		return nil, nil, ErrInvalidParallelRequest
	}
	selected := make(map[string]struct{}, len(plan.Nodes))
	for _, node := range plan.Nodes {
		if _, ok := factsByVersion[node.VersionID]; !ok {
			return nil, nil, ErrInvalidParallelRequest
		}
		selected[node.VersionID] = struct{}{}
	}
	edgeIDs := make(map[string]struct{}, len(plan.CompatibilityEdgeIDs))
	for _, id := range plan.CompatibilityEdgeIDs {
		edgeIDs[id] = struct{}{}
	}
	selectedEdges := make([]CompatibilityEdge, 0, len(edgeIDs))
	for index, edge := range graph.Edges {
		if index%256 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		if !budget.consume(1) {
			return nil, nil, errWorkLimit
		}
		_, source := selected[edge.SourceVersionID]
		_, target := selected[edge.TargetVersionID]
		_, recorded := edgeIDs[edge.ID]
		if (source && target) != recorded {
			return nil, nil, ErrInvalidParallelRequest
		}
		if recorded {
			selectedEdges = append(selectedEdges, edge)
			delete(edgeIDs, edge.ID)
		}
	}
	if len(edgeIDs) != 0 {
		return nil, nil, ErrInvalidParallelRequest
	}
	return factsByVersion, selectedEdges, nil
}

func groupAccepts(ctx context.Context, group []string, versionID string, facts map[string]ParallelismFacts, budget *workBudget) (bool, error) {
	for _, member := range group {
		conflict, err := parallelConflict(ctx, facts[member], facts[versionID], budget)
		if err != nil {
			return false, err
		}
		if conflict {
			return false, nil
		}
	}
	return true, nil
}

func parallelConflict(ctx context.Context, left, right ParallelismFacts, budget *workBudget) (bool, error) {
	if !budget.consume(1) {
		return false, errWorkLimit
	}
	if left.CompensationBoundary || right.CompensationBoundary || left.PolicySerial || right.PolicySerial {
		return true, nil
	}
	pairs := [][2][]string{
		{left.PolicyMutexKeys, right.PolicyMutexKeys},
		{left.WriteResourceIDs, right.ReadResourceIDs}, {left.WriteResourceIDs, right.WriteResourceIDs}, {right.WriteResourceIDs, left.ReadResourceIDs},
		{left.ExclusiveResourceIDs, right.ReadResourceIDs}, {left.ExclusiveResourceIDs, right.WriteResourceIDs}, {left.ExclusiveResourceIDs, right.ExclusiveResourceIDs},
		{right.ExclusiveResourceIDs, left.ReadResourceIDs}, {right.ExclusiveResourceIDs, left.WriteResourceIDs},
	}
	for _, pair := range pairs {
		conflict, err := intersectsBounded(ctx, pair[0], pair[1], budget)
		if err != nil || conflict {
			return conflict, err
		}
	}
	return false, nil
}

func intersectsBounded(ctx context.Context, left, right []string, budget *workBudget) (bool, error) {
	comparisons := 0
	for leftIndex, rightIndex := 0, 0; leftIndex < len(left) && rightIndex < len(right); {
		if comparisons%256 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		if !budget.consume(1) {
			return false, errWorkLimit
		}
		comparisons++
		switch {
		case left[leftIndex] == right[rightIndex]:
			return true, nil
		case left[leftIndex] < right[rightIndex]:
			leftIndex++
		default:
			rightIndex++
		}
	}
	return false, nil
}

func canonicalizeParallelGroup(group *ParallelGroup) error {
	group.ID, group.ContentHash, group.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(*group)
	if err != nil {
		return err
	}
	group.ID, group.ContentHash, group.CanonicalJSON = "pgroup_"+hash, hash, encoded
	return nil
}

func canonicalizeParallelSchedule(schedule *ParallelSchedule) error {
	schedule.ID, schedule.ContentHash, schedule.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(*schedule)
	if err != nil {
		return err
	}
	schedule.ID, schedule.ContentHash, schedule.CanonicalJSON = "psched_"+hash, hash, encoded
	return nil
}

func HydrateParallelSchedule(schedule ParallelSchedule) (ParallelSchedule, error) {
	for index := range schedule.Groups {
		group := &schedule.Groups[index]
		expectedID, expectedHash := group.ID, group.ContentHash
		group.ID, group.ContentHash, group.CanonicalJSON = "", "", nil
		encoded, hash, err := canonical.MarshalAndHash(*group)
		if err != nil || expectedID != "pgroup_"+hash || expectedHash != hash {
			return ParallelSchedule{}, ErrInvalidParallelRequest
		}
		group.ID, group.ContentHash, group.CanonicalJSON = expectedID, expectedHash, encoded
	}
	expectedID, expectedHash := schedule.ID, schedule.ContentHash
	schedule.ID, schedule.ContentHash, schedule.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(schedule)
	if err != nil || expectedID != "psched_"+hash || expectedHash != hash {
		return ParallelSchedule{}, ErrInvalidParallelRequest
	}
	schedule.ID, schedule.ContentHash, schedule.CanonicalJSON = expectedID, expectedHash, encoded
	if ValidateParallelSchedule(schedule) != nil {
		return ParallelSchedule{}, ErrInvalidParallelRequest
	}
	return schedule, nil
}

func ValidateParallelSchedule(schedule ParallelSchedule) error {
	if schedule.SchemaVersion != "parallel-schedule.v1" || schedule.ID != "psched_"+schedule.ContentHash || !sha256Pattern.MatchString(schedule.ContentHash) || !safeIdentity(string(schedule.TenantID)) || schedule.ProjectionEpoch == 0 || schedule.PlanID != "plan_"+schedule.PlanHash || !sha256Pattern.MatchString(schedule.PlanHash) || schedule.CompatibilityGraphID != "cgraph_"+schedule.CompatibilityGraphHash || !sha256Pattern.MatchString(schedule.CompatibilityGraphHash) || !safeIdentity(schedule.PlannerManifestID) || !safeIdentity(schedule.PolicyManifestID) || len(schedule.Groups) == 0 || len(schedule.Groups) > 32 || len(schedule.CanonicalJSON) > 16<<20 {
		return ErrInvalidParallelRequest
	}
	totalNodes := 0
	for index, group := range schedule.Groups {
		if group.SchemaVersion != "parallel-group.v1" || group.Ordinal != uint32(index) || group.PlanID != schedule.PlanID || group.ID != "pgroup_"+group.ContentHash || !sha256Pattern.MatchString(group.ContentHash) || len(group.CanonicalJSON) > 1<<20 || len(group.NodeVersionIDs) == 0 || len(group.NodeVersionIDs) > 32 || totalNodes > 32-len(group.NodeVersionIDs) {
			return ErrInvalidParallelRequest
		}
		totalNodes += len(group.NodeVersionIDs)
		for _, versionID := range group.NodeVersionIDs {
			if !safeIdentity(versionID) {
				return ErrInvalidParallelRequest
			}
		}
	}
	copyOfSchedule := schedule
	copyOfSchedule.ID, copyOfSchedule.ContentHash, copyOfSchedule.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfSchedule)
	if err != nil || hash != schedule.ContentHash || !bytes.Equal(encoded, schedule.CanonicalJSON) {
		return ErrInvalidParallelRequest
	}
	seen := make(map[string]struct{})
	for _, group := range schedule.Groups {
		copyOfGroup := group
		copyOfGroup.ID, copyOfGroup.ContentHash, copyOfGroup.CanonicalJSON = "", "", nil
		groupJSON, groupHash, groupErr := canonical.MarshalAndHash(copyOfGroup)
		if groupErr != nil || groupHash != group.ContentHash || !bytes.Equal(groupJSON, group.CanonicalJSON) {
			return ErrInvalidParallelRequest
		}
		for _, versionID := range group.NodeVersionIDs {
			if _, duplicate := seen[versionID]; duplicate {
				return ErrInvalidParallelRequest
			}
			seen[versionID] = struct{}{}
		}
	}
	return nil
}
