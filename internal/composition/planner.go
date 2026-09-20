package composition

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

var ErrInvalidPlanRequest = errors.New("invalid plan request")

type PlannerCandidate struct {
	VersionID         string
	Interface         domain.ProcedureInterface
	Lifecycle         string
	PlanningFactsHash string
	Seed              bool
	Eligible          bool
	PolicyManifestID  string
	GoalPredicateIDs  []string
	Parallelism       ParallelismFacts
	EvidenceStrength  int64
	ObservedEndToEnd  bool
	RiskCost          uint32
	ToolCost          uint32
}

type PlanRequest struct {
	TenantID                domain.TenantID
	ProjectionEpoch         uint64
	PolicyManifestID        string
	Graph                   CompatibilityGraph
	GoalPredicateIDs        []string
	SatisfiedRequirementIDs []string
	Candidates              []PlannerCandidate
}

type PlanNode struct {
	Ordinal   uint32 `json:"ordinal"`
	VersionID string `json:"procedure_version_id"`
	Bridge    bool   `json:"bridge"`
}

type PlanGap struct {
	VersionID       string `json:"procedure_version_id,omitempty"`
	RequirementID   string `json:"requirement_id,omitempty"`
	GoalPredicateID string `json:"goal_predicate_id,omitempty"`
	Code            string `json:"code"`
}

type Plan struct {
	SchemaVersion                     string          `json:"schema_version"`
	ID                                string          `json:"id,omitempty"`
	TenantID                          domain.TenantID `json:"tenant_id"`
	ProjectionEpoch                   uint64          `json:"projection_epoch"`
	PlannerManifestID                 string          `json:"planner_manifest_id"`
	PolicyManifestID                  string          `json:"policy_manifest_id"`
	CompatibilityGraphID              string          `json:"compatibility_graph_id"`
	CompatibilityGraphHash            string          `json:"compatibility_graph_hash"`
	CompatibilityMatrixHash           string          `json:"compatibility_matrix_hash"`
	CandidateSetHash                  string          `json:"candidate_set_hash"`
	Nodes                             []PlanNode      `json:"nodes"`
	CompatibilityEdgeIDs              []string        `json:"compatibility_edge_ids,omitempty"`
	ExternallySatisfiedRequirementIDs []string        `json:"externally_satisfied_requirement_ids,omitempty"`
	SatisfiedRequirementIDs           []string        `json:"satisfied_requirement_ids,omitempty"`
	ProducedResourceIDs               []string        `json:"produced_resource_ids,omitempty"`
	CoveredGoalPredicateIDs           []string        `json:"covered_goal_predicate_ids,omitempty"`
	RequestedGoalPredicateIDs         []string        `json:"requested_goal_predicate_ids"`
	SeedVersionIDs                    []string        `json:"seed_version_ids"`
	Gaps                              []PlanGap       `json:"gaps,omitempty"`
	LimitCodes                        []string        `json:"limit_codes,omitempty"`
	Complete                          bool            `json:"complete"`
	EvidenceStrength                  int64           `json:"evidence_strength"`
	AllObservedEndToEnd               bool            `json:"all_observed_end_to_end"`
	RiskCost                          uint64          `json:"risk_cost"`
	ToolCost                          uint64          `json:"tool_cost"`
	ContentHash                       string          `json:"content_hash,omitempty"`
	CanonicalJSON                     []byte          `json:"-"`
}

type searchState struct {
	plan    Plan
	nodeSet map[string]struct{}
	hash    string
}

type workBudget struct{ used, maximum uint64 }

func (b *workBudget) consume(units uint64) bool {
	if units > b.maximum-b.used {
		return false
	}
	b.used += units
	return true
}

func PlanProcedures(ctx context.Context, manifest PlannerManifest, request PlanRequest) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	budget := &workBudget{maximum: manifest.MaximumWorkUnits}
	candidates, goals, satisfied, err := validatePlanInputs(ctx, manifest, request, budget)
	if err != nil {
		return Plan{}, err
	}
	byVersion := make(map[string]PlannerCandidate, len(candidates))
	seedVersionIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		byVersion[candidate.VersionID] = candidate
		if candidate.Seed {
			seedVersionIDs = append(seedVersionIDs, candidate.VersionID)
		}
	}
	sort.Strings(seedVersionIDs)
	frontier := make([]searchState, 0)
	seen := make(map[string]struct{})
	limitCodes := make(map[string]struct{})
	var best searchState
	hasBest := false
	for _, candidate := range candidates {
		if !candidate.Seed {
			continue
		}
		if !budget.consume(1) {
			limitCodes["work_budget_exhausted"] = struct{}{}
			break
		}
		state, stateErr := buildSearchState(ctx, manifest, request, byVersion, seedVersionIDs, goals, satisfied, map[string]struct{}{candidate.VersionID: {}}, budget)
		if errors.Is(stateErr, errPlanLimit) {
			continue
		}
		if stateErr != nil {
			return Plan{}, stateErr
		}
		frontier = append(frontier, state)
		seen[state.hash] = struct{}{}
		if !hasBest || betterState(state, best) {
			best, hasBest = state, true
		}
	}
	if !hasBest {
		return Plan{}, ErrInvalidPlanRequest
	}
	sortStates(frontier)
	if len(frontier) > int(manifest.BeamWidth) {
		limitCodes["beam_truncated"] = struct{}{}
		frontier = frontier[:manifest.BeamWidth]
	}
	expansions := uint32(0)
	_, workExhausted := limitCodes["work_budget_exhausted"]
	for depth := uint32(1); depth < manifest.MaximumDepth && len(frontier) > 0 && !workExhausted; depth++ {
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
		next := make([]searchState, 0)
		for _, state := range frontier {
			if expansions >= manifest.MaximumExpansions {
				limitCodes["expansion_budget_exhausted"] = struct{}{}
				break
			}
			if state.plan.Complete || len(state.nodeSet) >= int(manifest.MaximumProcedures) {
				continue
			}
			adjacent, adjacentErr := adjacentVersions(ctx, request.Graph.Edges, state.nodeSet, budget)
			if errors.Is(adjacentErr, errWorkLimit) {
				limitCodes["work_budget_exhausted"] = struct{}{}
				workExhausted = true
				break
			}
			if adjacentErr != nil {
				return Plan{}, adjacentErr
			}
			for _, versionID := range adjacent {
				if expansions >= manifest.MaximumExpansions {
					limitCodes["expansion_budget_exhausted"] = struct{}{}
					break
				}
				expansions++
				nodes := cloneSet(state.nodeSet)
				nodes[versionID] = struct{}{}
				candidateState, buildErr := buildSearchState(ctx, manifest, request, byVersion, seedVersionIDs, goals, satisfied, nodes, budget)
				if errors.Is(buildErr, errWorkLimit) {
					limitCodes["work_budget_exhausted"] = struct{}{}
					workExhausted = true
					break
				}
				if errors.Is(buildErr, errCycle) || errors.Is(buildErr, errPlanLimit) {
					continue
				}
				if buildErr != nil {
					return Plan{}, buildErr
				}
				if _, duplicate := seen[candidateState.hash]; duplicate {
					continue
				}
				seen[candidateState.hash] = struct{}{}
				next = append(next, candidateState)
				if betterState(candidateState, best) {
					best = candidateState
				}
			}
			if workExhausted {
				break
			}
		}
		if workExhausted {
			break
		}
		sortStates(next)
		if len(next) > int(manifest.BeamWidth) {
			limitCodes["beam_truncated"] = struct{}{}
			next = next[:manifest.BeamWidth]
		}
		frontier = next
		if expansions >= manifest.MaximumExpansions {
			limitCodes["expansion_budget_exhausted"] = struct{}{}
			break
		}
	}
	if len(frontier) > 0 && !best.plan.Complete && !workExhausted && expansions < manifest.MaximumExpansions {
		limitCodes["depth_exhausted"] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	best.plan.LimitCodes = sortedSet(limitCodes)
	if len(best.plan.LimitCodes) > 0 {
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
		if err := canonicalizePlan(&best.plan); err != nil {
			return Plan{}, err
		}
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
	}
	return best.plan, nil
}

func BuildPlan(manifest PlannerManifest, request PlanRequest) (Plan, error) {
	return PlanProcedures(context.Background(), manifest, request)
}

var (
	errCycle     = errors.New("composition cycle")
	errPlanLimit = errors.New("composition plan limit")
	errWorkLimit = errors.New("composition work limit")
)

func validatePlanInputs(ctx context.Context, manifest PlannerManifest, request PlanRequest, budget *workBudget) ([]PlannerCandidate, []string, []string, error) {
	if ValidatePlannerManifest(manifest) != nil {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	charge := func(units uint64) error {
		if !budget.consume(units) {
			return errWorkLimit
		}
		return nil
	}
	if err := validateCompatibilityGraph(ctx, request.Graph, charge); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errWorkLimit) {
			return nil, nil, nil, err
		}
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	if request.TenantID == "" || request.ProjectionEpoch == 0 ||
		request.Graph.TenantID != request.TenantID || request.Graph.ProjectionEpoch != request.ProjectionEpoch || request.Graph.PlannerManifestID != manifest.ID || request.Graph.PolicyManifestID != request.PolicyManifestID || request.PolicyManifestID == "" || len(request.Candidates) == 0 {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	if len(request.Candidates) > int(manifest.MaximumCandidates) || len(request.GoalPredicateIDs) > int(manifest.MaximumGoalPredicates) || len(request.SatisfiedRequirementIDs) > int(manifest.MaximumSatisfiedRequirements) {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	goals, ok := normalizedIDs(request.GoalPredicateIDs, false)
	if !ok || len(goals) == 0 {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	satisfied, ok := normalizedIDs(request.SatisfiedRequirementIDs, true)
	if !ok {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	candidates := append([]PlannerCandidate(nil), request.Candidates...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].VersionID < candidates[j].VersionID })
	byVersion := make(map[string]struct{}, len(candidates))
	interfaceByVersion := make(map[string]string, len(candidates))
	graphCandidates := make([]Candidate, 0, len(candidates))
	hasSeed := false
	totalCandidateGoals := uint64(0)
	totalParallelEntries := uint64(0)
	for index := range candidates {
		candidate := &candidates[index]
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, nil, err
			}
		}
		goalCount := uint64(len(candidate.GoalPredicateIDs))
		if goalCount > uint64(manifest.MaximumGoalPredicates) || totalCandidateGoals+goalCount > uint64(manifest.MaximumTotalCandidateGoals) {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
		totalCandidateGoals += goalCount
		parallelEntries := parallelEntryCount(candidate.Parallelism)
		if parallelEntries > uint64(manifest.MaximumParallelEntries) || totalParallelEntries+parallelEntries > uint64(manifest.MaximumTotalParallelEntries) {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
		totalParallelEntries += parallelEntries
		// Normalization and planning-facts hashing each traverse candidate goals.
		if !budget.consume((goalCount+parallelEntries)*2 + 1) {
			return nil, nil, nil, errWorkLimit
		}
		var goalsOK, parallelOK bool
		candidate.GoalPredicateIDs, goalsOK = normalizedIDs(candidate.GoalPredicateIDs, true)
		candidate.Parallelism, parallelOK = normalizedParallelism(candidate.Parallelism)
		factsHash, factsErr := BuildPlanningFactsHash(PlanningFacts{ProcedureVersionID: candidate.VersionID, InterfaceHash: candidate.Interface.ContentHash, Lifecycle: candidate.Lifecycle, PolicyManifestID: candidate.PolicyManifestID, GoalPredicateIDs: candidate.GoalPredicateIDs, Parallelism: candidate.Parallelism, EvidenceStrength: candidate.EvidenceStrength, ObservedEndToEnd: candidate.ObservedEndToEnd, RiskCost: candidate.RiskCost, ToolCost: candidate.ToolCost})
		if !goalsOK || !parallelOK || candidate.VersionID == "" || strings.ContainsRune(candidate.VersionID, '\x00') || !candidate.Eligible || candidate.PolicyManifestID != request.PolicyManifestID || retrieval.ValidateProcedureInterface(candidate.Interface) != nil || candidate.EvidenceStrength < 0 || candidate.ToolCost == 0 || factsErr != nil || factsHash != candidate.PlanningFactsHash {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
		if _, duplicate := byVersion[candidate.VersionID]; duplicate {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
		byVersion[candidate.VersionID] = struct{}{}
		interfaceByVersion[candidate.VersionID] = candidate.Interface.ContentHash
		graphCandidates = append(graphCandidates, Candidate{TenantID: request.TenantID, ProcedureVersionID: candidate.VersionID, Lifecycle: candidate.Lifecycle, Interface: candidate.Interface, PlanningFactsHash: candidate.PlanningFactsHash})
		hasSeed = hasSeed || candidate.Seed
	}
	if !hasSeed {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	candidateSetHash, hashErr := buildCandidateSetHash(graphCandidates)
	if hashErr != nil || candidateSetHash != request.Graph.CandidateSetHash {
		return nil, nil, nil, ErrInvalidPlanRequest
	}
	for index, edge := range request.Graph.Edges {
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, nil, err
			}
		}
		if !budget.consume(1) {
			return nil, nil, nil, errWorkLimit
		}
		if _, ok = byVersion[edge.SourceVersionID]; !ok {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
		if _, ok = byVersion[edge.TargetVersionID]; !ok {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
		if interfaceByVersion[edge.SourceVersionID] != edge.SourceInterfaceHash || interfaceByVersion[edge.TargetVersionID] != edge.TargetInterfaceHash {
			return nil, nil, nil, ErrInvalidPlanRequest
		}
	}
	return candidates, goals, satisfied, nil
}

func buildSearchState(ctx context.Context, manifest PlannerManifest, request PlanRequest, byVersion map[string]PlannerCandidate, seedVersionIDs, goals, externallySatisfied []string, nodeSet map[string]struct{}, budget *workBudget) (searchState, error) {
	if err := ctx.Err(); err != nil {
		return searchState{}, err
	}
	edges, edgeErr := selectedEdges(ctx, request.Graph.Edges, nodeSet, budget)
	if edgeErr != nil {
		return searchState{}, edgeErr
	}
	ordered, err := topologicalOrder(nodeSet, edges)
	if err != nil {
		return searchState{}, err
	}
	satisfiedSet := sliceSet(externallySatisfied)
	edgeIDs := make([]string, len(edges))
	unobservedEdges := int64(0)
	for index, edge := range edges {
		edgeIDs[index] = edge.ID
		for _, requirementID := range edge.SatisfiedRequirementIDs {
			satisfiedSet[requirementID] = struct{}{}
		}
		if !byVersion[edge.SourceVersionID].ObservedEndToEnd || !byVersion[edge.TargetVersionID].ObservedEndToEnd {
			unobservedEdges++
		}
	}
	sort.Strings(edgeIDs)
	coveredSet, producedSet := make(map[string]struct{}), make(map[string]struct{})
	goalSet := sliceSet(goals)
	plan := Plan{
		SchemaVersion: "composition-plan.v1", TenantID: request.TenantID, ProjectionEpoch: request.ProjectionEpoch,
		PlannerManifestID: manifest.ID, PolicyManifestID: request.PolicyManifestID,
		CompatibilityGraphID: request.Graph.ID, CompatibilityGraphHash: request.Graph.ContentHash,
		CompatibilityMatrixHash: request.Graph.MatrixHash, CandidateSetHash: request.Graph.CandidateSetHash,
		CompatibilityEdgeIDs: edgeIDs, ExternallySatisfiedRequirementIDs: append([]string(nil), externallySatisfied...),
		RequestedGoalPredicateIDs: append([]string(nil), goals...), SeedVersionIDs: append([]string(nil), seedVersionIDs...),
		AllObservedEndToEnd: true, EvidenceStrength: math.MaxInt64,
	}
	bridges := uint32(0)
	for ordinal, versionID := range ordered {
		candidate := byVersion[versionID]
		if !candidate.Seed {
			bridges++
		}
		plan.Nodes = append(plan.Nodes, PlanNode{Ordinal: uint32(ordinal), VersionID: versionID, Bridge: !candidate.Seed})
		plan.RiskCost += uint64(candidate.RiskCost)
		plan.ToolCost += uint64(candidate.ToolCost)
		if candidate.EvidenceStrength < plan.EvidenceStrength {
			plan.EvidenceStrength = candidate.EvidenceStrength
		}
		plan.AllObservedEndToEnd = plan.AllObservedEndToEnd && candidate.ObservedEndToEnd
		for _, goal := range candidate.GoalPredicateIDs {
			if _, wanted := goalSet[goal]; wanted {
				coveredSet[goal] = struct{}{}
			}
		}
		for _, provision := range candidate.Interface.Provisions {
			producedSet[provision.ID] = struct{}{}
		}
		for _, requirement := range candidate.Interface.Requirements {
			if _, met := satisfiedSet[requirement.ID]; !met {
				plan.Gaps = append(plan.Gaps, PlanGap{VersionID: versionID, RequirementID: requirement.ID, Code: "unsatisfied_requirement"})
			}
		}
	}
	if bridges > manifest.MaximumBridges || plan.ToolCost > uint64(manifest.MaximumTotalToolCost) {
		return searchState{}, errPlanLimit
	}
	penalty, ok := checkedPlannerMul(int64(bridges), manifest.NoveltyPenalty)
	if !ok {
		return searchState{}, ErrInvalidPlanRequest
	}
	edgePenalty, ok := checkedPlannerMul(unobservedEdges, manifest.UnobservedEdgePenalty)
	if !ok || plan.EvidenceStrength < penalty || plan.EvidenceStrength-penalty < edgePenalty {
		plan.EvidenceStrength = 0
	} else {
		plan.EvidenceStrength -= penalty + edgePenalty
	}
	plan.SatisfiedRequirementIDs = sortedSet(satisfiedSet)
	plan.ProducedResourceIDs = sortedSet(producedSet)
	plan.CoveredGoalPredicateIDs = sortedSet(coveredSet)
	for _, goal := range goals {
		if _, covered := coveredSet[goal]; !covered {
			plan.Gaps = append(plan.Gaps, PlanGap{GoalPredicateID: goal, Code: "uncovered_goal"})
		}
	}
	sort.Slice(plan.Gaps, func(i, j int) bool {
		left := plan.Gaps[i].Code + "\x00" + plan.Gaps[i].VersionID + "\x00" + plan.Gaps[i].RequirementID + "\x00" + plan.Gaps[i].GoalPredicateID
		right := plan.Gaps[j].Code + "\x00" + plan.Gaps[j].VersionID + "\x00" + plan.Gaps[j].RequirementID + "\x00" + plan.Gaps[j].GoalPredicateID
		return left < right
	})
	plan.Complete = len(plan.Gaps) == 0
	if err := canonicalizePlan(&plan); err != nil {
		return searchState{}, err
	}
	if err := ctx.Err(); err != nil {
		return searchState{}, err
	}
	return searchState{plan: plan, nodeSet: cloneSet(nodeSet), hash: plan.ContentHash}, nil
}

func selectedEdges(ctx context.Context, edges []CompatibilityEdge, nodes map[string]struct{}, budget *workBudget) ([]CompatibilityEdge, error) {
	if len(nodes) < 2 {
		return nil, nil
	}
	result := make([]CompatibilityEdge, 0)
	for index, edge := range edges {
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if !budget.consume(1) {
			return nil, errWorkLimit
		}
		_, source := nodes[edge.SourceVersionID]
		_, target := nodes[edge.TargetVersionID]
		if source && target {
			result = append(result, edge)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func topologicalOrder(nodes map[string]struct{}, edges []CompatibilityEdge) ([]string, error) {
	indegree := make(map[string]int, len(nodes))
	adjacency := make(map[string][]string, len(nodes))
	for node := range nodes {
		indegree[node] = 0
	}
	for _, edge := range edges {
		indegree[edge.TargetVersionID]++
		adjacency[edge.SourceVersionID] = append(adjacency[edge.SourceVersionID], edge.TargetVersionID)
	}
	ready := make([]string, 0)
	for node, degree := range indegree {
		if degree == 0 {
			ready = append(ready, node)
		}
	}
	sort.Strings(ready)
	ordered := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		node := ready[0]
		ready = ready[1:]
		ordered = append(ordered, node)
		for _, target := range adjacency[node] {
			indegree[target]--
			if indegree[target] == 0 {
				ready = append(ready, target)
				sort.Strings(ready)
			}
		}
	}
	if len(ordered) != len(nodes) {
		return nil, errCycle
	}
	return ordered, nil
}

func adjacentVersions(ctx context.Context, edges []CompatibilityEdge, nodes map[string]struct{}, budget *workBudget) ([]string, error) {
	result := make(map[string]struct{})
	for index, edge := range edges {
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if !budget.consume(1) {
			return nil, errWorkLimit
		}
		_, source := nodes[edge.SourceVersionID]
		_, target := nodes[edge.TargetVersionID]
		if source != target {
			if source {
				result[edge.TargetVersionID] = struct{}{}
			} else {
				result[edge.SourceVersionID] = struct{}{}
			}
		}
	}
	return sortedSet(result), nil
}

func canonicalizePlan(plan *Plan) error {
	plan.ID, plan.ContentHash, plan.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(*plan)
	if err != nil {
		return err
	}
	plan.ID, plan.ContentHash, plan.CanonicalJSON = "plan_"+hash, hash, canonicalJSON
	return nil
}

func betterState(left, right searchState) bool {
	if left.plan.Complete != right.plan.Complete {
		return left.plan.Complete
	}
	if len(left.plan.CoveredGoalPredicateIDs) != len(right.plan.CoveredGoalPredicateIDs) {
		return len(left.plan.CoveredGoalPredicateIDs) > len(right.plan.CoveredGoalPredicateIDs)
	}
	if left.plan.EvidenceStrength != right.plan.EvidenceStrength {
		return left.plan.EvidenceStrength > right.plan.EvidenceStrength
	}
	if left.plan.AllObservedEndToEnd != right.plan.AllObservedEndToEnd {
		return left.plan.AllObservedEndToEnd
	}
	if len(left.plan.SatisfiedRequirementIDs) != len(right.plan.SatisfiedRequirementIDs) {
		return len(left.plan.SatisfiedRequirementIDs) > len(right.plan.SatisfiedRequirementIDs)
	}
	if left.plan.RiskCost != right.plan.RiskCost {
		return left.plan.RiskCost < right.plan.RiskCost
	}
	if left.plan.ToolCost != right.plan.ToolCost {
		return left.plan.ToolCost < right.plan.ToolCost
	}
	if len(left.plan.Nodes) != len(right.plan.Nodes) {
		return len(left.plan.Nodes) < len(right.plan.Nodes)
	}
	return left.hash < right.hash
}

func sortStates(values []searchState) {
	sort.Slice(values, func(i, j int) bool { return betterState(values[i], values[j]) })
}

func normalizedIDs(source []string, allowEmpty bool) ([]string, bool) {
	result := append([]string(nil), source...)
	for index := range result {
		result[index] = strings.TrimSpace(result[index])
		if len(result[index]) > 1_024 || !safeIdentity(result[index]) {
			return nil, false
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index-1] == result[index] {
			return nil, false
		}
	}
	return result, allowEmpty || len(result) > 0
}

func sliceSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source)+1)
	for value := range source {
		result[value] = struct{}{}
	}
	return result
}

func checkedPlannerMul(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	if left > math.MaxInt64/right {
		return 0, false
	}
	return left * right, true
}

func ValidatePlan(plan Plan) error {
	if plan.SchemaVersion != "composition-plan.v1" || plan.ID != "plan_"+plan.ContentHash || !sha256Pattern.MatchString(plan.ContentHash) {
		return ErrInvalidPlanRequest
	}
	copyOfPlan := plan
	copyOfPlan.ID, copyOfPlan.ContentHash, copyOfPlan.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfPlan)
	if err != nil || hash != plan.ContentHash || !bytes.Equal(encoded, plan.CanonicalJSON) {
		return ErrInvalidPlanRequest
	}
	if plan.TenantID == "" || plan.ProjectionEpoch == 0 || plan.PlannerManifestID == "" || plan.PolicyManifestID == "" || plan.CompatibilityGraphID != "cgraph_"+plan.CompatibilityGraphHash || !sha256Pattern.MatchString(plan.CompatibilityGraphHash) || !sha256Pattern.MatchString(plan.CompatibilityMatrixHash) || !sha256Pattern.MatchString(plan.CandidateSetHash) || len(plan.Nodes) == 0 || len(plan.RequestedGoalPredicateIDs) == 0 || len(plan.SeedVersionIDs) == 0 || plan.ToolCost == 0 || plan.EvidenceStrength < 0 ||
		!strictSorted(plan.RequestedGoalPredicateIDs) || !strictSorted(plan.SeedVersionIDs) || !strictSorted(plan.ExternallySatisfiedRequirementIDs) || !strictSorted(plan.SatisfiedRequirementIDs) || !strictSorted(plan.ProducedResourceIDs) || !strictSorted(plan.CoveredGoalPredicateIDs) ||
		len(plan.CompatibilityEdgeIDs) > 0 && !strictPrefixed(plan.CompatibilityEdgeIDs, "cedge_") || !strictSorted(plan.LimitCodes) || plan.Complete != (len(plan.Gaps) == 0) {
		return ErrInvalidPlanRequest
	}
	requested := sliceSet(plan.RequestedGoalPredicateIDs)
	seeds := sliceSet(plan.SeedVersionIDs)
	seenNodes := make(map[string]struct{}, len(plan.Nodes))
	for ordinal, node := range plan.Nodes {
		_, isSeed := seeds[node.VersionID]
		if node.Ordinal != uint32(ordinal) || node.VersionID == "" || node.Bridge == isSeed {
			return ErrInvalidPlanRequest
		}
		if _, duplicate := seenNodes[node.VersionID]; duplicate {
			return ErrInvalidPlanRequest
		}
		seenNodes[node.VersionID] = struct{}{}
	}
	for _, goal := range plan.CoveredGoalPredicateIDs {
		if _, ok := requested[goal]; !ok {
			return ErrInvalidPlanRequest
		}
	}
	for _, code := range plan.LimitCodes {
		if code != "beam_truncated" && code != "depth_exhausted" && code != "expansion_budget_exhausted" && code != "work_budget_exhausted" {
			return ErrInvalidPlanRequest
		}
	}
	for index, gap := range plan.Gaps {
		switch gap.Code {
		case "unsatisfied_requirement":
			if gap.VersionID == "" || gap.RequirementID == "" || gap.GoalPredicateID != "" {
				return ErrInvalidPlanRequest
			}
		case "uncovered_goal":
			if gap.VersionID != "" || gap.RequirementID != "" || gap.GoalPredicateID == "" {
				return ErrInvalidPlanRequest
			}
		default:
			return ErrInvalidPlanRequest
		}
		if index > 0 && gapKey(plan.Gaps[index-1]) >= gapKey(gap) {
			return ErrInvalidPlanRequest
		}
	}
	return nil
}

func gapKey(gap PlanGap) string {
	return gap.Code + "\x00" + gap.VersionID + "\x00" + gap.RequirementID + "\x00" + gap.GoalPredicateID
}
