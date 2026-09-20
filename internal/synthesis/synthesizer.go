package synthesis

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var ErrInvalidSynthesisRequest = errors.New("invalid synthesis request")

type Status string

const (
	StatusSynthesized Status = "synthesized"
	StatusAbstained   Status = "abstained"
)

type GoalRequirement struct {
	PredicateID   string
	VerifierNodes []domain.EventID
}

type FailedBranch struct {
	FailurePredicateID string
	EventIDs           []domain.EventID
	Scope              domain.CompatibilityScope
}

type Versions struct {
	Sanitizer    string `json:"sanitizer"`
	Registry     string `json:"registry"`
	Policy       string `json:"policy"`
	GraphBuilder string `json:"graph_builder"`
	Synthesizer  string `json:"synthesizer"`
}

func (v Versions) valid() bool {
	return v.Sanitizer != "" && v.Registry != "" && v.Policy != "" && v.GraphBuilder != "" && v.Synthesizer != ""
}

type Request struct {
	Graph           Graph
	OutcomeState    domain.OutcomeState
	Goals           []GoalRequirement
	RequiredEffects []domain.EventID
	IrrelevantNodes []domain.EventID
	FailedBranches  []FailedBranch
	Versions        Versions
}

type Result struct {
	SchemaVersion    string                 `json:"schema_version"`
	Status           Status                 `json:"status"`
	AbstentionCode   string                 `json:"abstention_code,omitempty"`
	TraceID          domain.TraceID         `json:"trace_id"`
	CausalGraphHash  string                 `json:"causal_graph_hash"`
	Steps            []domain.ProcedureStep `json:"steps,omitempty"`
	Edges            []Edge                 `json:"edges,omitempty"`
	NegativePaths    []domain.NegativePath  `json:"negative_paths,omitempty"`
	GoalPredicates   []string               `json:"goal_predicates,omitempty"`
	Versions         Versions               `json:"versions"`
	ObservedEndToEnd bool                   `json:"observed_end_to_end"`
	Hash             string                 `json:"-"`
}

func Synthesize(request Request) (Result, error) {
	base := Result{
		SchemaVersion: "synthesis.v2", TraceID: request.Graph.TraceID, CausalGraphHash: request.Graph.Hash,
		Versions: request.Versions, ObservedEndToEnd: true,
	}
	if request.Graph.TraceID == "" || len(request.Graph.Nodes) == 0 || request.Graph.Hash == "" || !request.Versions.valid() || len(request.Goals) == 0 {
		return Result{}, ErrInvalidSynthesisRequest
	}
	if request.OutcomeState != domain.OutcomeStateVerifiedSuccess {
		return abstain(base, "outcome_not_verified")
	}
	if !request.Graph.AutoPromotable {
		return abstain(base, "opaque_tool")
	}
	nodes := make(map[domain.EventID]Node, len(request.Graph.Nodes))
	for index, node := range request.Graph.Nodes {
		if node.ID == "" || node.Position != uint32(index) {
			return Result{}, ErrInvalidSynthesisRequest
		}
		nodes[node.ID] = node
	}
	goals := canonicalGoals(request.Goals)
	base.GoalPredicates = make([]string, len(goals))
	retained := make(map[domain.EventID]struct{})
	for index, goal := range goals {
		base.GoalPredicates[index] = goal.PredicateID
		if goal.PredicateID == "" || len(goal.VerifierNodes) == 0 {
			return abstain(base, "uncovered_goal")
		}
		covered := false
		for _, id := range goal.VerifierNodes {
			node, exists := nodes[id]
			if !exists {
				return Result{}, ErrInvalidSynthesisRequest
			}
			if node.Succeeded {
				retained[id] = struct{}{}
				covered = true
			}
		}
		if !covered {
			return abstain(base, "uncovered_goal")
		}
	}
	for _, id := range request.RequiredEffects {
		node, exists := nodes[id]
		if !exists {
			return Result{}, ErrInvalidSynthesisRequest
		}
		if !node.Succeeded {
			return abstain(base, "failed_required_effect")
		}
		retained[id] = struct{}{}
	}
	reverse := make(map[domain.EventID][]domain.EventID)
	for _, edge := range request.Graph.Edges {
		if _, fromOK := nodes[edge.From]; !fromOK {
			return Result{}, ErrInvalidSynthesisRequest
		}
		if _, toOK := nodes[edge.To]; !toOK {
			return Result{}, ErrInvalidSynthesisRequest
		}
		reverse[edge.To] = append(reverse[edge.To], edge.From)
	}
	queue := make([]domain.EventID, 0, len(retained))
	for id := range retained {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		for _, prerequisite := range reverse[id] {
			if _, exists := retained[prerequisite]; !exists {
				retained[prerequisite] = struct{}{}
				queue = append(queue, prerequisite)
			}
		}
	}
	for id := range retained {
		if !nodes[id].Succeeded {
			return abstain(base, "failed_prerequisite")
		}
	}
	irrelevant, err := eventSet(request.IrrelevantNodes, nodes)
	if err != nil {
		return Result{}, err
	}
	failed := make(map[domain.EventID]struct{})
	negativePaths, err := buildNegativePaths(request.FailedBranches, nodes, failed)
	if err != nil {
		return Result{}, err
	}
	base.NegativePaths = negativePaths
	selected := make(map[domain.EventID]struct{})
	resourceCatalog := make(map[string]domain.ProcedureResource)
	for _, node := range request.Graph.Nodes {
		_, proven := retained[node.ID]
		_, excluded := irrelevant[node.ID]
		_, failedNode := failed[node.ID]
		if failedNode || (!proven && excluded) || (!proven && !node.Succeeded) {
			continue
		}
		reads, writes, effects, preconditions, predicates, methods, metadataErr := canonicalStepMetadata(node, resourceCatalog)
		if metadataErr != nil {
			return Result{}, metadataErr
		}
		base.Steps = append(base.Steps, domain.ProcedureStep{
			EventID: node.ID, Ordinal: uint32(len(base.Steps)), OriginalPosition: node.Position,
			ToolName: node.ToolName, ToolVersion: node.ToolVersion, ToolContractVersionID: node.ToolContractVersionID,
			UncertainNecessity: !proven, CompensationBoundary: node.CompensationBoundary, SideEffect: string(node.SideEffect), Risk: string(node.Risk),
			Effects: effects, Preconditions: preconditions, SuccessPredicates: predicates, VerificationMethods: methods, Reads: reads, Writes: writes,
		})
		selected[node.ID] = struct{}{}
	}
	for _, edge := range request.Graph.Edges {
		_, from := selected[edge.From]
		_, to := selected[edge.To]
		if from && to {
			base.Edges = append(base.Edges, edge)
		}
	}
	positions := make(map[domain.EventID]int, len(request.Graph.Nodes))
	for index, node := range request.Graph.Nodes {
		positions[node.ID] = index
	}
	sortEdges(base.Edges, positions)
	base.Status = StatusSynthesized
	_, hash, err := canonical.MarshalAndHash(base)
	if err != nil {
		return Result{}, fmt.Errorf("hash synthesis result: %w", err)
	}
	base.Hash = hash
	return base, nil
}

func canonicalStepMetadata(node Node, catalog map[string]domain.ProcedureResource) ([]domain.ProcedureResource, []domain.ProcedureResource, []string, []domain.ProcedurePredicate, []string, []string, error) {
	if !validSideEffect(string(node.SideEffect)) || !validRisk(string(node.Risk)) {
		return nil, nil, nil, nil, nil, nil, ErrInvalidSynthesisRequest
	}
	reads, err := canonicalProcedureResources(node.Reads, catalog)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	writes, err := canonicalProcedureResources(node.Writes, catalog)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	effects, err := canonicalStringSet(node.Effects)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	preconditions, err := canonicalPredicates(node.Preconditions, reads, writes)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	predicates, err := canonicalStringSet(node.SuccessPredicates)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	methods, err := canonicalStringSet(node.VerificationMethods)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	return reads, writes, effects, preconditions, predicates, methods, nil
}

func canonicalPredicates(source []PredicateReference, reads, writes []domain.ProcedureResource) ([]domain.ProcedurePredicate, error) {
	resources := make(map[string]struct{}, len(reads)+len(writes))
	for _, resource := range append(append([]domain.ProcedureResource(nil), reads...), writes...) {
		resources[resource.Name] = struct{}{}
	}
	seen := make(map[string]domain.ProcedurePredicate, len(source))
	for _, item := range source {
		value := domain.ProcedurePredicate{ID: strings.TrimSpace(item.ID), ResourceName: strings.TrimSpace(item.ResourceName)}
		if value.ID == "" || len(value.ID) > 256 {
			return nil, ErrInvalidSynthesisRequest
		}
		if value.ResourceName != "" {
			if _, ok := resources[value.ResourceName]; !ok {
				return nil, ErrInvalidSynthesisRequest
			}
		}
		if prior, ok := seen[value.ID]; ok && prior != value {
			return nil, ErrInvalidSynthesisRequest
		}
		seen[value.ID] = value
	}
	result := make([]domain.ProcedurePredicate, 0, len(seen))
	for _, item := range seen {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func canonicalProcedureResources(source []Resource, catalog map[string]domain.ProcedureResource) ([]domain.ProcedureResource, error) {
	byID := make(map[string]domain.ProcedureResource, len(source))
	for _, item := range source {
		value := domain.ProcedureResource{ID: strings.TrimSpace(item.ID), Name: strings.TrimSpace(item.Name), Type: strings.TrimSpace(item.Type), Namespace: strings.TrimSpace(item.Namespace), IdentityHash: strings.TrimSpace(item.IdentityHash), SchemaVersion: strings.TrimSpace(item.SchemaVersion)}
		if value.ID != "res_"+value.IdentityHash || value.Name == "" || value.Type == "" || value.Namespace == "" || len(value.IdentityHash) != 64 || !lowerHex(value.IdentityHash) {
			return nil, ErrInvalidSynthesisRequest
		}
		if prior, exists := byID[value.ID]; exists && prior != value {
			return nil, ErrInvalidSynthesisRequest
		}
		if prior, exists := catalog[value.ID]; exists && prior != value {
			return nil, ErrInvalidSynthesisRequest
		}
		byID[value.ID] = value
		catalog[value.ID] = value
	}
	result := make([]domain.ProcedureResource, 0, len(byID))
	for _, item := range byID {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func canonicalStringSet(source []string) ([]string, error) {
	seen := make(map[string]struct{}, len(source))
	for _, item := range source {
		item = strings.TrimSpace(item)
		if item == "" || len(item) > 256 {
			return nil, ErrInvalidSynthesisRequest
		}
		seen[item] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for item := range seen {
		result = append(result, item)
	}
	sort.Strings(result)
	return result, nil
}

func lowerHex(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func validSideEffect(value string) bool {
	return value == "none" || value == "read" || value == "write" || value == "external" || value == "irreversible"
}

func validRisk(value string) bool {
	return value == "low" || value == "medium" || value == "high" || value == "critical"
}

func abstain(base Result, code string) (Result, error) {
	base.Status = StatusAbstained
	base.AbstentionCode = code
	base.ObservedEndToEnd = false
	_, hash, err := canonical.MarshalAndHash(base)
	if err != nil {
		return Result{}, err
	}
	base.Hash = hash
	return base, nil
}

func canonicalGoals(source []GoalRequirement) []GoalRequirement {
	merged := make(map[string]map[domain.EventID]struct{}, len(source))
	for _, goal := range source {
		if merged[goal.PredicateID] == nil {
			merged[goal.PredicateID] = make(map[domain.EventID]struct{})
		}
		for _, id := range goal.VerifierNodes {
			merged[goal.PredicateID][id] = struct{}{}
		}
	}
	result := make([]GoalRequirement, 0, len(merged))
	for predicate, ids := range merged {
		goal := GoalRequirement{PredicateID: predicate, VerifierNodes: make([]domain.EventID, 0, len(ids))}
		for id := range ids {
			goal.VerifierNodes = append(goal.VerifierNodes, id)
		}
		sort.Slice(goal.VerifierNodes, func(i, j int) bool { return goal.VerifierNodes[i] < goal.VerifierNodes[j] })
		result = append(result, goal)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PredicateID < result[j].PredicateID })
	return result
}

func eventSet(source []domain.EventID, nodes map[domain.EventID]Node) (map[domain.EventID]struct{}, error) {
	result := make(map[domain.EventID]struct{}, len(source))
	for _, id := range source {
		if _, exists := nodes[id]; !exists {
			return nil, ErrInvalidSynthesisRequest
		}
		result[id] = struct{}{}
	}
	return result, nil
}

func buildNegativePaths(source []FailedBranch, nodes map[domain.EventID]Node, failed map[domain.EventID]struct{}) ([]domain.NegativePath, error) {
	result := make([]domain.NegativePath, len(source))
	for index, branch := range source {
		if branch.FailurePredicateID == "" || !branch.Scope.Valid() || len(branch.EventIDs) == 0 {
			return nil, ErrInvalidSynthesisRequest
		}
		eventIDs := append([]domain.EventID(nil), branch.EventIDs...)
		sort.Slice(eventIDs, func(i, j int) bool { return nodes[eventIDs[i]].Position < nodes[eventIDs[j]].Position })
		for _, id := range eventIDs {
			if _, exists := nodes[id]; !exists {
				return nil, ErrInvalidSynthesisRequest
			}
			failed[id] = struct{}{}
		}
		path := domain.NegativePath{FailurePredicateID: branch.FailurePredicateID, EventIDs: eventIDs, Scope: branch.Scope}
		_, hash, err := canonical.MarshalAndHash(path)
		if err != nil {
			return nil, err
		}
		path.ID = "neg_" + hash
		result[index] = path
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
