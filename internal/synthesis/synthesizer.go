package synthesis

import (
	"errors"
	"fmt"
	"sort"

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
		SchemaVersion: "synthesis.v1", TraceID: request.Graph.TraceID, CausalGraphHash: request.Graph.Hash,
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
	for _, node := range request.Graph.Nodes {
		_, proven := retained[node.ID]
		_, excluded := irrelevant[node.ID]
		_, failedNode := failed[node.ID]
		if failedNode || (!proven && excluded) || (!proven && !node.Succeeded) {
			continue
		}
		base.Steps = append(base.Steps, domain.ProcedureStep{
			EventID: node.ID, Ordinal: uint32(len(base.Steps)), OriginalPosition: node.Position,
			ToolName: node.ToolName, ToolVersion: node.ToolVersion, ToolContractVersionID: node.ToolContractVersionID,
			UncertainNecessity: !proven, CompensationBoundary: node.CompensationBoundary,
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
