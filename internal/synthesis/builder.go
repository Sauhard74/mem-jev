package synthesis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

const graphSchemaVersion = "causal-graph.v3"

type Builder struct {
	registry toolcontract.Registry
}

func NewBuilder(registry toolcontract.Registry) *Builder { return &Builder{registry: registry} }

func (b *Builder) Build(ctx context.Context, request BuildRequest) (Graph, error) {
	if err := ctx.Err(); err != nil {
		return Graph{}, err
	}
	if b == nil || b.registry == nil || request.Batch.TenantID == "" || request.Batch.Trace.ID == "" || len(request.Batch.Events) == 0 {
		return Graph{}, ErrInvalidBuildRequest
	}
	graph := Graph{SchemaVersion: graphSchemaVersion, TraceID: request.Batch.Trace.ID, AutoPromotable: true}
	graph.Nodes = make([]Node, len(request.Batch.Events))
	positions := make(map[domain.EventID]int, len(request.Batch.Events))
	lastWriter := make(map[string]domain.EventID)
	edges := make(map[string]Edge)
	for index, event := range request.Batch.Events {
		if event.ID == "" || event.Position != uint32(index) {
			return Graph{}, ErrInvalidBuildRequest
		}
		if _, duplicate := positions[event.ID]; duplicate {
			return Graph{}, ErrInvalidBuildRequest
		}
		positions[event.ID] = index
		resolved, err := b.registry.Resolve(ctx, toolcontract.Query{Name: event.ToolName, Version: event.ToolVersion})
		if err != nil {
			return Graph{}, fmt.Errorf("resolve tool contract: %w", err)
		}
		node, err := buildNode(request.Batch.TenantID, event, resolved)
		if err != nil {
			return Graph{}, err
		}
		if node.Opaque {
			graph.AutoPromotable = false
		}
		for _, resource := range node.Reads {
			if producer, exists := lastWriter[resource.ID]; exists {
				addEdge(edges, Edge{
					From: producer, To: node.ID, Type: EdgeResourceFlow, ResourceID: resource.ID,
					ResourceName: resource.Name, ResourceType: resource.Type, ResourceNamespace: resource.Namespace,
				})
			}
		}
		for _, resource := range node.Writes {
			lastWriter[resource.ID] = node.ID
		}
		graph.Nodes[index] = node
	}
	if err := addDeclaredEdges(edges, positions, request.ControlDependencies, EdgeControl); err != nil {
		return Graph{}, err
	}
	if err := addDeclaredEdges(edges, positions, request.VerifierDependencies, EdgeVerifier); err != nil {
		return Graph{}, err
	}
	graph.Edges = make([]Edge, 0, len(edges))
	for _, edge := range edges {
		graph.Edges = append(graph.Edges, edge)
	}
	sortEdges(graph.Edges, positions)
	if hasCycle(graph.Nodes, graph.Edges) {
		return Graph{}, ErrCausalCycle
	}
	_, hash, err := canonical.MarshalAndHash(graph)
	if err != nil {
		return Graph{}, fmt.Errorf("hash causal graph: %w", err)
	}
	graph.Hash = hash
	return graph, nil
}

func buildNode(tenantID domain.TenantID, event domain.CanonicalEvent, resolved toolcontract.Resolution) (Node, error) {
	node := Node{
		ID: event.ID, Position: event.Position, ToolName: event.ToolName, ToolVersion: event.ToolVersion,
		ToolContractVersionID: resolved.Manifest.ID, Opaque: resolved.Opaque,
		Succeeded:  event.Result != nil && event.Result.State == "TOOL_RESULT_STATE_SUCCESS",
		SideEffect: resolved.Manifest.SideEffect, Risk: resolved.Manifest.Risk, CompensationBoundary: resolved.Manifest.Compensation != nil,
	}
	for _, effect := range resolved.Manifest.Effects {
		node.Effects = append(node.Effects, effect.Name)
	}
	for _, predicate := range resolved.Manifest.Preconditions {
		node.Preconditions = append(node.Preconditions, PredicateReference{ID: predicate.ID, ResourceName: predicate.Resource})
	}
	sort.Strings(node.Effects)
	for _, predicate := range resolved.Manifest.SuccessPredicates {
		node.SuccessPredicates = append(node.SuccessPredicates, predicate.ID)
	}
	for _, method := range resolved.Manifest.VerificationMethods {
		node.VerificationMethods = append(node.VerificationMethods, method.ID)
	}
	sort.Strings(node.SuccessPredicates)
	sort.Strings(node.VerificationMethods)
	sort.Slice(node.Preconditions, func(i, j int) bool {
		if node.Preconditions[i].ID != node.Preconditions[j].ID {
			return node.Preconditions[i].ID < node.Preconditions[j].ID
		}
		return node.Preconditions[i].ResourceName < node.Preconditions[j].ResourceName
	})
	if resolved.Opaque {
		return node, nil
	}
	values := make(map[string]string, len(event.Fields))
	for _, field := range event.Fields {
		values[field.Name] = field.Value
	}
	if event.Result != nil {
		for _, field := range event.Result.Evidence {
			values[field.Name] = field.Value
		}
	}
	var err error
	if node.Reads, err = resourcesFor(tenantID, resolved.Manifest.Reads, values); err != nil {
		return Node{}, err
	}
	if node.Writes, err = resourcesFor(tenantID, resolved.Manifest.Writes, values); err != nil {
		return Node{}, err
	}
	return node, nil
}

func resourcesFor(tenantID domain.TenantID, specs []toolcontract.ResourceSpec, values map[string]string) ([]Resource, error) {
	result := make([]Resource, len(specs))
	for index, spec := range specs {
		value, exists := values[spec.Field]
		if !exists || value == "" {
			return nil, fmt.Errorf("%w: %s", ErrMissingResource, spec.Name)
		}
		sum := sha256.Sum256([]byte(string(tenantID) + "\x00" + spec.Namespace + "\x00" + spec.Type + "\x00" + value))
		identityHash := hex.EncodeToString(sum[:])
		result[index] = Resource{ID: "res_" + identityHash, Name: spec.Name, Type: spec.Type, Namespace: spec.Namespace, IdentityHash: identityHash, SchemaVersion: spec.SchemaVersion}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func addDeclaredEdges(edges map[string]Edge, positions map[domain.EventID]int, dependencies []DeclaredDependency, edgeType EdgeType) error {
	for _, dependency := range dependencies {
		if dependency.Prerequisite == dependency.Dependent {
			return ErrCausalCycle
		}
		if _, exists := positions[dependency.Prerequisite]; !exists {
			return ErrUnknownDependency
		}
		if _, exists := positions[dependency.Dependent]; !exists {
			return ErrUnknownDependency
		}
		addEdge(edges, Edge{From: dependency.Prerequisite, To: dependency.Dependent, Type: edgeType})
	}
	return nil
}

func addEdge(edges map[string]Edge, edge Edge) {
	key := string(edge.From) + "\x00" + string(edge.To) + "\x00" + string(edge.Type) + "\x00" + edge.ResourceID + "\x00" + edge.ResourceNamespace + "\x00" + edge.ResourceType + "\x00" + edge.ResourceName
	edges[key] = edge
}

func sortEdges(edges []Edge, positions map[domain.EventID]int) {
	sort.Slice(edges, func(i, j int) bool {
		if positions[edges[i].From] != positions[edges[j].From] {
			return positions[edges[i].From] < positions[edges[j].From]
		}
		if positions[edges[i].To] != positions[edges[j].To] {
			return positions[edges[i].To] < positions[edges[j].To]
		}
		left := string(edges[i].From) + "\x00" + string(edges[i].To) + "\x00" + string(edges[i].Type) + "\x00" + edges[i].ResourceID + "\x00" + edges[i].ResourceNamespace + "\x00" + edges[i].ResourceType + "\x00" + edges[i].ResourceName
		right := string(edges[j].From) + "\x00" + string(edges[j].To) + "\x00" + string(edges[j].Type) + "\x00" + edges[j].ResourceID + "\x00" + edges[j].ResourceNamespace + "\x00" + edges[j].ResourceType + "\x00" + edges[j].ResourceName
		return left < right
	})
}

func hasCycle(nodes []Node, edges []Edge) bool {
	indegree := make(map[domain.EventID]int, len(nodes))
	adjacency := make(map[domain.EventID][]domain.EventID, len(nodes))
	for _, node := range nodes {
		indegree[node.ID] = 0
	}
	for _, edge := range edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		indegree[edge.To]++
	}
	queue := make([]domain.EventID, 0, len(nodes))
	for _, node := range nodes {
		if indegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}
	visited := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		visited++
		for _, dependent := range adjacency[id] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	return visited != len(nodes)
}
