package memory

import (
	"context"
	"sync"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
)

type CompositionRepository struct {
	mu     sync.RWMutex
	graphs map[compositionGraphKey]composition.CompatibilityGraph
}

type compositionGraphKey struct {
	tenantID          domain.TenantID
	epoch             uint64
	plannerManifestID string
	policyManifestID  string
}

func NewCompositionRepository() *CompositionRepository {
	return &CompositionRepository{graphs: make(map[compositionGraphKey]composition.CompatibilityGraph)}
}

func (r *CompositionRepository) Publish(ctx context.Context, graph composition.CompatibilityGraph) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil {
		return composition.ErrInvalidCompatibilityInput
	}
	if composition.ValidateCompatibilityGraph(graph) != nil {
		return composition.ErrInvalidCompatibilityInput
	}
	key := graphKey(graph.TenantID, graph.ProjectionEpoch, graph.PlannerManifestID, graph.PolicyManifestID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, found := r.graphs[key]; found {
		if existing.ContentHash != graph.ContentHash {
			return composition.ErrCompositionConflict
		}
		return nil
	}
	r.graphs[key] = cloneGraph(graph)
	return nil
}

func (r *CompositionRepository) Edges(ctx context.Context, tenantID domain.TenantID, epoch uint64, plannerManifestID, policyManifestID string) ([]composition.CompatibilityEdge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || tenantID == "" || epoch == 0 || plannerManifestID == "" || policyManifestID == "" {
		return nil, composition.ErrInvalidCompatibilityInput
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	graph, found := r.graphs[graphKey(tenantID, epoch, plannerManifestID, policyManifestID)]
	if !found {
		return nil, nil
	}
	return cloneEdges(graph.Edges), nil
}

func graphKey(tenantID domain.TenantID, epoch uint64, plannerManifestID, policyManifestID string) compositionGraphKey {
	return compositionGraphKey{tenantID, epoch, plannerManifestID, policyManifestID}
}

func cloneGraph(source composition.CompatibilityGraph) composition.CompatibilityGraph {
	result := source
	result.CanonicalJSON = append([]byte(nil), source.CanonicalJSON...)
	result.Edges = cloneEdges(source.Edges)
	return result
}

func cloneEdges(source []composition.CompatibilityEdge) []composition.CompatibilityEdge {
	result := append([]composition.CompatibilityEdge(nil), source...)
	for index := range result {
		result[index].SourceProvisionIDs = append([]string(nil), source[index].SourceProvisionIDs...)
		result[index].SatisfiedRequirementIDs = append([]string(nil), source[index].SatisfiedRequirementIDs...)
	}
	return result
}
