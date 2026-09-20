package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

type CompositionRepository interface {
	Publish(context.Context, composition.CompatibilityGraph) error
	Edges(context.Context, domain.TenantID, uint64, string, string) ([]composition.CompatibilityEdge, error)
}

type CompositionFactory func(*testing.T) CompositionRepository

func RunCompositionContract(t *testing.T, factory CompositionFactory) {
	t.Helper()
	t.Run("duplicate publication and defensive reads", func(t *testing.T) {
		repository := factory(t)
		graph := ValidCompatibilityGraph(t, "tenant_a", 1, "pv_source", "pv_target")
		if err := repository.Publish(context.Background(), graph); err != nil {
			t.Fatal(err)
		}
		if err := repository.Publish(context.Background(), graph); err != nil {
			t.Fatalf("duplicate: %v", err)
		}
		edges, err := repository.Edges(context.Background(), "tenant_a", 1, "planner_1", "policy_1")
		if err != nil || len(edges) != 1 {
			t.Fatalf("edges=%#v error=%v", edges, err)
		}
		edges[0].SatisfiedRequirementIDs[0] = "corrupt"
		again, err := repository.Edges(context.Background(), "tenant_a", 1, "planner_1", "policy_1")
		if err != nil || len(again) != 1 || again[0].SatisfiedRequirementIDs[0] == "corrupt" {
			t.Fatalf("repository leaked mutable state: edges=%#v error=%v", again, err)
		}
	})

	t.Run("conflict and tenant isolation", func(t *testing.T) {
		repository := factory(t)
		first := ValidCompatibilityGraph(t, "tenant_a", 3, "pv_a", "pv_b")
		conflict := ValidCompatibilityGraph(t, "tenant_a", 3, "pv_a", "pv_c")
		foreign := ValidCompatibilityGraph(t, "tenant_b", 3, "pv_a", "pv_b")
		alternateManifest := validCompatibilityGraph(t, "tenant_a", 3, "planner_2", "policy_1", "pv_a", "pv_b")
		if err := repository.Publish(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		if err := repository.Publish(context.Background(), conflict); !errors.Is(err, composition.ErrCompositionConflict) {
			t.Fatalf("conflict error=%v", err)
		}
		if err := repository.Publish(context.Background(), foreign); err != nil {
			t.Fatal(err)
		}
		if err := repository.Publish(context.Background(), alternateManifest); err != nil {
			t.Fatalf("independent manifest graph: %v", err)
		}
		alternateEdges, err := repository.Edges(context.Background(), "tenant_a", 3, "planner_2", "policy_1")
		if err != nil || len(alternateEdges) != 1 || alternateEdges[0].PlannerManifestID != "planner_2" {
			t.Fatalf("alternate edges=%#v error=%v", alternateEdges, err)
		}
		edges, err := repository.Edges(context.Background(), "tenant_b", 3, "planner_1", "policy_1")
		if err != nil || len(edges) != 1 || edges[0].TenantID != "tenant_b" {
			t.Fatalf("foreign edges=%#v error=%v", edges, err)
		}
	})
}

func ValidCompatibilityGraph(t *testing.T, tenant string, epoch uint64, sourceVersion, targetVersion string) composition.CompatibilityGraph {
	return validCompatibilityGraph(t, tenant, epoch, "planner_1", "policy_1", sourceVersion, targetVersion)
}

func validCompatibilityGraph(t *testing.T, tenant string, epoch uint64, plannerManifestID, policyManifestID, sourceVersion, targetVersion string) composition.CompatibilityGraph {
	t.Helper()
	identity := repeatedHex('a')
	sourceInterface, err := retrieval.NewProcedureInterface(nil, []domain.ProcedureProvision{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	targetInterface, err := retrieval.NewProcedureInterface([]domain.ProcedureRequirement{{ResourceType: "file", Namespace: "workspace", IdentityHash: identity, SchemaVersion: "file.v1", AccessMode: "read"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	matrix, err := composition.BuildCompatibilityMatrix(nil)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{
		TenantID: domain.TenantID(tenant), ProjectionEpoch: epoch, PlannerManifestID: plannerManifestID, PolicyManifestID: policyManifestID,
		Matrix: matrix,
		Candidates: []composition.Candidate{
			{TenantID: domain.TenantID(tenant), ProcedureVersionID: sourceVersion, Lifecycle: "active", Interface: sourceInterface},
			{TenantID: domain.TenantID(tenant), ProcedureVersionID: targetVersion, Lifecycle: "candidate", Interface: targetInterface},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func repeatedHex(value byte) string {
	result := make([]byte, 64)
	for index := range result {
		result[index] = value
	}
	return string(result)
}
