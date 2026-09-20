package maintenance_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/maintenance"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"github.com/sauhard74/mem-jev/internal/store/memory"
)

func TestCompatibilityHandlerAuthenticatesAndPublishesGraph(t *testing.T) {
	matrix, err := composition.BuildCompatibilityMatrix(nil)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := composition.BuildCompatibilityGraph(composition.GraphRequest{
		TenantID: "tenant_a", ProjectionEpoch: 7, PlannerManifestID: "planner.v1", PolicyManifestID: "policy.v1", Matrix: matrix,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := maintenance.NewCompatibilityPayload(graph)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := maintenance.NewSpec("tenant_a", maintenance.CompatibilityProjection, strings.Repeat("a", 64), payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	repository := memory.NewCompositionRepository()
	handler := maintenance.CompatibilityHandler{Publisher: repository}
	if err := handler.Handle(context.Background(), maintenance.Lease{Spec: spec}); err != nil {
		t.Fatal(err)
	}
	edges, err := repository.Edges(context.Background(), "tenant_a", 7, "planner.v1", "policy.v1")
	if err != nil || len(edges) != 0 {
		t.Fatalf("edges=%#v err=%v", edges, err)
	}
}

type captureRebuildPublisher struct {
	permit rebuild.ActivationPermit
}

func (p *captureRebuildPublisher) PublishActivation(_ context.Context, _, _ rebuild.DerivedSnapshot, permit rebuild.ActivationPermit) error {
	p.permit = permit
	return nil
}

func TestProjectionRebuildHandlerPublishesPermitOnlyForExactSnapshot(t *testing.T) {
	snapshot, err := rebuild.BuildDerivedSnapshot(rebuild.DerivedInput{TenantID: "tenant_a", ProjectionEpoch: 9})
	if err != nil {
		t.Fatal(err)
	}
	authorizedAt := time.Now().UTC()
	payload, err := maintenance.NewRebuildPayload(snapshot, snapshot, authorizedAt)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := maintenance.NewSpec("tenant_a", maintenance.ProjectionRebuild, strings.Repeat("d", 64), payload, authorizedAt)
	if err != nil {
		t.Fatal(err)
	}
	publisher := new(captureRebuildPublisher)
	if err := (maintenance.ProjectionRebuildHandler{Publisher: publisher}).Handle(context.Background(), maintenance.Lease{Spec: spec}); err != nil {
		t.Fatal(err)
	}
	if rebuild.ValidateActivationPermit(publisher.permit) != nil || publisher.permit.StoredSnapshotID != snapshot.ID {
		t.Fatalf("permit=%#v", publisher.permit)
	}
}

func TestCompatibilityHandlerRejectsCrossTenantLease(t *testing.T) {
	matrix, _ := composition.BuildCompatibilityMatrix(nil)
	graph, _ := composition.BuildCompatibilityGraph(composition.GraphRequest{TenantID: "tenant_a", ProjectionEpoch: 7, PlannerManifestID: "planner.v1", PolicyManifestID: "policy.v1", Matrix: matrix})
	payload, _ := maintenance.NewCompatibilityPayload(graph)
	spec, _ := maintenance.NewSpec("tenant_b", maintenance.CompatibilityProjection, strings.Repeat("b", 64), payload, time.Now().UTC())
	err := (maintenance.CompatibilityHandler{Publisher: memory.NewCompositionRepository()}).Handle(context.Background(), maintenance.Lease{Spec: spec})
	if !errors.Is(err, maintenance.ErrInvalidPayload) {
		t.Fatalf("error=%v", err)
	}
}
