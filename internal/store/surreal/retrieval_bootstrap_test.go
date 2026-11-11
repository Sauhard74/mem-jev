//go:build integration

package surreal

import (
	"context"
	"testing"

	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func defaultIndexes() []retrieval.SnapshotIndex {
	return []retrieval.SnapshotIndex{
		{Channel: retrieval.ChannelExact, ManifestID: "idx_exact.v1"}, {Channel: retrieval.ChannelLexical, ManifestID: "idx_lexical.v1"},
		{Channel: retrieval.ChannelFacet, ManifestID: "idx_facet.v1"}, {Channel: retrieval.ChannelGraph, ManifestID: "idx_graph.v1"},
		{Channel: retrieval.ChannelVector, ManifestID: "idx_vector.v1", Approximate: true},
	}
}

func TestEnsureDefaultServingActivatesAndUpgradesToJev(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	if _, err := NewProjectionRepository(db).Publish(ctx, storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	repository := NewRetrievalRunRepository(db)
	if _, err := repository.AcquireServingSnapshot(ctx, "tenant_a"); err == nil {
		t.Fatal("fresh tenant unexpectedly has a serving snapshot")
	}
	plain, err := retrieval.BuildDefaultServing(retrieval.DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := repository.EnsureDefaultServing(ctx, "tenant_a", plain, defaultIndexes())
	if err != nil || !activated {
		t.Fatalf("activated = %v, err = %v", activated, err)
	}
	snapshot, err := repository.AcquireServingSnapshot(ctx, "tenant_a")
	if err != nil || snapshot.RankerManifestID != plain.Ranker.ID || len(snapshot.Indexes) != 4 {
		t.Fatalf("snapshot = %#v, err = %v (vector must be skipped when not provisioned)", snapshot, err)
	}
	manifests, err := NewRetrievalManifestRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manifests.EligibilityPolicy(ctx, "tenant_a", plain.Policy.ID); err != nil {
		t.Fatalf("default policy unreadable: %v", err)
	}
	if _, err = manifests.Ranker(ctx, "tenant_a", plain.Ranker.ID); err != nil {
		t.Fatalf("default ranker unreadable: %v", err)
	}
	if activated, err = repository.EnsureDefaultServing(ctx, "tenant_a", plain, defaultIndexes()); err != nil || activated {
		t.Fatalf("repeat activation = %v, err = %v", activated, err)
	}

	semantic, err := retrieval.BuildDefaultServing(retrieval.DefaultServingInput{AllowedResidencyRegions: []string{"local"}, Semantic: true})
	if err != nil {
		t.Fatal(err)
	}
	if activated, err = repository.EnsureDefaultServing(ctx, "tenant_a", semantic, defaultIndexes()); err != nil || !activated {
		t.Fatalf("Jev upgrade activated = %v, err = %v", activated, err)
	}
	if snapshot, err = repository.AcquireServingSnapshot(ctx, "tenant_a"); err != nil || snapshot.RankerManifestID != semantic.Ranker.ID {
		t.Fatalf("snapshot after Jev upgrade = %#v, err = %v", snapshot, err)
	}

}

func TestEnsureDefaultServingNeverReplacesOperatorConfig(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	if _, err := NewProjectionRepository(db).Publish(ctx, storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	seedServingManifests(t, db)
	repository := NewRetrievalRunRepository(db)
	operator := storetest.ValidServingConfig(t, "tenant_a")
	if err := repository.ActivateServingConfig(ctx, operator); err != nil {
		t.Fatal(err)
	}
	plain, err := retrieval.BuildDefaultServing(retrieval.DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	if activated, ensureErr := repository.EnsureDefaultServing(ctx, "tenant_a", plain, defaultIndexes()); ensureErr != nil || activated {
		t.Fatalf("operator config replaced: activated = %v, err = %v", activated, ensureErr)
	}
	if snapshot, acquireErr := repository.AcquireServingSnapshot(ctx, "tenant_a"); acquireErr != nil || snapshot.ServingConfigID != operator.ID {
		t.Fatalf("snapshot = %#v, err = %v", snapshot, acquireErr)
	}
}
