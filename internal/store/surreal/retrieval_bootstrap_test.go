//go:build integration

package surreal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/ranking"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	surrealdb "github.com/surrealdb/surrealdb.go"
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

func TestConditionalActivationNeverOverwritesConcurrentOperatorChange(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	if _, err := NewProjectionRepository(db).Publish(ctx, storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	repository := NewRetrievalRunRepository(db)
	plain, err := retrieval.BuildDefaultServing(retrieval.DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repository.EnsureDefaultServing(ctx, "tenant_a", plain, defaultIndexes()); err != nil {
		t.Fatal(err)
	}
	staleHead, found, err := repository.activeServingConfig(ctx, "tenant_a")
	if err != nil || !found {
		t.Fatalf("default head missing: %v", err)
	}
	semantic, err := retrieval.BuildDefaultServing(retrieval.DefaultServingInput{AllowedResidencyRegions: []string{"local"}, Semantic: true})
	if err != nil {
		t.Fatal(err)
	}
	upgrade, err := retrieval.BuildServingConfig("tenant_a", semantic.Policy.ID, semantic.Ranker.ID, staleHead.Indexes)
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.ensureManifest(ctx, "ranker_manifest", "ranker_manifest_id", "tenant_a", semantic.Ranker.ID, map[string]any{
		"tenant_id": "tenant_a", "ranker_manifest_id": semantic.Ranker.ID, "version": semantic.Ranker.Version, "manifest": string(semantic.Ranker.CanonicalJSON),
		"created_at": time.Now().UTC(), "schema_version": "ranker.v1", "content_hash": strings.TrimPrefix(semantic.Ranker.ID, "rnk_"),
	}); err != nil {
		t.Fatal(err)
	}

	// An operator activates their own config after the bootstrap read the head.
	operatorRanker, err := ranking.NewManifest(ranking.ManifestSpec{Version: "operator.v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 10, Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 1_000_000}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.ensureManifest(ctx, "ranker_manifest", "ranker_manifest_id", "tenant_a", operatorRanker.ID, map[string]any{
		"tenant_id": "tenant_a", "ranker_manifest_id": operatorRanker.ID, "version": operatorRanker.Version, "manifest": string(operatorRanker.CanonicalJSON),
		"created_at": time.Now().UTC(), "schema_version": "ranker.v1", "content_hash": strings.TrimPrefix(operatorRanker.ID, "rnk_"),
	}); err != nil {
		t.Fatal(err)
	}
	operator, err := retrieval.BuildServingConfig("tenant_a", plain.Policy.ID, operatorRanker.ID, staleHead.Indexes)
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.ActivateServingConfig(ctx, operator); err != nil {
		t.Fatal(err)
	}

	if err = repository.activateServingConfig(ctx, upgrade, &staleHead.ID); !errors.Is(err, errServingHeadChanged) {
		t.Fatalf("stale conditional activation err = %v, want errServingHeadChanged", err)
	}
	if active, _, readErr := repository.activeServingConfig(ctx, "tenant_a"); readErr != nil || active.ID != operator.ID {
		t.Fatalf("operator config overwritten: active = %s, err = %v", active.ID, readErr)
	}
}

// A recall for a scope that learned nothing (an unknown person) writes no
// serving configuration and reports nothing_learned_yet.
func TestEnsureDefaultServingWritesNothingForAnEmptyScope(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	repository := NewRetrievalRunRepository(db)
	plain, err := retrieval.BuildDefaultServing(retrieval.DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	if activated, err := repository.EnsureDefaultServing(ctx, "tenant_a__nobody", plain, defaultIndexes()); activated || !errors.Is(err, retrieval.ErrNothingLearned) {
		t.Fatalf("empty scope activation = %v, %v", activated, err)
	}
	for _, table := range []string{"retrieval_serving_head", "retrieval_serving_config", "eligibility_policy_manifest", "ranker_manifest"} {
		rows, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM type::table($table) WHERE tenant_id = $tenant_id", map[string]any{"table": table, "tenant_id": "tenant_a__nobody"})
		if err != nil || len((*rows)[0].Result) != 0 {
			t.Fatalf("%s rows for an empty scope = %v, %v", table, rows, err)
		}
	}
	if _, err := repository.AcquireServingSnapshot(ctx, "tenant_a__nobody"); !errors.Is(err, retrieval.ErrNothingLearned) {
		t.Fatalf("snapshot error = %v, want nothing learned", err)
	}
}
