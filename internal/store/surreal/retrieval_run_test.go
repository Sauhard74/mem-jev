//go:build integration

package surreal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/ranking"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestSurrealRetrievalRunRepositoryContract(t *testing.T) {
	storetest.RunRetrievalRunContract(t, func(t *testing.T) store.RetrievalRunRepository {
		db := projectionDatabase(t)
		seedProjectionOutcomes(t, db)
		if _, err := NewProjectionRepository(db).Publish(context.Background(), storetest.ValidProjection(t, 1, false)); err != nil {
			t.Fatal(err)
		}
		seedServingManifests(t, db)
		return NewRetrievalRunRepository(db)
	})
}

func TestSurrealRetrievalRunRollsBackPartialWrite(t *testing.T) {
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	if _, err := NewProjectionRepository(db).Publish(context.Background(), storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	seedServingManifests(t, db)
	repository := &RetrievalRunRepository{db: db, failure: failureAfterRetrievalChildren}
	config := storetest.ValidServingConfig(t, "tenant_a")
	if err := repository.ActivateServingConfig(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.AcquireServingSnapshot(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	run := storetest.ValidRetrievalRun(t, snapshot, 'd')
	if err = repository.SaveRetrievalRun(context.Background(), run); !errors.Is(err, errInjectedFailure) {
		t.Fatalf("save error = %v", err)
	}
	counts, err := repository.ChildCounts(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for table, count := range counts {
		if count != 0 {
			t.Fatalf("%s count = %d after rollback", table, count)
		}
	}
	if _, err = repository.RetrievalRun(context.Background(), "tenant_a", run.ID); !errors.Is(err, store.ErrRetrievalRunNotFound) {
		t.Fatalf("run survived rollback: %v", err)
	}
}

func TestSurrealServingConfigFailsClosedOnMissingManifest(t *testing.T) {
	db := projectionDatabase(t)
	repository := NewRetrievalRunRepository(db)
	if err := repository.ActivateServingConfig(context.Background(), storetest.ValidServingConfig(t, "tenant_a")); !errors.Is(err, store.ErrServingConfigUnavailable) {
		t.Fatalf("activation error = %v", err)
	}
}

func TestSurrealRetrievalRunAcceptsCapturedHistoricalSnapshot(t *testing.T) {
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	projectionRepository := NewProjectionRepository(db)
	if _, err := projectionRepository.Publish(context.Background(), storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	seedServingManifests(t, db)
	repository := NewRetrievalRunRepository(db)
	config := storetest.ValidServingConfig(t, "tenant_a")
	if err := repository.ActivateServingConfig(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.AcquireServingSnapshot(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = projectionRepository.Publish(context.Background(), storetest.ValidProjection(t, 2, false)); err != nil {
		t.Fatal(err)
	}
	if err = repository.SaveRetrievalRun(context.Background(), storetest.ValidRetrievalRun(t, snapshot, 'e')); err != nil {
		t.Fatalf("save against captured snapshot: %v", err)
	}
}

func TestSurrealRetrievalRunPersistsSemanticProvenanceAtomically(t *testing.T) {
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	if _, err := NewProjectionRepository(db).Publish(context.Background(), storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	seedServingManifests(t, db)
	repository := NewRetrievalRunRepository(db)
	config := storetest.ValidServingConfig(t, "tenant_a")
	if err := repository.ActivateServingConfig(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.AcquireServingSnapshot(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	input := storetest.ValidRetrievalRunInput(t, snapshot, 'f')
	input.SemanticJudgments = []retrieval.PersistedSemanticJudgment{{
		VersionID: "pv_a", JudgmentKey: "jevj_" + strings.Repeat("a", 64), Disposition: "committed", ContentHash: strings.Repeat("b", 64),
		Provider: "typesafe", Model: "jev-1.13.0", RubricManifestID: "jevr_test", Features: []retrieval.PersistedFeature{{Name: "jev_intent_fit_micros", Value: 900_000}},
	}}
	run, err := retrieval.BuildRun(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.SaveRetrievalRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.RetrievalRun(context.Background(), "tenant_a", run.ID)
	if err != nil || len(loaded.SemanticJudgments) != 1 || loaded.SemanticJudgments[0].ContentHash != strings.Repeat("b", 64) {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	counts, err := repository.ChildCounts(context.Background(), run.ID)
	if err != nil || counts["retrieval_semantic_judgment"] != 1 {
		t.Fatalf("counts=%#v err=%v", counts, err)
	}
	if err = NewMaintenanceRepository(db).ExpireTransient(context.Background(), run.TenantID, run.ExpiresAt, 100); err != nil {
		t.Fatal(err)
	}
	counts, err = repository.ChildCounts(context.Background(), run.ID)
	if err != nil || counts["retrieval_semantic_judgment"] != 0 {
		t.Fatalf("semantic provenance survived expiry: counts=%#v err=%v", counts, err)
	}
	if _, err = repository.RetrievalRun(context.Background(), run.TenantID, run.ID); !errors.Is(err, store.ErrRetrievalRunNotFound) {
		t.Fatalf("retrieval run survived expiry: %v", err)
	}
}

func TestSurrealRetrievalManifestRepositoryValidatesContentAddressedManifests(t *testing.T) {
	db := projectionDatabase(t)
	policy, err := eligibility.NewPolicy(eligibility.PolicySpec{Version: "v1", AllowedLifecycle: []eligibility.Lifecycle{eligibility.LifecycleActive}, MaximumRisk: eligibility.RiskMedium, MaximumValidationAgeSeconds: 3600, CompatibleValidationPolicies: []string{"evidence.v1"}, AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	ranker, err := ranking.NewManifest(ranking.ManifestSpec{Version: "v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100, Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 1_000_000}}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, item := range []struct {
		statement string
		record    map[string]any
	}{
		{`CREATE ONLY eligibility_policy_manifest CONTENT $record`, map[string]any{"tenant_id": "tenant_a", "policy_manifest_id": policy.ID, "version": policy.Version, "manifest": string(policy.CanonicalJSON), "created_at": now, "schema_version": "eligibility-policy.v1", "content_hash": strings.TrimPrefix(policy.ID, "egp_")}},
		{`CREATE ONLY ranker_manifest CONTENT $record`, map[string]any{"tenant_id": "tenant_a", "ranker_manifest_id": ranker.ID, "version": ranker.Version, "manifest": string(ranker.CanonicalJSON), "created_at": now, "schema_version": "ranker.v1", "content_hash": strings.TrimPrefix(ranker.ID, "rnk_")}},
	} {
		if _, err = surrealdb.Query[any](context.Background(), db, item.statement, map[string]any{"record": item.record}); err != nil {
			t.Fatal(err)
		}
	}
	repository, err := NewRetrievalManifestRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	loadedPolicy, err := repository.EligibilityPolicy(context.Background(), "tenant_a", policy.ID)
	if err != nil || loadedPolicy.ID != policy.ID {
		t.Fatalf("policy=%#v err=%v", loadedPolicy, err)
	}
	loadedRanker, err := repository.Ranker(context.Background(), "tenant_a", ranker.ID)
	if err != nil || loadedRanker.ID != ranker.ID {
		t.Fatalf("ranker=%#v err=%v", loadedRanker, err)
	}
	if _, err = repository.Ranker(context.Background(), "tenant_b", ranker.ID); err == nil {
		t.Fatal("cross-tenant manifest read succeeded")
	}
}

func seedServingManifests(t *testing.T, db *surrealdb.DB) {
	t.Helper()
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	hash := strings.Repeat("a", 64)
	statements := []struct {
		statement string
		record    map[string]any
	}{
		{`CREATE ONLY ranker_manifest CONTENT $record`, map[string]any{"tenant_id": "tenant_a", "ranker_manifest_id": "ranker.v1", "version": "v1", "manifest": `{}`, "created_at": now, "schema_version": "ranker.v1", "content_hash": hash}},
		{`CREATE ONLY eligibility_policy_manifest CONTENT $record`, map[string]any{"tenant_id": "tenant_a", "policy_manifest_id": "policy.v1", "version": "v1", "manifest": `{}`, "created_at": now, "schema_version": "eligibility-policy.v1", "content_hash": strings.Repeat("b", 64)}},
		{`CREATE ONLY retrieval_index_manifest CONTENT $record`, map[string]any{"tenant_id": "tenant_a", "index_manifest_id": "index.exact.v1", "channel": "exact", "generation": 1, "index_name": "retrieval_document_exact", "approximate": false, "configuration": map[string]any{}, "created_at": now, "schema_version": "retrieval-index.v1", "content_hash": strings.Repeat("c", 64)}},
	}
	for index, item := range statements {
		if _, err := surrealdb.Query[any](context.Background(), db, item.statement, map[string]any{"record": item.record}); err != nil {
			t.Fatalf("seed manifest %d: %v", index, err)
		}
	}
}
