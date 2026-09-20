//go:build integration

package surreal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestCommitContract(t *testing.T) {
	storetest.RunIngestContract(t, func(t *testing.T) storetest.Repository {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		return NewIngestRepository(db)
	})
}

func TestOutcomeCommitContract(t *testing.T) {
	storetest.RunOutcomeContract(t, func(t *testing.T) (store.IngestRepository, storetest.OutcomeRepository) {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		return NewIngestRepository(db), NewOutcomeRepository(db)
	})
}

func TestProjectionContract(t *testing.T) {
	storetest.RunProjectionContract(t, func(t *testing.T) storetest.ProjectionRepository {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		seedProjectionOutcomes(t, db)
		return NewProjectionRepository(db)
	})
}

func seedProjectionOutcomes(t *testing.T, db *surrealdb.DB) {
	t.Helper()
	for number := 1; number <= 2; number++ {
		_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY outcome_evidence CONTENT $record`, map[string]any{
			"record": map[string]any{
				"tenant_id": "tenant_a", "outcome_id": fmt.Sprintf("out_%064x", number),
				"trace_id": fmt.Sprintf("tr_%064x", number), "state": "verified_success",
				"promotion_eligible": true, "policy_version": "policy.v1", "evidence_count": 1,
				"created_at": time.Date(2026, 9, 20, 0, 0, number, 0, time.UTC), "schema_version": "outcome.v1",
				"content_hash": fmt.Sprintf("%064x", number+200),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for index, contractID := range []string{"tcv_write", "tcv_verify", "tcv_verify_v2"} {
		_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY type::record("tool_contract_version", $id) CONTENT $record`, map[string]any{
			"id": contractID,
			"record": map[string]any{
				"tenant_id": "tenant_a", "contract_version_id": contractID, "tool_id": "tool_" + contractID,
				"tool_version": "1.0.0", "manifest": map[string]any{}, "created_at": time.Date(2026, 9, 20, 0, 0, index+1, 0, time.UTC),
				"schema_version": "tool-contract.v1", "content_hash": fmt.Sprintf("%064x", index+500),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommitRollsBackAfterEventFailure(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	repository := newIngestRepository(db, failureAfterEvents)
	_, err := repository.Commit(context.Background(), storetest.ValidCommitRequest(t))
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("error = %v, want %v", err, errInjectedFailure)
	}
	storetest.AssertCounts(t, repository, store.AggregateCounts{})
}

func TestProjectionRollsBackAfterGraphFailure(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedProjectionOutcomes(t, db)
	repository := newProjectionRepository(db, failureAfterEvents)
	_, err := repository.Publish(context.Background(), storetest.ValidProjection(t, 1, false))
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("error = %v, want %v", err, errInjectedFailure)
	}
	counts, countErr := repository.Counts(context.Background())
	if countErr != nil {
		t.Fatal(countErr)
	}
	if counts != (projection.Counts{}) {
		t.Fatalf("partial projection escaped transaction: %#v", counts)
	}
}

func TestProjectionFailsClosedAcrossTenants(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedProjectionOutcomes(t, db)
	repository := NewProjectionRepository(db)
	_, err := repository.Publish(context.Background(), storetest.ValidProjectionForTenant(t, "tenant_b", 1, false))
	if !errors.Is(err, projection.ErrInvalidProjection) {
		t.Fatalf("error = %v, want %v", err, projection.ErrInvalidProjection)
	}
	counts, countErr := repository.Counts(context.Background())
	if countErr != nil {
		t.Fatal(countErr)
	}
	if counts != (projection.Counts{}) {
		t.Fatalf("cross-tenant projection escaped transaction: %#v", counts)
	}
}

func TestProjectionCanonicalRecordIsImmutable(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedProjectionOutcomes(t, db)
	repository := NewProjectionRepository(db)
	value := storetest.ValidProjection(t, 1, false)
	if _, err := repository.Publish(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	_, _ = surrealdb.Query[any](context.Background(), db, `UPDATE procedure_version
		SET canonical_projection = "tampered" WHERE procedure_version_id = $version_id`, map[string]any{
		"version_id": value.Version.ID,
	})
	got, err := repository.Canonical(context.Background(), value.TenantID, value.Version.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(value.CanonicalProjectionJSON) {
		t.Fatalf("immutable canonical projection changed: %s", got)
	}
}
