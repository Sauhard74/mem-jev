//go:build integration

package surreal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

const surrealImage = "surrealdb/surrealdb:v3.2.4"

func TestCoreRetrievalMigrationIsIdempotent(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	m := NewMigrator(db)
	if err := m.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := schemaVersion(t, db); got != 3 {
		t.Fatalf("schema version = %d, want 3", got)
	}
}

func TestOpenAuthenticatesDatabaseScopedCredentials(t *testing.T) {
	instance := testinfra.StartSurrealInstance(t, surrealImage)
	if _, err := surrealdb.Query[any](context.Background(), instance.DB,
		`DEFINE USER app_user ON DATABASE PASSWORD 'app_password' ROLES OWNER`, nil); err != nil {
		t.Fatal(err)
	}
	db, err := Open(context.Background(), Config{
		Endpoint: instance.Endpoint, Namespace: instance.Namespace, Database: instance.Database,
		Username: "app_user", Password: "app_password", AuthScope: AuthScopeDatabase,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	if _, err := surrealdb.Query[any](context.Background(), db, "INFO FOR DB", nil); err != nil {
		t.Fatal(err)
	}
}

func TestFoundationSchemaRejectsMissingTenant(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := surrealdb.Query[any](context.Background(), db, `
		CREATE canonical_event CONTENT {
			trace_id: "trace_1",
			event_id: "event_1",
			sequence: 0,
			kind: "tool",
			payload: {},
			created_at: time::now(),
			schema_version: "v1",
			content_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}`, nil)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tenant_id") {
		t.Fatalf("error = %v, want missing tenant_id rejection", err)
	}
}

func TestEvidenceSchemaRejectsMissingTenant(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := surrealdb.Query[any](context.Background(), db, `
		CREATE outcome_evidence CONTENT {
			outcome_id: "out_1",
			trace_id: "tr_1",
			state: "verified_success",
			promotion_eligible: true,
			policy_version: "policy.v1",
			evidence_count: 1,
			created_at: time::now(),
			schema_version: "outcome.v1",
			content_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}`, nil)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tenant_id") {
		t.Fatalf("error = %v, want missing tenant_id rejection", err)
	}
}

func TestEvidenceSchemaRejectsInvalidOutcomeState(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := surrealdb.Query[any](context.Background(), db, `
		CREATE outcome_evidence CONTENT {
			tenant_id: "tenant_1", outcome_id: "out_1", trace_id: "tr_1",
			state: "made_up", promotion_eligible: true, policy_version: "policy.v1",
			evidence_count: -1, created_at: time::now(), schema_version: "outcome.v1",
			content_hash: "short"
		}`, nil)
	if err == nil {
		t.Fatal("invalid outcome state, count, and hash were accepted")
	}
}

func TestEvidenceRecordsAreImmutable(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	results, err := surrealdb.Query[[]struct {
		ID string `json:"id"`
	}](context.Background(), db, `
		CREATE outcome_evidence CONTENT {
			tenant_id: "tenant_1",
			outcome_id: "out_1",
			trace_id: "tr_1",
			state: "verified_success",
			promotion_eligible: true,
			policy_version: "policy.v1",
			evidence_count: 1,
			created_at: time::now(),
			schema_version: "outcome.v1",
			content_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		} RETURN id`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		t.Fatal("outcome record was not created")
	}
	_, _ = surrealdb.Query[any](context.Background(), db, `
		UPDATE outcome_evidence SET content_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`, nil)
	type hashRow struct {
		ContentHash string `json:"content_hash"`
	}
	selected, err := surrealdb.Query[[]hashRow](context.Background(), db, `SELECT content_hash FROM outcome_evidence`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected == nil || len(*selected) == 0 || len((*selected)[0].Result) != 1 || (*selected)[0].Result[0].ContentHash != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("immutable content_hash changed: %#v", selected)
	}
}

func TestEvidenceGraphRejectsCrossTenantEdge(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := surrealdb.Query[any](context.Background(), db, `
		CREATE procedure_version:one CONTENT {
			tenant_id: "tenant-a", procedure_version_id: "pv_one", procedure_id: "p_one",
			graph_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			environment_scope_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			policy_version: "policy.v1", goal_predicates: [], canonical_projection: "{}",
			lifecycle_state: "candidate", observed_end_to_end: true,
			opaque_step_count: 0, verified_success_count: 1, unsafe_outcome_count: 0,
			last_evidence_at: time::now(), created_at: time::now(), schema_version: "procedure.v1",
			content_hash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		};
		CREATE step:one CONTENT {
			tenant_id: "tenant-b", step_id: "step_one", procedure_version_id: "pv_one",
			ordinal: 0, event_id: "ev_one", opaque: false, uncertain_necessity: false,
			payload: {}, created_at: time::now(), schema_version: "step.v1",
			content_hash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
		};
		RELATE procedure_version:one->contains->step:one SET
			tenant_id = "tenant-a", created_at = time::now(), schema_version = "edge.v1",
			content_hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
	`, nil)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tenant") {
		t.Fatalf("error = %v, want cross-tenant relation rejection", err)
	}
}

func TestFoundationMigrationRejectsChecksumDrift(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	m := NewMigrator(db)
	if err := m.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	tampered := append([]store.Migration(nil), m.migrations...)
	tampered[0] = store.NewMigration(
		tampered[0].Version,
		tampered[0].Name,
		tampered[0].Statements+"\n-- changed after application\n",
	)

	err := newMigrator(db, tampered).Apply(context.Background())
	if !errors.Is(err, store.ErrMigrationChecksum) {
		t.Fatalf("error = %v, want %v", err, store.ErrMigrationChecksum)
	}
}

func TestConcurrentMigrationsConverge(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	const replicas = 8
	start := make(chan struct{})
	errorsSeen := make(chan error, replicas)
	var wait sync.WaitGroup
	for range replicas {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errorsSeen <- NewMigrator(db).Apply(context.Background())
		}()
	}
	close(start)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("concurrent migration: %v", err)
		}
	}
	if got := schemaVersion(t, db); got != 3 {
		t.Fatalf("schema version = %d, want 3", got)
	}
}

func TestRetrievalSchemaEnforcesTenantEpochAndImmutableRun(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := surrealdb.Query[any](context.Background(), db, `
		CREATE projection_epoch:tenant_a CONTENT {
			tenant_id: "tenant-a", epoch: 1, document_set_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			created_at: time::now(), schema_version: "projection-epoch.v1", content_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		};
		CREATE retrieval_run:run_a CONTENT {
			tenant_id: "tenant-a", retrieval_run_id: "run_a", query_hash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			canonical_query_envelope: "enc.v1.test", projection_epoch: 1,
			document_set_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", serving_config_id: "config_1",
			policy_manifest_id: "policy_1", ranker_manifest_id: "ranker_1", index_manifest_ids: [],
			candidate_version_ids: [], approximate_channels: [], degraded_channels: [], disposition: "abstained",
			abstention_code: "no_candidates", decision_code: "no_candidates", canonical_run: "{}",
			created_at: time::now(), completed_at: time::now(), expires_at: time::now() + 1d,
			schema_version: "retrieval-run.v1", content_hash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
		};`, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = surrealdb.Query[any](context.Background(), db, `UPDATE retrieval_run:run_a SET query_hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"`, nil)
	type row struct {
		QueryHash string `json:"query_hash"`
	}
	rows, err := surrealdb.Query[[]row](context.Background(), db, `SELECT query_hash FROM retrieval_run:run_a`, nil)
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) != 1 || (*rows)[0].Result[0].QueryHash != "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" {
		t.Fatalf("immutable retrieval run changed: rows=%#v err=%v", rows, err)
	}
	_, err = surrealdb.Query[any](context.Background(), db, `CREATE projection_epoch CONTENT { epoch: 2, document_set_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", created_at: time::now(), schema_version: "v1", content_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }`, nil)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tenant_id") {
		t.Fatalf("missing tenant accepted: %v", err)
	}
	plan, err := surrealdb.Query[any](context.Background(), db,
		`SELECT id FROM retrieval_document WHERE task_text @0@ "release" EXPLAIN FULL`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rendered := strings.ToLower(fmt.Sprintf("%#v", plan)); !strings.Contains(rendered, "retrieval_document_task_text") {
		t.Fatalf("full-text query did not use its index: %s", rendered)
	}
}

func schemaVersion(t *testing.T, db *surrealdb.DB) int {
	t.Helper()
	type row struct {
		Version int `json:"version"`
	}
	results, err := surrealdb.Query[[]row](context.Background(), db,
		"SELECT version FROM schema_migration ORDER BY version DESC LIMIT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(*results) == 0 || len((*results)[0].Result) == 0 {
		return 0
	}
	return (*results)[0].Result[0].Version
}
