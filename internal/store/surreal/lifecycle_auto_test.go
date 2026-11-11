//go:build integration

package surreal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/lifecycle"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

var creditSequence int

func seedCausalCredit(t *testing.T, db *surrealdb.DB, tenantID domain.TenantID, versionID, class string, at time.Time) {
	t.Helper()
	creditSequence++
	_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY outcome_credit CONTENT $record`, map[string]any{"record": map[string]any{
		"tenant_id": string(tenantID), "outcome_credit_id": fmt.Sprintf("ocred_%d", creditSequence), "outcome_id": fmt.Sprintf("out_credit_%d", creditSequence),
		"injection_id": fmt.Sprintf("inj_%d", creditSequence), "task_execution_id": fmt.Sprintf("texec_%d", creditSequence), "procedure_version_id": versionID,
		"credit_class": class, "rule_manifest_id": "credit.v1", "selection_hash": fmt.Sprintf("%064x", 7000+creditSequence), "reason_codes": []string{}, "canonical_credit": "{}",
		"created_at": models.CustomDateTime{Time: at}, "expires_at": models.CustomDateTime{Time: at.Add(24 * time.Hour)},
		"schema_version": "outcome-credit.v1", "content_hash": fmt.Sprintf("%064x", 9000+creditSequence),
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func documentLifecycle(t *testing.T, db *surrealdb.DB, tenantID domain.TenantID, versionID string) string {
	t.Helper()
	document, _, err := currentLifecycleDocument(context.Background(), db, tenantID, versionID)
	if err != nil {
		t.Fatal(err)
	}
	return document.Lifecycle
}

func TestEvaluateScopePromotesAndDemotesOnVerifiedCausalOutcomes(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	value := storetest.ValidProjectionForTenant(t, "tenant_a", 1, false)
	if _, err := NewProjectionRepository(db).Publish(ctx, value); err != nil {
		t.Fatal(err)
	}
	versionID := value.Version.ID
	manifest, err := lifecycle.DefaultPolicy()
	if err != nil {
		t.Fatal(err)
	}
	repository := NewLifecycleRepository(db)
	start := time.Now().UTC().Truncate(time.Second)

	// Never tried: no decision is recorded.
	if decisions, err := repository.EvaluateScope(ctx, "tenant_a", manifest, start); err != nil || len(decisions) != 0 {
		t.Fatalf("untried procedure decisions = %#v, %v", decisions, err)
	}
	// Two successes: evidence changed, so the maintained state is recorded.
	for range 2 {
		seedCausalCredit(t, db, "tenant_a", versionID, "causal_success", start.Add(-time.Minute))
	}
	decisions, err := repository.EvaluateScope(ctx, "tenant_a", manifest, start)
	if err != nil || len(decisions) != 1 || decisions[0].NextState != lifecycle.Candidate {
		t.Fatalf("two successes = %#v, %v", decisions, err)
	}
	// Nothing changed: nothing new is recorded.
	if decisions, err = repository.EvaluateScope(ctx, "tenant_a", manifest, start.Add(time.Second)); err != nil || len(decisions) != 0 {
		t.Fatalf("unchanged evidence = %#v, %v", decisions, err)
	}
	// Third success promotes to trial and republishes the document.
	seedCausalCredit(t, db, "tenant_a", versionID, "causal_success", start)
	decisions, err = repository.EvaluateScope(ctx, "tenant_a", manifest, start.Add(time.Minute))
	if err != nil || len(decisions) != 1 || decisions[0].NextState != lifecycle.Trial || documentLifecycle(t, db, "tenant_a", versionID) != "trial" {
		t.Fatalf("promotion to trial = %#v, %v", decisions, err)
	}
	// Ten successes after an hour in trial promote to active.
	for range 7 {
		seedCausalCredit(t, db, "tenant_a", versionID, "causal_success", start.Add(time.Hour))
	}
	decisions, err = repository.EvaluateScope(ctx, "tenant_a", manifest, start.Add(2*time.Hour))
	if err != nil || len(decisions) != 1 || decisions[0].NextState != lifecycle.Active || documentLifecycle(t, db, "tenant_a", versionID) != "active" {
		t.Fatalf("promotion to active = %#v, %v", decisions, err)
	}
	// Verified failures push the success-rate bound below the active floor.
	for range 5 {
		seedCausalCredit(t, db, "tenant_a", versionID, "causal_failure", start.Add(3*time.Hour))
	}
	decisions, err = repository.EvaluateScope(ctx, "tenant_a", manifest, start.Add(3*time.Hour))
	if err != nil || len(decisions) != 1 || decisions[0].NextState != lifecycle.Retired || documentLifecycle(t, db, "tenant_a", versionID) != "retired" {
		t.Fatalf("demotion = %#v, %v", decisions, err)
	}
	rows, err := surrealdb.Query[[]map[string]any](ctx, db, `SELECT next_state, created_at FROM lifecycle_decision WHERE tenant_id = "tenant_a" ORDER BY created_at`, nil)
	if err != nil || len((*rows)[0].Result) != 4 {
		t.Fatalf("lifecycle_decision rows = %#v, %v", rows, err)
	}
}

func TestEvaluateScopeKeepsPeopleSeparate(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	seedScopeOutcomes(t, db, "tenant_a__alice")
	shared := storetest.ValidProjectionForTenant(t, "tenant_a", 1, false)
	alice := storetest.ValidProjectionForTenant(t, "tenant_a__alice", 1, false)
	if _, err := NewProjectionRepository(db).Publish(ctx, shared); err != nil {
		t.Fatal(err)
	}
	if _, err := NewProjectionRepository(db).Publish(ctx, alice); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for range 3 {
		seedCausalCredit(t, db, "tenant_a__alice", alice.Version.ID, "causal_success", now.Add(-time.Minute))
	}
	manifest, err := lifecycle.DefaultPolicy()
	if err != nil {
		t.Fatal(err)
	}
	var decided []domain.TenantID
	err = lifecycle.EvaluateAll(ctx, NewLifecycleRepository(db), manifest, now, nil, func(decision lifecycle.Decision) {
		decided = append(decided, decision.TenantID)
	})
	if err != nil || len(decided) != 1 || decided[0] != "tenant_a__alice" {
		t.Fatalf("decided scopes = %v, %v", decided, err)
	}
	if documentLifecycle(t, db, "tenant_a__alice", alice.Version.ID) != "trial" || documentLifecycle(t, db, "tenant_a", shared.Version.ID) != "candidate" {
		t.Fatal("one person's outcomes changed another scope's procedure")
	}
}

// Learning the same procedure again must keep its promotion, and later
// evaluations must not be blocked by the newer document epoch.
func TestRelearningKeepsPromotionAndEvaluationContinues(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	first := storetest.ValidProjectionForTenant(t, "tenant_a", 1, false)
	if _, err := NewProjectionRepository(db).Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	versionID := first.Version.ID
	manifest, err := lifecycle.DefaultPolicy()
	if err != nil {
		t.Fatal(err)
	}
	repository := NewLifecycleRepository(db)
	now := time.Now().UTC().Truncate(time.Second)
	for range 3 {
		seedCausalCredit(t, db, "tenant_a", versionID, "causal_success", now.Add(-time.Minute))
	}
	if decisions, err := repository.EvaluateScope(ctx, "tenant_a", manifest, now); err != nil || len(decisions) != 1 || decisions[0].NextState != lifecycle.Trial {
		t.Fatalf("promotion = %#v, %v", decisions, err)
	}
	second := storetest.ValidProjectionForTenant(t, "tenant_a", 2, false)
	if second.Version.ID != versionID {
		t.Skip("fixture does not relearn the same version")
	}
	if _, err := NewProjectionRepository(db).Publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	if state := documentLifecycle(t, db, "tenant_a", versionID); state != "trial" {
		t.Fatalf("relearning reset lifecycle to %s", state)
	}
	seedCausalCredit(t, db, "tenant_a", versionID, "causal_failure", now)
	if decisions, err := repository.EvaluateScope(ctx, "tenant_a", manifest, now.Add(time.Minute)); err != nil || len(decisions) != 1 || decisions[0].PriorState != lifecycle.Trial {
		t.Fatalf("evaluation after relearning = %#v, %v", decisions, err)
	}
}
