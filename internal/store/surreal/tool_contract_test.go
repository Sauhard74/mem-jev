//go:build integration

package surreal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func printContract(version string, risk toolcontract.RiskClass) toolcontract.Manifest {
	return toolcontract.Manifest{
		SchemaVersion: "tool-contract.v1", ToolID: "quickstart_print", Version: version,
		Inputs:     []toolcontract.FieldSpec{{Name: "command", Type: "string", Required: true, Sanitizer: toolcontract.SanitizerRedactSecrets}},
		SideEffect: toolcontract.SideEffectNone, Risk: risk,
		Idempotency:         toolcontract.IdempotencySpec{Mode: toolcontract.IdempotencyGuaranteed},
		Retry:               toolcontract.RetrySpec{Mode: toolcontract.RetryOnDeclaredTransient, MaximumAttempts: 2},
		SuccessPredicates:   []toolcontract.PredicateSpec{{ID: "quickstart-goal", Field: "command", Operator: "exists"}},
		VerificationMethods: []toolcontract.VerificationMethod{{ID: "quickstart-user", EvidenceClass: string(domain.EvidenceClassGoalPredicate)}},
		Compatibility:       []toolcontract.CompatibilityRange{{MinimumInclusive: "1.0.0", MaximumExclusive: "2.0.0"}},
	}
}

func TestToolContractRegistrationAppliesToTenantAndItsPeople(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	repository := NewToolContractRepository(db)

	first, err := repository.Register(ctx, "tenant_a", printContract("1.0.0", toolcontract.RiskLow))
	if err != nil || !first.Created || first.ContractVersionID == "" {
		t.Fatalf("first registration = %#v, %v", first, err)
	}
	again, err := repository.Register(ctx, "tenant_a", printContract("1.0.0", toolcontract.RiskLow))
	if err != nil || again.Created || again.ContractVersionID != first.ContractVersionID {
		t.Fatalf("repeat registration = %#v, %v", again, err)
	}
	if _, err = repository.Register(ctx, "tenant_a", printContract("1.0.0", toolcontract.RiskHigh)); !errors.Is(err, toolcontract.ErrVersionConflict) {
		t.Fatalf("changed contract for a registered version: err = %v", err)
	}
	if _, err = repository.Register(ctx, "tenant_a__alice", printContract("1.0.0", toolcontract.RiskLow)); err == nil {
		t.Fatal("registration for a per-person scope accepted")
	}
	// The same contract in another tenant is a separate record.
	other, err := repository.Register(ctx, "tenant_b", printContract("1.0.0", toolcontract.RiskLow))
	if err != nil || !other.Created || other.ContractVersionID != first.ContractVersionID {
		t.Fatalf("other tenant registration = %#v, %v", other, err)
	}

	provider := NewTenantRegistryProvider(db)
	for _, scope := range []domain.TenantID{"tenant_a", "tenant_a__alice", "tenant_a__h_0123456789abcdef0123456789abcdef01234567"} {
		resolution, err := provider.ForTenant(scope).Resolve(ctx, toolcontract.Query{Name: "quickstart_print", Version: "1.0.0"})
		if err != nil || resolution.Opaque || !resolution.AutoPromotable || resolution.Manifest.ID != first.ContractVersionID {
			t.Fatalf("%s resolution = %#v, %v", scope, resolution, err)
		}
	}
	resolution, err := provider.ForTenant("tenant_c__alice").Resolve(ctx, toolcontract.Query{Name: "quickstart_print", Version: "1.0.0"})
	if err != nil || !resolution.Opaque {
		t.Fatalf("contract leaked to an unrelated tenant: %#v, %v", resolution, err)
	}
}

func seedScopeOutcomes(t *testing.T, db *surrealdb.DB, tenantID domain.TenantID) {
	t.Helper()
	for number := 1; number <= 2; number++ {
		_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY outcome_evidence CONTENT $record`, map[string]any{
			"record": map[string]any{
				"tenant_id": string(tenantID), "outcome_id": fmt.Sprintf("out_%064x", number),
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
}

// Two people learning the same routine share prefix hashes. Each scope must
// get its own prefix record instead of failing on the second publish.
func TestSameRoutineLearnedInTwoScopesPublishesInBoth(t *testing.T) {
	ctx := context.Background()
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	seedScopeOutcomes(t, db, "tenant_a__alice")
	repository := NewProjectionRepository(db)
	if _, err := repository.Publish(ctx, storetest.ValidProjectionForTenant(t, "tenant_a", 1, false)); err != nil {
		t.Fatalf("shared scope publish: %v", err)
	}
	// Alice's scope uses the contracts registered for tenant_a.
	if _, err := repository.Publish(ctx, storetest.ValidProjectionForTenant(t, "tenant_a__alice", 1, false)); err != nil {
		t.Fatalf("person scope publish of the same routine: %v", err)
	}
	rows, err := surrealdb.Query[[]struct {
		TenantID string `json:"tenant_id"`
	}](ctx, db, `SELECT tenant_id FROM procedure_prefix`, nil)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, row := range (*rows)[0].Result {
		counts[row.TenantID]++
	}
	if counts["tenant_a"] == 0 || counts["tenant_a"] != counts["tenant_a__alice"] {
		t.Fatalf("prefix records per scope = %v", counts)
	}
}
