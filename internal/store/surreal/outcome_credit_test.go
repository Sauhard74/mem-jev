//go:build integration

package surreal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/experiment"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOutcomeCreditCommitsAtomicallyAndRejectsMultipleClaims(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := NewIngestRepository(db).Commit(context.Background(), storetest.ValidCommitRequest(t)); err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	selections, err := NewSelectionRepository(db, selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	selections.now = func() time.Time { return created }
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	receipt, err := selections.Commit(context.Background(), selection.CommitRequest{TenantID: "tenant_a", IdempotencyIdentityHash: strings.Repeat("d", 64), Draft: draft})
	if err != nil {
		t.Fatal(err)
	}
	rules, _ := credit.NewRuleManifest("credit.v1", 0)
	build := func(clientEvidenceID string) store.CommitOutcomeRequest {
		outcomeValue, canonicalErr := evidence.Canonicalize("tenant_a", &memjevv1.RecordOutcomeRequest{TraceId: "tr_" + strings.Repeat("a", 64), InjectionId: receipt.Record.InjectionID, TaskExecutionId: selection.TaskExecutionID(receipt.Record.InjectionID), Evidence: []*memjevv1.OutcomeEvidence{{ClientEvidenceId: clientEvidenceID, Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE, Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, PredicateId: "goal.done", VerifierId: "ci", ObservedAt: timestamppb.New(created.Add(time.Minute))}}})
		if canonicalErr != nil {
			t.Fatal(canonicalErr)
		}
		result := evidence.Result{State: domain.OutcomeStateVerifiedSuccess, PromotionEligible: true, PolicyVersion: "outcome-policy.v1"}
		assigned, assignErr := credit.Assign(credit.AssignmentRequest{Manifest: rules, Selection: receipt.Record, Outcome: outcomeValue, Evaluation: result, InjectionID: outcomeValue.InjectionID, TaskExecutionID: outcomeValue.TaskExecutionID, Now: created.Add(2 * time.Minute)})
		if assignErr != nil {
			t.Fatal(assignErr)
		}
		return store.CommitOutcomeRequest{TenantID: "tenant_a", IdempotencyKeyHash: strings.Repeat(clientEvidenceID[:1], 64), Outcome: outcomeValue, Evaluation: result, Credit: &assigned}
	}
	repository := NewOutcomeRepository(db)
	repository.now = func() time.Time { return created.Add(2 * time.Minute) }
	request := build("credit-a")
	accepted, err := repository.CommitOutcome(context.Background(), request)
	if err != nil || accepted.OutcomeCreditID == "" || accepted.CreditClass != credit.CausalSuccess {
		t.Fatalf("receipt=%#v error=%v", accepted, err)
	}
	duplicate, err := repository.CommitOutcome(context.Background(), request)
	if err != nil || duplicate.Disposition != store.OutcomeDispositionDuplicate || duplicate.OutcomeCreditID != accepted.OutcomeCreditID {
		t.Fatalf("duplicate=%#v error=%v", duplicate, err)
	}
	second := build("different")
	if _, err = repository.CommitOutcome(context.Background(), second); !errors.Is(err, store.ErrOutcomeCreditClaimed) {
		t.Fatalf("multiple claim error=%v", err)
	}
}

func TestCausalChallengerFailureUpdatesUnsafeBudgetsAtomically(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := NewIngestRepository(db).Commit(context.Background(), storetest.ValidCommitRequest(t)); err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 9, 20, 0, 10, 0, 0, time.UTC)
	manifest, err := experiment.NewManifest(experiment.ManifestSpec{Version: "experiment.v1", TenantID: "tenant_a", ProcedureID: "proc_a", ChampionVersionID: "pv_champion", ChallengerVersionIDs: []string{"pv_challenger"}, ExperimentEpoch: 1, ChallengerExposurePPM: experiment.BucketCount, MaximumConcurrentTrials: 1, TenantExposureLimit: 100, GlobalExposureLimit: 1000, TenantUnsafeOutcomeCeiling: 10, GlobalUnsafeOutcomeCeiling: 100, ExposureWindowSeconds: 3600, AssignmentTTLSeconds: 1800})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := NewExperimentRepository(db).Reserve(context.Background(), experiment.AssignmentRequest{Manifest: manifest, QueryBucketHash: strings.Repeat("e", 64), Risk: experiment.RiskLow, AssignedAt: created})
	if err != nil || !reservation.Assignment.Challenger {
		t.Fatalf("reservation=%#v err=%v", reservation, err)
	}
	base := storetest.SelectionDraft(t, "tenant_a", 'f')
	base.ExperimentManifestID, base.ExperimentAssignmentID = manifest.ID, reservation.Assignment.ID
	draft, err := selection.NewDraft(base)
	if err != nil {
		t.Fatal(err)
	}
	selections, err := NewSelectionRepository(db, selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	selections.now = func() time.Time { return created }
	selected, err := selections.Commit(context.Background(), selection.CommitRequest{TenantID: "tenant_a", IdempotencyIdentityHash: strings.Repeat("f", 64), Draft: draft})
	if err != nil {
		t.Fatal(err)
	}
	outcomeValue, err := evidence.Canonicalize("tenant_a", &memjevv1.RecordOutcomeRequest{TraceId: "tr_" + strings.Repeat("a", 64), InjectionId: selected.Record.InjectionID, TaskExecutionId: selection.TaskExecutionID(selected.Record.InjectionID), Evidence: []*memjevv1.OutcomeEvidence{{ClientEvidenceId: "unsafe", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE, Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED, PredicateId: "goal.done", VerifierId: "ci", ObservedAt: timestamppb.New(created.Add(time.Minute))}}})
	if err != nil {
		t.Fatal(err)
	}
	result := evidence.Result{State: domain.OutcomeStateVerifiedFailure, PolicyVersion: "outcome-policy.v1"}
	rules, _ := credit.NewRuleManifest("credit.v1", 0)
	assigned, err := credit.Assign(credit.AssignmentRequest{Manifest: rules, Selection: selected.Record, Outcome: outcomeValue, Evaluation: result, InjectionID: outcomeValue.InjectionID, TaskExecutionID: outcomeValue.TaskExecutionID, Now: created.Add(2 * time.Minute)})
	if err != nil || assigned.Class != credit.CausalFailure {
		t.Fatalf("credit=%#v err=%v", assigned, err)
	}
	repository := NewOutcomeRepository(db)
	repository.now = func() time.Time { return created.Add(2 * time.Minute) }
	request := store.CommitOutcomeRequest{TenantID: "tenant_a", IdempotencyKeyHash: strings.Repeat("9", 64), Outcome: outcomeValue, Evaluation: result, Credit: &assigned}
	if _, err := repository.CommitOutcome(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CommitOutcome(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	tenantBudget, err := readExperimentBudget(context.Background(), db, experimentBudgetID("experiment_tenant_budget_head", "tenant_a", reservation.Assignment.WindowStart))
	if err != nil {
		t.Fatal(err)
	}
	globalBudget, err := readExperimentBudget(context.Background(), db, experimentBudgetID("experiment_global_budget_head", "_global", reservation.Assignment.WindowStart))
	if err != nil || tenantBudget.Unsafe != 1 || globalBudget.Unsafe != 1 {
		t.Fatalf("tenant=%#v global=%#v err=%v", tenantBudget, globalBudget, err)
	}
	if err := NewMaintenanceRepository(db).VerifyExposureBudgets(context.Background(), "tenant_a", reservation.Assignment.WindowStart); err != nil {
		t.Fatalf("exposure audit: %v", err)
	}
}
