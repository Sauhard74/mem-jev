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
