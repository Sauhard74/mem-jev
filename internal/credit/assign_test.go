package credit_test

import (
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAssignUsesConservativeVersionedRules(t *testing.T) {
	created := time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC)
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	selected, err := selection.NewRecord(draft, strings.Repeat("d", 64), created, created.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := credit.NewRuleManifest("credit.v1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		state   domain.OutcomeState
		verdict memjevv1.EvidenceVerdict
		goal    string
		at      time.Time
		want    credit.Class
	}{
		{name: "causal success", state: domain.OutcomeStateVerifiedSuccess, verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, goal: "goal.done", at: created.Add(time.Minute), want: credit.CausalSuccess},
		{name: "associated success without goal proof", state: domain.OutcomeStateVerifiedSuccess, verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, goal: "other", at: created.Add(time.Minute), want: credit.AssociatedSuccess},
		{name: "causal failure", state: domain.OutcomeStateVerifiedFailure, verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED, goal: "goal.done", at: created.Add(time.Minute), want: credit.CausalFailure},
		{name: "delayed outside window", state: domain.OutcomeStateVerifiedSuccess, verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, goal: "goal.done", at: created.Add(2 * time.Hour), want: credit.AssociatedSuccess},
		{name: "inconclusive", state: domain.OutcomeStateInconclusive, verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_UNKNOWN, goal: "goal.done", at: created.Add(time.Minute), want: credit.Unattributable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome, canonicalErr := evidence.Canonicalize("tenant_a", &memjevv1.RecordOutcomeRequest{TraceId: "tr_" + strings.Repeat("a", 64), Evidence: []*memjevv1.OutcomeEvidence{{ClientEvidenceId: "ev_1", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE, Verdict: test.verdict, PredicateId: test.goal, VerifierId: "verifier", ObservedAt: timestamppb.New(test.at)}}})
			if canonicalErr != nil {
				t.Fatal(canonicalErr)
			}
			record, assignErr := credit.Assign(credit.AssignmentRequest{Manifest: manifest, Selection: selected, Outcome: outcome, Evaluation: evidence.Result{State: test.state}, InjectionID: selected.InjectionID, TaskExecutionID: selection.TaskExecutionID(selected.InjectionID), Now: created.Add(10 * time.Minute)})
			if assignErr != nil || record.Class != test.want || credit.Validate(record) != nil {
				t.Fatalf("record=%#v error=%v", record, assignErr)
			}
		})
	}
}

func TestAssignRejectsForgedExpiredAndCrossTenantLinkage(t *testing.T) {
	created := time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC)
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	selected, err := selection.NewRecord(draft, strings.Repeat("d", 64), created, created.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := credit.NewRuleManifest("credit.v1", 0)
	outcome, err := evidence.Canonicalize("tenant_a", &memjevv1.RecordOutcomeRequest{TraceId: "tr_" + strings.Repeat("a", 64), Evidence: []*memjevv1.OutcomeEvidence{{ClientEvidenceId: "ev_1", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE, Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, PredicateId: "goal.done", VerifierId: "verifier", ObservedAt: timestamppb.New(created.Add(time.Minute))}}})
	if err != nil {
		t.Fatal(err)
	}
	base := credit.AssignmentRequest{Manifest: manifest, Selection: selected, Outcome: outcome, Evaluation: evidence.Result{State: domain.OutcomeStateVerifiedSuccess}, InjectionID: selected.InjectionID, TaskExecutionID: selection.TaskExecutionID(selected.InjectionID), Now: created.Add(10 * time.Minute)}
	for name, mutate := range map[string]func(*credit.AssignmentRequest){
		"forged execution": func(value *credit.AssignmentRequest) { value.TaskExecutionID = "texec_forged" },
		"forged injection": func(value *credit.AssignmentRequest) { value.InjectionID = "inj_forged" },
		"expired":          func(value *credit.AssignmentRequest) { value.Now = selected.ExpiresAt },
		"foreign tenant":   func(value *credit.AssignmentRequest) { value.Outcome.TenantID = "tenant_b" },
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			if _, assignErr := credit.Assign(request); assignErr == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

// The planner's own task goal ("goal_<hash>") is never shown to harnesses, so
// it is judged from all goal evidence on the outcome linked to the plan.
func TestAssignJudgesPlannerTaskGoalFromOutcomeGoalEvidence(t *testing.T) {
	created := time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC)
	draft := storetest.SelectionDraftWithGoal(t, "tenant_a", 'a', "goal_"+strings.Repeat("e", 64))
	selected, err := selection.NewRecord(draft, strings.Repeat("d", 64), created, created.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := credit.NewRuleManifest("credit.v1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	satisfied, failed := memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED
	tests := []struct {
		name     string
		state    domain.OutcomeState
		verdicts []memjevv1.EvidenceVerdict
		want     credit.Class
	}{
		{name: "verified success with all goals satisfied", state: domain.OutcomeStateVerifiedSuccess, verdicts: []memjevv1.EvidenceVerdict{satisfied, satisfied}, want: credit.CausalSuccess},
		{name: "provisional success never counts", state: domain.OutcomeStateProvisionalSuccess, verdicts: []memjevv1.EvidenceVerdict{satisfied}, want: credit.AssociatedSuccess},
		{name: "verified failure", state: domain.OutcomeStateVerifiedFailure, verdicts: []memjevv1.EvidenceVerdict{satisfied, failed}, want: credit.CausalFailure},
		{name: "verified success with a failed goal", state: domain.OutcomeStateVerifiedSuccess, verdicts: []memjevv1.EvidenceVerdict{satisfied, failed}, want: credit.AssociatedSuccess},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			facts := make([]*memjevv1.OutcomeEvidence, len(test.verdicts))
			for index, verdict := range test.verdicts {
				facts[index] = &memjevv1.OutcomeEvidence{ClientEvidenceId: "ev_" + string(rune('a'+index)), Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE, Verdict: verdict, PredicateId: "harness-goal-" + string(rune('a'+index)), VerifierId: "verifier", ObservedAt: timestamppb.New(created.Add(time.Minute))}
			}
			outcome, err := evidence.Canonicalize("tenant_a", &memjevv1.RecordOutcomeRequest{TraceId: "tr_" + strings.Repeat("a", 64), Evidence: facts})
			if err != nil {
				t.Fatal(err)
			}
			record, err := credit.Assign(credit.AssignmentRequest{Manifest: manifest, Selection: selected, Outcome: outcome, Evaluation: evidence.Result{State: test.state}, InjectionID: selected.InjectionID, TaskExecutionID: selection.TaskExecutionID(selected.InjectionID), Now: created.Add(10 * time.Minute)})
			if err != nil || record.Class != test.want {
				t.Fatalf("class = %s, want %s (err %v)", record.Class, test.want, err)
			}
		})
	}
}
