package evidence_test

import (
	"bytes"
	"slices"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCanonicalOutcomeIsIndependentOfEvidenceAndFieldOrder(t *testing.T) {
	requestA := outcomeRequest()
	requestA.Evidence = append(requestA.Evidence, &memjevv1.OutcomeEvidence{
		ClientEvidenceId: "evidence-2",
		Class:            memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
		Verdict:          memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED,
		PredicateId:      "artifact-exists",
		VerifierId:       "artifact-store",
		ObservedAt:       timestamppb.New(time.Unix(3, 0).UTC()),
		Fields: []*memjevv1.Field{
			{Name: "z", StringValue: "last"},
			{Name: "a", StringValue: "first"},
		},
	})
	requestB := proto.Clone(requestA).(*memjevv1.RecordOutcomeRequest)
	slices.Reverse(requestB.Evidence)
	slices.Reverse(requestB.Evidence[0].Fields)

	gotA, err := evidence.Canonicalize(domain.TenantID("tenant-1"), requestA)
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := evidence.Canonicalize(domain.TenantID("tenant-1"), requestB)
	if err != nil {
		t.Fatal(err)
	}
	if gotA.ID != gotB.ID || gotA.Hash != gotB.Hash || !bytes.Equal(gotA.CanonicalJSON, gotB.CanonicalJSON) {
		t.Fatalf("canonical outcomes differ:\n%#v\n%#v", gotA, gotB)
	}
}

func TestCanonicalOutcomeIdentityIsTenantBound(t *testing.T) {
	request := outcomeRequest()
	first, err := evidence.Canonicalize(domain.TenantID("tenant-1"), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := evidence.Canonicalize(domain.TenantID("tenant-2"), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.Evidence[0].ID == second.Evidence[0].ID {
		t.Fatal("outcome and evidence identities must be tenant-bound")
	}
}

func TestCanonicalOutcomeRejectsFieldEmptyAfterNormalization(t *testing.T) {
	request := outcomeRequest()
	request.Evidence[0].Fields = []*memjevv1.Field{{Name: "  ", StringValue: "value"}}

	if _, err := evidence.Canonicalize(domain.TenantID("tenant-1"), request); err == nil {
		t.Fatal("Canonicalize() error = nil; want normalized empty field rejection")
	}
}

func TestEvaluateEvidenceHierarchy(t *testing.T) {
	policy := evidence.Policy{
		Version:               "outcome-policy.v1",
		RequiredPredicates:    []string{"goal"},
		SuccessClassCeiling:   domain.EvidenceClassGoalPredicate,
		FailureClassCeiling:   domain.EvidenceClassGoalPredicate,
		StrongConflictCeiling: domain.EvidenceClassHarnessAssertion,
	}
	tests := []struct {
		name      string
		evidence  []domain.OutcomeEvidence
		wantState domain.OutcomeState
		promote   bool
	}{
		{
			name:      "independent success promotes",
			evidence:  []domain.OutcomeEvidence{fact("one", domain.EvidenceClassIndependentVerifier, domain.EvidenceVerdictSatisfied)},
			wantState: domain.OutcomeStateVerifiedSuccess, promote: true,
		},
		{
			name:      "goal failure is verified failure",
			evidence:  []domain.OutcomeEvidence{fact("one", domain.EvidenceClassGoalPredicate, domain.EvidenceVerdictFailed)},
			wantState: domain.OutcomeStateVerifiedFailure,
		},
		{
			name:      "harness success remains provisional",
			evidence:  []domain.OutcomeEvidence{fact("one", domain.EvidenceClassHarnessAssertion, domain.EvidenceVerdictSatisfied)},
			wantState: domain.OutcomeStateProvisionalSuccess,
		},
		{
			name:      "exit status cannot prove the goal",
			evidence:  []domain.OutcomeEvidence{fact("one", domain.EvidenceClassExitStatusOrSelfReport, domain.EvidenceVerdictSatisfied)},
			wantState: domain.OutcomeStateProvisionalSuccess,
		},
		{
			name: "strong contradiction is inconclusive",
			evidence: []domain.OutcomeEvidence{
				fact("one", domain.EvidenceClassGoalPredicate, domain.EvidenceVerdictSatisfied),
				fact("two", domain.EvidenceClassGoalPredicate, domain.EvidenceVerdictFailed),
			},
			wantState: domain.OutcomeStateInconclusive,
		},
		{
			name: "stronger proof dominates weaker self report",
			evidence: []domain.OutcomeEvidence{
				fact("one", domain.EvidenceClassIndependentVerifier, domain.EvidenceVerdictSatisfied),
				fact("two", domain.EvidenceClassExitStatusOrSelfReport, domain.EvidenceVerdictFailed),
			},
			wantState: domain.OutcomeStateVerifiedSuccess, promote: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evidence.Evaluate(policy, tt.evidence, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != tt.wantState || got.PromotionEligible != tt.promote {
				t.Fatalf("Evaluate() = %#v; want state %q promote=%v", got, tt.wantState, tt.promote)
			}
		})
	}
}

func TestEvaluateIgnoresExplicitlySupersededEvidence(t *testing.T) {
	old := fact("old", domain.EvidenceClassGoalPredicate, domain.EvidenceVerdictFailed)
	replacement := fact("new", domain.EvidenceClassGoalPredicate, domain.EvidenceVerdictSatisfied)
	got, err := evidence.Evaluate(evidence.Policy{
		Version:               "outcome-policy.v1",
		RequiredPredicates:    []string{"goal"},
		SuccessClassCeiling:   domain.EvidenceClassGoalPredicate,
		FailureClassCeiling:   domain.EvidenceClassGoalPredicate,
		StrongConflictCeiling: domain.EvidenceClassHarnessAssertion,
	}, []domain.OutcomeEvidence{old, replacement}, map[domain.EvidenceID]struct{}{old.ID: {}})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.OutcomeStateVerifiedSuccess || !got.PromotionEligible {
		t.Fatalf("Evaluate() = %#v; superseded failure must not conflict", got)
	}
}

func TestEvaluateFailsClosedForInvalidPolicy(t *testing.T) {
	if _, err := evidence.Evaluate(evidence.Policy{}, []domain.OutcomeEvidence{fact("one", domain.EvidenceClassGoalPredicate, domain.EvidenceVerdictSatisfied)}, nil); err == nil {
		t.Fatal("Evaluate() error = nil; want invalid policy")
	}
}

func outcomeRequest() *memjevv1.RecordOutcomeRequest {
	return &memjevv1.RecordOutcomeRequest{
		TraceId:     "tr_abc",
		ExecutionId: "execution-1",
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "evidence-1",
			Class:            memjevv1.EvidenceClass_EVIDENCE_CLASS_INDEPENDENT_VERIFIER,
			Verdict:          memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED,
			PredicateId:      "goal",
			VerifierId:       "ci",
			VerifierVersion:  "1.0.0",
			ObservedAt:       timestamppb.New(time.Unix(2, 0).UTC()),
		}},
	}
}

func fact(id string, class domain.EvidenceClass, verdict domain.EvidenceVerdict) domain.OutcomeEvidence {
	return domain.OutcomeEvidence{
		ID:               domain.EvidenceID("oe_" + id),
		ClientEvidenceID: id,
		Class:            class,
		Verdict:          verdict,
		PredicateID:      "goal",
		VerifierID:       "verifier",
		ObservedAt:       "1970-01-01T00:00:02Z",
	}
}
