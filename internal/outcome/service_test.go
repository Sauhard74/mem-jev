package outcome_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServiceEvaluatesAndCommitsVerifiedOutcome(t *testing.T) {
	repository := &recordingRepository{}
	service := outcome.NewService(repository, outcome.DefaultPolicy())
	result, err := service.Record(context.Background(), validCommand())
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.OutcomeStateVerifiedSuccess || !result.PromotionEligible {
		t.Fatalf("result = %#v", result)
	}
	if repository.request.TenantID != "tenant_a" || repository.request.Outcome.TenantID != "tenant_a" || repository.request.Evaluation.State != domain.OutcomeStateVerifiedSuccess {
		t.Fatalf("commit = %#v", repository.request)
	}
}

func TestServiceKeepsWeakSuccessProvisional(t *testing.T) {
	repository := &recordingRepository{}
	service := outcome.NewService(repository, outcome.DefaultPolicy())
	command := validCommand()
	command.Request.Evidence[0].Class = memjevv1.EvidenceClass_EVIDENCE_CLASS_HARNESS_ASSERTION
	result, err := service.Record(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.OutcomeStateProvisionalSuccess || result.PromotionEligible {
		t.Fatalf("result = %#v", result)
	}
}

func TestServiceMarksStrongConflictInconclusive(t *testing.T) {
	repository := &recordingRepository{}
	service := outcome.NewService(repository, outcome.DefaultPolicy())
	command := validCommand()
	failure := proto.Clone(command.Request.Evidence[0]).(*memjevv1.OutcomeEvidence)
	failure.ClientEvidenceId = "evidence-2"
	failure.Verdict = memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED
	command.Request.Evidence = append(command.Request.Evidence, failure)
	result, err := service.Record(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.OutcomeStateInconclusive || result.PromotionEligible || len(repository.request.Evaluation.ConflictPredicates) != 1 {
		t.Fatalf("result=%#v evaluation=%#v", result, repository.request.Evaluation)
	}
}

func TestServiceAttributesOutcomeToCommittedInjection(t *testing.T) {
	now := time.Now().UTC()
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	selected, err := selection.NewRecord(draft, strings.Repeat("d", 64), now.Add(-time.Minute), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	rules, err := credit.NewRuleManifest("credit.v1", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	repository := &recordingRepository{}
	service := outcome.NewService(repository, outcome.DefaultPolicy(), outcome.Attribution{Selections: fixedSelectionRepository{record: selected}, Rules: rules})
	command := validCommand()
	command.Request.InjectionId = selected.InjectionID
	command.Request.TaskExecutionId = selection.TaskExecutionID(selected.InjectionID)
	command.Request.Evidence[0].PredicateId = "goal.done"
	command.Request.Evidence[0].ObservedAt = timestamppb.New(now)
	result, err := service.Record(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if repository.request.Credit == nil || repository.request.Credit.Class != credit.CausalSuccess || result.CreditClass != credit.CausalSuccess || result.OutcomeCreditID == "" {
		t.Fatalf("request=%#v result=%#v", repository.request, result)
	}
}

func TestServiceRejectsAuthorizationAndSensitiveEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*outcome.Command)
		want   error
	}{
		{name: "tenant", mutate: func(command *outcome.Command) { command.Principal.TenantID = "" }, want: outcome.ErrInvalidCommand},
		{name: "scope", mutate: func(command *outcome.Command) { command.Principal.Scopes = nil }, want: outcome.ErrPermissionDenied},
		{name: "consent", mutate: func(command *outcome.Command) { command.Principal.Consent = policy.RecallOnly }, want: policy.ErrConsentDenied},
		{name: "idempotency", mutate: func(command *outcome.Command) { command.IdempotencyKeyHash = "raw" }, want: outcome.ErrInvalidCommand},
		{name: "secret identity", mutate: func(command *outcome.Command) {
			command.Request.Evidence[0].VerifierId = "sk-live-AbCdEfGhIjKlMnOpQrStUvWx"
		}, want: outcome.ErrSensitiveEvidence},
		{name: "secret value", mutate: func(command *outcome.Command) {
			command.Request.Evidence[0].Fields = []*memjevv1.Field{{Name: "message", StringValue: "Authorization: Bearer sk-secret-value"}}
		}, want: outcome.ErrSensitiveEvidence},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &recordingRepository{}
			service := outcome.NewService(repository, outcome.DefaultPolicy())
			command := validCommand()
			tt.mutate(&command)
			_, err := service.Record(context.Background(), command)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Record() error = %v; want %v", err, tt.want)
			}
			if strings.Contains(err.Error(), "sk-") || repository.called {
				t.Fatalf("unsafe error or repository call: error=%v called=%v", err, repository.called)
			}
		})
	}
}

func validCommand() outcome.Command {
	return outcome.Command{
		Principal: security.Principal{
			TenantID: "tenant_a", Consent: policy.LearnAndRecall,
			Scopes: map[string]struct{}{security.ScopeOutcomeWrite: {}},
		},
		IdempotencyKeyHash: strings.Repeat("c", 64),
		Request: &memjevv1.RecordOutcomeRequest{
			TraceId: "tr_" + strings.Repeat("a", 64), ExecutionId: "execution-1",
			Evidence: []*memjevv1.OutcomeEvidence{{
				ClientEvidenceId: "evidence-1", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
				Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, PredicateId: "goal",
				VerifierId: "ci", VerifierVersion: "1.0.0", ObservedAt: timestamppb.New(time.Unix(2, 0).UTC()),
			}},
		},
	}
}

type recordingRepository struct {
	called  bool
	request store.CommitOutcomeRequest
	err     error
}

type fixedSelectionRepository struct{ record selection.Record }

func (r fixedSelectionRepository) Commit(context.Context, selection.CommitRequest) (selection.Receipt, error) {
	return selection.Receipt{}, errors.New("not implemented")
}
func (r fixedSelectionRepository) FindByInjectionID(_ context.Context, tenantID domain.TenantID, injectionID string) (selection.Record, error) {
	if tenantID != r.record.TenantID || injectionID != r.record.InjectionID {
		return selection.Record{}, selection.ErrSelectionNotFound
	}
	return selection.CloneRecord(r.record), nil
}
func (r fixedSelectionRepository) FindByRetrievalRunID(context.Context, domain.TenantID, string) (selection.Record, error) {
	return selection.Record{}, selection.ErrSelectionNotFound
}

func (r *recordingRepository) CommitOutcome(_ context.Context, request store.CommitOutcomeRequest) (store.OutcomeReceipt, error) {
	r.called = true
	r.request = request
	if r.err != nil {
		return store.OutcomeReceipt{}, r.err
	}
	return store.OutcomeReceipt{
		ID: "orcpt_1", TenantID: request.TenantID, OutcomeID: request.Outcome.ID, TraceID: request.Outcome.TraceID,
		ContentHash: request.Outcome.Hash, State: request.Evaluation.State, PromotionEligible: request.Evaluation.PromotionEligible,
		PolicyVersion: request.Evaluation.PolicyVersion, Disposition: store.OutcomeDispositionAccepted,
		OutcomeCreditID: func() string {
			if request.Credit != nil {
				return request.Credit.ID
			}
			return ""
		}(),
		CreditClass: func() credit.Class {
			if request.Credit != nil {
				return request.Credit.Class
			}
			return ""
		}(),
	}, nil
}
