package outcome

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store"
)

var (
	ErrInvalidCommand    = errors.New("invalid outcome command")
	ErrPermissionDenied  = errors.New("outcome permission denied")
	ErrSensitiveEvidence = errors.New("outcome evidence contains credential material")
	ErrMisconfigured     = errors.New("outcome service is misconfigured")
)

var idempotencyHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Command struct {
	Principal          security.Principal
	IdempotencyKeyHash string
	Request            *memjevv1.RecordOutcomeRequest
}

type Result struct {
	ReceiptID         string
	OutcomeID         domain.OutcomeID
	TraceID           domain.TraceID
	State             domain.OutcomeState
	PromotionEligible bool
	PolicyVersion     string
	Disposition       store.OutcomeDisposition
	OutcomeCreditID   string
	CreditClass       credit.Class
}

type Service struct {
	repository  store.OutcomeRepository
	policy      evidence.Policy
	attribution *Attribution
	clock       func() time.Time
}

type Attribution struct {
	Selections selection.Repository
	Rules      credit.RuleManifest
}

func DefaultPolicy() evidence.Policy {
	return evidence.Policy{
		Version: "outcome-policy.v1", SuccessClassCeiling: domain.EvidenceClassGoalPredicate,
		FailureClassCeiling: domain.EvidenceClassGoalPredicate, StrongConflictCeiling: domain.EvidenceClassHarnessAssertion,
	}
}

func NewService(repository store.OutcomeRepository, evaluationPolicy evidence.Policy, attribution ...Attribution) *Service {
	cloned := evaluationPolicy
	cloned.RequiredPredicates = append([]string(nil), evaluationPolicy.RequiredPredicates...)
	service := &Service{repository: repository, policy: cloned, clock: time.Now}
	if len(attribution) == 1 && attribution[0].Selections != nil && attribution[0].Rules.ID != "" {
		value := attribution[0]
		service.attribution = &value
	}
	return service
}

func (s *Service) Record(ctx context.Context, command Command) (Result, error) {
	startedAt := time.Now()
	ctx, span := observability.StartSpan(ctx, "outcome.record")
	resultCode := "rejected"
	conflict := false
	defer func() {
		span.End()
		facts := observability.OutcomeMetrics{ResultCode: resultCode, Latency: time.Since(startedAt), Conflict: conflict}
		if command.Request != nil {
			facts.Evidence = len(command.Request.GetEvidence())
		}
		observability.RecordOutcome(ctx, facts)
	}()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if s == nil || s.repository == nil || s.policy.Version == "" || s.clock == nil {
		return Result{}, ErrMisconfigured
	}
	if command.Principal.TenantID == "" || command.Request == nil || !idempotencyHashPattern.MatchString(command.IdempotencyKeyHash) {
		return Result{}, ErrInvalidCommand
	}
	if !command.Principal.HasScope(security.ScopeOutcomeWrite) {
		return Result{}, ErrPermissionDenied
	}
	if err := policy.AuthorizeIngest(command.Principal.Consent); err != nil {
		return Result{}, fmt.Errorf("authorize outcome: %w", err)
	}
	if err := contracts.ValidateOutcome(command.Request); err != nil {
		return Result{}, fmt.Errorf("validate outcome: %w", ErrInvalidCommand)
	}
	if containsSensitiveEvidence(command.Request) {
		return Result{}, ErrSensitiveEvidence
	}
	canonicalOutcome, err := evidence.Canonicalize(command.Principal.TenantID, command.Request)
	if err != nil {
		return Result{}, fmt.Errorf("canonicalize outcome: %w", ErrInvalidCommand)
	}
	evaluationPolicy := s.policy
	if len(evaluationPolicy.RequiredPredicates) == 0 {
		evaluationPolicy.RequiredPredicates = inferredPredicates(canonicalOutcome.Evidence)
	}
	evaluation, err := evidence.Evaluate(evaluationPolicy, canonicalOutcome.Evidence, nil)
	if err != nil {
		return Result{}, fmt.Errorf("evaluate outcome: %w", ErrMisconfigured)
	}
	conflict = len(evaluation.ConflictPredicates) > 0
	var outcomeCredit *credit.Record
	if canonicalOutcome.InjectionID != "" || canonicalOutcome.TaskExecutionID != "" {
		if canonicalOutcome.InjectionID == "" || canonicalOutcome.TaskExecutionID == "" || s.attribution == nil {
			return Result{}, ErrInvalidCommand
		}
		selected, findErr := s.attribution.Selections.FindByInjectionID(ctx, command.Principal.TenantID, canonicalOutcome.InjectionID)
		if findErr != nil {
			return Result{}, fmt.Errorf("resolve injected plan: %w", findErr)
		}
		assigned, assignErr := credit.Assign(credit.AssignmentRequest{Manifest: s.attribution.Rules, Selection: selected, Outcome: canonicalOutcome, Evaluation: evaluation, InjectionID: canonicalOutcome.InjectionID, TaskExecutionID: canonicalOutcome.TaskExecutionID, Now: s.clock().UTC()})
		if assignErr != nil {
			return Result{}, fmt.Errorf("attribute outcome: %w", ErrInvalidCommand)
		}
		outcomeCredit = &assigned
	}
	receipt, err := s.repository.CommitOutcome(ctx, store.CommitOutcomeRequest{
		TenantID: command.Principal.TenantID, IdempotencyKeyHash: command.IdempotencyKeyHash,
		Outcome: canonicalOutcome, Evaluation: evaluation, Credit: outcomeCredit,
	})
	if err != nil {
		return Result{}, fmt.Errorf("commit outcome: %w", err)
	}
	resultCode = string(receipt.State)
	return Result{
		ReceiptID: receipt.ID, OutcomeID: receipt.OutcomeID, TraceID: receipt.TraceID, State: receipt.State,
		PromotionEligible: receipt.PromotionEligible, PolicyVersion: receipt.PolicyVersion, Disposition: receipt.Disposition, OutcomeCreditID: receipt.OutcomeCreditID, CreditClass: receipt.CreditClass,
	}, nil
}

func inferredPredicates(facts []domain.OutcomeEvidence) []string {
	seen := make(map[string]struct{}, len(facts))
	result := make([]string, 0, len(facts))
	for _, fact := range facts {
		if _, exists := seen[fact.PredicateID]; !exists {
			seen[fact.PredicateID] = struct{}{}
			result = append(result, fact.PredicateID)
		}
	}
	sort.Strings(result)
	return result
}

func containsSensitiveEvidence(request *memjevv1.RecordOutcomeRequest) bool {
	values := []string{request.GetTraceId(), request.GetExecutionId(), request.GetSelectionId(), request.GetInjectionId(), request.GetTaskExecutionId(), request.GetSupersedesOutcomeId(), request.GetCorrectionReason()}
	for _, fact := range request.GetEvidence() {
		values = append(values, fact.GetClientEvidenceId(), fact.GetPredicateId(), fact.GetVerifierId(), fact.GetVerifierVersion())
		for _, field := range fact.GetFields() {
			values = append(values, field.GetName(), field.GetStringValue())
		}
	}
	for _, value := range values {
		if security.ContainsCredentialMaterial(value) {
			return true
		}
	}
	return false
}
