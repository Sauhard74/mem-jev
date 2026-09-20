package jev

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
)

type fakeEvaluator struct {
	calls atomic.Int32
	wait  <-chan struct{}
	err   error
}

func (e *fakeEvaluator) Evaluate(ctx context.Context, _ any) (Evaluation, error) {
	e.calls.Add(1)
	if e.wait != nil {
		select {
		case <-ctx.Done():
			return Evaluation{}, ctx.Err()
		case <-e.wait:
		}
	}
	if e.err != nil {
		return Evaluation{}, e.err
	}
	return Evaluation{Model: "jev-1.13.0", Judgment: testJudgment(), Usage: Usage{InputTokens: 50, OutputTokens: 10}}, nil
}

func TestServiceCachesAndSingleFlightsJudgments(t *testing.T) {
	evaluator := &fakeEvaluator{}
	service := newTestService(t, evaluator, NewMemoryRepository(), ServiceLimits{})
	request := testServiceRequest(t, "tenant_a", 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	const callers = 32
	results := make(chan ServiceResult, callers)
	errorsFound := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := service.Judge(ctx, request)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- result
		}()
	}
	group.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	contentHash := ""
	for result := range results {
		if result.Disposition != DispositionCommitted && result.Disposition != DispositionCacheHit {
			t.Fatalf("unexpected disposition %q", result.Disposition)
		}
		if contentHash == "" {
			contentHash = result.Record.ContentHash
		}
		if result.Record.ContentHash != contentHash || len(result.Features) != 5 {
			t.Fatalf("inconsistent result %#v", result)
		}
	}
	if evaluator.calls.Load() != 1 {
		t.Fatalf("provider calls = %d; want 1", evaluator.calls.Load())
	}
	warm, err := service.Judge(ctx, request)
	if err != nil || warm.Disposition != DispositionCacheHit || evaluator.calls.Load() != 1 {
		t.Fatalf("warm result = %#v, %v calls=%d", warm, err, evaluator.calls.Load())
	}
}

func TestServiceRenewsExpiredJudgmentAsImmutableSuccessor(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	evaluator := &fakeEvaluator{}
	repository := NewMemoryRepository()
	service := newTestService(t, evaluator, repository, ServiceLimits{ReuseDuration: time.Hour})
	service.clock = func() time.Time { return now }
	request := testServiceRequest(t, "tenant_a", 77)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first, err := service.Judge(ctx, request)
	if err != nil || first.Disposition != DispositionCommitted {
		t.Fatalf("first judgment = %#v, %v", first, err)
	}
	now = now.Add(2 * time.Hour)
	renewed, err := service.Judge(ctx, request)
	if err != nil || renewed.Disposition != DispositionCommitted {
		t.Fatalf("renewed judgment = %#v, %v", renewed, err)
	}
	if renewed.Record.Key == first.Record.Key || renewed.Record.BaseKey != first.Record.BaseKey || renewed.Record.PredecessorHash != first.Record.ContentHash {
		t.Fatalf("renewal did not form a successor chain: first=%#v renewed=%#v", first.Record, renewed.Record)
	}
	warm, err := service.Judge(ctx, request)
	if err != nil || warm.Disposition != DispositionCacheHit || warm.Record.ContentHash != renewed.Record.ContentHash || evaluator.calls.Load() != 2 {
		t.Fatalf("warm renewed judgment = %#v, %v calls=%d", warm, err, evaluator.calls.Load())
	}
	current, err := repository.Current(ctx, request.KeyInput.TenantID, first.Record.BaseKey)
	if err != nil || current.ContentHash != renewed.Record.ContentHash {
		t.Fatalf("current judgment = %#v, %v", current, err)
	}
}

func TestServiceDegradesAndOpensCircuit(t *testing.T) {
	evaluator := &fakeEvaluator{err: &ProviderError{Code: FailureProvider, StatusCode: 500}}
	service := newTestService(t, evaluator, NewMemoryRepository(), ServiceLimits{CircuitFailureThreshold: 2, CircuitOpenDuration: time.Minute})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for index, want := range []string{FailureProvider, FailureProvider, DispositionCircuitOpen} {
		result, err := service.Judge(ctx, testServiceRequest(t, "tenant_a", index+1))
		if err != nil || result.Disposition != want || result.Record.Key != "" {
			t.Fatalf("attempt %d = %#v, %v; want %s", index, result, err, want)
		}
	}
	if evaluator.calls.Load() != 2 {
		t.Fatalf("provider calls = %d; want 2", evaluator.calls.Load())
	}
}

func TestServiceCapacityAndDeadlineDegradeWithoutProviderDependency(t *testing.T) {
	release := make(chan struct{})
	evaluator := &fakeEvaluator{wait: release}
	service := newTestService(t, evaluator, NewMemoryRepository(), ServiceLimits{MaximumConcurrent: 1, MinimumRemaining: 100 * time.Millisecond})
	firstDone := make(chan ServiceResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		result, _ := service.Judge(ctx, testServiceRequest(t, "tenant_a", 1))
		firstDone <- result
	}()
	deadline := time.Now().Add(time.Second)
	for evaluator.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, err := service.Judge(ctx, testServiceRequest(t, "tenant_a", 2))
	if err != nil || second.Disposition != DispositionCapacity {
		t.Fatalf("capacity result = %#v, %v", second, err)
	}
	close(release)
	if result := <-firstDone; result.Disposition != DispositionCommitted {
		t.Fatalf("first result = %#v", result)
	}
	short, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()
	deadlineResult, err := service.Judge(short, testServiceRequest(t, "tenant_a", 3))
	if err != nil || deadlineResult.Disposition != DispositionDeadline {
		t.Fatalf("deadline result = %#v, %v", deadlineResult, err)
	}
}

func TestServiceEnforcesTenantTokenBudget(t *testing.T) {
	evaluator := &fakeEvaluator{}
	service := newTestService(t, evaluator, NewMemoryRepository(), ServiceLimits{TenantTokenRate: 0.0001, TenantTokenBurst: 150})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := testServiceRequest(t, "tenant_a", 1)
	first.EstimatedTokens = 100
	if result, err := service.Judge(ctx, first); err != nil || result.Disposition != DispositionCommitted {
		t.Fatalf("first result = %#v, %v", result, err)
	}
	second := testServiceRequest(t, "tenant_a", 2)
	second.EstimatedTokens = 100
	if result, err := service.Judge(ctx, second); err != nil || result.Disposition != DispositionTenantRate {
		t.Fatalf("second result = %#v, %v", result, err)
	}
	if evaluator.calls.Load() != 1 {
		t.Fatalf("provider calls = %d; want 1", evaluator.calls.Load())
	}
}

func TestServiceAuthenticationFailureOpensCircuitImmediately(t *testing.T) {
	evaluator := &fakeEvaluator{err: &ProviderError{Code: FailureAuthentication, StatusCode: 401}}
	service := newTestService(t, evaluator, NewMemoryRepository(), ServiceLimits{CircuitFailureThreshold: 10})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := service.Judge(ctx, testServiceRequest(t, "tenant_a", 1))
	if err != nil || first.Disposition != FailureAuthentication {
		t.Fatalf("first result = %#v, %v", first, err)
	}
	second, err := service.Judge(ctx, testServiceRequest(t, "tenant_a", 2))
	if err != nil || second.Disposition != DispositionCircuitOpen || evaluator.calls.Load() != 1 {
		t.Fatalf("second result = %#v, %v calls=%d", second, err, evaluator.calls.Load())
	}
}

func TestLedgerFailureReleasesHalfOpenCircuitProbe(t *testing.T) {
	evaluator := &fakeEvaluator{}
	repository := &toggleCommitRepository{MemoryRepository: NewMemoryRepository(), fail: true}
	service := newTestService(t, evaluator, repository, ServiceLimits{CircuitOpenDuration: time.Millisecond})
	now := time.Now().UTC()
	service.clock = func() time.Time { return now.Add(2 * time.Millisecond) }
	service.circuit.Failure(now, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := service.Judge(ctx, testServiceRequest(t, "tenant_a", 91))
	if err != nil || first.Disposition != DispositionLedgerUnavailable {
		t.Fatalf("ledger failure = %#v, %v", first, err)
	}
	repository.fail = false
	second, err := service.Judge(ctx, testServiceRequest(t, "tenant_a", 92))
	if err != nil || second.Disposition != DispositionCommitted {
		t.Fatalf("probe remained stuck after ledger recovery: %#v, %v", second, err)
	}
}

type toggleCommitRepository struct {
	*MemoryRepository
	fail bool
}

func (repository *toggleCommitRepository) Commit(ctx context.Context, baseKey, expectedPredecessorHash string, record JudgmentRecord) (JudgmentRecord, bool, error) {
	if repository.fail {
		return JudgmentRecord{}, false, errors.New("ledger unavailable")
	}
	return repository.MemoryRepository.Commit(ctx, baseKey, expectedPredecessorHash, record)
}

func newTestService(t *testing.T, evaluator Evaluator, repository Repository, overrides ServiceLimits) *Service {
	t.Helper()
	rubric, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	limits := ServiceLimits{
		ReuseDuration: 24 * time.Hour, MinimumRemaining: time.Millisecond, MaximumConcurrent: 8,
		GlobalTokenRate: 10_000_000, GlobalTokenBurst: 10_000, TenantTokenRate: 10_000_000, TenantTokenBurst: 10_000,
		MaximumTenantLimiters: 1_024, CircuitFailureThreshold: 5, CircuitOpenDuration: time.Second,
	}
	if overrides.ReuseDuration != 0 {
		limits.ReuseDuration = overrides.ReuseDuration
	}
	if overrides.MinimumRemaining != 0 {
		limits.MinimumRemaining = overrides.MinimumRemaining
	}
	if overrides.MaximumConcurrent != 0 {
		limits.MaximumConcurrent = overrides.MaximumConcurrent
	}
	if overrides.GlobalTokenRate != 0 {
		limits.GlobalTokenRate = overrides.GlobalTokenRate
	}
	if overrides.GlobalTokenBurst != 0 {
		limits.GlobalTokenBurst = overrides.GlobalTokenBurst
	}
	if overrides.TenantTokenRate != 0 {
		limits.TenantTokenRate = overrides.TenantTokenRate
	}
	if overrides.TenantTokenBurst != 0 {
		limits.TenantTokenBurst = overrides.TenantTokenBurst
	}
	if overrides.CircuitFailureThreshold != 0 {
		limits.CircuitFailureThreshold = overrides.CircuitFailureThreshold
	}
	if overrides.CircuitOpenDuration != 0 {
		limits.CircuitOpenDuration = overrides.CircuitOpenDuration
	}
	service, err := NewService(ServiceConfig{Evaluator: evaluator, Repository: repository, Rubric: rubric, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testServiceRequest(t *testing.T, tenant string, suffix int) ServiceRequest {
	t.Helper()
	rubric, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	return ServiceRequest{
		KeyInput:        JudgmentKeyInput{TenantID: domainTenant(tenant), QueryHash: fmt.Sprintf("%064x", suffix), ProcedureVersionID: fmt.Sprintf("pver_%d", suffix), DocumentHash: fmt.Sprintf("%064x", suffix+100), EnvironmentHash: fmt.Sprintf("%064x", suffix+200), PolicyManifestID: "epol_1", RubricManifestID: rubric.ID, Provider: ProviderTypeSafe, Model: rubric.Model},
		State:           map[string]any{"task": "test", "procedure": suffix},
		EstimatedTokens: 100,
	}
}

func domainTenant(value string) domain.TenantID { return domain.TenantID(value) }

func testJudgment() Judgment {
	return Judgment{
		IntentFit:                    ScoreAnswer{ScoreMicros: 3_500_000, ConfidenceMicros: 800_000, ProbabilitiesMicros: []int32{0, 50_000, 100_000, 250_000, 600_000}},
		PreconditionsLikelySatisfied: NoulAnswer{NoulMicros: 900_000},
		TaskCoverage:                 ScoreAnswer{ScoreMicros: 2_000_000, ConfidenceMicros: 700_000, ProbabilitiesMicros: []int32{50_000, 100_000, 550_000, 250_000, 50_000}},
		ContradictsRequest:           NoulAnswer{NoulMicros: 125_000}, UsefulAsPartialPlan: NoulAnswer{NoulMicros: 600_000},
	}
}

func TestServiceRejectsInvalidRequest(t *testing.T) {
	service := newTestService(t, &fakeEvaluator{}, NewMemoryRepository(), ServiceLimits{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := testServiceRequest(t, "tenant_a", 1)
	request.KeyInput.QueryHash = "bad"
	_, err := service.Judge(ctx, request)
	if !errors.Is(err, ErrInvalidJudgment) {
		t.Fatalf("invalid request error = %v", err)
	}
}
