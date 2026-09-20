package jev

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

const (
	DispositionCacheHit          = "cache_hit"
	DispositionCommitted         = "committed"
	DispositionDeadline          = "deadline_budget"
	DispositionTenantRate        = "tenant_token_budget_exhausted"
	DispositionGlobalRate        = "global_token_budget_exhausted"
	DispositionCapacity          = "capacity_exhausted"
	DispositionCircuitOpen       = "circuit_open"
	DispositionLedgerUnavailable = "ledger_unavailable"
)

type Evaluator interface {
	Evaluate(context.Context, any) (Evaluation, error)
}

type ServiceLimits struct {
	ReuseDuration           time.Duration
	MinimumRemaining        time.Duration
	MaximumConcurrent       int
	GlobalTokenRate         rate.Limit
	GlobalTokenBurst        int
	TenantTokenRate         rate.Limit
	TenantTokenBurst        int
	MaximumTenantLimiters   int
	CircuitFailureThreshold uint32
	CircuitOpenDuration     time.Duration
}

type ServiceConfig struct {
	Evaluator  Evaluator
	Repository Repository
	Rubric     RubricManifest
	Limits     ServiceLimits
	Clock      func() time.Time
}

type ServiceRequest struct {
	KeyInput        JudgmentKeyInput
	State           any
	EstimatedTokens int
}

type ServiceResult struct {
	Disposition    string
	Record         JudgmentRecord
	Features       []Feature
	ProviderCalled bool
}

type tenantLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type Service struct {
	evaluator  Evaluator
	repository Repository
	rubric     RubricManifest
	limits     ServiceLimits
	clock      func() time.Time
	global     *rate.Limiter
	capacity   chan struct{}
	single     singleflight.Group
	tenantMu   sync.Mutex
	tenants    map[string]tenantLimiter
	circuit    circuitBreaker
}

func NewService(config ServiceConfig) (*Service, error) {
	limits := config.Limits
	if config.Evaluator == nil || config.Repository == nil || ValidateRubricManifest(config.Rubric) != nil || limits.ReuseDuration <= 0 || limits.ReuseDuration > 365*24*time.Hour || limits.MinimumRemaining <= 0 || limits.MaximumConcurrent <= 0 || limits.MaximumConcurrent > 10_000 || limits.GlobalTokenRate <= 0 || limits.GlobalTokenBurst <= 0 || limits.TenantTokenRate <= 0 || limits.TenantTokenBurst <= 0 || limits.MaximumTenantLimiters <= 0 || limits.CircuitFailureThreshold == 0 || limits.CircuitOpenDuration <= 0 {
		return nil, ErrInvalidJudgment
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Service{
		evaluator: config.Evaluator, repository: config.Repository, rubric: config.Rubric, limits: limits, clock: clock,
		global: rate.NewLimiter(limits.GlobalTokenRate, limits.GlobalTokenBurst), capacity: make(chan struct{}, limits.MaximumConcurrent), tenants: make(map[string]tenantLimiter),
		circuit: circuitBreaker{threshold: limits.CircuitFailureThreshold, openDuration: limits.CircuitOpenDuration},
	}, nil
}

func (service *Service) Judge(ctx context.Context, request ServiceRequest) (ServiceResult, error) {
	if service == nil || ctx == nil || request.KeyInput.PredecessorHash != "" || request.KeyInput.RubricManifestID != service.rubric.ID || request.KeyInput.Model != service.rubric.Model || request.KeyInput.Provider != ProviderTypeSafe || request.EstimatedTokens <= 0 || request.EstimatedTokens > service.limits.GlobalTokenBurst || request.EstimatedTokens > service.limits.TenantTokenBurst {
		return ServiceResult{}, ErrInvalidJudgment
	}
	key, err := NewJudgmentKey(request.KeyInput)
	if err != nil {
		return ServiceResult{}, err
	}
	if err = ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ServiceResult{Disposition: FailureTimeout}, nil
		}
		return ServiceResult{Disposition: FailureCancelled}, nil
	}
	now := service.clock().UTC()
	if record, lookupErr := service.repository.LookupReusable(ctx, request.KeyInput.TenantID, key, now); lookupErr == nil {
		return resultFromRecord(DispositionCacheHit, record, false), nil
	} else if !errors.Is(lookupErr, ErrJudgmentNotFound) {
		return ServiceResult{Disposition: DispositionLedgerUnavailable}, nil
	}
	channel := service.single.DoChan(key, func() (any, error) {
		operationContext := context.WithoutCancel(ctx)
		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			operationContext, cancel = context.WithDeadline(operationContext, deadline)
			defer cancel()
		}
		return service.judgeMiss(operationContext, key, request)
	})
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ServiceResult{Disposition: FailureTimeout}, nil
		}
		return ServiceResult{Disposition: FailureCancelled}, nil
	case outcome := <-channel:
		if outcome.Err != nil {
			return ServiceResult{}, outcome.Err
		}
		return outcome.Val.(ServiceResult), nil
	}
}

func (service *Service) judgeMiss(ctx context.Context, key string, request ServiceRequest) (ServiceResult, error) {
	now := service.clock().UTC()
	if record, err := service.repository.LookupReusable(ctx, request.KeyInput.TenantID, key, now); err == nil {
		return resultFromRecord(DispositionCacheHit, record, false), nil
	} else if !errors.Is(err, ErrJudgmentNotFound) {
		return ServiceResult{Disposition: DispositionLedgerUnavailable}, nil
	}
	keyInput := request.KeyInput
	expectedPredecessorHash := ""
	if current, err := service.repository.Current(ctx, request.KeyInput.TenantID, key); err == nil {
		expectedPredecessorHash = current.ContentHash
		keyInput.PredecessorHash = current.ContentHash
	} else if !errors.Is(err, ErrJudgmentNotFound) {
		return ServiceResult{Disposition: DispositionLedgerUnavailable}, nil
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.Sub(now) < service.limits.MinimumRemaining {
		return ServiceResult{Disposition: DispositionDeadline}, nil
	}
	if !service.tenantRateAllows(string(request.KeyInput.TenantID), now, request.EstimatedTokens) {
		return ServiceResult{Disposition: DispositionTenantRate}, nil
	}
	if !service.global.AllowN(now, request.EstimatedTokens) {
		return ServiceResult{Disposition: DispositionGlobalRate}, nil
	}
	select {
	case service.capacity <- struct{}{}:
		defer func() { <-service.capacity }()
	default:
		return ServiceResult{Disposition: DispositionCapacity}, nil
	}
	if !service.circuit.Allow(now) {
		return ServiceResult{Disposition: DispositionCircuitOpen}, nil
	}
	evaluation, err := service.evaluator.Evaluate(ctx, request.State)
	if err != nil {
		code, immediate := classifyEvaluationError(ctx, err)
		service.circuit.Failure(service.clock().UTC(), immediate)
		return ServiceResult{Disposition: code, ProviderCalled: true}, nil
	}
	if evaluation.Model != service.rubric.Model {
		service.circuit.Failure(service.clock().UTC(), false)
		return ServiceResult{Disposition: FailureInvalidResponse, ProviderCalled: true}, nil
	}
	record, err := NewJudgmentRecord(JudgmentRecordInput{KeyInput: keyInput, Judgment: evaluation.Judgment, Usage: evaluation.Usage, CreatedAt: now, ReusableUntil: now.Add(service.limits.ReuseDuration)})
	if err != nil {
		service.circuit.Failure(service.clock().UTC(), false)
		return ServiceResult{Disposition: FailureInvalidResponse, ProviderCalled: true}, nil
	}
	winner, _, err := service.repository.Commit(ctx, key, expectedPredecessorHash, record)
	if err != nil {
		service.circuit.AbandonProbe()
		return ServiceResult{Disposition: DispositionLedgerUnavailable, ProviderCalled: true}, nil
	}
	service.circuit.Success()
	return resultFromRecord(DispositionCommitted, winner, true), nil
}

func resultFromRecord(disposition string, record JudgmentRecord, providerCalled bool) ServiceResult {
	return ServiceResult{Disposition: disposition, Record: record, Features: append([]Feature(nil), record.Features...), ProviderCalled: providerCalled}
}

func (service *Service) tenantRateAllows(tenant string, now time.Time, tokens int) bool {
	service.tenantMu.Lock()
	defer service.tenantMu.Unlock()
	entry, ok := service.tenants[tenant]
	if !ok {
		if len(service.tenants) >= service.limits.MaximumTenantLimiters {
			oldestKey := ""
			var oldest time.Time
			for key, candidate := range service.tenants {
				if oldestKey == "" || candidate.lastSeen.Before(oldest) || candidate.lastSeen.Equal(oldest) && key < oldestKey {
					oldestKey, oldest = key, candidate.lastSeen
				}
			}
			delete(service.tenants, oldestKey)
		}
		entry = tenantLimiter{limiter: rate.NewLimiter(service.limits.TenantTokenRate, service.limits.TenantTokenBurst)}
	}
	entry.lastSeen = now
	service.tenants[tenant] = entry
	return entry.limiter.AllowN(now, tokens)
}

func classifyEvaluationError(ctx context.Context, err error) (string, bool) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout, false
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return FailureCancelled, false
	}
	var providerError *ProviderError
	if errors.As(err, &providerError) {
		return providerError.Code, providerError.Code == FailureAuthentication
	}
	return FailureTransport, false
}

type circuitBreaker struct {
	mu           sync.Mutex
	threshold    uint32
	openDuration time.Duration
	failures     uint32
	openUntil    time.Time
	probe        bool
}

func (breaker *circuitBreaker) Allow(now time.Time) bool {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.openUntil.IsZero() {
		return true
	}
	if now.Before(breaker.openUntil) || breaker.probe {
		return false
	}
	breaker.probe = true
	return true
}

func (breaker *circuitBreaker) Success() {
	breaker.mu.Lock()
	breaker.failures, breaker.openUntil, breaker.probe = 0, time.Time{}, false
	breaker.mu.Unlock()
}

func (breaker *circuitBreaker) Failure(now time.Time, immediate bool) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.probe = false
	breaker.failures++
	if immediate || breaker.failures >= breaker.threshold {
		breaker.openUntil = now.Add(breaker.openDuration)
	}
}

func (breaker *circuitBreaker) AbandonProbe() {
	breaker.mu.Lock()
	breaker.probe = false
	breaker.mu.Unlock()
}
