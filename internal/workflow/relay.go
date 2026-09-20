package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/store"
)

type RelayConfig struct {
	WorkerID        string
	BatchSize       int
	LeaseDuration   time.Duration
	PollInterval    time.Duration
	MaximumAttempts int
	MaximumBackoff  time.Duration
}

type Relay struct {
	repository store.OutboxRepository
	starter    Starter
	config     RelayConfig
}

func NewRelay(repository store.OutboxRepository, starter Starter, config RelayConfig) (*Relay, error) {
	if repository == nil || starter == nil || config.WorkerID == "" || config.BatchSize < 1 || config.BatchSize > 100 ||
		config.LeaseDuration < time.Second || config.PollInterval <= 0 || config.MaximumAttempts < 1 || config.MaximumBackoff < time.Second {
		return nil, ErrInvalidStartRequest
	}
	return &Relay{repository: repository, starter: starter, config: config}, nil
}

func (r *Relay) Run(ctx context.Context) error {
	for {
		if _, err := r.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	leases, err := r.repository.ClaimOutbox(ctx, store.ClaimOutboxRequest{
		WorkerID: r.config.WorkerID, JobTypes: []store.OutboxJobType{store.OutboxJobSynthesizeTrace, store.OutboxJobSynthesizeOutcome},
		Limit: r.config.BatchSize, LeaseDuration: r.config.LeaseDuration,
	})
	if err != nil {
		return 0, err
	}
	var processed atomic.Int64
	errorsSeen := make(chan error, len(leases))
	var wait sync.WaitGroup
	for _, lease := range leases {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if dispatchErr := r.dispatch(ctx, lease); dispatchErr != nil {
				errorsSeen <- dispatchErr
				return
			}
			processed.Add(1)
		}()
	}
	wait.Wait()
	close(errorsSeen)
	var failures []error
	for dispatchErr := range errorsSeen {
		failures = append(failures, dispatchErr)
	}
	return int(processed.Load()), errors.Join(failures...)
}

func (r *Relay) dispatch(ctx context.Context, lease store.OutboxLease) error {
	request, err := RequestFromLease(lease)
	if err == nil {
		deadline := lease.LeaseExpiresAt
		startCtx, cancel := context.WithDeadline(ctx, deadline)
		_, err = r.starter.Start(startCtx, request)
		cancel()
	}
	if err == nil {
		return r.repository.CompleteOutbox(ctx, store.CompleteOutboxRequest{
			TenantID: lease.TenantID, WorkflowID: lease.WorkflowID, WorkerID: lease.LeaseOwner, FencingToken: lease.FencingToken,
		})
	}
	retryable, code := dispatchError(err)
	deadLetter := !retryable || lease.AttemptCount >= r.config.MaximumAttempts
	retryDelay := time.Duration(0)
	if !deadLetter {
		retryDelay = min(time.Second<<min(lease.AttemptCount-1, 20), r.config.MaximumBackoff)
	}
	err = r.repository.ReleaseOutbox(ctx, store.ReleaseOutboxRequest{
		TenantID: lease.TenantID, WorkflowID: lease.WorkflowID, WorkerID: lease.LeaseOwner, FencingToken: lease.FencingToken,
		RetryDelay: retryDelay, ErrorCode: code, DeadLetter: deadLetter,
	})
	if err == nil {
		observability.RecordOutboxRetry(ctx, code, deadLetter)
	}
	return err
}

func dispatchError(err error) (bool, string) {
	var providerError *ProviderError
	if errors.As(err, &providerError) {
		return providerError.Retryable, "temporal_" + providerError.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, "dispatch_deadline"
	}
	if errors.Is(err, context.Canceled) {
		return true, "dispatch_canceled"
	}
	return false, "invalid_dispatch"
}
