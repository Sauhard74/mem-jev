package maintenance

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sauhard74/mem-jev/internal/observability"
)

type Handler interface {
	Handle(context.Context, Lease) error
}
type HandlerFunc func(context.Context, Lease) error

func (f HandlerFunc) Handle(ctx context.Context, lease Lease) error { return f(ctx, lease) }

type RunnerConfig struct {
	WorkerID                                    string
	BatchSize                                   int
	LeaseDuration, PollInterval, MaximumBackoff time.Duration
	MaximumAttempts                             uint32
}
type Runner struct {
	repository Repository
	handlers   map[Kind]Handler
	config     RunnerConfig
	now        func() time.Time
}

func NewRunner(repository Repository, handlers map[Kind]Handler, config RunnerConfig) (*Runner, error) {
	if repository == nil || config.WorkerID == "" || config.BatchSize < 1 || config.BatchSize > 100 || config.LeaseDuration < time.Second || config.PollInterval <= 0 || config.MaximumBackoff < time.Second || config.MaximumAttempts == 0 {
		return nil, ErrInvalidJob
	}
	copyHandlers := make(map[Kind]Handler, len(handlers))
	for kind, handler := range handlers {
		if !validKind(kind) || handler == nil {
			return nil, ErrInvalidJob
		}
		copyHandlers[kind] = handler
	}
	if len(copyHandlers) == 0 {
		return nil, ErrInvalidJob
	}
	return &Runner{repository: repository, handlers: copyHandlers, config: config, now: time.Now}, nil
}

// Run continuously polls the durable queue until cancellation. Repository and
// handler-transition errors are fatal so a process supervisor can restart it.
func (r *Runner) Run(ctx context.Context) error {
	for {
		if _, err := r.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	kinds := make([]Kind, 0, len(r.handlers))
	for kind := range r.handlers {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	if stats, err := r.repository.Stats(ctx); err == nil {
		byKind := make(map[Kind]QueueStats, len(stats))
		for _, value := range stats {
			byKind[value.Kind] = value
		}
		// Emit zeros for drained queues so synchronous gauge series do not keep
		// presenting the last non-zero sample after recovery.
		for _, kind := range kinds {
			value := byKind[kind]
			observability.RecordMaintenanceQueue(ctx, string(kind), value.Pending, value.OldestPendingAge)
		}
	}
	leases, err := r.repository.Claim(ctx, ClaimRequest{WorkerID: r.config.WorkerID, Kinds: kinds, Limit: r.config.BatchSize, LeaseDuration: r.config.LeaseDuration})
	if err != nil {
		return 0, err
	}
	var completed atomic.Int64
	errorsSeen := make(chan error, len(leases))
	var wait sync.WaitGroup
	for _, lease := range leases {
		wait.Add(1)
		go func() {
			defer wait.Done()
			didComplete, err := r.processLease(ctx, lease)
			if err != nil {
				errorsSeen <- err
				return
			}
			if didComplete {
				completed.Add(1)
			}
		}()
	}
	wait.Wait()
	close(errorsSeen)
	var failures []error
	for err := range errorsSeen {
		failures = append(failures, err)
	}
	return int(completed.Load()), errors.Join(failures...)
}

func (r *Runner) processLease(ctx context.Context, lease Lease) (bool, error) {
	handlerCtx, cancel := context.WithDeadline(ctx, lease.LeaseExpiresAt)
	handleErr := r.handlers[lease.Kind].Handle(handlerCtx, lease)
	cancel()
	finish := FinishRequest{TenantID: lease.TenantID, JobID: lease.ID, WorkerID: lease.LeaseOwner, FencingToken: lease.FencingToken}
	if handleErr == nil {
		err := r.repository.Complete(ctx, finish)
		return err == nil, err
	}
	dead := lease.AttemptCount >= r.config.MaximumAttempts
	delay := time.Duration(0)
	if !dead {
		delay = min(time.Second<<min(lease.AttemptCount-1, 20), r.config.MaximumBackoff)
	}
	err := r.repository.Fail(ctx, FailRequest{FinishRequest: finish, ErrorCode: "handler_failed", RetryAt: r.now().UTC().Add(delay), DeadLetter: dead})
	if err == nil {
		observability.RecordMaintenanceFailure(ctx, string(lease.Kind), dead)
	}
	return false, err
}
