package maintenance_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/maintenance"
	"github.com/sauhard74/mem-jev/internal/store/memory"
)

func TestRunnerCompletesDurableJob(t *testing.T) {
	now := time.Now().UTC()
	repository := memory.NewMaintenanceRepository()
	spec, err := maintenance.NewSpec("tenant_a", maintenance.CompatibilityProjection, strings.Repeat("a", 64), []byte(`{"epoch":1}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Enqueue(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	called := 0
	runner, err := maintenance.NewRunner(repository, map[maintenance.Kind]maintenance.Handler{maintenance.CompatibilityProjection: maintenance.HandlerFunc(func(_ context.Context, lease maintenance.Lease) error {
		called++
		if lease.ID != spec.ID {
			t.Fatal("wrong job")
		}
		return nil
	})}, maintenance.RunnerConfig{WorkerID: "worker_a", BatchSize: 10, LeaseDuration: time.Minute, PollInterval: time.Second, MaximumAttempts: 3, MaximumBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := runner.RunOnce(context.Background())
	if err != nil || completed != 1 || called != 1 {
		t.Fatalf("completed=%d called=%d err=%v", completed, called, err)
	}
	completed, err = runner.RunOnce(context.Background())
	if err != nil || completed != 0 || called != 1 {
		t.Fatalf("replayed completed job: completed=%d called=%d err=%v", completed, called, err)
	}
}

func TestRunnerRetriesThenDeadLetters(t *testing.T) {
	now := time.Now().UTC()
	repository := memory.NewMaintenanceRepository()
	spec, _ := maintenance.NewSpec("tenant_a", maintenance.LifecycleEvaluation, strings.Repeat("b", 64), []byte(`{"version":"pv_a"}`), now)
	if err := repository.Enqueue(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	runner, err := maintenance.NewRunner(repository, map[maintenance.Kind]maintenance.Handler{maintenance.LifecycleEvaluation: maintenance.HandlerFunc(func(context.Context, maintenance.Lease) error { return errors.New("injected") })}, maintenance.RunnerConfig{WorkerID: "worker_a", BatchSize: 1, LeaseDuration: time.Minute, PollInterval: time.Second, MaximumAttempts: 1, MaximumBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if completed, err := runner.RunOnce(context.Background()); err != nil || completed != 0 {
		t.Fatalf("completed=%d err=%v", completed, err)
	}
	if leases, err := repository.Claim(context.Background(), maintenance.ClaimRequest{WorkerID: "worker_b", Kinds: []maintenance.Kind{maintenance.LifecycleEvaluation}, Limit: 1, LeaseDuration: time.Minute}); err != nil || len(leases) != 0 {
		t.Fatalf("dead letter reclaimed: %#v %v", leases, err)
	}
}

func TestRunnerRunStopsOnCancellation(t *testing.T) {
	repository := memory.NewMaintenanceRepository()
	runner, err := maintenance.NewRunner(repository, map[maintenance.Kind]maintenance.Handler{
		maintenance.TransientExpiry: maintenance.HandlerFunc(func(context.Context, maintenance.Lease) error { return nil }),
	}, maintenance.RunnerConfig{WorkerID: "worker_a", BatchSize: 1, LeaseDuration: time.Minute, PollInterval: time.Millisecond, MaximumAttempts: 1, MaximumBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestRunnerProcessesClaimedBatchConcurrently(t *testing.T) {
	now := time.Now().UTC()
	repository := memory.NewMaintenanceRepository()
	for _, key := range []string{strings.Repeat("c", 64), strings.Repeat("d", 64)} {
		spec, err := maintenance.NewSpec("tenant_a", maintenance.CompatibilityProjection, key, []byte(`{"epoch":1}`), now)
		if err != nil {
			t.Fatal(err)
		}
		if err := repository.Enqueue(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int64
	runner, err := maintenance.NewRunner(repository, map[maintenance.Kind]maintenance.Handler{
		maintenance.CompatibilityProjection: maintenance.HandlerFunc(func(ctx context.Context, _ maintenance.Lease) error {
			calls.Add(1)
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	}, maintenance.RunnerConfig{WorkerID: "worker_a", BatchSize: 2, LeaseDuration: time.Minute, PollInterval: time.Second, MaximumAttempts: 3, MaximumBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		completed, runErr := runner.RunOnce(context.Background())
		if runErr == nil && completed != 2 {
			runErr = errors.New("runner did not complete both jobs")
		}
		result <- runErr
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatalf("only %d handlers started before release", calls.Load())
		}
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
