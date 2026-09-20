package workflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/store"
	"go.temporal.io/api/serviceerror"
)

type relayRepository struct {
	leases    []store.OutboxLease
	completed []store.CompleteOutboxRequest
	released  []store.ReleaseOutboxRequest
}

func (r *relayRepository) ClaimOutbox(context.Context, store.ClaimOutboxRequest) ([]store.OutboxLease, error) {
	return append([]store.OutboxLease(nil), r.leases...), nil
}
func (r *relayRepository) ReleaseOutbox(_ context.Context, request store.ReleaseOutboxRequest) error {
	r.released = append(r.released, request)
	return nil
}
func (r *relayRepository) CompleteOutbox(_ context.Context, request store.CompleteOutboxRequest) error {
	r.completed = append(r.completed, request)
	return nil
}

type relayStarter struct{ err error }

func (s relayStarter) Start(context.Context, StartRequest) (StartReceipt, error) {
	return StartReceipt{Disposition: StartDispositionStarted}, s.err
}

func TestRelayCompletesStartedOrExistingWorkflow(t *testing.T) {
	repository := &relayRepository{leases: []store.OutboxLease{validLease(t, 1)}}
	relay := newTestRelay(t, repository, relayStarter{})
	processed, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 || len(repository.completed) != 1 || len(repository.released) != 0 {
		t.Fatalf("processed=%d completed=%v released=%v", processed, repository.completed, repository.released)
	}
}

func TestRelaySchedulesRetryableFailureWithoutLeakingProviderMessage(t *testing.T) {
	repository := &relayRepository{leases: []store.OutboxLease{validLease(t, 1)}}
	errorWithSecret := serviceerror.NewUnavailable("provider failed with sk-secret-value")
	relay := newTestRelay(t, repository, relayStarter{err: classifyProviderError(errorWithSecret)})
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repository.released) != 1 || repository.released[0].DeadLetter || repository.released[0].RetryDelay <= 0 ||
		strings.Contains(repository.released[0].ErrorCode, "secret") {
		t.Fatalf("release = %#v", repository.released)
	}
}

func TestRelayDeadLettersPermanentOrExhaustedFailure(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		lease   store.OutboxLease
		failure error
	}{
		{name: "permanent", lease: validLease(t, 1), failure: ErrInvalidStartRequest},
		{name: "exhausted", lease: validLease(t, 5), failure: classifyProviderError(serviceerror.NewUnavailable("down"))},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repository := &relayRepository{leases: []store.OutboxLease{testCase.lease}}
			relay := newTestRelay(t, repository, relayStarter{err: testCase.failure})
			if _, err := relay.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(repository.released) != 1 || !repository.released[0].DeadLetter || repository.released[0].RetryDelay != 0 {
				t.Fatalf("release = %#v", repository.released)
			}
		})
	}
}

func newTestRelay(t *testing.T, repository store.OutboxRepository, starter Starter) *Relay {
	t.Helper()
	relay, err := NewRelay(repository, starter, RelayConfig{
		WorkerID: "relay-a", BatchSize: 10, LeaseDuration: time.Minute, PollInterval: time.Second,
		MaximumAttempts: 5, MaximumBackoff: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return relay
}

func validLease(t *testing.T, attempt int) store.OutboxLease {
	t.Helper()
	request := validStartRequest(t)
	return store.OutboxLease{
		TenantID: request.Input.TenantID, WorkflowID: request.WorkflowID, TraceID: request.Input.TraceID,
		OutcomeID: request.Input.OutcomeID, JobType: request.Input.JobType, ContentHash: request.Input.ContentHash,
		AttemptCount: attempt, FencingToken: uint64(attempt), LeaseOwner: "relay-a", LeaseExpiresAt: time.Now().Add(time.Minute),
	}
}

var _ store.OutboxRepository = (*relayRepository)(nil)
