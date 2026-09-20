package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/store"
)

func TestValidateClaimOutbox(t *testing.T) {
	valid := store.ClaimOutboxRequest{WorkerID: "worker-a", JobTypes: []store.OutboxJobType{store.OutboxJobSynthesizeOutcome}, Limit: 10, LeaseDuration: 30 * time.Second}
	if err := store.ValidateClaimOutbox(valid); err != nil {
		t.Fatal(err)
	}
	cases := []store.ClaimOutboxRequest{
		{},
		{WorkerID: "worker-a", JobTypes: valid.JobTypes, Limit: 101, LeaseDuration: valid.LeaseDuration},
		{WorkerID: "worker-a", JobTypes: valid.JobTypes, Limit: 1, LeaseDuration: time.Millisecond},
		{WorkerID: "worker-a", JobTypes: []store.OutboxJobType{"unknown"}, Limit: 1, LeaseDuration: valid.LeaseDuration},
		{WorkerID: "worker-a", JobTypes: []store.OutboxJobType{store.OutboxJobSynthesizeOutcome, store.OutboxJobSynthesizeOutcome}, Limit: 1, LeaseDuration: valid.LeaseDuration},
	}
	for _, testCase := range cases {
		if err := store.ValidateClaimOutbox(testCase); !errors.Is(err, store.ErrInvalidLeaseRequest) {
			t.Fatalf("ValidateClaimOutbox(%#v) = %v", testCase, err)
		}
	}
}

func TestValidateOutboxTransitions(t *testing.T) {
	if err := store.ValidateCompleteOutbox(store.CompleteOutboxRequest{TenantID: "tenant", WorkflowID: "workflow", WorkerID: "worker", FencingToken: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateReleaseOutbox(store.ReleaseOutboxRequest{TenantID: "tenant", WorkflowID: "workflow", WorkerID: "worker", FencingToken: 1, ErrorCode: "unavailable", RetryDelay: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateReleaseOutbox(store.ReleaseOutboxRequest{TenantID: "tenant", WorkflowID: "workflow", WorkerID: "worker", FencingToken: 1, ErrorCode: "fatal", RetryDelay: time.Second, DeadLetter: true}); !errors.Is(err, store.ErrInvalidLeaseRequest) {
		t.Fatalf("error = %v", err)
	}
}
