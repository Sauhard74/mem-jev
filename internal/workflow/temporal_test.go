package workflow

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/store"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type fakeTemporalExecutor struct {
	options      client.StartWorkflowOptions
	workflowName interface{}
	args         []interface{}
	err          error
}

func (f *fakeTemporalExecutor) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflowName interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.options, f.workflowName, f.args = options, workflowName, args
	return nil, f.err
}

func TestTemporalStarterUsesDeterministicIdentityAndSanitizedInput(t *testing.T) {
	executor := &fakeTemporalExecutor{}
	starter, err := NewTemporalStarter(executor, TemporalStarterConfig{TaskQueue: "synthesis"})
	if err != nil {
		t.Fatal(err)
	}
	request := validStartRequest(t)
	receipt, err := starter.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.WorkflowID != request.WorkflowID || receipt.Disposition != StartDispositionStarted {
		t.Fatalf("receipt = %#v", receipt)
	}
	if executor.options.ID != request.WorkflowID || executor.options.TaskQueue != "synthesis" ||
		executor.options.WorkflowIDReusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE ||
		executor.options.WorkflowIDConflictPolicy != enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL ||
		!executor.options.WorkflowExecutionErrorWhenAlreadyStarted || executor.workflowName != DefaultSynthesisWorkflowName {
		t.Fatalf("options = %#v workflow = %#v", executor.options, executor.workflowName)
	}
	encoded, err := json.Marshal(executor.args)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"worker-a", "api-key", "lease", "secret", "prompt"} {
		if strings.Contains(strings.ToLower(string(encoded)), forbidden) {
			t.Fatalf("workflow payload contains forbidden value %q: %s", forbidden, encoded)
		}
	}
}

func TestTemporalStarterTreatsAlreadyStartedAsSuccess(t *testing.T) {
	executor := &fakeTemporalExecutor{err: serviceerror.NewWorkflowExecutionAlreadyStarted("exists", "request", "run-1")}
	starter, err := NewTemporalStarter(executor, TemporalStarterConfig{TaskQueue: "synthesis"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := starter.Start(context.Background(), validStartRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Disposition != StartDispositionExisting || receipt.RunID != "run-1" {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestTemporalStarterClassifiesProviderErrors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "unavailable", err: serviceerror.NewUnavailable("down"), retryable: true},
		{name: "invalid", err: serviceerror.NewInvalidArgument("bad"), retryable: false},
		{name: "deadline", err: context.DeadlineExceeded, retryable: true},
		{name: "canceled", err: context.Canceled, retryable: false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &fakeTemporalExecutor{err: testCase.err}
			starter, err := NewTemporalStarter(executor, TemporalStarterConfig{TaskQueue: "synthesis"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = starter.Start(context.Background(), validStartRequest(t))
			if err == nil || IsRetryable(err) != testCase.retryable || !errors.Is(err, testCase.err) {
				t.Fatalf("error = %v retryable=%v", err, IsRetryable(err))
			}
		})
	}
}

func TestRequestFromLeaseRejectsNonDeterministicWorkflowID(t *testing.T) {
	request := validStartRequest(t)
	lease := store.OutboxLease{
		TenantID: request.Input.TenantID, WorkflowID: "attacker-selected", TraceID: request.Input.TraceID,
		OutcomeID: request.Input.OutcomeID, JobType: request.Input.JobType, ContentHash: request.Input.ContentHash,
	}
	if _, err := RequestFromLease(lease); !errors.Is(err, ErrInvalidStartRequest) {
		t.Fatalf("error = %v", err)
	}
}

func TestTemporalClientOptionsCloneTLSAndSupportAPIKey(t *testing.T) {
	tlsConfig := &tls.Config{
		ServerName: "temporal.example.com", MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{{1, 2, 3}}}},
	}
	options, err := temporalClientOptions(TemporalConnectionConfig{
		Address: "temporal.example.com:7233", Namespace: "production", APIKey: "api-key", TLS: tlsConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if options.ConnectionOptions.TLS == tlsConfig || options.ConnectionOptions.TLS.ServerName != tlsConfig.ServerName || options.Credentials == nil {
		t.Fatalf("options = %#v", options)
	}
}

func TestTemporalClientOptionsRequireTLSForAPIKey(t *testing.T) {
	_, err := temporalClientOptions(TemporalConnectionConfig{
		Address: "temporal.example.com:7233", Namespace: "production", APIKey: "api-key",
	})
	if !errors.Is(err, ErrInvalidStartRequest) {
		t.Fatalf("error = %v", err)
	}
}

func validStartRequest(t *testing.T) StartRequest {
	t.Helper()
	lease := store.OutboxLease{
		TenantID: "tenant-a", TraceID: "tr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		OutcomeID: "out_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		JobType:   store.OutboxJobSynthesizeOutcome, ContentHash: strings.Repeat("c", 64),
		LeaseOwner: "worker-a", FencingToken: 1, LeaseExpiresAt: time.Now().Add(time.Minute),
	}
	lease.WorkflowID = store.OutcomeWorkflowID(lease.TenantID, lease.TraceID, lease.OutcomeID)
	request, err := RequestFromLease(lease)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
