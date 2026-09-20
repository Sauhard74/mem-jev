package workflow

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
)

const DefaultSynthesisWorkflowName = "memjev.synthesis.v1"

type temporalExecutor interface {
	ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error)
}

type TemporalStarter struct {
	client temporalExecutor
	config TemporalStarterConfig
}

func NewTemporalStarter(temporalClient temporalExecutor, config TemporalStarterConfig) (*TemporalStarter, error) {
	if temporalClient == nil || config.TaskQueue == "" {
		return nil, ErrInvalidStartRequest
	}
	if config.WorkflowName == "" {
		config.WorkflowName = DefaultSynthesisWorkflowName
	}
	if config.WorkflowExecutionTimeout == 0 {
		config.WorkflowExecutionTimeout = 30 * time.Minute
	}
	if config.WorkflowTaskTimeout == 0 {
		config.WorkflowTaskTimeout = 10 * time.Second
	}
	if config.WorkflowExecutionTimeout < time.Minute || config.WorkflowTaskTimeout < time.Second ||
		config.WorkflowTaskTimeout > time.Minute || config.WorkflowTaskTimeout >= config.WorkflowExecutionTimeout {
		return nil, ErrInvalidStartRequest
	}
	return &TemporalStarter{client: temporalClient, config: config}, nil
}

func (s *TemporalStarter) Start(ctx context.Context, request StartRequest) (StartReceipt, error) {
	if err := ctx.Err(); err != nil {
		return StartReceipt{}, err
	}
	if s == nil || s.client == nil {
		return StartReceipt{}, errors.New("temporal starter is not configured")
	}
	if err := ValidateStartRequest(request); err != nil {
		return StartReceipt{}, err
	}
	options := client.StartWorkflowOptions{
		ID: request.WorkflowID, TaskQueue: s.config.TaskQueue,
		WorkflowExecutionTimeout: s.config.WorkflowExecutionTimeout, WorkflowTaskTimeout: s.config.WorkflowTaskTimeout,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
	run, err := s.client.ExecuteWorkflow(ctx, options, s.config.WorkflowName, request.Input)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return StartReceipt{WorkflowID: request.WorkflowID, RunID: alreadyStarted.RunId, Disposition: StartDispositionExisting}, nil
		}
		return StartReceipt{}, classifyProviderError(err)
	}
	receipt := StartReceipt{WorkflowID: request.WorkflowID, Disposition: StartDispositionStarted}
	if run != nil {
		receipt.RunID = run.GetRunID()
	}
	return receipt, nil
}

func DialTemporal(ctx context.Context, config TemporalConnectionConfig) (client.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := temporalClientOptions(config)
	if err != nil {
		return nil, err
	}
	return client.DialContext(ctx, options)
}

func temporalClientOptions(config TemporalConnectionConfig) (client.Options, error) {
	if config.Address == "" || config.Namespace == "" || (config.APIKey != "" && config.TLS == nil) {
		return client.Options{}, ErrInvalidStartRequest
	}
	options := client.Options{HostPort: config.Address, Namespace: config.Namespace}
	if config.TLS != nil {
		tlsConfig := config.TLS.Clone()
		if tlsConfig.MinVersion == 0 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
		options.ConnectionOptions.TLS = tlsConfig
	}
	if config.APIKey != "" {
		options.Credentials = client.NewAPIKeyStaticCredentials(config.APIKey)
	}
	return options, nil
}

func classifyProviderError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &ProviderError{Code: codes.DeadlineExceeded.String(), Retryable: true, Err: err}
	}
	if errors.Is(err, context.Canceled) {
		return &ProviderError{Code: codes.Canceled.String(), Retryable: false, Err: err}
	}
	code := serviceerror.ToStatus(err).Code()
	retryable := code == codes.Unavailable || code == codes.ResourceExhausted || code == codes.DeadlineExceeded ||
		code == codes.Aborted || code == codes.Internal
	return &ProviderError{Code: code.String(), Retryable: retryable, Err: err}
}

func IsRetryable(err error) bool {
	var providerError *ProviderError
	return errors.As(err, &providerError) && providerError.Retryable
}

var _ Starter = (*TemporalStarter)(nil)
