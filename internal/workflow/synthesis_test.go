package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

type deterministicProcessor struct {
	mu           sync.Mutex
	calls        []Stage
	failStage    Stage
	failures     int
	nonretryable bool
	terminalAt   Stage
	blockAt      Stage
}

func (p *deterministicProcessor) Load(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.run(ctx, StageLoad, request)
}
func (p *deterministicProcessor) Evaluate(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.run(ctx, StageEvaluate, request)
}
func (p *deterministicProcessor) BuildGraph(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.run(ctx, StageBuildGraph, request)
}
func (p *deterministicProcessor) Synthesize(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.run(ctx, StageSynthesize, request)
}
func (p *deterministicProcessor) Publish(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.run(ctx, StagePublish, request)
}

func (p *deterministicProcessor) run(ctx context.Context, stage Stage, _ StageRequest) (StageResult, error) {
	if err := ctx.Err(); err != nil {
		return StageResult{}, err
	}
	p.mu.Lock()
	p.calls = append(p.calls, stage)
	block := p.blockAt == stage
	if p.failStage == stage && p.failures > 0 {
		p.failures--
		nonretryable := p.nonretryable
		p.mu.Unlock()
		code := "archive_unavailable"
		if nonretryable {
			code = "archive_corrupt"
		}
		return StageResult{}, &StageError{Code: code, Retryable: !nonretryable, Err: errors.New("injected")}
	}
	terminal := p.terminalAt == stage
	p.mu.Unlock()
	if block {
		<-ctx.Done()
		return StageResult{}, ctx.Err()
	}
	status := StageStatusReady
	code := ""
	if terminal {
		status, code = StageStatusTerminal, "not_eligible"
	}
	return StageResult{SchemaVersion: "stage-result.v1", Stage: stage, ArtifactHash: fmt.Sprintf("%064x", stageNumber(stage)), Status: status, Code: code}, nil
}

func TestSynthesisWorkflowIsDeterministicAndBounded(t *testing.T) {
	first, firstCalls, firstErr := executeTestWorkflow(t, &deterministicProcessor{})
	second, secondCalls, secondErr := executeTestWorkflow(t, &deterministicProcessor{})
	if firstErr != nil || secondErr != nil {
		t.Fatalf("errors: first=%v second=%v", firstErr, secondErr)
	}
	if first != second || fmt.Sprint(firstCalls) != fmt.Sprint(secondCalls) {
		t.Fatalf("non-deterministic results: %#v %#v calls=%v/%v", first, second, firstCalls, secondCalls)
	}
	want := []Stage{StageLoad, StageEvaluate, StageBuildGraph, StageSynthesize, StagePublish}
	if fmt.Sprint(firstCalls) != fmt.Sprint(want) {
		t.Fatalf("calls = %v, want %v", firstCalls, want)
	}
}

func TestSynthesisWorkflowRetriesRetryableActivity(t *testing.T) {
	processor := &deterministicProcessor{failStage: StageLoad, failures: 1}
	_, calls, err := executeTestWorkflow(t, processor)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 6 || calls[0] != StageLoad || calls[1] != StageLoad {
		t.Fatalf("calls = %v", calls)
	}
}

func TestSynthesisWorkflowStopsOnNonRetryableCorruption(t *testing.T) {
	processor := &deterministicProcessor{failStage: StageLoad, failures: 1, nonretryable: true}
	_, calls, err := executeTestWorkflow(t, processor)
	if err == nil || len(calls) != 1 || !strings.Contains(err.Error(), "archive_corrupt") {
		t.Fatalf("error=%v calls=%v", err, calls)
	}
}

func TestSynthesisWorkflowStopsAtTerminalStage(t *testing.T) {
	result, calls, err := executeTestWorkflow(t, &deterministicProcessor{terminalAt: StageEvaluate})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StageStatusTerminal || result.FinalStage != StageEvaluate || len(calls) != 2 {
		t.Fatalf("result=%#v calls=%v", result, calls)
	}
}

func TestSynthesisWorkflowPropagatesCancellation(t *testing.T) {
	processor := &deterministicProcessor{blockAt: StageLoad}
	activities, err := NewPipelineActivities(processor)
	if err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflow(SynthesisWorkflow)
	environment.RegisterActivity(activities)
	request := validStartRequest(t)
	environment.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: request.WorkflowID})
	environment.RegisterDelayedCallback(environment.CancelWorkflow, time.Second)
	environment.ExecuteWorkflow(SynthesisWorkflow, request.Input)
	if err := environment.GetWorkflowError(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "cancel") {
		t.Fatalf("error = %v", err)
	}
}

func TestSynthesisWorkflowStageTimeoutsAreBounded(t *testing.T) {
	for _, stage := range []Stage{StageLoad, StageEvaluate, StageBuildGraph, StageSynthesize, StagePublish} {
		options := activityOptions(stage)
		if options.StartToCloseTimeout <= 0 || options.StartToCloseTimeout > 2*time.Minute || options.HeartbeatTimeout != 15*time.Second ||
			options.RetryPolicy == nil || options.RetryPolicy.MaximumAttempts != 5 || !options.WaitForCancellation {
			t.Fatalf("%s options = %#v", stage, options)
		}
	}
}

func executeTestWorkflow(t *testing.T, processor *deterministicProcessor) (PipelineResult, []Stage, error) {
	t.Helper()
	activities, err := NewPipelineActivities(processor)
	if err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflow(SynthesisWorkflow)
	environment.RegisterActivity(activities)
	request := validStartRequest(t)
	environment.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: request.WorkflowID})
	environment.ExecuteWorkflow(SynthesisWorkflow, request.Input)
	workflowErr := environment.GetWorkflowError()
	var result PipelineResult
	if workflowErr == nil {
		workflowErr = environment.GetWorkflowResult(&result)
	}
	processor.mu.Lock()
	calls := append([]Stage(nil), processor.calls...)
	processor.mu.Unlock()
	return result, calls, workflowErr
}

func stageNumber(stage Stage) int {
	switch stage {
	case StageLoad:
		return 1
	case StageEvaluate:
		return 2
	case StageBuildGraph:
		return 3
	case StageSynthesize:
		return 4
	case StagePublish:
		return 5
	default:
		return 0
	}
}

var _ StageProcessor = (*deterministicProcessor)(nil)
