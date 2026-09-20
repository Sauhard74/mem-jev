package workflow

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type StageProcessor interface {
	Load(context.Context, StageRequest) (StageResult, error)
	Evaluate(context.Context, StageRequest) (StageResult, error)
	BuildGraph(context.Context, StageRequest) (StageResult, error)
	Synthesize(context.Context, StageRequest) (StageResult, error)
	Publish(context.Context, StageRequest) (StageResult, error)
}

type StageError struct {
	Code      string
	Retryable bool
	Err       error
}

func (e *StageError) Error() string { return fmt.Sprintf("pipeline stage %s: %v", e.Code, e.Err) }
func (e *StageError) Unwrap() error { return e.Err }

type PipelineActivities struct {
	processor StageProcessor
}

func NewPipelineActivities(processor StageProcessor) (*PipelineActivities, error) {
	if processor == nil {
		return nil, errors.New("stage processor is required")
	}
	return &PipelineActivities{processor: processor}, nil
}

func (a *PipelineActivities) Load(ctx context.Context, request StageRequest) (StageResult, error) {
	return a.execute(ctx, StageLoad, request, a.processor.Load)
}

func (a *PipelineActivities) Evaluate(ctx context.Context, request StageRequest) (StageResult, error) {
	return a.execute(ctx, StageEvaluate, request, a.processor.Evaluate)
}

func (a *PipelineActivities) BuildGraph(ctx context.Context, request StageRequest) (StageResult, error) {
	return a.execute(ctx, StageBuildGraph, request, a.processor.BuildGraph)
}

func (a *PipelineActivities) Synthesize(ctx context.Context, request StageRequest) (StageResult, error) {
	return a.execute(ctx, StageSynthesize, request, a.processor.Synthesize)
}

func (a *PipelineActivities) Publish(ctx context.Context, request StageRequest) (StageResult, error) {
	return a.execute(ctx, StagePublish, request, a.processor.Publish)
}

func (a *PipelineActivities) execute(
	ctx context.Context,
	stage Stage,
	request StageRequest,
	processor func(context.Context, StageRequest) (StageResult, error),
) (StageResult, error) {
	if err := ctx.Err(); err != nil {
		return StageResult{}, err
	}
	if request.SchemaVersion != "stage-request.v1" || request.Stage != stage || request.Input.TenantID == "" || request.Input.TraceID == "" {
		return StageResult{}, temporal.NewNonRetryableApplicationError("invalid stage request", "invalid_input", ErrInvalidStartRequest)
	}
	activity.RecordHeartbeat(ctx, string(stage))
	result, err := processor(ctx, request)
	if err != nil {
		var stageError *StageError
		if errors.As(err, &stageError) && !stageError.Retryable {
			return StageResult{}, temporal.NewNonRetryableApplicationError("pipeline stage rejected", stageError.Code, stageError)
		}
		if errors.As(err, &stageError) {
			return StageResult{}, temporal.NewApplicationError("pipeline stage retryable failure", stageError.Code, stageError)
		}
		return StageResult{}, err
	}
	if err := validateStageResult(stage, result); err != nil {
		return StageResult{}, temporal.NewNonRetryableApplicationError("invalid stage result", "invalid_stage_result", err)
	}
	activity.RecordHeartbeat(ctx, result.ArtifactHash)
	return result, nil
}
