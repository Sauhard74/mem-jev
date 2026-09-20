package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	StageLoad       Stage = "load"
	StageEvaluate   Stage = "evaluate"
	StageBuildGraph Stage = "build_graph"
	StageSynthesize Stage = "synthesize"
	StagePublish    Stage = "publish"
)

type Stage string

type StageStatus string

const (
	StageStatusReady    StageStatus = "ready"
	StageStatusTerminal StageStatus = "terminal"
)

type StageRequest struct {
	SchemaVersion string         `json:"schema_version"`
	Input         SynthesisInput `json:"input"`
	Stage         Stage          `json:"stage"`
	PriorHash     string         `json:"prior_hash,omitempty"`
}

type StageResult struct {
	SchemaVersion string      `json:"schema_version"`
	Stage         Stage       `json:"stage"`
	ArtifactHash  string      `json:"artifact_hash"`
	Status        StageStatus `json:"status"`
	Code          string      `json:"code,omitempty"`
}

type PipelineResult struct {
	SchemaVersion string      `json:"schema_version"`
	WorkflowID    string      `json:"workflow_id"`
	FinalStage    Stage       `json:"final_stage"`
	ArtifactHash  string      `json:"artifact_hash"`
	Status        StageStatus `json:"status"`
	Code          string      `json:"code,omitempty"`
}

var stageHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func SynthesisWorkflow(ctx workflow.Context, input SynthesisInput) (PipelineResult, error) {
	info := workflow.GetInfo(ctx)
	request := StartRequest{WorkflowID: info.WorkflowExecution.ID, Input: input}
	if err := ValidateStartRequest(request); err != nil {
		return PipelineResult{}, temporal.NewNonRetryableApplicationError("invalid synthesis workflow input", "invalid_input", err)
	}
	stages := []Stage{StageLoad, StageEvaluate, StageBuildGraph, StageSynthesize, StagePublish}
	priorHash := ""
	for _, stage := range stages {
		options := activityOptions(stage)
		activityCtx := workflow.WithActivityOptions(ctx, options)
		stageRequest := StageRequest{SchemaVersion: "stage-request.v1", Input: input, Stage: stage, PriorHash: priorHash}
		var result StageResult
		if err := workflow.ExecuteActivity(activityCtx, stageActivityName(stage), stageRequest).Get(activityCtx, &result); err != nil {
			return PipelineResult{}, fmt.Errorf("%s stage: %w", stage, err)
		}
		if err := validateStageResult(stage, result); err != nil {
			return PipelineResult{}, temporal.NewNonRetryableApplicationError("invalid stage result", "invalid_stage_result", err)
		}
		priorHash = result.ArtifactHash
		if result.Status == StageStatusTerminal {
			return pipelineResult(info.WorkflowExecution.ID, result), nil
		}
	}
	return PipelineResult{SchemaVersion: "pipeline-result.v1", WorkflowID: info.WorkflowExecution.ID, FinalStage: StagePublish, ArtifactHash: priorHash, Status: StageStatusReady}, nil
}

func activityOptions(stage Stage) workflow.ActivityOptions {
	timeout := 2 * time.Minute
	switch stage {
	case StageEvaluate:
		timeout = 30 * time.Second
	case StageSynthesize:
		timeout = time.Minute
	}
	return workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		HeartbeatTimeout:    15 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 5,
			NonRetryableErrorTypes: []string{"archive_corrupt", "invalid_input", "tenant_mismatch", "unsupported_schema", "invalid_stage_result"},
		},
	}
}

func stageActivityName(stage Stage) string {
	switch stage {
	case StageLoad:
		return "Load"
	case StageEvaluate:
		return "Evaluate"
	case StageBuildGraph:
		return "BuildGraph"
	case StageSynthesize:
		return "Synthesize"
	case StagePublish:
		return "Publish"
	default:
		return ""
	}
}

func validateStageResult(want Stage, result StageResult) error {
	if result.SchemaVersion != "stage-result.v1" || result.Stage != want || !stageHashPattern.MatchString(result.ArtifactHash) ||
		(result.Status != StageStatusReady && result.Status != StageStatusTerminal) ||
		(result.Status == StageStatusTerminal && result.Code == "") {
		return errors.New("stage result failed deterministic contract")
	}
	return nil
}

func pipelineResult(workflowID string, stage StageResult) PipelineResult {
	return PipelineResult{
		SchemaVersion: "pipeline-result.v1", WorkflowID: workflowID, FinalStage: stage.Stage,
		ArtifactHash: stage.ArtifactHash, Status: stage.Status, Code: stage.Code,
	}
}
