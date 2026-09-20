//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/synthesis"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
	memworkflow "github.com/sauhard74/mem-jev/internal/workflow"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEvidencePipelineProductionCases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := openSurreal(t, ctx)
	defer func() { _ = db.Close(context.Background()) }()
	seedWriterContract(t, ctx, db)

	t.Run("verified success trims failure and publishes scoped negative path", func(t *testing.T) {
		traceID := ingestPipelineTrace(t, ctx, "qualified", "writer", true)
		outcome := recordPipelineOutcome(t, ctx, traceID, "qualified-success", satisfiedEvidence("success"), requiredEnv(t, "MEMJEV_E2E_TOKEN"))
		if !outcome.Msg.GetPromotionEligible() || outcome.Msg.GetState() != memjevv1.OutcomeState_OUTCOME_STATE_VERIFIED_SUCCESS {
			t.Fatalf("outcome = %#v", outcome.Msg)
		}
		manifest := waitForRow(t, ctx, db, "synthesis_manifest", "tenant_id = $tenant AND outcome_id = $outcome", map[string]any{"tenant": "tenant_e2e", "outcome": outcome.Msg.GetOutcomeId()})
		if manifest["status"] != "synthesized" || manifest["procedure_version_id"] == nil {
			t.Fatalf("manifest = %#v", manifest)
		}
		path := waitForRow(t, ctx, db, "negative_path", "tenant_id = $tenant", map[string]any{"tenant": "tenant_e2e"})
		for _, field := range []string{"environment_scope_hash", "tool_scope_hash", "resource_scope_hash"} {
			if value, ok := path[field].(string); !ok || len(value) != 64 {
				t.Fatalf("negative path %s = %#v", field, path[field])
			}
		}
		before := canonicalProjection(t, ctx, db, manifest["procedure_version_id"])
		assertByteIdenticalRebuild(t, ctx, db, traceID, outcome.Msg.GetOutcomeId(), fmt.Sprint(manifest["procedure_version_id"]), before)
		duplicate := recordPipelineOutcome(t, ctx, traceID, "qualified-success", satisfiedEvidence("success"), requiredEnv(t, "MEMJEV_E2E_TOKEN"))
		if duplicate.Msg.GetOutcomeId() != outcome.Msg.GetOutcomeId() || duplicate.Msg.GetDisposition() != memjevv1.OutcomeDisposition_OUTCOME_DISPOSITION_DUPLICATE {
			t.Fatalf("duplicate = %#v", duplicate.Msg)
		}
		workflowID := store.OutcomeWorkflowID("tenant_e2e", domain.TraceID(traceID), domain.OutcomeID(outcome.Msg.GetOutcomeId()))
		waitForRow(t, ctx, db, "outbox_job", "tenant_id = $tenant AND workflow_id = $workflow AND state = 'completed'", map[string]any{"tenant": "tenant_e2e", "workflow": workflowID})
		if _, err := surrealdb.Query[any](ctx, db, `UPDATE outbox_job SET state = "pending", available_at = time::now(), lease_expires_at = NONE
			WHERE tenant_id = $tenant AND workflow_id = $workflow`, map[string]any{"tenant": "tenant_e2e", "workflow": workflowID}); err != nil {
			t.Fatal(err)
		}
		waitForRow(t, ctx, db, "outbox_job", "tenant_id = $tenant AND workflow_id = $workflow AND state = 'completed' AND attempt_count >= 2", map[string]any{"tenant": "tenant_e2e", "workflow": workflowID})
		after := canonicalProjection(t, ctx, db, manifest["procedure_version_id"])
		if before != after {
			t.Fatal("duplicate delivery changed canonical projection bytes")
		}
	})

	t.Run("contradictory evidence and unknown tools abstain", func(t *testing.T) {
		traceID := ingestPipelineTrace(t, ctx, "conflict", "writer", false)
		facts := satisfiedEvidence("conflict")
		facts = append(facts, &memjevv1.OutcomeEvidence{
			ClientEvidenceId: "conflict-failure", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
			Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED, PredicateId: "goal", VerifierId: "ci",
			ObservedAt: timestamppb.New(time.Unix(4, 0).UTC()),
		})
		outcome := recordPipelineOutcome(t, ctx, traceID, "qualified-conflict", facts, requiredEnv(t, "MEMJEV_E2E_TOKEN"))
		row := waitForRow(t, ctx, db, "synthesis_manifest", "tenant_id = $tenant AND outcome_id = $outcome", map[string]any{"tenant": "tenant_e2e", "outcome": outcome.Msg.GetOutcomeId()})
		if row["status"] != "abstained" || row["abstention_code"] != "outcome_not_verified" {
			t.Fatalf("conflict manifest = %#v", row)
		}

		unknownTrace := ingestPipelineTrace(t, ctx, "opaque", "unregistered-tool", false)
		unknownOutcome := recordPipelineOutcome(t, ctx, unknownTrace, "qualified-opaque", satisfiedEvidence("opaque"), requiredEnv(t, "MEMJEV_E2E_TOKEN"))
		row = waitForRow(t, ctx, db, "synthesis_manifest", "tenant_id = $tenant AND outcome_id = $outcome", map[string]any{"tenant": "tenant_e2e", "outcome": unknownOutcome.Msg.GetOutcomeId()})
		if row["status"] != "abstained" || row["abstention_code"] != "opaque_tool" {
			t.Fatalf("opaque manifest = %#v", row)
		}
	})

	t.Run("tenant isolation denies foreign outcome", func(t *testing.T) {
		traceID := ingestPipelineTrace(t, ctx, "isolation", "writer", false)
		client := memjevv1connect.NewOutcomeServiceClient(http.DefaultClient, requiredEnv(t, "MEMJEV_E2E_API_URL"))
		request := connect.NewRequest(&memjevv1.RecordOutcomeRequest{TraceId: traceID, Evidence: satisfiedEvidence("foreign")})
		request.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_OTHER_TOKEN"))
		request.Header().Set("Idempotency-Key", "qualified-foreign-outcome")
		if _, err := client.RecordOutcome(ctx, request); connect.CodeOf(err) != connect.CodeNotFound {
			t.Fatalf("foreign outcome error = %v", err)
		}
	})

	t.Run("corrupted encrypted archive is quarantined", func(t *testing.T) {
		traceID := ingestPipelineTrace(t, ctx, "corrupt", "writer", false)
		row := waitForRow(t, ctx, db, "trace_run", "tenant_id = $tenant AND trace_id = $trace", map[string]any{"tenant": "tenant_e2e", "trace": traceID})
		key := fmt.Sprint(row["archive_key"])
		parts := strings.Split(key, "/")
		if len(parts) < 2 {
			t.Fatalf("archive key = %q", key)
		}
		hash := strings.TrimSuffix(parts[len(parts)-1], ".json")
		_, err := openS3(t, ctx).PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(requiredEnv(t, "MEMJEV_E2E_S3_BUCKET")), Key: aws.String(key), Body: bytes.NewReader([]byte(`{"corrupt":true}`)),
			ContentType: aws.String("application/json"), ServerSideEncryption: types.ServerSideEncryptionAes256,
			Metadata: map[string]string{"schema-version": "canonical.v1", "content-sha256": hash},
		})
		if err != nil {
			t.Fatal(err)
		}
		outcome := recordPipelineOutcome(t, ctx, traceID, "qualified-corrupt", satisfiedEvidence("corrupt"), requiredEnv(t, "MEMJEV_E2E_TOKEN"))
		audit := waitForRow(t, ctx, db, "audit_event", "tenant_id = $tenant AND subject_id = $subject AND event_type = 'pipeline_quarantine'", map[string]any{
			"tenant": "tenant_e2e", "subject": store.OutcomeWorkflowID("tenant_e2e", domain.TraceID(traceID), domain.OutcomeID(outcome.Msg.GetOutcomeId())),
		})
		if audit["reason_code"] != "archive_corrupt" {
			t.Fatalf("audit = %#v", audit)
		}
		assertNoRow(t, ctx, db, "synthesis_manifest", "tenant_id = $tenant AND outcome_id = $outcome", map[string]any{"tenant": "tenant_e2e", "outcome": outcome.Msg.GetOutcomeId()})
	})
}

func TestPrepareExpiredLeaseForWorkerRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	traceID := ingestPipelineTrace(t, ctx, "worker-recovery", "writer", false)
	db := openSurreal(t, ctx)
	defer func() { _ = db.Close(context.Background()) }()
	workflowID := store.WorkflowID("tenant_e2e", domain.TraceID(traceID))
	row := waitForRow(t, ctx, db, "outbox_job", "tenant_id = $tenant AND workflow_id = $workflow AND state = 'pending'", map[string]any{"tenant": "tenant_e2e", "workflow": workflowID})
	if fmt.Sprint(row["attempt_count"]) != "0" {
		t.Fatalf("new recovery job = %#v", row)
	}
	_, err := surrealdb.Query[any](ctx, db, `UPDATE outbox_job SET state = "leased", lease_owner = "crashed-worker",
		lease_generation = 1, lease_expires_at = time::now() - 2s, attempt_count = 1
		WHERE tenant_id = $tenant AND workflow_id = $workflow`, map[string]any{"tenant": "tenant_e2e", "workflow": workflowID})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRecoversExpiredLeaseAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	traceID := ingestPipelineTrace(t, ctx, "worker-recovery", "writer", false)
	db := openSurreal(t, ctx)
	defer func() { _ = db.Close(context.Background()) }()
	workflowID := store.WorkflowID("tenant_e2e", domain.TraceID(traceID))
	row := waitForRow(t, ctx, db, "outbox_job", "tenant_id = $tenant AND workflow_id = $workflow AND state = 'completed' AND lease_generation >= 2", map[string]any{"tenant": "tenant_e2e", "workflow": workflowID})
	if fmt.Sprint(row["lease_owner"]) == "crashed-worker" {
		t.Fatalf("expired lease was not fenced: %#v", row)
	}
	stage := waitForRow(t, ctx, db, "pipeline_stage_artifact", "tenant_id = $tenant AND workflow_id = $workflow AND stage = 'evaluate'", map[string]any{"tenant": "tenant_e2e", "workflow": workflowID})
	if stage["status"] != "terminal" || stage["code"] != "awaiting_outcome" {
		t.Fatalf("recovered workflow stage = %#v", stage)
	}
}

func seedWriterContract(t *testing.T, ctx context.Context, db *surrealdb.DB) {
	t.Helper()
	manifest, err := toolcontract.Canonicalize(toolcontract.Manifest{
		SchemaVersion: "tool-contract.v1", ToolID: "writer", Version: "1.0.0",
		Inputs:     []toolcontract.FieldSpec{{Name: "path", Type: "string", Required: true, Sanitizer: toolcontract.SanitizerRelativePath}},
		Writes:     []toolcontract.ResourceSpec{{Name: "workspace_file", Type: "file", Namespace: "workspace", Field: "path"}},
		SideEffect: toolcontract.SideEffectWrite, Risk: toolcontract.RiskLow,
		Idempotency:         toolcontract.IdempotencySpec{Mode: toolcontract.IdempotencyGuaranteed},
		Retry:               toolcontract.RetrySpec{Mode: toolcontract.RetryOnDeclaredTransient, MaximumAttempts: 2},
		SuccessPredicates:   []toolcontract.PredicateSpec{{ID: "goal", Field: "path", Operator: "exists"}},
		VerificationMethods: []toolcontract.VerificationMethod{{ID: "ci", EvidenceClass: string(domain.EvidenceClassGoalPredicate)}},
		Compatibility:       []toolcontract.CompatibilityRange{{MinimumInclusive: "1.0.0", MaximumExclusive: "2.0.0"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(manifest)
	var stored map[string]any
	if err := json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	_, err = surrealdb.Query[any](ctx, db, `UPSERT type::record("tool_contract_version", $id) CONTENT $record`, map[string]any{"id": manifest.ID, "record": map[string]any{
		"tenant_id": "tenant_e2e", "contract_version_id": manifest.ID, "tool_id": manifest.ToolID, "tool_version": manifest.Version,
		"manifest": stored, "created_at": time.Now().UTC(), "schema_version": manifest.SchemaVersion, "content_hash": manifest.ContentHash,
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func ingestPipelineTrace(t *testing.T, ctx context.Context, suffix, tool string, includeFailure bool) string {
	t.Helper()
	events := make([]*memjevv1.TraceEvent, 0, 2)
	if includeFailure {
		events = append(events, pipelineEvent(suffix+"-failed", tool, "TOOL_RESULT_STATE_FAILURE", 1))
	}
	events = append(events, pipelineEvent(suffix+"-success", tool, "TOOL_RESULT_STATE_SUCCESS", 2))
	client := memjevv1connect.NewIngestServiceClient(http.DefaultClient, requiredEnv(t, "MEMJEV_E2E_API_URL"))
	request := connect.NewRequest(&memjevv1.IngestTraceRequest{ClientTraceId: "pipeline-" + suffix, Harness: "e2e", Task: "write output", Events: events})
	request.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_TOKEN"))
	request.Header().Set("Idempotency-Key", "qualified-ingest-"+suffix)
	response, err := client.IngestTrace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return response.Msg.GetTraceId()
}

func pipelineEvent(id, tool, state string, second int64) *memjevv1.TraceEvent {
	resultState := memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS
	if state == "TOOL_RESULT_STATE_FAILURE" {
		resultState = memjevv1.ToolResultState_TOOL_RESULT_STATE_FAILURE
	}
	return &memjevv1.TraceEvent{ClientEventId: id, OccurredAt: timestamppb.New(time.Unix(second, 0).UTC()), Kind: memjevv1.EventKind_EVENT_KIND_EXECUTE,
		ToolName: tool, ToolVersion: "1.0.0", Fields: []*memjevv1.Field{{Name: "path", StringValue: "output.txt"}}, Result: &memjevv1.ToolResult{State: resultState}}
}

func satisfiedEvidence(id string) []*memjevv1.OutcomeEvidence {
	return []*memjevv1.OutcomeEvidence{{ClientEvidenceId: id, Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
		Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, PredicateId: "goal", VerifierId: "ci", ObservedAt: timestamppb.New(time.Unix(3, 0).UTC())}}
}

func recordPipelineOutcome(t *testing.T, ctx context.Context, traceID, key string, facts []*memjevv1.OutcomeEvidence, token string) *connect.Response[memjevv1.RecordOutcomeResponse] {
	t.Helper()
	client := memjevv1connect.NewOutcomeServiceClient(http.DefaultClient, requiredEnv(t, "MEMJEV_E2E_API_URL"))
	request := connect.NewRequest(&memjevv1.RecordOutcomeRequest{TraceId: traceID, ExecutionId: key, Evidence: facts})
	request.Header().Set("Authorization", "Bearer "+token)
	request.Header().Set("Idempotency-Key", key)
	response, err := client.RecordOutcome(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func waitForRow(t *testing.T, ctx context.Context, db *surrealdb.DB, table, predicate string, variables map[string]any) map[string]any {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM "+table+" WHERE "+predicate+" LIMIT 1", variables)
		if err == nil && rows != nil && len(*rows) > 0 && len((*rows)[0].Result) == 1 {
			return (*rows)[0].Result[0]
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %s: %v (last query error %v)", table, ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func assertNoRow(t *testing.T, ctx context.Context, db *surrealdb.DB, table, predicate string, variables map[string]any) {
	t.Helper()
	rows, err := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM "+table+" WHERE "+predicate+" LIMIT 1", variables)
	if err != nil {
		t.Fatal(err)
	}
	if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) != 0 {
		t.Fatalf("unexpected %s row: %#v", table, (*rows)[0].Result)
	}
}

func canonicalProjection(t *testing.T, ctx context.Context, db *surrealdb.DB, id any) string {
	t.Helper()
	row := waitForRow(t, ctx, db, "procedure_version", "tenant_id = $tenant AND procedure_version_id = $id", map[string]any{"tenant": "tenant_e2e", "id": fmt.Sprint(id)})
	return fmt.Sprint(row["canonical_projection"])
}

func assertByteIdenticalRebuild(t *testing.T, ctx context.Context, db *surrealdb.DB, traceID, outcomeID, versionID, want string) {
	t.Helper()
	workflowID := store.OutcomeWorkflowID("tenant_e2e", domain.TraceID(traceID), domain.OutcomeID(outcomeID))
	var source memworkflow.LoadedSource
	decodeStagePayload(t, ctx, db, workflowID, memworkflow.StageLoad, &source)
	var graph struct {
		IntentHash          string `json:"intent_hash"`
		EffectSignatureHash string `json:"effect_signature_hash"`
		EnvironmentHash     string `json:"environment_hash"`
	}
	decodeStagePayload(t, ctx, db, workflowID, memworkflow.StageBuildGraph, &graph)
	var synthesized struct {
		Result     synthesis.Result `json:"result"`
		ResultHash string           `json:"result_hash"`
	}
	decodeStagePayload(t, ctx, db, workflowID, memworkflow.StageSynthesize, &synthesized)
	synthesized.Result.Hash = synthesized.ResultHash
	rows, err := surrealdb.Query[[]struct {
		Canonical string `json:"canonical_document"`
		Epoch     uint64 `json:"projection_epoch"`
	}](ctx, db, `SELECT canonical_document, projection_epoch FROM retrieval_document WHERE tenant_id = $tenant AND procedure_version_id = $version ORDER BY projection_epoch DESC LIMIT 1`, map[string]any{"tenant": "tenant_e2e", "version": versionID})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) != 1 {
		t.Fatalf("load serving document: rows=%#v err=%v", rows, err)
	}
	var document retrieval.Document
	if err := json.Unmarshal([]byte((*rows)[0].Result[0].Canonical), &document); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := projection.Build(projection.BuildRequest{
		TenantID: "tenant_e2e", OutcomeID: domain.OutcomeID(outcomeID), IntentHash: graph.IntentHash,
		EffectSignatureHash: graph.EffectSignatureHash, EnvironmentScopeHash: graph.EnvironmentHash,
		ArchiveHash: source.ArchiveHash, CanonicalEventStart: 0, CanonicalEventEnd: uint32(len(source.Batch.Events) - 1),
		CreatedAt: time.Unix(100, 0).UTC(), Synthesis: synthesized.Result,
		Serving: projection.ServingMetadata{TaskText: document.TaskText, Harness: document.Harness, Environment: document.Environment,
			Resources: document.Resources, Effects: document.Effects, RiskClass: document.RiskClass, VerificationStrength: document.VerificationStrength,
			LearnedWithRecallConsent: document.LearnedWithRecallConsent, ResidencyRegion: document.ResidencyRegion},
	})
	if err != nil {
		t.Fatal(err)
	}
	emptyTarget := storememory.NewProjectionRepository()
	if _, err := emptyTarget.Publish(ctx, rebuilt); err != nil {
		t.Fatal(err)
	}
	restored, err := emptyTarget.Canonical(ctx, "tenant_e2e", rebuilt.Version.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rebuild.CompareProjection(ctx, []byte(want), restored); err != nil {
		t.Fatal(err)
	}
}

func decodeStagePayload(t *testing.T, ctx context.Context, db *surrealdb.DB, workflowID string, stage memworkflow.Stage, target any) {
	t.Helper()
	row := waitForRow(t, ctx, db, "pipeline_stage_artifact", "tenant_id = $tenant AND workflow_id = $workflow AND stage = $stage", map[string]any{
		"tenant": "tenant_e2e", "workflow": workflowID, "stage": string(stage),
	})
	payload, ok := row["payload"].(string)
	if !ok {
		t.Fatalf("stage %s payload = %#v", stage, row["payload"])
	}
	if err := json.Unmarshal([]byte(payload), target); err != nil {
		t.Fatalf("decode %s stage: %v", stage, err)
	}
}
