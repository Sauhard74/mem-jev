// Command go-agent demonstrates the complete memJev lifecycle around an agent
// execution. Replace executeAgent with your framework's invocation and tool
// callback capture; the memJev calls remain the same.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	endpoint := requiredEnv("MEMJEV_URL")
	token := requiredEnv("MEMJEV_TOKEN")
	task := "write a hello file"
	if len(os.Args) > 1 {
		task = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runID := newID()

	retrievalClient := memjevv1connect.NewRetrievalServiceClient(http.DefaultClient, endpoint)
	retrieve := connect.NewRequest(&memjevv1.RetrieveRequest{
		Task:          task,
		Tools:         []*memjevv1.AvailableTool{{Name: "shell", ContractVersionId: "shell.v1"}},
		Harness:       &memjevv1.HarnessIdentity{Name: "go-agent-example", Version: "1"},
		RiskClass:     memjevv1.RiskClass_RISK_CLASS_LOW,
		LatencyClass:  memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE,
		MaxCandidates: 5,
	})
	authorize(retrieve, token, "retrieve-"+runID)
	recalled, err := retrievalClient.Retrieve(ctx, retrieve)
	if err != nil {
		log.Fatalf("retrieve plan: %v", err)
	}

	// An abstention is a valid response: run the agent normally. When selected,
	// pass recalled.Msg.GetPlan() to the agent as advisory context.
	events, verified := executeAgent(task, recalled.Msg.GetPlan())

	ingestClient := memjevv1connect.NewIngestServiceClient(http.DefaultClient, endpoint)
	ingest := connect.NewRequest(&memjevv1.IngestTraceRequest{
		ClientTraceId:  "trace-" + runID,
		Harness:        "go-agent-example",
		HarnessVersion: "1",
		Task:           task,
		Events:         events,
	})
	authorize(ingest, token, "ingest-"+runID)
	stored, err := ingestClient.IngestTrace(ctx, ingest)
	if err != nil {
		log.Fatalf("ingest trace: %v", err)
	}

	verdict := memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED
	if verified {
		verdict = memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED
	}
	outcomeRequest := &memjevv1.RecordOutcomeRequest{
		TraceId:     stored.Msg.GetTraceId(),
		ExecutionId: "execution-" + runID,
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "goal-" + runID,
			Class:            memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
			Verdict:          verdict,
			PredicateId:      "task-complete",
			VerifierId:       "application-verifier",
			VerifierVersion:  "1",
			ObservedAt:       timestamppb.Now(),
		}},
	}
	if plan := recalled.Msg.GetPlan(); plan != nil {
		outcomeRequest.InjectionId = plan.GetInjectionId()
		outcomeRequest.TaskExecutionId = plan.GetTaskExecutionId()
	}
	outcomeClient := memjevv1connect.NewOutcomeServiceClient(http.DefaultClient, endpoint)
	outcome := connect.NewRequest(outcomeRequest)
	authorize(outcome, token, "outcome-"+runID)
	recorded, err := outcomeClient.RecordOutcome(ctx, outcome)
	if err != nil {
		log.Fatalf("record outcome: %v", err)
	}

	fmt.Printf("retrieval=%s trace=%s outcome=%s\n",
		recalled.Msg.GetDisposition(), stored.Msg.GetTraceId(), recorded.Msg.GetState())
}

// executeAgent is the only framework-specific seam. Give plan to the model as
// advisory context, execute the task, capture every tool call in order, and
// return independently verified success rather than trusting the model's claim.
func executeAgent(task string, plan *memjevv1.ExecutableProcedurePlan) ([]*memjevv1.TraceEvent, bool) {
	fmt.Printf("agent task: %s; recalled plan nodes: %d\n", task, len(plan.GetNodes()))
	return []*memjevv1.TraceEvent{{
		ClientEventId: "tool-call-1",
		OccurredAt:    timestamppb.Now(),
		Kind:          memjevv1.EventKind_EVENT_KIND_EXECUTE,
		ToolName:      "shell",
		ToolVersion:   "1",
		Fields:        []*memjevv1.Field{{Name: "command", StringValue: "printf hello"}},
		Result: &memjevv1.ToolResult{
			State:    memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS,
			ExitCode: int32Pointer(0),
		},
	}}, true
}

type authenticatedRequest interface {
	Header() http.Header
}

func authorize(request authenticatedRequest, token, idempotencyKey string) {
	request.Header().Set("Authorization", "Bearer "+token)
	request.Header().Set("Idempotency-Key", idempotencyKey)
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func newID() string {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		log.Fatalf("generate run ID: %v", err)
	}
	return hex.EncodeToString(value)
}

func int32Pointer(value int32) *int32 { return &value }
