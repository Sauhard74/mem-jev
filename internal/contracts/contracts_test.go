package contracts_test

import (
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIngestRequestDoesNotExposeTenantID(t *testing.T) {
	fields := (&memjevv1.IngestTraceRequest{}).ProtoReflect().Descriptor().Fields()
	if fields.ByName("tenant_id") != nil {
		t.Fatal("tenant_id must come from authentication, never the request")
	}
	if fields.ByName("idempotency_key") != nil {
		t.Fatal("idempotency_key belongs to authenticated transport metadata")
	}
}

func TestValidateIngestRejectsDuplicateClientEventID(t *testing.T) {
	req := validRequest()
	req.Events = append(req.Events, req.Events[0])

	err := contracts.ValidateIngest(req)
	if err == nil || !strings.Contains(err.Error(), "events[1].client_event_id") {
		t.Fatalf("ValidateIngest() error = %v; want duplicate field path", err)
	}
}

func TestValidateIngestRejectsMessageLargerThanOneMiB(t *testing.T) {
	req := validRequest()
	req.Task = strings.Repeat("x", 1<<20)

	err := contracts.ValidateIngest(req)
	if err == nil || !strings.Contains(err.Error(), "request_size") {
		t.Fatalf("ValidateIngest() error = %v; want request_size violation", err)
	}
}

func validRequest() *memjevv1.IngestTraceRequest {
	return &memjevv1.IngestTraceRequest{
		ClientTraceId:  "trace-1",
		Harness:        "test-harness",
		HarnessVersion: "1.0.0",
		Task:           "run the checks",
		Events: []*memjevv1.TraceEvent{{
			ClientEventId: "event-1",
			OccurredAt:    timestamppb.New(time.Unix(1, 0).UTC()),
			Kind:          memjevv1.EventKind_EVENT_KIND_EXECUTE,
			ToolName:      "shell",
			ToolVersion:   "1.0.0",
			Result: &memjevv1.ToolResult{
				State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS,
			},
		}},
	}
}
