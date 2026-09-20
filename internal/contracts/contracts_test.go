package contracts_test

import (
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func TestOutcomeRequestDoesNotExposeTenantOrIdempotency(t *testing.T) {
	fields := (&memjevv1.RecordOutcomeRequest{}).ProtoReflect().Descriptor().Fields()
	for _, forbidden := range []string{"tenant_id", "idempotency_key"} {
		if fields.ByName(protoreflect.Name(forbidden)) != nil {
			t.Fatalf("%s must come from authenticated transport metadata", forbidden)
		}
	}
}

func TestValidateOutcomeRequiresTraceAndEvidence(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*memjevv1.RecordOutcomeRequest)
		wantErr string
	}{
		{name: "trace", mutate: func(req *memjevv1.RecordOutcomeRequest) { req.TraceId = " " }, wantErr: "trace_id"},
		{name: "evidence", mutate: func(req *memjevv1.RecordOutcomeRequest) { req.Evidence = nil }, wantErr: "evidence"},
		{name: "predicate", mutate: func(req *memjevv1.RecordOutcomeRequest) { req.Evidence[0].PredicateId = " " }, wantErr: "predicate_id"},
		{name: "verifier", mutate: func(req *memjevv1.RecordOutcomeRequest) { req.Evidence[0].VerifierId = " " }, wantErr: "verifier_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validOutcomeRequest()
			tt.mutate(req)
			if err := contracts.ValidateOutcome(req); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateOutcome() error = %v; want field %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateOutcomeRejectsDuplicateEvidenceID(t *testing.T) {
	req := validOutcomeRequest()
	duplicate := proto.Clone(req.Evidence[0]).(*memjevv1.OutcomeEvidence)
	req.Evidence = append(req.Evidence, duplicate)

	err := contracts.ValidateOutcome(req)
	if err == nil || !strings.Contains(err.Error(), "evidence[1].client_evidence_id") {
		t.Fatalf("ValidateOutcome() error = %v; want duplicate evidence path", err)
	}
}

func TestValidateOutcomeRejectsInvalidSupersession(t *testing.T) {
	req := validOutcomeRequest()
	req.SupersedesOutcomeId = "outcome-1"
	req.CorrectionReason = ""

	err := contracts.ValidateOutcome(req)
	if err == nil || !strings.Contains(err.Error(), "correction_reason") {
		t.Fatalf("ValidateOutcome() error = %v; want correction_reason", err)
	}
}

func validOutcomeRequest() *memjevv1.RecordOutcomeRequest {
	return &memjevv1.RecordOutcomeRequest{
		TraceId:     "tr_abc",
		ExecutionId: "execution-1",
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "evidence-1",
			Class:            memjevv1.EvidenceClass_EVIDENCE_CLASS_INDEPENDENT_VERIFIER,
			Verdict:          memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED,
			PredicateId:      "tests-pass",
			VerifierId:       "ci",
			VerifierVersion:  "1.0.0",
			ObservedAt:       timestamppb.New(time.Unix(2, 0).UTC()),
		}},
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
