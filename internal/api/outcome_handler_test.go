package api

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOutcomeHTTPInfersVerifiedStateAndDeduplicates(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	ingestClient := memjevv1connect.NewIngestServiceClient(server.Client(), server.URL)
	ingested, err := ingestClient.IngestTrace(context.Background(), validConnectRequest("trace-key-123456789", minimalTrace()))
	if err != nil {
		t.Fatal(err)
	}
	client := memjevv1connect.NewOutcomeServiceClient(server.Client(), server.URL)
	request := validOutcomeConnectRequest("outcome-key-123456", ingested.Msg.GetTraceId())
	first, err := client.RecordOutcome(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Msg.GetState() != memjevv1.OutcomeState_OUTCOME_STATE_VERIFIED_SUCCESS || !first.Msg.GetPromotionEligible() || first.Msg.GetDisposition() != memjevv1.OutcomeDisposition_OUTCOME_DISPOSITION_ACCEPTED {
		t.Fatalf("response = %#v", first.Msg)
	}
	second, err := client.RecordOutcome(context.Background(), validOutcomeConnectRequest("outcome-key-123456", ingested.Msg.GetTraceId()))
	if err != nil {
		t.Fatal(err)
	}
	if second.Msg.GetOutcomeId() != first.Msg.GetOutcomeId() || second.Msg.GetDisposition() != memjevv1.OutcomeDisposition_OUTCOME_DISPOSITION_DUPLICATE {
		t.Fatalf("first=%#v second=%#v", first.Msg, second.Msg)
	}
}

func TestOutcomeHTTPDoesNotRevealCrossTenantTrace(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	client := memjevv1connect.NewOutcomeServiceClient(server.Client(), server.URL)
	_, err := client.RecordOutcome(context.Background(), validOutcomeConnectRequest("outcome-key-654321", "tr_missing"))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("code = %v error=%v; want not found", connect.CodeOf(err), err)
	}
}

func validOutcomeConnectRequest(key, traceID string) *connect.Request[memjevv1.RecordOutcomeRequest] {
	request := connect.NewRequest(&memjevv1.RecordOutcomeRequest{
		TraceId: traceID, ExecutionId: "execution-1",
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "evidence-1", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
			Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED, PredicateId: "goal",
			VerifierId: "ci", VerifierVersion: "1.0.0", ObservedAt: timestamppb.New(time.Unix(2, 0).UTC()),
		}},
	})
	request.Header().Set("Authorization", "Bearer "+testToken)
	request.Header().Set("Idempotency-Key", key)
	return request
}
