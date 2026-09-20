package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/security"
)

type recordingRetrievalAPI struct {
	retrieveRequest retrieval.ServiceRequest
	explainTenant   domain.TenantID
	explainRunID    string
	retrieveResult  retrieval.ServiceResponse
	explainResult   retrieval.Explanation
	err             error
}

func (s *recordingRetrievalAPI) Retrieve(_ context.Context, request retrieval.ServiceRequest) (retrieval.ServiceResponse, error) {
	s.retrieveRequest = request
	return s.retrieveResult, s.err
}

func (s *recordingRetrievalAPI) Explain(_ context.Context, tenantID domain.TenantID, runID string) (retrieval.Explanation, error) {
	s.explainTenant, s.explainRunID = tenantID, runID
	return s.explainResult, s.err
}

func TestRetrievalHTTPBindsServerAuthorityAndMapsResponse(t *testing.T) {
	fake := &recordingRetrievalAPI{retrieveResult: retrieval.ServiceResponse{
		RunID: "rrun_" + strings.Repeat("a", 64), Disposition: retrieval.RunSelected, QueryHash: strings.Repeat("b", 64), Approximate: true,
		Snapshot:   retrieval.ServingSnapshot{ProjectionEpoch: 42, PolicyManifestID: "epm_policy", RankerManifestID: "rkm_ranker", Indexes: []retrieval.SnapshotIndex{{Channel: retrieval.ChannelExact, ManifestID: "idx_exact"}}},
		Degraded:   []retrieval.DegradedChannel{{Channel: retrieval.ChannelGraph, Code: "deadline"}},
		Candidates: []retrieval.SelectedCandidate{{VersionID: "pv_1", ProcedureID: "p_1", FinalScore: 77, Rank: 1, Lifecycle: "active", ObservedEndToEnd: true}},
		Plan:       &retrieval.PlanArtifact{InjectionID: "inj_1", SelectionHash: strings.Repeat("1", 64), PlanHash: strings.Repeat("2", 64), ProjectionEpoch: 42, NoveltyClass: "exact", Complete: true, PlannerManifestID: "pman_1", PolicyManifestID: "epm_policy", RankerManifestID: "rkm_ranker", ServingConfigID: "rsc_1", DocumentSetHash: strings.Repeat("3", 64), CompatibilityGraphID: "cgraph_1", CompatibilityGraphHash: strings.Repeat("4", 64), CompatibilityMatrixHash: strings.Repeat("5", 64), CandidateSetHash: strings.Repeat("6", 64), Nodes: []retrieval.PlanNode{{Ordinal: 0, VersionID: "pv_1", InterfaceHash: strings.Repeat("7", 64)}}, ParallelGroups: []retrieval.ParallelGroup{{Ordinal: 0, NodeVersionIDs: []string{"pv_1"}}}},
	}}
	server := httptest.NewServer(retrievalTestHandler(fake, policy.LearnAndRecall))
	defer server.Close()
	client := memjevv1connect.NewRetrievalServiceClient(server.Client(), server.URL)
	request := connect.NewRequest(validRetrieveRequest())
	request.Header().Set("Authorization", "Bearer "+testToken)
	request.Header().Set("Idempotency-Key", testIdempotency)
	response, err := client.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	wantIdentity := sha256.Sum256([]byte(testIdempotency))
	got := fake.retrieveRequest
	if got.TenantID != "tenant_a" || got.CurrentPolicyVersion != "policy-current" || got.RequestIdentityHash != hex.EncodeToString(wantIdentity[:]) || !got.RecallAllowed || len(got.AllowedResidencyRegions) != 1 || got.AllowedResidencyRegions[0] != "local" {
		t.Fatalf("server authority binding = %#v", got)
	}
	if got.Input.Task != "Deploy app" || got.Input.Harness.Name != "codex" || len(got.Input.Tools) != 1 {
		t.Fatalf("typed input = %#v", got.Input)
	}
	if response.Msg.GetRetrievalRunId() != fake.retrieveResult.RunID || response.Msg.GetDisposition() != memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED || len(response.Msg.GetCandidates()) != 1 || !response.Msg.GetProvenance().GetApproximateCandidates() || response.Msg.GetProvenance().GetProjectionEpoch() != 42 {
		t.Fatalf("response = %#v", response.Msg)
	}
	if response.Msg.GetPlan().GetInjectionId() != "inj_1" || len(response.Msg.GetPlan().GetNodes()) != 1 || !response.Msg.GetPlan().GetComplete() || response.Msg.GetPlan().GetProvenance().GetSelectionHash() != strings.Repeat("1", 64) {
		t.Fatalf("plan = %#v", response.Msg.GetPlan())
	}
	first, err := proto.MarshalOptions{Deterministic: true}.Marshal(response.Msg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := proto.MarshalOptions{Deterministic: true}.Marshal(retrieveResponse(fake.retrieveResult))
	if err != nil || string(first) != string(second) {
		t.Fatalf("response bytes were not stable: error=%v", err)
	}
}

func TestRetrievalHTTPRequiresIdempotencyOnlyForRetrieve(t *testing.T) {
	fake := &recordingRetrievalAPI{explainResult: retrieval.Explanation{RunID: "rrun_" + strings.Repeat("c", 64), Disposition: retrieval.RunAbstained, DecisionCode: "no_candidates", QueryHash: strings.Repeat("d", 64), Approximate: true, Snapshot: retrieval.ServingSnapshot{ProjectionEpoch: 7, PolicyManifestID: "epm", RankerManifestID: "rkm"}}}
	server := httptest.NewServer(retrievalTestHandler(fake, policy.RecallOnly))
	defer server.Close()
	client := memjevv1connect.NewRetrievalServiceClient(server.Client(), server.URL)

	retrieve := connect.NewRequest(validRetrieveRequest())
	retrieve.Header().Set("Authorization", "Bearer "+testToken)
	if _, err := client.Retrieve(context.Background(), retrieve); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("retrieve code = %v, error=%v", connect.CodeOf(err), err)
	}
	explain := connect.NewRequest(&memjevv1.ExplainRetrievalRequest{RetrievalRunId: fake.explainResult.RunID})
	explain.Header().Set("Authorization", "Bearer "+testToken)
	response, err := client.ExplainRetrieval(context.Background(), explain)
	if err != nil {
		t.Fatal(err)
	}
	if fake.explainTenant != "tenant_a" || fake.explainRunID != fake.explainResult.RunID || !response.Msg.GetProvenance().GetApproximateCandidates() {
		t.Fatalf("tenant=%q run=%q response=%#v", fake.explainTenant, fake.explainRunID, response.Msg)
	}
}

func TestRetrievalHTTPEnforcesScopeConsentAndSafeErrors(t *testing.T) {
	t.Run("scope", func(t *testing.T) {
		principal := testPrincipal(policy.LearnAndRecall)
		delete(principal.Scopes, security.ScopeRetrievalRead)
		server := httptest.NewServer(retrievalHandlerWithPrincipal(&recordingRetrievalAPI{}, principal))
		defer server.Close()
		client := memjevv1connect.NewRetrievalServiceClient(server.Client(), server.URL)
		request := retrievalConnectRequest()
		_, err := client.Retrieve(context.Background(), request)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("code = %v, error=%v", connect.CodeOf(err), err)
		}
	})
	t.Run("deny consent and safe error", func(t *testing.T) {
		fake := &recordingRetrievalAPI{err: &retrieval.ServiceError{Code: "effect_inference_failed", Err: context.DeadlineExceeded}}
		server := httptest.NewServer(retrievalTestHandler(fake, policy.Deny))
		defer server.Close()
		client := memjevv1connect.NewRetrievalServiceClient(server.Client(), server.URL)
		_, err := client.Retrieve(context.Background(), retrievalConnectRequest())
		if !strings.Contains(err.Error(), "request failed") || strings.Contains(err.Error(), "effect inference") || connect.CodeOf(err) != connect.CodeDeadlineExceeded {
			t.Fatalf("unsafe or wrong error: %v", err)
		}
		if fake.retrieveRequest.RecallAllowed {
			t.Fatal("deny consent was converted to recall permission")
		}
	})
}

func TestRetrievalHTTPMapsIdempotencyConflict(t *testing.T) {
	fake := &recordingRetrievalAPI{err: &retrieval.ServiceError{Code: "idempotency_conflict", Err: retrieval.ErrIdempotencyConflict}}
	server := httptest.NewServer(retrievalTestHandler(fake, policy.LearnAndRecall))
	defer server.Close()
	client := memjevv1connect.NewRetrievalServiceClient(server.Client(), server.URL)
	_, err := client.Retrieve(context.Background(), retrievalConnectRequest())
	if connect.CodeOf(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), "request failed") {
		t.Fatalf("conflict error = %v", err)
	}
}

func retrievalTestHandler(service retrievalAPI, consent policy.ConsentMode) http.Handler {
	return retrievalHandlerWithPrincipal(service, testPrincipal(consent))
}

func retrievalHandlerWithPrincipal(service retrievalAPI, principal security.Principal) http.Handler {
	resolver := credentialResolver{tokenHash(testToken): principal}
	return NewHandler(Dependencies{Retrieval: service, RetrievalPolicyVersion: "policy-current", Authenticator: security.NewBearerAuthenticator(resolver)})
}

func retrievalConnectRequest() *connect.Request[memjevv1.RetrieveRequest] {
	request := connect.NewRequest(validRetrieveRequest())
	request.Header().Set("Authorization", "Bearer "+testToken)
	request.Header().Set("Idempotency-Key", testIdempotency)
	return request
}

func validRetrieveRequest() *memjevv1.RetrieveRequest {
	return &memjevv1.RetrieveRequest{Task: "Deploy app", Tools: []*memjevv1.AvailableTool{{Name: "shell", ContractVersionId: "v1"}}, Harness: &memjevv1.HarnessIdentity{Name: "codex", Version: "1"}, RiskClass: memjevv1.RiskClass_RISK_CLASS_LOW, LatencyClass: memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE, MaxCandidates: 5}
}
