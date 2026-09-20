package api

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/security"
)

type retrievalAPI interface {
	Retrieve(context.Context, retrieval.ServiceRequest) (retrieval.ServiceResponse, error)
	Explain(context.Context, domain.TenantID, string) (retrieval.Explanation, error)
}

type retrievalHandler struct {
	service              retrievalAPI
	currentPolicyVersion string
}

func (h *retrievalHandler) Retrieve(ctx context.Context, request *connect.Request[memjevv1.RetrieveRequest]) (*connect.Response[memjevv1.RetrieveResponse], error) {
	started := time.Now()
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeUnauthenticated, "authentication_required", false)
	}
	metadata, ok := security.RequestMetadataFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeInvalidArgument, "idempotency_key_required", false)
	}
	if h.service == nil || h.currentPolicyVersion == "" {
		return nil, safeConnectError(ctx, connect.CodeUnavailable, "retrieval_unavailable", true)
	}
	result, err := h.service.Retrieve(ctx, retrieval.ServiceRequest{TenantID: principal.TenantID, CurrentPolicyVersion: h.currentPolicyVersion, RequestIdentityHash: metadata.IdempotencyKeyHash, Input: retrieval.InputFromProto(request.Msg), RecallAllowed: principal.Consent == policy.RecallOnly || principal.Consent == policy.LearnAndRecall, AllowedResidencyRegions: []string{principal.Region}})
	if err != nil {
		observability.RecordRetrieval(ctx, observability.RetrievalMetrics{ResultCode: retrievalMetricCode(err), Latency: time.Since(started)})
		return nil, mapDomainError(ctx, err)
	}
	observability.RecordRetrieval(ctx, observability.RetrievalMetrics{ResultCode: "ok", Disposition: string(result.Disposition), Latency: time.Since(started), Candidates: len(result.Candidates), Replayed: result.Replayed})
	return connect.NewResponse(retrieveResponse(result)), nil
}

func retrievalMetricCode(err error) string {
	var serviceErr *retrieval.ServiceError
	if errors.As(err, &serviceErr) {
		return serviceErr.Code
	}
	if errors.Is(err, retrieval.ErrRecallDenied) {
		return "recall_not_permitted"
	}
	if errors.Is(err, retrieval.ErrInvalidQuery) || errors.Is(err, retrieval.ErrInvalidRun) {
		return "invalid_retrieval"
	}
	return "failed"
}

func (h *retrievalHandler) ExplainRetrieval(ctx context.Context, request *connect.Request[memjevv1.ExplainRetrievalRequest]) (*connect.Response[memjevv1.ExplainRetrievalResponse], error) {
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeUnauthenticated, "authentication_required", false)
	}
	if h.service == nil {
		return nil, safeConnectError(ctx, connect.CodeUnavailable, "retrieval_unavailable", true)
	}
	result, err := h.service.Explain(ctx, principal.TenantID, request.Msg.GetRetrievalRunId())
	if err != nil {
		return nil, mapDomainError(ctx, err)
	}
	return connect.NewResponse(explainResponse(result)), nil
}

func retrieveResponse(result retrieval.ServiceResponse) *memjevv1.RetrieveResponse {
	candidates := make([]*memjevv1.RetrievalCandidate, len(result.Candidates))
	for index, item := range result.Candidates {
		candidates[index] = &memjevv1.RetrievalCandidate{ProcedureVersionId: item.VersionID, ProcedureId: item.ProcedureID, FinalScore: item.FinalScore, Rank: item.Rank, Lifecycle: lifecycle(item.Lifecycle), ObservedEndToEnd: item.ObservedEndToEnd, AdvisoryOnly: item.AdvisoryOnly}
	}
	return &memjevv1.RetrieveResponse{RetrievalRunId: result.RunID, Disposition: disposition(result.Disposition), Candidates: candidates, AbstentionCode: result.DecisionCode, Provenance: provenance(result.Snapshot, result.QueryHash, result.Degraded, result.Approximate)}
}

func explainResponse(result retrieval.Explanation) *memjevv1.ExplainRetrievalResponse {
	candidates := make([]*memjevv1.CandidateExplanation, len(result.Candidates))
	for index, item := range result.Candidates {
		facts := make([]*memjevv1.ExplanationFact, len(item.Facts))
		for factIndex, fact := range item.Facts {
			facts[factIndex] = &memjevv1.ExplanationFact{Code: string(fact.Code), SubjectId: fact.SubjectID, Field: fact.Field, Expected: fact.Expected, Observed: fact.Observed}
		}
		channels := make([]string, len(item.SourceChannels))
		for channelIndex, channel := range item.SourceChannels {
			channels[channelIndex] = string(channel)
		}
		candidates[index] = &memjevv1.CandidateExplanation{ProcedureVersionId: item.VersionID, Eligible: item.Eligible, SourceChannels: channels, Facts: facts, FusedRankScore: item.RRFScore, FinalScore: item.FinalScore, FinalRank: item.FinalRank}
	}
	return &memjevv1.ExplainRetrievalResponse{RetrievalRunId: result.RunID, Disposition: disposition(result.Disposition), AbstentionCode: result.DecisionCode, Provenance: provenance(result.Snapshot, result.QueryHash, result.Degraded, result.Approximate), Candidates: candidates}
}

func provenance(snapshot retrieval.ServingSnapshot, queryHash string, degraded []retrieval.DegradedChannel, approximate bool) *memjevv1.RetrievalProvenance {
	indexes := make([]string, len(snapshot.Indexes))
	for index, item := range snapshot.Indexes {
		indexes[index] = item.ManifestID
	}
	degradedCodes := make([]string, len(degraded))
	for index, item := range degraded {
		degradedCodes[index] = string(item.Channel) + ":" + item.Code
	}
	return &memjevv1.RetrievalProvenance{ProjectionEpoch: snapshot.ProjectionEpoch, QueryHash: queryHash, PolicyVersion: snapshot.PolicyManifestID, RankerVersion: snapshot.RankerManifestID, IndexManifestIds: indexes, DegradedChannels: degradedCodes, ApproximateCandidates: approximate}
}

func disposition(value retrieval.RunDisposition) memjevv1.RetrievalDisposition {
	if value == retrieval.RunSelected {
		return memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED
	}
	if value == retrieval.RunAbstained {
		return memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_ABSTAINED
	}
	return memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_UNSPECIFIED
}

func lifecycle(value string) memjevv1.CandidateLifecycle {
	switch value {
	case "candidate":
		return memjevv1.CandidateLifecycle_CANDIDATE_LIFECYCLE_CANDIDATE
	case "trial":
		return memjevv1.CandidateLifecycle_CANDIDATE_LIFECYCLE_TRIAL
	case "active":
		return memjevv1.CandidateLifecycle_CANDIDATE_LIFECYCLE_ACTIVE
	default:
		return memjevv1.CandidateLifecycle_CANDIDATE_LIFECYCLE_UNSPECIFIED
	}
}
