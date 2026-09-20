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
	result, err := h.service.Retrieve(ctx, retrieval.ServiceRequest{TenantID: principal.TenantID, CurrentPolicyVersion: h.currentPolicyVersion, RequestIdentityHash: metadata.IdempotencyKeyHash, Input: retrieval.InputFromProto(request.Msg), RecallAllowed: principal.Consent == policy.RecallOnly || principal.Consent == policy.LearnAndRecall, ExternalInferenceAllowed: principal.AllowExternalInference, AllowedResidencyRegions: []string{principal.Region}})
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
	return &memjevv1.RetrieveResponse{RetrievalRunId: result.RunID, Disposition: disposition(result.Disposition), Candidates: candidates, AbstentionCode: result.DecisionCode, Provenance: provenance(result.Snapshot, result.QueryHash, result.Degraded, result.EnhancementDegraded, result.Approximate), Plan: executablePlan(result.Plan)}
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
		candidate := &memjevv1.CandidateExplanation{ProcedureVersionId: item.VersionID, Eligible: item.Eligible, SourceChannels: channels, Facts: facts, FusedRankScore: item.RRFScore, FinalScore: item.FinalScore, FinalRank: item.FinalRank}
		if item.Semantic != nil {
			features := make([]*memjevv1.RankedFeature, len(item.Semantic.Features))
			for featureIndex, feature := range item.Semantic.Features {
				features[featureIndex] = &memjevv1.RankedFeature{Name: feature.Name, Value: feature.Value}
			}
			candidate.SemanticDisposition = item.Semantic.Disposition
			candidate.SemanticJudgmentKey = item.Semantic.JudgmentKey
			candidate.SemanticContentHash = item.Semantic.ContentHash
			candidate.SemanticProvider = item.Semantic.Provider
			candidate.SemanticModel = item.Semantic.Model
			candidate.SemanticRubricManifestId = item.Semantic.RubricManifestID
			candidate.SemanticFeatures = features
		}
		candidates[index] = candidate
	}
	response := &memjevv1.ExplainRetrievalResponse{RetrievalRunId: result.RunID, Disposition: disposition(result.Disposition), AbstentionCode: result.DecisionCode, Provenance: provenance(result.Snapshot, result.QueryHash, result.Degraded, result.EnhancementDegraded, result.Approximate), Candidates: candidates}
	if result.Plan != nil {
		response.CompositionEdges = planDependencies(result.Plan.Dependencies)
		response.PlanGaps = planGaps(result.Plan.Gaps)
	}
	return response
}

func executablePlan(plan *retrieval.PlanArtifact) *memjevv1.ExecutableProcedurePlan {
	if plan == nil {
		return nil
	}
	nodes := make([]*memjevv1.ProcedurePlanNode, len(plan.Nodes))
	for index, node := range plan.Nodes {
		nodes[index] = &memjevv1.ProcedurePlanNode{Ordinal: node.Ordinal, ProcedureVersionId: node.VersionID, InterfaceHash: node.InterfaceHash, Bridge: node.Bridge}
	}
	groups := []*memjevv1.ProcedureParallelGroup{}
	if plan.ParallelGroups != nil {
		groups = make([]*memjevv1.ProcedureParallelGroup, len(plan.ParallelGroups))
		for index, group := range plan.ParallelGroups {
			groups[index] = &memjevv1.ProcedureParallelGroup{Ordinal: group.Ordinal, ProcedureVersionIds: append([]string(nil), group.NodeVersionIDs...)}
		}
	}
	return &memjevv1.ExecutableProcedurePlan{
		InjectionId: plan.InjectionID, TaskExecutionId: plan.TaskExecutionID, Nodes: nodes, Dependencies: planDependencies(plan.Dependencies), ParallelGroups: groups,
		Gaps: planGaps(plan.Gaps), LimitCodes: append([]string(nil), plan.LimitCodes...), NoveltyClass: noveltyClass(plan.NoveltyClass), Complete: plan.Complete,
		Provenance: &memjevv1.ProcedurePlanProvenance{
			ProjectionEpoch: plan.ProjectionEpoch, SelectionHash: plan.SelectionHash, PlanHash: plan.PlanHash,
			CompatibilityGraphId: plan.CompatibilityGraphID, CompatibilityGraphHash: plan.CompatibilityGraphHash,
			CompatibilityMatrixHash: plan.CompatibilityMatrixHash, CandidateSetHash: plan.CandidateSetHash,
			PlannerManifestId: plan.PlannerManifestID, PolicyManifestId: plan.PolicyManifestID, RankerManifestId: plan.RankerManifestID,
			ServingConfigId: plan.ServingConfigID, DocumentSetHash: plan.DocumentSetHash,
		},
	}
}

func planDependencies(source []retrieval.PlanDependency) []*memjevv1.ProcedurePlanDependency {
	result := make([]*memjevv1.ProcedurePlanDependency, len(source))
	for index, dependency := range source {
		result[index] = &memjevv1.ProcedurePlanDependency{CompatibilityEdgeId: dependency.CompatibilityEdgeID, SourceProcedureVersionId: dependency.SourceVersionID, TargetProcedureVersionId: dependency.TargetVersionID, SourceProvisionIds: append([]string(nil), dependency.SourceProvisionIDs...), SatisfiedRequirementIds: append([]string(nil), dependency.SatisfiedRequirementIDs...)}
	}
	return result
}

func planGaps(source []retrieval.PlanGap) []*memjevv1.ProcedurePlanGap {
	result := make([]*memjevv1.ProcedurePlanGap, len(source))
	for index, gap := range source {
		result[index] = &memjevv1.ProcedurePlanGap{ProcedureVersionId: gap.VersionID, RequirementId: gap.RequirementID, GoalPredicateId: gap.GoalPredicateID, Code: gap.Code}
	}
	return result
}

func noveltyClass(value string) memjevv1.PlanNoveltyClass {
	switch value {
	case "exact":
		return memjevv1.PlanNoveltyClass_PLAN_NOVELTY_CLASS_EXACT
	case "known_shape":
		return memjevv1.PlanNoveltyClass_PLAN_NOVELTY_CLASS_KNOWN_SHAPE
	case "bridged":
		return memjevv1.PlanNoveltyClass_PLAN_NOVELTY_CLASS_BRIDGED
	case "partial":
		return memjevv1.PlanNoveltyClass_PLAN_NOVELTY_CLASS_PARTIAL
	case "unseen":
		return memjevv1.PlanNoveltyClass_PLAN_NOVELTY_CLASS_UNSEEN
	default:
		return memjevv1.PlanNoveltyClass_PLAN_NOVELTY_CLASS_UNSPECIFIED
	}
}

func provenance(snapshot retrieval.ServingSnapshot, queryHash string, degraded []retrieval.DegradedChannel, enhancementDegraded []string, approximate bool) *memjevv1.RetrievalProvenance {
	indexes := make([]string, len(snapshot.Indexes))
	for index, item := range snapshot.Indexes {
		indexes[index] = item.ManifestID
	}
	degradedCodes := make([]string, len(degraded))
	for index, item := range degraded {
		degradedCodes[index] = string(item.Channel) + ":" + item.Code
	}
	return &memjevv1.RetrievalProvenance{ProjectionEpoch: snapshot.ProjectionEpoch, QueryHash: queryHash, PolicyVersion: snapshot.PolicyManifestID, RankerVersion: snapshot.RankerManifestID, IndexManifestIds: indexes, DegradedChannels: degradedCodes, ApproximateCandidates: approximate, DegradedEnhancements: append([]string(nil), enhancementDegraded...)}
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
