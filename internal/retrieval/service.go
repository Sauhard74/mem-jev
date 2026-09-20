package retrieval

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/ranking"
)

var (
	ErrServiceUnavailable  = errors.New("retrieval service unavailable")
	ErrRecallDenied        = errors.New("retrieval recall not permitted")
	ErrIdempotencyConflict = errors.New("retrieval idempotency conflict")
)

type DecisionRepository interface {
	AcquireServingSnapshot(context.Context, domain.TenantID) (ServingSnapshot, error)
	SaveRetrievalRun(context.Context, Run) error
	RetrievalRun(context.Context, domain.TenantID, string) (Run, error)
}

type DocumentReader interface {
	RetrievalDocuments(context.Context, domain.TenantID, []string, uint64) ([]Document, error)
}

type ManifestResolver interface {
	EligibilityPolicy(context.Context, domain.TenantID, string) (eligibility.Policy, error)
	Ranker(context.Context, domain.TenantID, string) (ranking.Manifest, error)
}

type EffectInferer interface {
	InferEffectSignature(context.Context, domain.TenantID, Query) (string, error)
}

type EnvelopeCipher interface {
	Encrypt(context.Context, domain.TenantID, []byte) (string, error)
}

type RunIDSource interface{ NewRunID() (string, error) }

type SemanticJudge interface {
	Judge(context.Context, SemanticJudgmentRequest) (SemanticJudgment, error)
}

type SemanticJudgmentRequest struct{ QueryHash, VersionID, RubricManifestID string }
type SemanticJudgment struct {
	Score       int32
	ReasonCodes []string
	ManifestID  string
}

type ServiceConfig struct {
	Channels          []Channel
	RequiredChannels  []ChannelName
	ChannelTimeout    time.Duration
	Retention         time.Duration
	MinimumScore      int64
	MaximumSelections uint32
	Aliases           AliasSet
}

type Service struct {
	repository DecisionRepository
	documents  DocumentReader
	manifests  ManifestResolver
	effects    EffectInferer
	cipher     EnvelopeCipher
	ids        RunIDSource
	clock      func() time.Time
	config     ServiceConfig
	semantic   SemanticJudge
}

type ServiceRequest struct {
	TenantID                domain.TenantID
	CurrentPolicyVersion    string
	RequestIdentityHash     string
	Input                   Input
	RecallAllowed           bool
	AllowedResidencyRegions []string
}

type SelectedCandidate struct {
	VersionID        string
	ProcedureID      string
	FinalScore       int64
	Rank             uint32
	Lifecycle        string
	ObservedEndToEnd bool
	AdvisoryOnly     bool
}

type ServiceResponse struct {
	RunID        string
	Disposition  RunDisposition
	DecisionCode string
	Candidates   []SelectedCandidate
	Snapshot     ServingSnapshot
	QueryHash    string
	Degraded     []DegradedChannel
	Approximate  bool
	Replayed     bool
}

type ServiceError struct {
	Code, RunID string
	Err         error
}

func (e *ServiceError) Error() string { return fmt.Sprintf("retrieval %s: %v", e.Code, e.Err) }
func (e *ServiceError) Unwrap() error { return e.Err }

func NewService(repository DecisionRepository, documents DocumentReader, manifests ManifestResolver, effects EffectInferer, cipher EnvelopeCipher, ids RunIDSource, config ServiceConfig) (*Service, error) {
	if repository == nil || documents == nil || manifests == nil || cipher == nil || ids == nil || config.ChannelTimeout <= 0 || config.Retention <= 0 || config.Retention > 30*24*time.Hour || config.MaximumSelections == 0 || config.MaximumSelections > 100 || len(config.Channels) == 0 {
		return nil, ErrServiceUnavailable
	}
	seen := map[ChannelName]bool{}
	config.Channels = append([]Channel(nil), config.Channels...)
	config.RequiredChannels = append([]ChannelName(nil), config.RequiredChannels...)
	for _, channel := range config.Channels {
		if channel == nil || !validChannel(channel.Name()) || seen[channel.Name()] || channel.Approximate() != (channel.Name() == ChannelVector) {
			return nil, ErrServiceUnavailable
		}
		seen[channel.Name()] = true
	}
	requiredSeen := map[ChannelName]bool{}
	for _, required := range config.RequiredChannels {
		if !seen[required] || requiredSeen[required] {
			return nil, ErrServiceUnavailable
		}
		requiredSeen[required] = true
	}
	sort.Slice(config.RequiredChannels, func(i, j int) bool { return config.RequiredChannels[i] < config.RequiredChannels[j] })
	return &Service{repository: repository, documents: documents, manifests: manifests, effects: effects, cipher: cipher, ids: ids, clock: time.Now, config: config}, nil
}

func (s *Service) Retrieve(ctx context.Context, request ServiceRequest) (ServiceResponse, error) {
	if err := ctx.Err(); err != nil {
		return ServiceResponse{}, err
	}
	if !request.RecallAllowed {
		return ServiceResponse{}, ErrRecallDenied
	}
	if s == nil || len(request.AllowedResidencyRegions) == 0 {
		return ServiceResponse{}, ErrServiceUnavailable
	}
	query, err := BuildQuery(request.TenantID, request.CurrentPolicyVersion, s.config.Aliases, request.Input)
	if err != nil {
		return ServiceResponse{}, err
	}
	if s.effects != nil {
		effectHash, inferErr := s.effects.InferEffectSignature(ctx, request.TenantID, query)
		if inferErr != nil {
			return ServiceResponse{}, &ServiceError{Code: "effect_inference_failed", Err: inferErr}
		}
		query, err = BindEffectSignature(query, effectHash)
		if err != nil {
			return ServiceResponse{}, err
		}
	}
	requestContextHash, err := retrievalRequestContextHash(request, query)
	if err != nil {
		return ServiceResponse{}, err
	}
	runID, err := s.runID(request.TenantID, request.RequestIdentityHash)
	if err != nil {
		return ServiceResponse{}, err
	}
	if request.RequestIdentityHash != "" {
		existing, findErr := s.repository.RetrievalRun(ctx, request.TenantID, runID)
		if findErr == nil {
			if existing.QueryHash != query.Hash || existing.RequestContextHash != requestContextHash {
				observability.RecordRetrievalReplayMismatch(ctx)
				return ServiceResponse{}, &ServiceError{Code: "idempotency_conflict", RunID: runID, Err: ErrIdempotencyConflict}
			}
			return s.responseFromRun(ctx, existing)
		}
		if !errors.Is(findErr, ErrRunNotFound) {
			return ServiceResponse{}, &ServiceError{Code: "run_lookup_failed", RunID: runID, Err: findErr}
		}
	}
	snapshot, err := s.repository.AcquireServingSnapshot(ctx, request.TenantID)
	if err != nil {
		return ServiceResponse{}, &ServiceError{Code: "snapshot_unavailable", Err: err}
	}
	activeChannels, err := s.snapshotChannels(snapshot)
	if err != nil {
		return ServiceResponse{}, &ServiceError{Code: "serving_config_mismatch", Err: err}
	}
	observability.RecordRetrievalSnapshot(ctx, s.clock().UTC(), snapshot.ProjectionCreatedAt, snapshot.VectorIndexCreatedAt, snapshot.RankerManifestID)
	policy, err := s.manifests.EligibilityPolicy(ctx, request.TenantID, snapshot.PolicyManifestID)
	if err != nil || policy.ID != snapshot.PolicyManifestID {
		return ServiceResponse{}, &ServiceError{Code: "policy_manifest_unavailable", Err: ErrServiceUnavailable}
	}
	ranker, err := s.manifests.Ranker(ctx, request.TenantID, snapshot.RankerManifestID)
	if err != nil || ranker.ID != snapshot.RankerManifestID {
		return ServiceResponse{}, &ServiceError{Code: "ranker_manifest_unavailable", Err: ErrServiceUnavailable}
	}
	envelope, err := s.cipher.Encrypt(ctx, request.TenantID, query.CanonicalJSON)
	if err != nil {
		return ServiceResponse{}, &ServiceError{Code: "query_encryption_failed", RunID: runID, Err: err}
	}
	started := s.clock().UTC()
	collection, err := CollectCandidates(ctx, CollectRequest{Request: ChannelRequest{TenantID: request.TenantID, ProjectionEpoch: snapshot.ProjectionEpoch, Query: query, Limit: request.Input.MaxCandidates}, Channels: activeChannels, Timeout: s.config.ChannelTimeout})
	if err != nil {
		return ServiceResponse{}, err
	}
	for _, result := range collection.Results {
		observability.RecordRetrievalChannel(ctx, string(result.Channel), "complete", time.Duration(result.LatencyMicros)*time.Microsecond)
	}
	for _, item := range collection.Degraded {
		observability.RecordRetrievalChannel(ctx, string(item.Channel), item.Code, time.Duration(item.LatencyMicros)*time.Microsecond)
	}
	executions, hits := persistedChannels(collection)
	if missing := requiredDegradation(collection.Degraded, s.config.RequiredChannels); missing != "" {
		run, buildErr := s.buildRun(runID, request.TenantID, query, requestContextHash, envelope, snapshot, executions, hits, nil, nil, RunFailed, "required_channel_unavailable:"+missing, nil, started)
		if buildErr != nil {
			return ServiceResponse{}, buildErr
		}
		return s.persistRun(ctx, run, collection)
	}
	versionIDs := make([]string, len(collection.Candidates))
	for index, item := range collection.Candidates {
		versionIDs[index] = item.VersionID
	}
	if len(versionIDs) == 0 {
		return s.persistOutcome(ctx, runID, request.TenantID, query, requestContextHash, envelope, snapshot, executions, hits, nil, nil, RunAbstained, "no_candidates", nil, started, collection)
	}
	documents, err := s.documents.RetrievalDocuments(ctx, request.TenantID, versionIDs, snapshot.ProjectionEpoch)
	if err != nil || len(documents) != len(versionIDs) {
		run, buildErr := s.buildRun(runID, request.TenantID, query, requestContextHash, envelope, snapshot, executions, hits, nil, nil, RunFailed, "candidate_snapshot_missing", nil, started)
		if buildErr != nil {
			return ServiceResponse{}, buildErr
		}
		return s.persistRun(ctx, run, collection)
	}
	docByID := make(map[string]Document, len(documents))
	for _, document := range documents {
		if _, exists := docByID[document.ProcedureVersionID]; exists {
			return ServiceResponse{}, &ServiceError{Code: "candidate_snapshot_missing", RunID: runID, Err: ErrServiceUnavailable}
		}
		docByID[document.ProcedureVersionID] = document
	}
	if len(docByID) != len(versionIDs) {
		return ServiceResponse{}, &ServiceError{Code: "candidate_snapshot_missing", RunID: runID, Err: ErrServiceUnavailable}
	}
	eligibilityContext := eligibilityContextFrom(request, query, s.clock().UTC())
	gates := make([]PersistedGate, 0, len(versionIDs))
	rankingCandidates := make([]ranking.Candidate, 0, len(versionIDs))
	decisionByID := make(map[string]eligibility.Decision, len(versionIDs))
	for _, candidateSet := range collection.Candidates {
		document, ok := docByID[candidateSet.VersionID]
		if !ok {
			return ServiceResponse{}, &ServiceError{Code: "candidate_snapshot_missing", RunID: runID, Err: ErrServiceUnavailable}
		}
		decision := eligibility.Evaluate(policy.PolicySpec, eligibilityContext, eligibilityCandidateFrom(document))
		for _, rejection := range decision.Rejections {
			observability.RecordRetrievalGateRejection(ctx, string(rejection.Code))
		}
		decisionByID[document.ProcedureVersionID] = decision
		gate, gateErr := persistedGate(document.ProcedureVersionID, decision)
		if gateErr != nil {
			return ServiceResponse{}, gateErr
		}
		gates = append(gates, gate)
		if !decision.Eligible {
			continue
		}
		ranks := make([]ranking.ChannelRank, len(candidateSet.Hits))
		for index, hit := range candidateSet.Hits {
			ranks[index] = ranking.ChannelRank{Channel: string(hit.Channel), Rank: hit.Rank}
		}
		rankingCandidates = append(rankingCandidates, ranking.Candidate{VersionID: document.ProcedureVersionID, ChannelRanks: ranks, Features: rankingFeatures(ranker, document), VerificationStrength: int32(document.VerificationStrength), ObservedEndToEnd: document.ObservedEndToEnd})
	}
	if len(rankingCandidates) == 0 {
		return s.persistOutcome(ctx, runID, request.TenantID, query, requestContextHash, envelope, snapshot, executions, hits, gates, nil, RunAbstained, "no_eligible_candidates", nil, started, collection)
	}
	ranked, err := ranking.Rank(ranker, rankingCandidates)
	if err != nil {
		return ServiceResponse{}, &ServiceError{Code: "ranking_failed", RunID: runID, Err: err}
	}
	persistedRanks := persistedRanking(ranked, rankingCandidates)
	selectedIDs := make([]string, 0, min(int(s.config.MaximumSelections), len(ranked)))
	responseCandidates := make([]SelectedCandidate, 0, len(ranked))
	for _, item := range ranked {
		document := docByID[item.VersionID]
		decision := decisionByID[item.VersionID]
		if decision.AdvisoryOnly {
			responseCandidates = append(responseCandidates, SelectedCandidate{VersionID: item.VersionID, ProcedureID: document.ProcedureID, FinalScore: item.FinalScore, Rank: item.Rank, Lifecycle: document.Lifecycle, ObservedEndToEnd: document.ObservedEndToEnd, AdvisoryOnly: true})
			continue
		}
		if item.FinalScore < s.config.MinimumScore || len(selectedIDs) >= int(s.config.MaximumSelections) {
			continue
		}
		selectedIDs = append(selectedIDs, item.VersionID)
		responseCandidates = append(responseCandidates, SelectedCandidate{VersionID: item.VersionID, ProcedureID: document.ProcedureID, FinalScore: item.FinalScore, Rank: item.Rank, Lifecycle: document.Lifecycle, ObservedEndToEnd: document.ObservedEndToEnd})
	}
	if len(selectedIDs) == 0 {
		response, persistErr := s.persistOutcome(ctx, runID, request.TenantID, query, requestContextHash, envelope, snapshot, executions, hits, gates, persistedRanks, RunAbstained, "below_confidence_threshold", nil, started, collection)
		response.Candidates = responseCandidates
		return response, persistErr
	}
	response, err := s.persistOutcome(ctx, runID, request.TenantID, query, requestContextHash, envelope, snapshot, executions, hits, gates, persistedRanks, RunSelected, "", selectedIDs, started, collection)
	response.Candidates = responseCandidates
	return response, err
}

func (s *Service) snapshotChannels(snapshot ServingSnapshot) ([]Channel, error) {
	configured := make(map[ChannelName]Channel, len(s.config.Channels))
	for _, channel := range s.config.Channels {
		configured[channel.Name()] = channel
	}
	active := make([]Channel, 0, len(snapshot.Indexes))
	present := make(map[ChannelName]struct{}, len(snapshot.Indexes))
	for _, index := range snapshot.Indexes {
		channel, ok := configured[index.Channel]
		if !ok || channel.ManifestID() != index.ManifestID || channel.Approximate() != index.Approximate {
			return nil, ErrServiceUnavailable
		}
		active = append(active, channel)
		present[index.Channel] = struct{}{}
	}
	for _, required := range s.config.RequiredChannels {
		if _, ok := present[required]; !ok {
			return nil, ErrServiceUnavailable
		}
	}
	if len(active) == 0 {
		return nil, ErrServiceUnavailable
	}
	return active, nil
}

func (s *Service) runID(tenantID domain.TenantID, requestIdentityHash string) (string, error) {
	if requestIdentityHash == "" {
		id, err := s.ids.NewRunID()
		if err != nil {
			return "", &ServiceError{Code: "run_id_unavailable", Err: err}
		}
		return id, nil
	}
	if !sha256Pattern.MatchString(requestIdentityHash) {
		return "", ErrInvalidQuery
	}
	sum := sha256.Sum256([]byte(string(tenantID) + "\x00" + requestIdentityHash))
	return "rrun_" + hex.EncodeToString(sum[:]), nil
}

func (s *Service) responseFromRun(ctx context.Context, run Run) (ServiceResponse, error) {
	response := ServiceResponse{RunID: run.ID, Disposition: run.Disposition, DecisionCode: run.DecisionCode, Snapshot: run.Snapshot, QueryHash: run.QueryHash, Replayed: true}
	for _, execution := range run.ChannelExecutions {
		response.Approximate = response.Approximate || execution.Approximate && execution.Complete
		if !execution.Complete {
			response.Degraded = append(response.Degraded, DegradedChannel{Channel: execution.Channel, Code: execution.DegradationCode, IndexManifestID: execution.IndexManifestID, Approximate: execution.Approximate, LatencyMicros: execution.LatencyMicros})
		}
	}
	if run.Disposition == RunFailed {
		code := run.DecisionCode
		if index := strings.IndexByte(code, ':'); index >= 0 {
			code = code[:index]
		}
		return ServiceResponse{}, &ServiceError{Code: code, RunID: run.ID, Err: ErrServiceUnavailable}
	}
	include := make(map[string]bool)
	for _, id := range run.SelectedVersionIDs {
		include[id] = true
	}
	for _, gate := range run.Gates {
		if gate.AdvisoryOnly {
			include[gate.VersionID] = true
		}
	}
	if len(include) == 0 {
		return response, nil
	}
	ids := make([]string, 0, len(include))
	for id := range include {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	documents, err := s.documents.RetrievalDocuments(ctx, run.TenantID, ids, run.Snapshot.ProjectionEpoch)
	if err != nil || len(documents) != len(ids) {
		return ServiceResponse{}, &ServiceError{Code: "candidate_snapshot_missing", RunID: run.ID, Err: ErrServiceUnavailable}
	}
	docByID := make(map[string]Document, len(documents))
	for _, item := range documents {
		if _, exists := docByID[item.ProcedureVersionID]; exists {
			return ServiceResponse{}, &ServiceError{Code: "candidate_snapshot_missing", RunID: run.ID, Err: ErrServiceUnavailable}
		}
		docByID[item.ProcedureVersionID] = item
	}
	gateByID := make(map[string]PersistedGate, len(run.Gates))
	for _, item := range run.Gates {
		gateByID[item.VersionID] = item
	}
	for _, item := range run.Ranked {
		if !include[item.VersionID] {
			continue
		}
		document, ok := docByID[item.VersionID]
		if !ok {
			return ServiceResponse{}, &ServiceError{Code: "candidate_snapshot_missing", RunID: run.ID, Err: ErrServiceUnavailable}
		}
		response.Candidates = append(response.Candidates, SelectedCandidate{VersionID: item.VersionID, ProcedureID: document.ProcedureID, FinalScore: item.FinalScore, Rank: item.Rank, Lifecycle: document.Lifecycle, ObservedEndToEnd: document.ObservedEndToEnd, AdvisoryOnly: gateByID[item.VersionID].AdvisoryOnly})
	}
	return response, nil
}

func (s *Service) persistOutcome(ctx context.Context, runID string, tenantID domain.TenantID, query Query, requestContextHash, envelope string, snapshot ServingSnapshot, executions []ChannelExecution, hits []PersistedHit, gates []PersistedGate, ranks []PersistedRank, disposition RunDisposition, code string, selected []string, started time.Time, collection CandidateCollection) (ServiceResponse, error) {
	run, err := s.buildRun(runID, tenantID, query, requestContextHash, envelope, snapshot, executions, hits, gates, ranks, disposition, code, selected, started)
	if err != nil {
		return ServiceResponse{}, err
	}
	return s.persistRun(ctx, run, collection)
}

func (s *Service) persistRun(ctx context.Context, run Run, collection CandidateCollection) (ServiceResponse, error) {
	if err := s.repository.SaveRetrievalRun(ctx, run); err != nil {
		winner, lookupErr := s.persistedWinner(ctx, run.TenantID, run.ID)
		if lookupErr == nil {
			if winner.QueryHash != run.QueryHash || winner.RequestContextHash != run.RequestContextHash {
				observability.RecordRetrievalReplayMismatch(ctx)
				return ServiceResponse{}, &ServiceError{Code: "idempotency_conflict", RunID: run.ID, Err: ErrIdempotencyConflict}
			}
			return s.responseFromRun(ctx, winner)
		}
		observability.RecordRetrievalPersistenceFailure(ctx, run.DecisionCode)
		return ServiceResponse{}, &ServiceError{Code: "run_persistence_failed", RunID: run.ID, Err: err}
	}
	if run.Disposition == RunFailed {
		code := run.DecisionCode
		if index := strings.IndexByte(code, ':'); index >= 0 {
			code = code[:index]
		}
		return ServiceResponse{}, &ServiceError{Code: code, RunID: run.ID, Err: ErrServiceUnavailable}
	}
	return ServiceResponse{RunID: run.ID, Disposition: run.Disposition, DecisionCode: run.DecisionCode, Snapshot: run.Snapshot, QueryHash: run.QueryHash, Degraded: append([]DegradedChannel(nil), collection.Degraded...), Approximate: collection.Approximate}, nil
}

func (s *Service) persistedWinner(ctx context.Context, tenantID domain.TenantID, runID string) (Run, error) {
	var lastErr error
	for attempt := range 3 {
		run, err := s.repository.RetrievalRun(ctx, tenantID, runID)
		if err == nil || !errors.Is(err, ErrRunNotFound) {
			return run, err
		}
		lastErr = err
		if attempt == 2 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Run{}, ctx.Err()
		case <-timer.C:
		}
	}
	return Run{}, lastErr
}

func (s *Service) buildRun(id string, tenantID domain.TenantID, query Query, requestContextHash, envelope string, snapshot ServingSnapshot, executions []ChannelExecution, hits []PersistedHit, gates []PersistedGate, ranks []PersistedRank, disposition RunDisposition, code string, selected []string, started time.Time) (Run, error) {
	completed := s.clock().UTC()
	if completed.Before(started) {
		completed = started
	}
	return BuildRun(RunInput{ID: id, TenantID: tenantID, Query: query, RequestContextHash: requestContextHash, QueryEnvelope: envelope, Snapshot: snapshot, ChannelExecutions: executions, Hits: hits, Gates: gates, Ranked: ranks, Disposition: disposition, DecisionCode: code, SelectedVersionIDs: selected, CreatedAt: started, CompletedAt: completed, ExpiresAt: completed.Add(s.config.Retention)})
}

func retrievalRequestContextHash(request ServiceRequest, query Query) (string, error) {
	regions := append([]string(nil), request.AllowedResidencyRegions...)
	sort.Strings(regions)
	unique := regions[:0]
	for _, region := range regions {
		region = strings.TrimSpace(region)
		if region == "" {
			return "", ErrInvalidQuery
		}
		if len(unique) == 0 || unique[len(unique)-1] != region {
			unique = append(unique, region)
		}
	}
	_, hash, err := canonical.MarshalAndHash(struct {
		TenantID                domain.TenantID `json:"tenant_id"`
		QueryHash               string          `json:"query_hash"`
		RecallAllowed           bool            `json:"recall_allowed"`
		AllowedResidencyRegions []string        `json:"allowed_residency_regions"`
	}{request.TenantID, query.Hash, request.RecallAllowed, unique})
	if err != nil {
		return "", fmt.Errorf("canonicalize retrieval request context: %w", err)
	}
	return hash, nil
}

func persistedChannels(collection CandidateCollection) ([]ChannelExecution, []PersistedHit) {
	executions := make([]ChannelExecution, 0, len(collection.Results)+len(collection.Degraded))
	hits := []PersistedHit{}
	for _, result := range collection.Results {
		executions = append(executions, ChannelExecution{Channel: result.Channel, IndexManifestID: result.IndexManifestID, HitCount: result.HitCount, Approximate: result.Approximate, Complete: true, LatencyMicros: result.LatencyMicros})
	}
	for _, item := range collection.Degraded {
		executions = append(executions, ChannelExecution{Channel: item.Channel, IndexManifestID: item.IndexManifestID, Approximate: item.Approximate, Complete: false, DegradationCode: item.Code, LatencyMicros: item.LatencyMicros})
	}
	for _, candidate := range collection.Candidates {
		for _, hit := range candidate.Hits {
			hits = append(hits, PersistedHit{Channel: hit.Channel, VersionID: candidate.VersionID, Rank: hit.Rank, RawScoreQuantized: hit.RawScoreQuantized, IndexManifestID: hit.IndexManifestID, Approximate: hit.Approximate})
		}
	}
	return executions, hits
}

func requiredDegradation(degraded []DegradedChannel, required []ChannelName) string {
	set := map[ChannelName]bool{}
	for _, item := range degraded {
		set[item.Channel] = true
	}
	for _, name := range required {
		if set[name] {
			return string(name)
		}
	}
	return ""
}

func eligibilityContextFrom(request ServiceRequest, query Query, now time.Time) eligibility.Context {
	tools := make([]eligibility.Tool, len(query.Tools))
	for index, item := range query.Tools {
		tools[index] = eligibility.Tool{Name: item.Name, ContractVersionID: item.ContractVersionID}
	}
	facts := make([]eligibility.Fact, len(query.Environment))
	for index, item := range query.Environment {
		facts[index] = eligibility.Fact{Name: item.Name, Value: item.Value}
	}
	resources := make([]eligibility.Resource, len(query.Resources))
	for index, item := range query.Resources {
		sum := sha256.Sum256([]byte(string(request.TenantID) + "\x00" + item.Namespace + "\x00" + item.Type + "\x00" + item.Identity))
		resources[index] = eligibility.Resource{Type: item.Type, Namespace: item.Namespace, IdentityHash: hex.EncodeToString(sum[:]), SchemaVersion: item.SchemaVersion}
	}
	return eligibility.Context{TenantID: string(request.TenantID), AsOfUnix: now.Unix(), RecallAllowed: request.RecallAllowed, Tools: tools, Harness: eligibility.Harness{Name: query.Harness.Name, Version: query.Harness.Version}, Environment: facts, Resources: resources, ForbiddenEffects: query.ForbiddenEffects, MaximumRisk: eligibilityRisk(query.RiskClass), AllowedResidencyRegions: append([]string(nil), request.AllowedResidencyRegions...)}
}

func eligibilityCandidateFrom(document Document) eligibility.Candidate {
	tools := make([]eligibility.Tool, len(document.Tools))
	for index, item := range document.Tools {
		tools[index] = eligibility.Tool{Name: item.Name, ContractVersionID: item.ContractVersionID}
	}
	facts := make([]eligibility.Fact, len(document.Environment))
	for index, item := range document.Environment {
		facts[index] = eligibility.Fact{Name: item.Name, Value: item.Value}
	}
	resources := make([]eligibility.Resource, len(document.Resources))
	for index, item := range document.Resources {
		resources[index] = eligibility.Resource{Type: item.Type, Namespace: item.Namespace, IdentityHash: item.IdentityHash, SchemaVersion: item.SchemaVersion}
	}
	validated, _ := time.Parse(time.RFC3339Nano, document.ValidatedAt)
	return eligibility.Candidate{VersionID: document.ProcedureVersionID, OwnerTenantID: string(document.TenantID), Lifecycle: eligibility.Lifecycle(document.Lifecycle), RequiredTools: tools, RequiredHarness: eligibility.Harness{Name: document.Harness.Name, Version: document.Harness.Version}, RequiredEnvironment: facts, RequiredResources: resources, Effects: append([]string(nil), document.Effects...), Risk: eligibilityRisk(RiskClass(document.RiskClass)), ValidatedAtUnix: validated.Unix(), ValidationPolicyVersion: document.ValidationPolicyVersion, RecallAllowed: document.LearnedWithRecallConsent, ResidencyRegion: document.ResidencyRegion}
}

func eligibilityRisk(value RiskClass) eligibility.Risk {
	return map[RiskClass]eligibility.Risk{RiskLow: eligibility.RiskLow, RiskMedium: eligibility.RiskMedium, RiskHigh: eligibility.RiskHigh, RiskCritical: eligibility.RiskCritical}[value]
}

func persistedGate(versionID string, decision eligibility.Decision) (PersistedGate, error) {
	facts, _, err := canonical.MarshalAndHash(decision.Rejections)
	if err != nil {
		return PersistedGate{}, err
	}
	codes := make([]string, 0, len(decision.Rejections))
	seen := map[string]bool{}
	for _, rejection := range decision.Rejections {
		code := string(rejection.Code)
		if !seen[code] {
			seen[code] = true
			codes = append(codes, code)
		}
	}
	sort.Strings(codes)
	return PersistedGate{VersionID: versionID, Eligible: decision.Eligible, AdvisoryOnly: decision.AdvisoryOnly, RejectionCodes: codes, CanonicalFacts: string(facts)}, nil
}

func rankingFeatures(manifest ranking.Manifest, document Document) []ranking.FeatureValue {
	values := map[string]int64{"verified_success_count": int64(document.VerifiedSuccessCount), "unsafe_outcome_count": int64(document.UnsafeOutcomeCount), "verification_strength": int64(document.VerificationStrength)}
	result := []ranking.FeatureValue{}
	for _, spec := range manifest.Features {
		value, ok := values[spec.Name]
		if !ok {
			continue
		}
		if value < int64(spec.Minimum) {
			value = int64(spec.Minimum)
		}
		if value > int64(spec.Maximum) {
			value = int64(spec.Maximum)
		}
		result = append(result, ranking.FeatureValue{Name: spec.Name, Value: int32(value)})
	}
	return result
}

func persistedRanking(results []ranking.Result, candidates []ranking.Candidate) []PersistedRank {
	features := make(map[string][]ranking.FeatureValue, len(candidates))
	for _, item := range candidates {
		features[item.VersionID] = item.Features
	}
	persisted := make([]PersistedRank, len(results))
	for index, item := range results {
		values := make([]PersistedFeature, len(features[item.VersionID]))
		for featureIndex, feature := range features[item.VersionID] {
			values[featureIndex] = PersistedFeature{Name: feature.Name, Value: feature.Value}
		}
		missing := []string{}
		for name := range item.MissingFeatures {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		persisted[index] = PersistedRank{VersionID: item.VersionID, RRFScore: item.RRFScore, FinalScore: item.FinalScore, Rank: item.Rank, VerificationStrength: item.VerificationStrength, ObservedEndToEnd: item.ObservedEndToEnd, Features: values, MissingFeatures: missing}
	}
	return persisted
}

type RandomRunIDSource struct{}

func (RandomRunIDSource) NewRunID() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "rrun_" + hex.EncodeToString(value[:]), nil
}
