package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/synthesis"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

var (
	ErrStageArtifactNotFound = errors.New("stage artifact not found")
	ErrStageArtifactConflict = errors.New("stage artifact conflicts with immutable content")
)

type StoredOutcome struct {
	ID                domain.OutcomeID         `json:"id"`
	ContentHash       string                   `json:"content_hash"`
	State             domain.OutcomeState      `json:"state"`
	PromotionEligible bool                     `json:"promotion_eligible"`
	PolicyVersion     string                   `json:"policy_version"`
	Evidence          []domain.OutcomeEvidence `json:"evidence"`
	CreatedAt         time.Time                `json:"created_at"`
}

type LoadedSource struct {
	SchemaVersion string                `json:"schema_version"`
	Batch         domain.CanonicalBatch `json:"batch"`
	ArchiveHash   string                `json:"archive_hash"`
	Outcome       *StoredOutcome        `json:"outcome,omitempty"`
}

type SourceLoader interface {
	Load(context.Context, SynthesisInput) (LoadedSource, error)
}

type RegistryProvider interface {
	ForTenant(domain.TenantID) toolcontract.Registry
}

type VersionedRegistry interface {
	toolcontract.Registry
	SnapshotVersion() string
}

type StageArtifact struct {
	TenantID    domain.TenantID `json:"tenant_id"`
	WorkflowID  string          `json:"workflow_id"`
	Stage       Stage           `json:"stage"`
	InputHash   string          `json:"input_hash"`
	ContentHash string          `json:"content_hash"`
	Payload     json.RawMessage `json:"payload"`
	Status      StageStatus     `json:"status"`
	Code        string          `json:"code,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
}

type stageArtifactIdentity struct {
	TenantID   domain.TenantID `json:"tenant_id"`
	WorkflowID string          `json:"workflow_id"`
	Stage      Stage           `json:"stage"`
	InputHash  string          `json:"input_hash"`
	Payload    json.RawMessage `json:"payload"`
	Status     StageStatus     `json:"status"`
	Code       string          `json:"code,omitempty"`
}

type StageArtifactRepository interface {
	PutStageArtifact(context.Context, StageArtifact) error
	GetStageArtifact(context.Context, domain.TenantID, string, Stage) (StageArtifact, error)
}

type ProcessorConfig struct {
	ArtifactRetention time.Duration
	Versions          synthesis.Versions
}

type Processor struct {
	sources     SourceLoader
	registries  RegistryProvider
	artifacts   StageArtifactRepository
	projections projection.Repository
	config      ProcessorConfig
	now         func() time.Time
}

func NewProcessor(sources SourceLoader, registries RegistryProvider, artifacts StageArtifactRepository, projections projection.Repository, config ProcessorConfig) (*Processor, error) {
	if sources == nil || registries == nil || artifacts == nil || projections == nil || config.ArtifactRetention < time.Hour ||
		config.Versions.Sanitizer == "" || config.Versions.Registry == "" || config.Versions.Policy == "" ||
		config.Versions.GraphBuilder == "" || config.Versions.Synthesizer == "" {
		return nil, errors.New("invalid pipeline processor configuration")
	}
	return &Processor{sources: sources, registries: registries, artifacts: artifacts, projections: projections, config: config, now: time.Now}, nil
}

type evaluationArtifact struct {
	State             domain.OutcomeState      `json:"state"`
	PromotionEligible bool                     `json:"promotion_eligible"`
	PolicyVersion     string                   `json:"policy_version"`
	Evidence          []domain.OutcomeEvidence `json:"evidence"`
}

type graphArtifact struct {
	Graph               synthesis.Graph             `json:"graph"`
	GraphHash           string                      `json:"graph_hash"`
	Goals               []synthesis.GoalRequirement `json:"goals"`
	RequiredEffects     []domain.EventID            `json:"required_effects"`
	FailedBranches      []synthesis.FailedBranch    `json:"failed_branches,omitempty"`
	IntentHash          string                      `json:"intent_hash"`
	EffectSignatureHash string                      `json:"effect_signature_hash"`
	EnvironmentHash     string                      `json:"environment_hash"`
	RegistryVersion     string                      `json:"registry_version"`
}

type synthesisArtifact struct {
	Result     synthesis.Result `json:"result"`
	ResultHash string           `json:"result_hash"`
}

func (p *Processor) Load(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.runStage(ctx, request, nil, func() (any, StageStatus, string, error) {
		source, err := p.sources.Load(ctx, request.Input)
		if err != nil {
			return nil, "", "", err
		}
		if source.SchemaVersion != "loaded-source.v1" || source.Batch.TenantID != request.Input.TenantID || source.Batch.Trace.ID != request.Input.TraceID || source.ArchiveHash != source.Batch.Hash {
			return nil, "", "", &StageError{Code: "tenant_mismatch", Err: ErrInvalidStartRequest}
		}
		if request.Input.JobType == store.OutboxJobSynthesizeTrace && (source.Outcome != nil || request.Input.ContentHash != source.ArchiveHash) {
			return nil, "", "", &StageError{Code: "invalid_input", Err: ErrInvalidStartRequest}
		}
		if request.Input.JobType == store.OutboxJobSynthesizeOutcome && (source.Outcome == nil || source.Outcome.ID != request.Input.OutcomeID || source.Outcome.ContentHash != request.Input.ContentHash) {
			return nil, "", "", &StageError{Code: "invalid_input", Err: ErrInvalidStartRequest}
		}
		return source, StageStatusReady, "", nil
	})
}

func (p *Processor) Evaluate(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.runStage(ctx, request, []Stage{StageLoad}, func() (any, StageStatus, string, error) {
		var source LoadedSource
		if err := p.readPayload(ctx, request, StageLoad, &source); err != nil {
			return nil, "", "", err
		}
		if source.Outcome == nil {
			return evaluationArtifact{}, StageStatusTerminal, "awaiting_outcome", nil
		}
		policy := outcome.DefaultPolicy()
		policy.RequiredPredicates = predicates(source.Outcome.Evidence)
		result, err := evidence.Evaluate(policy, source.Outcome.Evidence, nil)
		if err != nil {
			return nil, "", "", &StageError{Code: "invalid_evidence", Err: err}
		}
		if result.State != source.Outcome.State || result.PromotionEligible != source.Outcome.PromotionEligible || result.PolicyVersion != source.Outcome.PolicyVersion {
			return nil, "", "", &StageError{Code: "evidence_mismatch", Err: ErrInvalidStartRequest}
		}
		return evaluationArtifact{State: result.State, PromotionEligible: result.PromotionEligible, PolicyVersion: result.PolicyVersion, Evidence: source.Outcome.Evidence}, StageStatusReady, "", nil
	})
}

func (p *Processor) BuildGraph(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.runStage(ctx, request, []Stage{StageEvaluate}, func() (any, StageStatus, string, error) {
		var source LoadedSource
		if err := p.readPayload(ctx, request, StageLoad, &source); err != nil {
			return nil, "", "", err
		}
		var evaluated evaluationArtifact
		if err := p.readPayload(ctx, request, StageEvaluate, &evaluated); err != nil {
			return nil, "", "", err
		}
		registry := p.registries.ForTenant(request.Input.TenantID)
		graph, err := synthesis.NewBuilder(registry).Build(ctx, synthesis.BuildRequest{Batch: source.Batch})
		if err != nil {
			return nil, "", "", &StageError{Code: "graph_build_failed", Err: err}
		}
		goals := inferGoals(graph, evaluated.Evidence)
		required := requiredEffects(graph)
		intentHash, effectHash, environmentHash, err := projectionIdentities(source.Batch, graph, required)
		if err != nil {
			return nil, "", "", err
		}
		registryVersion := p.config.Versions.Registry
		if versioned, ok := registry.(VersionedRegistry); ok && versioned.SnapshotVersion() != "" {
			registryVersion = versioned.SnapshotVersion()
		}
		failedBranches, err := inferFailedBranches(graph, source.Batch, environmentHash)
		if err != nil {
			return nil, "", "", err
		}
		return graphArtifact{Graph: graph, GraphHash: graph.Hash, Goals: goals, RequiredEffects: required, FailedBranches: failedBranches, IntentHash: intentHash, EffectSignatureHash: effectHash, EnvironmentHash: environmentHash, RegistryVersion: registryVersion}, StageStatusReady, "", nil
	})
}

func (p *Processor) Synthesize(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.runStage(ctx, request, []Stage{StageBuildGraph}, func() (any, StageStatus, string, error) {
		var evaluated evaluationArtifact
		if err := p.readPayload(ctx, request, StageEvaluate, &evaluated); err != nil {
			return nil, "", "", err
		}
		var graph graphArtifact
		if err := p.readPayload(ctx, request, StageBuildGraph, &graph); err != nil {
			return nil, "", "", err
		}
		graph.Graph.Hash = graph.GraphHash
		versions := p.config.Versions
		versions.Registry = graph.RegistryVersion
		versions.Policy = evaluated.PolicyVersion
		result, err := synthesis.Synthesize(synthesis.Request{
			Graph: graph.Graph, OutcomeState: evaluated.State, Goals: graph.Goals,
			RequiredEffects: graph.RequiredEffects, FailedBranches: graph.FailedBranches, Versions: versions,
		})
		if err != nil {
			return nil, "", "", &StageError{Code: "synthesis_failed", Err: err}
		}
		if result.Status == synthesis.StatusAbstained {
			observability.RecordSynthesisAbstention(ctx, result.AbstentionCode)
		}
		return synthesisArtifact{Result: result, ResultHash: result.Hash}, StageStatusReady, "", nil
	})
}

func (p *Processor) Publish(ctx context.Context, request StageRequest) (StageResult, error) {
	return p.runStage(ctx, request, []Stage{StageSynthesize}, func() (any, StageStatus, string, error) {
		var source LoadedSource
		if err := p.readPayload(ctx, request, StageLoad, &source); err != nil {
			return nil, "", "", err
		}
		var graph graphArtifact
		if err := p.readPayload(ctx, request, StageBuildGraph, &graph); err != nil {
			return nil, "", "", err
		}
		var synthesizedArtifact synthesisArtifact
		if err := p.readPayload(ctx, request, StageSynthesize, &synthesizedArtifact); err != nil {
			return nil, "", "", err
		}
		synthesized := synthesizedArtifact.Result
		synthesized.Hash = synthesizedArtifact.ResultHash
		value, err := projection.Build(projection.BuildRequest{
			TenantID: request.Input.TenantID, OutcomeID: request.Input.OutcomeID,
			IntentHash: graph.IntentHash, EffectSignatureHash: graph.EffectSignatureHash, EnvironmentScopeHash: graph.EnvironmentHash,
			ArchiveHash: source.ArchiveHash, CanonicalEventStart: 0, CanonicalEventEnd: uint32(len(source.Batch.Events) - 1),
			CreatedAt: p.now().UTC(), Synthesis: synthesized,
		})
		if err != nil {
			return nil, "", "", &StageError{Code: "projection_build_failed", Err: err}
		}
		receipt, err := p.projections.Publish(ctx, value)
		if err != nil {
			return nil, "", "", err
		}
		if source.Outcome != nil && !source.Outcome.CreatedAt.IsZero() {
			lag := p.now().UTC().Sub(source.Outcome.CreatedAt)
			if lag < 0 {
				lag = 0
			}
			observability.RecordProjectionLag(ctx, lag)
		}
		payload := struct {
			ManifestID  string                 `json:"manifest_id"`
			VersionID   string                 `json:"procedure_version_id,omitempty"`
			Disposition projection.Disposition `json:"disposition"`
		}{value.Manifest.ID, value.Version.ID, receipt.Disposition}
		code := ""
		if synthesized.Status == synthesis.StatusAbstained {
			code = synthesized.AbstentionCode
		}
		return payload, StageStatusReady, code, nil
	})
}

func (p *Processor) runStage(ctx context.Context, request StageRequest, prerequisites []Stage, build func() (any, StageStatus, string, error)) (StageResult, error) {
	if existing, err := p.artifacts.GetStageArtifact(ctx, request.Input.TenantID, request.InputWorkflowID(), request.Stage); err == nil {
		if existing.InputHash != stageInputHash(request) {
			return StageResult{}, ErrStageArtifactConflict
		}
		return resultForArtifact(existing), nil
	} else if !errors.Is(err, ErrStageArtifactNotFound) {
		return StageResult{}, err
	}
	if err := p.validatePrior(ctx, request, prerequisites); err != nil {
		return StageResult{}, err
	}
	payload, status, code, err := build()
	if err != nil {
		return StageResult{}, err
	}
	encoded, _, err := canonical.MarshalAndHash(payload)
	if err != nil {
		return StageResult{}, err
	}
	now := p.now().UTC()
	inputHash := stageInputHash(request)
	hash, err := StageArtifactHash(StageArtifact{
		TenantID: request.Input.TenantID, WorkflowID: request.InputWorkflowID(), Stage: request.Stage,
		InputHash: inputHash, Payload: encoded, Status: status, Code: code,
	})
	if err != nil {
		return StageResult{}, err
	}
	artifact := StageArtifact{
		TenantID: request.Input.TenantID, WorkflowID: request.InputWorkflowID(), Stage: request.Stage,
		InputHash: inputHash, ContentHash: hash, Payload: encoded, Status: status, Code: code,
		CreatedAt: now, ExpiresAt: now.Add(p.config.ArtifactRetention),
	}
	if err := p.artifacts.PutStageArtifact(ctx, artifact); err != nil {
		return StageResult{}, err
	}
	return resultForArtifact(artifact), nil
}

func (p *Processor) validatePrior(ctx context.Context, request StageRequest, prerequisites []Stage) error {
	if len(prerequisites) == 0 {
		if request.PriorHash != "" {
			return &StageError{Code: "invalid_input", Err: ErrInvalidStartRequest}
		}
		return nil
	}
	prior, err := p.artifacts.GetStageArtifact(ctx, request.Input.TenantID, request.InputWorkflowID(), prerequisites[len(prerequisites)-1])
	if err != nil {
		return err
	}
	if prior.ContentHash != request.PriorHash {
		return &StageError{Code: "invalid_input", Err: ErrStageArtifactConflict}
	}
	return nil
}

func (p *Processor) readPayload(ctx context.Context, request StageRequest, stage Stage, target any) error {
	artifact, err := p.artifacts.GetStageArtifact(ctx, request.Input.TenantID, request.InputWorkflowID(), stage)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(artifact.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return &StageError{Code: "artifact_corrupt", Err: err}
	}
	return nil
}

func (request StageRequest) InputWorkflowID() string {
	if request.Input.JobType == store.OutboxJobSynthesizeOutcome {
		return store.OutcomeWorkflowID(request.Input.TenantID, request.Input.TraceID, request.Input.OutcomeID)
	}
	return store.WorkflowID(request.Input.TenantID, request.Input.TraceID)
}

func resultForArtifact(artifact StageArtifact) StageResult {
	return StageResult{SchemaVersion: "stage-result.v1", Stage: artifact.Stage, ArtifactHash: artifact.ContentHash, Status: artifact.Status, Code: artifact.Code}
}

func ValidateStageArtifact(artifact StageArtifact) error {
	if artifact.TenantID == "" || artifact.WorkflowID == "" || artifact.Stage == "" ||
		!stageHashPattern.MatchString(artifact.InputHash) || !stageHashPattern.MatchString(artifact.ContentHash) ||
		len(artifact.Payload) == 0 || artifact.CreatedAt.IsZero() || !artifact.ExpiresAt.After(artifact.CreatedAt) ||
		(artifact.Status != StageStatusReady && artifact.Status != StageStatusTerminal) ||
		(artifact.Status == StageStatusTerminal && artifact.Code == "") {
		return ErrStageArtifactConflict
	}
	hash, err := StageArtifactHash(artifact)
	if err != nil || hash != artifact.ContentHash {
		return ErrStageArtifactConflict
	}
	return nil
}

func StageArtifactHash(artifact StageArtifact) (string, error) {
	_, hash, err := canonical.MarshalAndHash(stageArtifactIdentity{
		TenantID: artifact.TenantID, WorkflowID: artifact.WorkflowID, Stage: artifact.Stage,
		InputHash: artifact.InputHash, Payload: artifact.Payload, Status: artifact.Status, Code: artifact.Code,
	})
	return hash, err
}

func stageInputHash(request StageRequest) string {
	if request.PriorHash != "" {
		return request.PriorHash
	}
	_, hash, _ := canonical.MarshalAndHash(request.Input)
	return hash
}

func predicates(facts []domain.OutcomeEvidence) []string {
	seen := make(map[string]struct{}, len(facts))
	result := make([]string, 0, len(facts))
	for _, fact := range facts {
		seen[fact.PredicateID] = struct{}{}
	}
	for predicate := range seen {
		result = append(result, predicate)
	}
	sort.Strings(result)
	return result
}

func inferGoals(graph synthesis.Graph, facts []domain.OutcomeEvidence) []synthesis.GoalRequirement {
	goals := make(map[string]map[domain.EventID]struct{})
	for _, fact := range facts {
		if goals[fact.PredicateID] == nil {
			goals[fact.PredicateID] = make(map[domain.EventID]struct{})
		}
		for _, node := range graph.Nodes {
			if node.Succeeded && (contains(node.SuccessPredicates, fact.PredicateID) || contains(node.VerificationMethods, fact.VerifierID)) {
				goals[fact.PredicateID][node.ID] = struct{}{}
			}
		}
	}
	result := make([]synthesis.GoalRequirement, 0, len(goals))
	for predicate, nodes := range goals {
		goal := synthesis.GoalRequirement{PredicateID: predicate}
		for node := range nodes {
			goal.VerifierNodes = append(goal.VerifierNodes, node)
		}
		sort.Slice(goal.VerifierNodes, func(i, j int) bool { return goal.VerifierNodes[i] < goal.VerifierNodes[j] })
		result = append(result, goal)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PredicateID < result[j].PredicateID })
	return result
}

func requiredEffects(graph synthesis.Graph) []domain.EventID {
	result := make([]domain.EventID, 0)
	for _, node := range graph.Nodes {
		if node.Succeeded && node.SideEffect != toolcontract.SideEffectNone && node.SideEffect != toolcontract.SideEffectRead {
			result = append(result, node.ID)
		}
	}
	return result
}

func inferFailedBranches(graph synthesis.Graph, batch domain.CanonicalBatch, environmentHash string) ([]synthesis.FailedBranch, error) {
	events := make(map[domain.EventID]domain.CanonicalEvent, len(batch.Events))
	for _, event := range batch.Events {
		events[event.ID] = event
	}
	result := make([]synthesis.FailedBranch, 0)
	for _, node := range graph.Nodes {
		if node.Succeeded || node.Opaque {
			continue
		}
		event, found := events[node.ID]
		if !found || event.Result == nil {
			return nil, ErrInvalidStartRequest
		}
		_, toolHash, err := canonical.MarshalAndHash(struct {
			ContractVersionID string                  `json:"contract_version_id"`
			Inputs            []domain.CanonicalField `json:"inputs"`
		}{node.ToolContractVersionID, event.Fields})
		if err != nil {
			return nil, err
		}
		_, failureHash, err := canonical.MarshalAndHash(struct {
			State    string                  `json:"state"`
			ExitCode *int32                  `json:"exit_code,omitempty"`
			Evidence []domain.CanonicalField `json:"evidence,omitempty"`
		}{event.Result.State, event.Result.ExitCode, event.Result.Evidence})
		if err != nil {
			return nil, err
		}
		resources := make([]string, 0, len(node.Reads)+len(node.Writes))
		for _, resource := range node.Reads {
			resources = append(resources, resource.IdentityHash)
		}
		for _, resource := range node.Writes {
			resources = append(resources, resource.IdentityHash)
		}
		sort.Strings(resources)
		_, resourceHash, err := canonical.MarshalAndHash(resources)
		if err != nil {
			return nil, err
		}
		result = append(result, synthesis.FailedBranch{
			FailurePredicateID: "tool_failure:" + node.ToolContractVersionID + ":" + failureHash,
			EventIDs:           []domain.EventID{node.ID},
			Scope:              domain.CompatibilityScope{EnvironmentHash: environmentHash, ToolHash: toolHash, ResourceHash: resourceHash},
		})
	}
	return result, nil
}

func projectionIdentities(batch domain.CanonicalBatch, graph synthesis.Graph, required []domain.EventID) (string, string, string, error) {
	_, intentHash, err := canonical.MarshalAndHash(struct {
		Task           string `json:"task"`
		Harness        string `json:"harness"`
		HarnessVersion string `json:"harness_version,omitempty"`
	}{batch.Trace.Task, batch.Trace.Harness, batch.Trace.HarnessVersion})
	if err != nil {
		return "", "", "", err
	}
	requiredSet := make(map[domain.EventID]struct{}, len(required))
	for _, id := range required {
		requiredSet[id] = struct{}{}
	}
	effects := make([]struct {
		ToolContractVersionID string   `json:"tool_contract_version_id"`
		Resources             []string `json:"resources,omitempty"`
	}, 0, len(required))
	for _, node := range graph.Nodes {
		if _, needed := requiredSet[node.ID]; !needed {
			continue
		}
		resources := make([]string, 0, len(node.Writes))
		for _, resource := range node.Writes {
			resources = append(resources, resource.Namespace+"\x00"+resource.Type+"\x00"+resource.Name)
		}
		sort.Strings(resources)
		effects = append(effects, struct {
			ToolContractVersionID string   `json:"tool_contract_version_id"`
			Resources             []string `json:"resources,omitempty"`
		}{node.ToolContractVersionID, resources})
	}
	_, effectHash, err := canonical.MarshalAndHash(effects)
	if err != nil {
		return "", "", "", err
	}
	_, environmentHash, err := canonical.MarshalAndHash(batch.Trace.Environment)
	return intentHash, effectHash, environmentHash, err
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

var _ StageProcessor = (*Processor)(nil)
