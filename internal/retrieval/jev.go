package retrieval

import (
	"context"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/jev"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/ranking"
	"github.com/sauhard74/mem-jev/internal/security"
)

const semanticQuestionTokenReserve = 600

type JevSemanticJudge struct {
	service   *jev.Service
	rubric    jev.RubricManifest
	admission jev.AdmissionPolicy
}

func NewJevSemanticJudge(service *jev.Service, rubric jev.RubricManifest, admission jev.AdmissionPolicy) (*JevSemanticJudge, error) {
	if service == nil || jev.ValidateRubricManifest(rubric) != nil || admission.MaximumCandidates < 2 || admission.MaximumCandidates > 32 || admission.AmbiguityScoreDistance < 0 {
		return nil, ErrServiceUnavailable
	}
	return &JevSemanticJudge{service: service, rubric: rubric, admission: admission}, nil
}

func (judge *JevSemanticJudge) Admit(candidates []ranking.Result) ([]string, error) {
	if judge == nil {
		return nil, jev.ErrInvalidAdmission
	}
	values := make([]jev.RankedCandidate, len(candidates))
	for index, item := range candidates {
		values[index] = jev.RankedCandidate{VersionID: item.VersionID, Rank: item.Rank, FinalScore: item.FinalScore}
	}
	return jev.AdmitAmbiguousCandidates(judge.admission, values)
}

func (judge *JevSemanticJudge) Judge(ctx context.Context, request SemanticJudgmentRequest) SemanticJudgment {
	started := time.Now()
	metrics := observability.JevJudgmentMetrics{Disposition: "invalid_state"}
	defer func() {
		metrics.Latency = time.Since(started)
		observability.RecordJevJudgment(ctx, metrics)
	}()
	if judge == nil {
		return SemanticJudgment{VersionID: request.Document.ProcedureVersionID, Disposition: "invalid_state"}
	}
	keyInput := jev.JudgmentKeyInput{TenantID: request.TenantID, QueryHash: request.Query.Hash, ProcedureVersionID: request.Document.ProcedureVersionID, DocumentHash: request.Document.ContentHash, EnvironmentHash: request.Query.EnvironmentHash, PolicyManifestID: request.PolicyManifestID, RubricManifestID: judge.rubric.ID, Provider: jev.ProviderTypeSafe, Model: judge.rubric.Model}
	key, err := jev.NewJudgmentKey(keyInput)
	base := SemanticJudgment{VersionID: request.Document.ProcedureVersionID, JudgmentKey: key, Provider: jev.ProviderTypeSafe, Model: judge.rubric.Model, RubricManifestID: judge.rubric.ID}
	if err != nil || ValidateQuery(request.Query) != nil || ValidateDocument(request.Document) != nil || request.Query.TenantID != request.TenantID || request.Document.TenantID != request.TenantID {
		base.Disposition = "invalid_state"
		return base
	}
	state, stateBytes, err := semanticStateFor(request.Query, request.Document)
	if err != nil {
		metrics.Disposition = "sensitive_state"
		base.Disposition = "sensitive_state"
		return base
	}
	estimatedTokens := (len(stateBytes)+3)/4 + semanticQuestionTokenReserve
	result, err := judge.service.Judge(ctx, jev.ServiceRequest{KeyInput: keyInput, State: state, EstimatedTokens: estimatedTokens})
	if err != nil {
		base.Disposition = "invalid_state"
		return base
	}
	metrics.Disposition, metrics.ProviderCalled = result.Disposition, result.ProviderCalled
	if result.ProviderCalled {
		metrics.InputTokens, metrics.OutputTokens = result.Record.Usage.InputTokens, result.Record.Usage.OutputTokens
	}
	base.Disposition = result.Disposition
	if result.Record.Key == "" {
		return base
	}
	base.JudgmentKey, base.ContentHash = result.Record.Key, result.Record.ContentHash
	base.Provider, base.Model, base.RubricManifestID = result.Record.Provider, result.Record.Model, result.Record.RubricManifestID
	base.Features = make([]ranking.FeatureValue, len(result.Features))
	for index, feature := range result.Features {
		base.Features[index] = ranking.FeatureValue{Name: feature.Name, Value: feature.Value}
	}
	return base
}

type jevQueryState struct {
	Task             string   `json:"task"`
	Tools            []Tool   `json:"available_tools"`
	Harness          Harness  `json:"harness"`
	Environment      []Fact   `json:"environment,omitempty"`
	Constraints      []Fact   `json:"constraints,omitempty"`
	ForbiddenEffects []string `json:"forbidden_effects,omitempty"`
}

type jevProcedureState struct {
	TaskText    string            `json:"task_text"`
	Tools       []ToolRequirement `json:"required_tools"`
	Harness     Harness           `json:"required_harness"`
	Environment []Fact            `json:"required_environment,omitempty"`
	Effects     []string          `json:"effects,omitempty"`
}

type jevState struct {
	Request   jevQueryState     `json:"request"`
	Procedure jevProcedureState `json:"procedure"`
}

func semanticStateFor(query Query, document Document) (jevState, []byte, error) {
	state := jevState{
		Request:   jevQueryState{Task: query.Task, Tools: append([]Tool(nil), query.Tools...), Harness: query.Harness, Environment: append([]Fact(nil), query.Environment...), Constraints: append([]Fact(nil), query.Constraints...), ForbiddenEffects: append([]string(nil), query.ForbiddenEffects...)},
		Procedure: jevProcedureState{TaskText: document.TaskText, Tools: append([]ToolRequirement(nil), document.Tools...), Harness: document.Harness, Environment: append([]Fact(nil), document.Environment...), Effects: append([]string(nil), document.Effects...)},
	}
	for _, value := range semanticStateStrings(state) {
		if security.ContainsCredentialMaterial(value) {
			return jevState{}, nil, ErrServiceUnavailable
		}
	}
	encoded, _, err := canonical.MarshalAndHash(state)
	if err != nil {
		return jevState{}, nil, err
	}
	return state, encoded, nil
}

func semanticStateStrings(state jevState) []string {
	values := []string{state.Request.Task, state.Request.Harness.Name, state.Request.Harness.Version, state.Procedure.TaskText, state.Procedure.Harness.Name, state.Procedure.Harness.Version}
	for _, item := range state.Request.Tools {
		values = append(values, item.Name, item.ContractVersionID)
	}
	for _, item := range state.Procedure.Tools {
		values = append(values, item.Name, item.ContractVersionID)
	}
	for _, facts := range [][]Fact{state.Request.Environment, state.Request.Constraints, state.Procedure.Environment} {
		for _, item := range facts {
			values = append(values, item.Name, item.Value)
		}
	}
	values = append(values, state.Request.ForbiddenEffects...)
	values = append(values, state.Procedure.Effects...)
	for index := range values {
		values[index] = strings.TrimSpace(values[index])
	}
	return values
}

var _ SemanticJudge = (*JevSemanticJudge)(nil)
