package projection

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/synthesis"
)

var ErrInvalidBuildRequest = errors.New("invalid projection build request")

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type BuildRequest struct {
	TenantID             domain.TenantID
	OutcomeID            domain.OutcomeID
	IntentHash           string
	EffectSignatureHash  string
	EnvironmentScopeHash string
	ArchiveHash          string
	CanonicalEventStart  uint32
	CanonicalEventEnd    uint32
	CreatedAt            time.Time
	Synthesis            synthesis.Result
	Serving              ServingMetadata
}

type ServingMetadata struct {
	TaskText                 string
	Harness                  retrieval.Harness
	Environment              []retrieval.Fact
	Resources                []retrieval.ResourceRequirement
	Effects                  []string
	RiskClass                string
	VerificationStrength     uint8
	LearnedWithRecallConsent bool
	ResidencyRegion          string
}

type Family struct {
	ID                  string `json:"id"`
	IntentHash          string `json:"intent_hash"`
	EffectSignatureHash string `json:"effect_signature_hash"`
	ContentHash         string `json:"content_hash"`
}

type Version struct {
	ID                   string `json:"id"`
	ProcedureID          string `json:"procedure_id"`
	GraphHash            string `json:"graph_hash"`
	EnvironmentScopeHash string `json:"environment_scope_hash"`
	PolicyVersion        string `json:"policy_version"`
	ObservedEndToEnd     bool   `json:"observed_end_to_end"`
	ContentHash          string `json:"content_hash"`
}

type Step struct {
	ID                    string `json:"id"`
	Ordinal               uint32 `json:"ordinal"`
	ToolName              string `json:"tool_name"`
	ToolVersion           string `json:"tool_version,omitempty"`
	ToolContractVersionID string `json:"tool_contract_version_id"`
	UncertainNecessity    bool   `json:"uncertain_necessity"`
	CompensationBoundary  bool   `json:"compensation_boundary"`
	ContentHash           string `json:"content_hash"`
}

type Edge struct {
	ID                string             `json:"id"`
	FromOrdinal       uint32             `json:"from_ordinal"`
	ToOrdinal         uint32             `json:"to_ordinal"`
	Type              synthesis.EdgeType `json:"type"`
	ResourceName      string             `json:"resource_name,omitempty"`
	ResourceType      string             `json:"resource_type,omitempty"`
	ResourceNamespace string             `json:"resource_namespace,omitempty"`
	ContentHash       string             `json:"content_hash"`
}

type Manifest struct {
	ID                  string             `json:"id"`
	TenantID            domain.TenantID    `json:"tenant_id"`
	OutcomeID           domain.OutcomeID   `json:"outcome_id"`
	TraceID             domain.TraceID     `json:"trace_id"`
	ProcedureVersionID  string             `json:"procedure_version_id,omitempty"`
	ArchiveHash         string             `json:"archive_hash"`
	CanonicalEventStart uint32             `json:"canonical_event_start"`
	CanonicalEventEnd   uint32             `json:"canonical_event_end"`
	Versions            synthesis.Versions `json:"versions"`
	Status              synthesis.Status   `json:"status"`
	AbstentionCode      string             `json:"abstention_code,omitempty"`
	SynthesisHash       string             `json:"synthesis_hash"`
	ContentHash         string             `json:"content_hash"`
}

type Projection struct {
	TenantID                domain.TenantID       `json:"tenant_id"`
	Family                  Family                `json:"family"`
	Version                 Version               `json:"version"`
	Steps                   []Step                `json:"steps,omitempty"`
	Edges                   []Edge                `json:"edges,omitempty"`
	NegativePaths           []domain.NegativePath `json:"negative_paths,omitempty"`
	GoalPredicates          []string              `json:"goal_predicates,omitempty"`
	Manifest                Manifest              `json:"manifest"`
	CreatedAt               time.Time             `json:"-"`
	CanonicalProjectionJSON []byte                `json:"-"`
	RetrievalDocument       retrieval.Document    `json:"-"`
}

type logicalStep struct {
	Ordinal               uint32 `json:"ordinal"`
	ToolName              string `json:"tool_name"`
	ToolVersion           string `json:"tool_version,omitempty"`
	ToolContractVersionID string `json:"tool_contract_version_id"`
	UncertainNecessity    bool   `json:"uncertain_necessity"`
	CompensationBoundary  bool   `json:"compensation_boundary"`
}

type logicalEdge struct {
	FromOrdinal       uint32             `json:"from_ordinal"`
	ToOrdinal         uint32             `json:"to_ordinal"`
	Type              synthesis.EdgeType `json:"type"`
	ResourceName      string             `json:"resource_name,omitempty"`
	ResourceType      string             `json:"resource_type,omitempty"`
	ResourceNamespace string             `json:"resource_namespace,omitempty"`
}

type logicalGraph struct {
	Steps                []logicalStep `json:"steps"`
	Edges                []logicalEdge `json:"edges,omitempty"`
	GoalPredicates       []string      `json:"goal_predicates"`
	EnvironmentScopeHash string        `json:"environment_scope_hash"`
}

func Build(request BuildRequest) (Projection, error) {
	if request.TenantID == "" || request.OutcomeID == "" || !sha256Pattern.MatchString(request.ArchiveHash) ||
		!sha256Pattern.MatchString(request.IntentHash) || !sha256Pattern.MatchString(request.EffectSignatureHash) ||
		!sha256Pattern.MatchString(request.EnvironmentScopeHash) || request.Synthesis.Hash == "" ||
		request.Synthesis.TraceID == "" || request.CanonicalEventEnd < request.CanonicalEventStart || request.CreatedAt.IsZero() {
		return Projection{}, ErrInvalidBuildRequest
	}
	projection := Projection{TenantID: request.TenantID, CreatedAt: request.CreatedAt.UTC(), NegativePaths: cloneNegativePaths(request.Synthesis.NegativePaths)}
	if request.Synthesis.Status == synthesis.StatusSynthesized {
		if len(request.Synthesis.Steps) == 0 {
			return Projection{}, ErrInvalidBuildRequest
		}
		servingIntentHash, hashErr := retrieval.CanonicalIntentHash(request.Serving.TaskText, request.Serving.Harness)
		if hashErr != nil || servingIntentHash != request.IntentHash {
			return Projection{}, ErrInvalidBuildRequest
		}
		family, version, steps, edges, canonicalJSON, err := buildProcedure(request)
		if err != nil {
			return Projection{}, err
		}
		projection.Family, projection.Version, projection.Steps, projection.Edges = family, version, steps, edges
		projection.GoalPredicates = append([]string(nil), request.Synthesis.GoalPredicates...)
		sort.Strings(projection.GoalPredicates)
		projection.CanonicalProjectionJSON = canonicalJSON
		toolRequirements := make([]retrieval.ToolRequirement, len(projection.Steps))
		orderedContracts := make([]string, len(projection.Steps))
		for index, step := range projection.Steps {
			toolRequirements[index] = retrieval.ToolRequirement{Name: step.ToolName, ContractVersionID: step.ToolContractVersionID}
			orderedContracts[index] = step.ToolContractVersionID
		}
		projection.RetrievalDocument, err = retrieval.BuildDocument(retrieval.DocumentInput{
			TenantID: request.TenantID, ProcedureVersionID: version.ID, ProcedureID: family.ID,
			TaskText: request.Serving.TaskText, IntentHash: family.IntentHash, EffectSignatureHash: family.EffectSignatureHash,
			Tools: toolRequirements, OrderedStepContractIDs: orderedContracts, Resources: request.Serving.Resources,
			Effects: request.Serving.Effects, EnvironmentScopeHash: version.EnvironmentScopeHash,
			Harness: request.Serving.Harness, Environment: request.Serving.Environment,
			Lifecycle: "candidate", ObservedEndToEnd: version.ObservedEndToEnd,
			VerificationStrength: request.Serving.VerificationStrength, VerifiedSuccessCount: 1,
			ValidatedAt: request.CreatedAt, ValidationPolicyVersion: version.PolicyVersion,
			LearnedWithRecallConsent: request.Serving.LearnedWithRecallConsent,
			ResidencyRegion:          request.Serving.ResidencyRegion, RiskClass: request.Serving.RiskClass,
		})
		if err != nil {
			return Projection{}, err
		}
	} else if request.Synthesis.Status != synthesis.StatusAbstained || request.Synthesis.AbstentionCode == "" {
		return Projection{}, ErrInvalidBuildRequest
	}
	manifest := Manifest{
		TenantID: request.TenantID, OutcomeID: request.OutcomeID, TraceID: request.Synthesis.TraceID,
		ProcedureVersionID: projection.Version.ID, ArchiveHash: request.ArchiveHash,
		CanonicalEventStart: request.CanonicalEventStart, CanonicalEventEnd: request.CanonicalEventEnd,
		Versions: request.Synthesis.Versions, Status: request.Synthesis.Status,
		AbstentionCode: request.Synthesis.AbstentionCode, SynthesisHash: request.Synthesis.Hash,
	}
	_, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return Projection{}, err
	}
	manifest.ID, manifest.ContentHash = "syn_"+hash, hash
	projection.Manifest = manifest
	return projection, nil
}

func buildProcedure(request BuildRequest) (Family, Version, []Step, []Edge, []byte, error) {
	familyIdentity := struct {
		TenantID            domain.TenantID `json:"tenant_id"`
		IntentHash          string          `json:"intent_hash"`
		EffectSignatureHash string          `json:"effect_signature_hash"`
	}{request.TenantID, request.IntentHash, request.EffectSignatureHash}
	_, familyHash, err := canonical.MarshalAndHash(familyIdentity)
	if err != nil {
		return Family{}, Version{}, nil, nil, nil, err
	}
	family := Family{ID: "proc_" + familyHash, IntentHash: request.IntentHash, EffectSignatureHash: request.EffectSignatureHash, ContentHash: familyHash}

	ordinals := make(map[domain.EventID]uint32, len(request.Synthesis.Steps))
	logicalSteps := make([]logicalStep, len(request.Synthesis.Steps))
	for index, source := range request.Synthesis.Steps {
		if source.Ordinal != uint32(index) || source.EventID == "" || source.ToolContractVersionID == "" {
			return Family{}, Version{}, nil, nil, nil, ErrInvalidBuildRequest
		}
		ordinals[source.EventID] = source.Ordinal
		logicalSteps[index] = logicalStep{
			Ordinal: source.Ordinal, ToolName: source.ToolName, ToolVersion: source.ToolVersion,
			ToolContractVersionID: source.ToolContractVersionID, UncertainNecessity: source.UncertainNecessity,
			CompensationBoundary: source.CompensationBoundary,
		}
	}
	logicalEdges := make([]logicalEdge, len(request.Synthesis.Edges))
	for index, source := range request.Synthesis.Edges {
		from, fromOK := ordinals[source.From]
		to, toOK := ordinals[source.To]
		if !fromOK || !toOK || from >= to {
			return Family{}, Version{}, nil, nil, nil, ErrInvalidBuildRequest
		}
		logicalEdges[index] = logicalEdge{
			FromOrdinal: from, ToOrdinal: to, Type: source.Type, ResourceName: source.ResourceName,
			ResourceType: source.ResourceType, ResourceNamespace: source.ResourceNamespace,
		}
	}
	sort.Slice(logicalEdges, func(i, j int) bool {
		left := fmt.Sprintf("%010d\x00%010d\x00%s\x00%s\x00%s\x00%s", logicalEdges[i].FromOrdinal, logicalEdges[i].ToOrdinal, logicalEdges[i].Type, logicalEdges[i].ResourceNamespace, logicalEdges[i].ResourceType, logicalEdges[i].ResourceName)
		right := fmt.Sprintf("%010d\x00%010d\x00%s\x00%s\x00%s\x00%s", logicalEdges[j].FromOrdinal, logicalEdges[j].ToOrdinal, logicalEdges[j].Type, logicalEdges[j].ResourceNamespace, logicalEdges[j].ResourceType, logicalEdges[j].ResourceName)
		return left < right
	})
	goals := append([]string(nil), request.Synthesis.GoalPredicates...)
	sort.Strings(goals)
	graph := logicalGraph{Steps: logicalSteps, Edges: logicalEdges, GoalPredicates: goals, EnvironmentScopeHash: request.EnvironmentScopeHash}
	_, graphHash, err := canonical.MarshalAndHash(graph)
	if err != nil {
		return Family{}, Version{}, nil, nil, nil, err
	}
	versionIdentity := struct {
		TenantID         domain.TenantID `json:"tenant_id"`
		ProcedureID      string          `json:"procedure_id"`
		GraphHash        string          `json:"graph_hash"`
		PolicyVersion    string          `json:"policy_version"`
		ObservedEndToEnd bool            `json:"observed_end_to_end"`
	}{request.TenantID, family.ID, graphHash, request.Synthesis.Versions.Policy, request.Synthesis.ObservedEndToEnd}
	_, versionHash, err := canonical.MarshalAndHash(versionIdentity)
	if err != nil {
		return Family{}, Version{}, nil, nil, nil, err
	}
	version := Version{
		ID: "pv_" + versionHash, ProcedureID: family.ID, GraphHash: graphHash,
		EnvironmentScopeHash: request.EnvironmentScopeHash, PolicyVersion: request.Synthesis.Versions.Policy,
		ObservedEndToEnd: request.Synthesis.ObservedEndToEnd, ContentHash: versionHash,
	}
	steps := make([]Step, len(logicalSteps))
	for index, logical := range logicalSteps {
		_, hash, hashErr := canonical.MarshalAndHash(struct {
			VersionID string      `json:"version_id"`
			Step      logicalStep `json:"step"`
		}{version.ID, logical})
		if hashErr != nil {
			return Family{}, Version{}, nil, nil, nil, hashErr
		}
		steps[index] = Step{
			ID: "step_" + hash, Ordinal: logical.Ordinal, ToolName: logical.ToolName, ToolVersion: logical.ToolVersion,
			ToolContractVersionID: logical.ToolContractVersionID, UncertainNecessity: logical.UncertainNecessity,
			CompensationBoundary: logical.CompensationBoundary, ContentHash: hash,
		}
	}
	edges := make([]Edge, len(logicalEdges))
	for index, logical := range logicalEdges {
		_, hash, hashErr := canonical.MarshalAndHash(struct {
			VersionID string      `json:"version_id"`
			Edge      logicalEdge `json:"edge"`
		}{version.ID, logical})
		if hashErr != nil {
			return Family{}, Version{}, nil, nil, nil, hashErr
		}
		edges[index] = Edge{
			ID: "edge_" + hash, FromOrdinal: logical.FromOrdinal, ToOrdinal: logical.ToOrdinal, Type: logical.Type,
			ResourceName: logical.ResourceName, ResourceType: logical.ResourceType,
			ResourceNamespace: logical.ResourceNamespace, ContentHash: hash,
		}
	}
	canonicalProjection := struct {
		Family  Family  `json:"family"`
		Version Version `json:"version"`
		Steps   []Step  `json:"steps"`
		Edges   []Edge  `json:"edges,omitempty"`
	}{family, version, steps, edges}
	encoded, _, err := canonical.MarshalAndHash(canonicalProjection)
	return family, version, steps, edges, encoded, err
}

func cloneNegativePaths(source []domain.NegativePath) []domain.NegativePath {
	result := make([]domain.NegativePath, len(source))
	for index, path := range source {
		result[index] = path
		result[index].EventIDs = append([]domain.EventID(nil), path.EventIDs...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
