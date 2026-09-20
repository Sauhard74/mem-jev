package composition

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

const maximumCompatibilityGraphEdges = 100_000

var (
	ErrInvalidCompatibilityInput = errors.New("invalid compatibility input")
	ErrCompositionConflict       = errors.New("composition graph conflict")
	sha256Pattern                = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type SchemaCompatibility struct {
	ResourceType   string `json:"resource_type"`
	Namespace      string `json:"namespace"`
	ProvidedSchema string `json:"provided_schema"`
	RequiredSchema string `json:"required_schema"`
}

type CompatibilityMatrix struct {
	SchemaVersion string                `json:"schema_version"`
	ID            string                `json:"id,omitempty"`
	Rules         []SchemaCompatibility `json:"rules,omitempty"`
	Limits        CompatibilityLimits   `json:"limits"`
	ContentHash   string                `json:"content_hash,omitempty"`
	CanonicalJSON []byte                `json:"-"`
}

type CompatibilityLimits struct {
	MaximumCandidates            uint32 `json:"maximum_candidates"`
	MaximumInterfaceEntries      uint32 `json:"maximum_interface_entries"`
	MaximumTotalInterfaceEntries uint32 `json:"maximum_total_interface_entries"`
	MaximumProvidersPerResource  uint32 `json:"maximum_providers_per_resource"`
	MaximumEdges                 uint32 `json:"maximum_edges"`
	MaximumMatchComparisons      uint64 `json:"maximum_match_comparisons"`
}

type Candidate struct {
	TenantID           domain.TenantID
	ProcedureVersionID string
	Lifecycle          string
	Interface          domain.ProcedureInterface
	PlanningFactsHash  string
}

type PlanningFacts struct {
	ProcedureVersionID string   `json:"procedure_version_id"`
	InterfaceHash      string   `json:"interface_hash"`
	Lifecycle          string   `json:"lifecycle"`
	PolicyManifestID   string   `json:"policy_manifest_id"`
	GoalPredicateIDs   []string `json:"goal_predicate_ids,omitempty"`
	EvidenceStrength   int64    `json:"evidence_strength"`
	ObservedEndToEnd   bool     `json:"observed_end_to_end"`
	RiskCost           uint32   `json:"risk_cost"`
	ToolCost           uint32   `json:"tool_cost"`
}

func BuildPlanningFactsHash(facts PlanningFacts) (string, error) {
	facts.ProcedureVersionID = strings.TrimSpace(facts.ProcedureVersionID)
	facts.Lifecycle = strings.TrimSpace(facts.Lifecycle)
	facts.PolicyManifestID = strings.TrimSpace(facts.PolicyManifestID)
	facts.GoalPredicateIDs = append([]string(nil), facts.GoalPredicateIDs...)
	for index := range facts.GoalPredicateIDs {
		facts.GoalPredicateIDs[index] = strings.TrimSpace(facts.GoalPredicateIDs[index])
	}
	sort.Strings(facts.GoalPredicateIDs)
	if !safeIdentity(facts.ProcedureVersionID) || !sha256Pattern.MatchString(facts.InterfaceHash) || !safeIdentity(facts.Lifecycle) || !safeIdentity(facts.PolicyManifestID) || facts.EvidenceStrength < 0 || facts.ToolCost == 0 || !strictSorted(facts.GoalPredicateIDs) {
		return "", ErrInvalidCompatibilityInput
	}
	_, hash, err := canonical.MarshalAndHash(facts)
	return hash, err
}

type CompatibilityEdge struct {
	ID                      string          `json:"id"`
	TenantID                domain.TenantID `json:"tenant_id"`
	ProjectionEpoch         uint64          `json:"projection_epoch"`
	SourceVersionID         string          `json:"source_procedure_version_id"`
	TargetVersionID         string          `json:"target_procedure_version_id"`
	SourceInterfaceHash     string          `json:"source_interface_hash"`
	TargetInterfaceHash     string          `json:"target_interface_hash"`
	SourceProvisionIDs      []string        `json:"source_provision_ids"`
	SatisfiedRequirementIDs []string        `json:"satisfied_requirement_ids"`
	PlannerManifestID       string          `json:"planner_manifest_id"`
	PolicyManifestID        string          `json:"policy_manifest_id"`
	SchemaVersion           string          `json:"schema_version"`
	ContentHash             string          `json:"content_hash,omitempty"`
}

type CompatibilityGraph struct {
	SchemaVersion     string              `json:"schema_version"`
	ID                string              `json:"id,omitempty"`
	TenantID          domain.TenantID     `json:"tenant_id"`
	ProjectionEpoch   uint64              `json:"projection_epoch"`
	PlannerManifestID string              `json:"planner_manifest_id"`
	PolicyManifestID  string              `json:"policy_manifest_id"`
	MatrixHash        string              `json:"matrix_hash"`
	CandidateSetHash  string              `json:"candidate_set_hash"`
	Edges             []CompatibilityEdge `json:"edges,omitempty"`
	ContentHash       string              `json:"content_hash,omitempty"`
	CanonicalJSON     []byte              `json:"-"`
}

type GraphRequest struct {
	TenantID          domain.TenantID
	ProjectionEpoch   uint64
	PlannerManifestID string
	PolicyManifestID  string
	Matrix            CompatibilityMatrix
	Candidates        []Candidate
}

func ValidateCompatibilityMatrix(matrix CompatibilityMatrix) error {
	if matrix.SchemaVersion != "compatibility-matrix.v1" || matrix.ID != "cmat_"+matrix.ContentHash || !sha256Pattern.MatchString(matrix.ContentHash) {
		return ErrInvalidCompatibilityInput
	}
	copyOfMatrix := matrix
	copyOfMatrix.ID, copyOfMatrix.ContentHash, copyOfMatrix.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(copyOfMatrix)
	if err != nil || hash != matrix.ContentHash || !bytes.Equal(canonicalJSON, matrix.CanonicalJSON) {
		return ErrInvalidCompatibilityInput
	}
	normalized, normalizedHash, err := normalizeCompatibility(matrix.Rules)
	if err != nil || normalizedHash == "" || !sameCompatibilityRules(normalized, matrix.Rules) || !validCompatibilityLimits(matrix.Limits) {
		return ErrInvalidCompatibilityInput
	}
	return nil
}

func sameCompatibilityRules(left, right []SchemaCompatibility) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func ValidateCompatibilityGraph(graph CompatibilityGraph) error {
	return validateCompatibilityGraph(context.Background(), graph, nil)
}

func validateCompatibilityGraph(ctx context.Context, graph CompatibilityGraph, charge func(uint64) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if graph.SchemaVersion != "compatibility-graph.v1" || graph.ID != "cgraph_"+graph.ContentHash || !sha256Pattern.MatchString(graph.ContentHash) {
		return ErrInvalidCompatibilityInput
	}
	if len(graph.Edges) > maximumCompatibilityGraphEdges {
		return ErrInvalidCompatibilityInput
	}
	if charge != nil {
		// Canonical graph authentication is linear in the number of edges.
		if err := charge(uint64(len(graph.Edges)) + 1); err != nil {
			return err
		}
	}
	copyOfGraph := graph
	copyOfGraph.ID, copyOfGraph.ContentHash, copyOfGraph.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(copyOfGraph)
	if err != nil || hash != graph.ContentHash || !bytes.Equal(canonicalJSON, graph.CanonicalJSON) {
		return ErrInvalidCompatibilityInput
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if graph.TenantID == "" || graph.ProjectionEpoch == 0 || graph.PlannerManifestID == "" || graph.PolicyManifestID == "" || !sha256Pattern.MatchString(graph.MatrixHash) || !sha256Pattern.MatchString(graph.CandidateSetHash) {
		return ErrInvalidCompatibilityInput
	}
	if charge != nil {
		// Structural validation performs a second complete edge scan.
		if err = charge(uint64(len(graph.Edges))); err != nil {
			return err
		}
	}
	seenPairs := make(map[string]struct{}, len(graph.Edges))
	for index, edge := range graph.Edges {
		if index%256 == 0 {
			if err = ctx.Err(); err != nil {
				return err
			}
		}
		pair := edge.SourceVersionID + "\x00" + edge.TargetVersionID
		_, duplicate := seenPairs[pair]
		if duplicate || validateCompatibilityEdge(edge, graph) != nil || index > 0 && graph.Edges[index-1].ID >= edge.ID {
			return ErrInvalidCompatibilityInput
		}
		seenPairs[pair] = struct{}{}
	}
	return nil
}

func validateCompatibilityEdge(edge CompatibilityEdge, graph CompatibilityGraph) error {
	if edge.SchemaVersion != "compatibility-edge.v1" || edge.ID != "cedge_"+edge.ContentHash || !sha256Pattern.MatchString(edge.ContentHash) ||
		edge.TenantID != graph.TenantID || edge.ProjectionEpoch != graph.ProjectionEpoch || edge.PlannerManifestID != graph.PlannerManifestID || edge.PolicyManifestID != graph.PolicyManifestID ||
		edge.SourceVersionID == "" || edge.TargetVersionID == "" || edge.SourceVersionID == edge.TargetVersionID || !sha256Pattern.MatchString(edge.SourceInterfaceHash) || !sha256Pattern.MatchString(edge.TargetInterfaceHash) ||
		len(edge.SourceProvisionIDs) == 0 || len(edge.SatisfiedRequirementIDs) == 0 || !strictPrefixed(edge.SourceProvisionIDs, "prov_") || !strictPrefixed(edge.SatisfiedRequirementIDs, "req_") {
		return ErrInvalidCompatibilityInput
	}
	identity := edge
	identity.ID, identity.ContentHash = "", ""
	_, hash, err := canonical.MarshalAndHash(identity)
	if err != nil || hash != edge.ContentHash {
		return ErrInvalidCompatibilityInput
	}
	return nil
}

func strictSorted(values []string) bool {
	for index, value := range values {
		if value == "" || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

func strictPrefixed(values []string, prefix string) bool {
	if !strictSorted(values) {
		return false
	}
	for _, value := range values {
		if len(value) != len(prefix)+64 || value[:len(prefix)] != prefix || !sha256Pattern.MatchString(value[len(prefix):]) {
			return false
		}
	}
	return true
}

func validCandidate(candidate Candidate, tenantID domain.TenantID) bool {
	if candidate.TenantID != tenantID || !safeIdentity(string(candidate.TenantID)) || !safeIdentity(candidate.ProcedureVersionID) || !sha256Pattern.MatchString(candidate.PlanningFactsHash) ||
		(candidate.Lifecycle != "candidate" && candidate.Lifecycle != "trial" && candidate.Lifecycle != "active") || retrieval.ValidateProcedureInterface(candidate.Interface) != nil {
		return false
	}
	for _, requirement := range candidate.Interface.Requirements {
		if !safeIdentity(requirement.ResourceType) || !safeIdentity(requirement.Namespace) || requirement.SchemaVersion != "" && !safeIdentity(requirement.SchemaVersion) {
			return false
		}
	}
	for _, provision := range candidate.Interface.Provisions {
		if !safeIdentity(provision.ResourceType) || !safeIdentity(provision.Namespace) || provision.SchemaVersion != "" && !safeIdentity(provision.SchemaVersion) {
			return false
		}
	}
	return true
}

func safeIdentity(value string) bool {
	return value != "" && !strings.ContainsRune(value, '\x00')
}

func validCompatibilityLimits(value CompatibilityLimits) bool {
	return value.MaximumCandidates > 0 && value.MaximumCandidates <= 10_000 &&
		value.MaximumInterfaceEntries > 0 && value.MaximumInterfaceEntries <= 1_024 &&
		value.MaximumTotalInterfaceEntries > 0 && value.MaximumTotalInterfaceEntries <= 1_000_000 &&
		value.MaximumProvidersPerResource > 0 && value.MaximumProvidersPerResource <= 1_024 &&
		value.MaximumEdges > 0 && value.MaximumEdges <= 100_000 &&
		value.MaximumMatchComparisons > 0 && value.MaximumMatchComparisons <= 10_000_000
}
