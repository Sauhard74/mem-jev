package synthesis

import (
	"errors"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

var (
	ErrInvalidBuildRequest = errors.New("invalid causal graph request")
	ErrMissingResource     = errors.New("tool event is missing a declared resource identity")
	ErrUnknownDependency   = errors.New("declared dependency references an unknown event")
	ErrCausalCycle         = errors.New("causal graph contains a cycle")
)

type EdgeType string

const (
	EdgeResourceFlow EdgeType = "resource_flow"
	EdgeControl      EdgeType = "control"
	EdgeVerifier     EdgeType = "verifier"
)

type Resource struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Namespace     string `json:"namespace"`
	IdentityHash  string `json:"identity_hash"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

type PredicateReference struct {
	ID           string `json:"id"`
	ResourceName string `json:"resource_name,omitempty"`
}

type Node struct {
	ID                    domain.EventID               `json:"id"`
	Position              uint32                       `json:"position"`
	ToolName              string                       `json:"tool_name"`
	ToolVersion           string                       `json:"tool_version"`
	ToolContractVersionID string                       `json:"tool_contract_version_id"`
	Opaque                bool                         `json:"opaque"`
	Succeeded             bool                         `json:"succeeded"`
	SideEffect            toolcontract.SideEffectClass `json:"side_effect"`
	Risk                  toolcontract.RiskClass       `json:"risk"`
	Effects               []string                     `json:"effects,omitempty"`
	CompensationBoundary  bool                         `json:"compensation_boundary"`
	Preconditions         []PredicateReference         `json:"preconditions,omitempty"`
	SuccessPredicates     []string                     `json:"success_predicates,omitempty"`
	VerificationMethods   []string                     `json:"verification_methods,omitempty"`
	Reads                 []Resource                   `json:"reads,omitempty"`
	Writes                []Resource                   `json:"writes,omitempty"`
}

type Edge struct {
	From              domain.EventID `json:"from"`
	To                domain.EventID `json:"to"`
	Type              EdgeType       `json:"type"`
	ResourceID        string         `json:"resource_id,omitempty"`
	ResourceName      string         `json:"resource_name,omitempty"`
	ResourceType      string         `json:"resource_type,omitempty"`
	ResourceNamespace string         `json:"resource_namespace,omitempty"`
}

type Graph struct {
	SchemaVersion  string         `json:"schema_version"`
	TraceID        domain.TraceID `json:"trace_id"`
	Nodes          []Node         `json:"nodes"`
	Edges          []Edge         `json:"edges,omitempty"`
	AutoPromotable bool           `json:"auto_promotable"`
	Hash           string         `json:"-"`
}

type DeclaredDependency struct {
	Prerequisite domain.EventID
	Dependent    domain.EventID
}

type BuildRequest struct {
	Batch                domain.CanonicalBatch
	ControlDependencies  []DeclaredDependency
	VerifierDependencies []DeclaredDependency
}
