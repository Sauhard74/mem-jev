package domain

type ProcedureResource struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Namespace     string `json:"namespace"`
	IdentityHash  string `json:"identity_hash"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

type ProcedurePredicate struct {
	ID           string `json:"id"`
	ResourceName string `json:"resource_name,omitempty"`
}

type ProcedureRequirement struct {
	ID            string   `json:"id,omitempty"`
	ResourceType  string   `json:"resource_type"`
	Namespace     string   `json:"namespace"`
	IdentityHash  string   `json:"identity_hash"`
	SchemaVersion string   `json:"schema_version,omitempty"`
	AccessMode    string   `json:"access_mode"`
	PredicateIDs  []string `json:"predicate_ids,omitempty"`
}

type ProcedureProvision struct {
	ID                  string   `json:"id,omitempty"`
	ResourceType        string   `json:"resource_type"`
	Namespace           string   `json:"namespace"`
	IdentityHash        string   `json:"identity_hash"`
	SchemaVersion       string   `json:"schema_version,omitempty"`
	ProducedEffects     []string `json:"produced_effects,omitempty"`
	SuccessPredicateIDs []string `json:"success_predicate_ids,omitempty"`
}

type ProcedureInterface struct {
	SchemaVersion string                 `json:"schema_version"`
	ID            string                 `json:"id,omitempty"`
	Requirements  []ProcedureRequirement `json:"requirements,omitempty"`
	Provisions    []ProcedureProvision   `json:"provisions,omitempty"`
	ContentHash   string                 `json:"content_hash,omitempty"`
}

type ProcedureStep struct {
	EventID               EventID              `json:"event_id"`
	Ordinal               uint32               `json:"ordinal"`
	OriginalPosition      uint32               `json:"original_position"`
	ToolName              string               `json:"tool_name"`
	ToolVersion           string               `json:"tool_version,omitempty"`
	ToolContractVersionID string               `json:"tool_contract_version_id"`
	UncertainNecessity    bool                 `json:"uncertain_necessity"`
	CompensationBoundary  bool                 `json:"compensation_boundary"`
	SideEffect            string               `json:"side_effect"`
	Risk                  string               `json:"risk"`
	Effects               []string             `json:"effects,omitempty"`
	Preconditions         []ProcedurePredicate `json:"preconditions,omitempty"`
	SuccessPredicates     []string             `json:"success_predicates,omitempty"`
	VerificationMethods   []string             `json:"verification_methods,omitempty"`
	Reads                 []ProcedureResource  `json:"reads,omitempty"`
	Writes                []ProcedureResource  `json:"writes,omitempty"`
}
