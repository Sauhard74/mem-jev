package toolcontract

type Sanitizer string

const (
	SanitizerToken         Sanitizer = "token"
	SanitizerText          Sanitizer = "text"
	SanitizerRelativePath  Sanitizer = "relative_path"
	SanitizerRedactSecrets Sanitizer = "redact_secrets"
	SanitizerInteger       Sanitizer = "integer"
	SanitizerDrop          Sanitizer = "drop"
)

type SideEffectClass string

const (
	SideEffectNone         SideEffectClass = "none"
	SideEffectRead         SideEffectClass = "read"
	SideEffectWrite        SideEffectClass = "write"
	SideEffectExternal     SideEffectClass = "external"
	SideEffectIrreversible SideEffectClass = "irreversible"
)

type RiskClass string

const (
	RiskLow      RiskClass = "low"
	RiskMedium   RiskClass = "medium"
	RiskHigh     RiskClass = "high"
	RiskCritical RiskClass = "critical"
)

type IdempotencyMode string

const (
	IdempotencyGuaranteed  IdempotencyMode = "guaranteed"
	IdempotencyConditional IdempotencyMode = "conditional"
	IdempotencyNone        IdempotencyMode = "none"
)

type RetryMode string

const (
	RetryNever               RetryMode = "never"
	RetryOnDeclaredTransient RetryMode = "declared_transient_only"
)

type FieldSpec struct {
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Required  bool      `json:"required"`
	Sanitizer Sanitizer `json:"sanitizer"`
}

type ResourceSpec struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Namespace     string `json:"namespace"`
	Field         string `json:"field"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

type EffectSpec struct {
	Name string    `json:"name"`
	Risk RiskClass `json:"risk"`
}

type IdempotencySpec struct {
	Mode     IdempotencyMode `json:"mode"`
	KeyField string          `json:"key_field,omitempty"`
}

type RetrySpec struct {
	Mode            RetryMode `json:"mode"`
	MaximumAttempts uint32    `json:"maximum_attempts"`
}

type PredicateSpec struct {
	ID       string `json:"id"`
	Resource string `json:"resource,omitempty"`
	Field    string `json:"field,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
}

type CompensationSpec struct {
	ToolID      string `json:"tool_id"`
	PredicateID string `json:"predicate_id"`
}

type VerificationMethod struct {
	ID            string `json:"id"`
	EvidenceClass string `json:"evidence_class"`
}

type CompatibilityRange struct {
	MinimumInclusive string `json:"minimum_inclusive"`
	MaximumExclusive string `json:"maximum_exclusive"`
}

type Manifest struct {
	ID                  string               `json:"id,omitempty"`
	SchemaVersion       string               `json:"schema_version"`
	ToolID              string               `json:"tool_id"`
	Version             string               `json:"version"`
	Aliases             []string             `json:"aliases,omitempty"`
	Inputs              []FieldSpec          `json:"inputs,omitempty"`
	Outputs             []FieldSpec          `json:"outputs,omitempty"`
	Reads               []ResourceSpec       `json:"reads,omitempty"`
	Writes              []ResourceSpec       `json:"writes,omitempty"`
	Effects             []EffectSpec         `json:"effects,omitempty"`
	SideEffect          SideEffectClass      `json:"side_effect"`
	Risk                RiskClass            `json:"risk"`
	Idempotency         IdempotencySpec      `json:"idempotency"`
	Retry               RetrySpec            `json:"retry"`
	Preconditions       []PredicateSpec      `json:"preconditions,omitempty"`
	SuccessPredicates   []PredicateSpec      `json:"success_predicates,omitempty"`
	Compensation        *CompensationSpec    `json:"compensation,omitempty"`
	VerificationMethods []VerificationMethod `json:"verification_methods,omitempty"`
	Compatibility       []CompatibilityRange `json:"compatibility,omitempty"`
	ContentHash         string               `json:"-"`
}

type Query struct {
	Name    string
	Version string
}

type Resolution struct {
	Manifest       Manifest
	Opaque         bool
	AutoPromotable bool
}
