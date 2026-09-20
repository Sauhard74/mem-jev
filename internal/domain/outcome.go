package domain

type EvidenceClass string

const (
	EvidenceClassIndependentVerifier    EvidenceClass = "independent_verifier"
	EvidenceClassGoalPredicate          EvidenceClass = "goal_predicate"
	EvidenceClassHarnessAssertion       EvidenceClass = "harness_assertion"
	EvidenceClassToolPostcondition      EvidenceClass = "tool_postcondition"
	EvidenceClassExitStatusOrSelfReport EvidenceClass = "exit_status_or_self_report"
)

type EvidenceVerdict string

const (
	EvidenceVerdictSatisfied EvidenceVerdict = "satisfied"
	EvidenceVerdictFailed    EvidenceVerdict = "failed"
	EvidenceVerdictUnknown   EvidenceVerdict = "unknown"
)

type OutcomeState string

const (
	OutcomeStateVerifiedSuccess    OutcomeState = "verified_success"
	OutcomeStateProvisionalSuccess OutcomeState = "provisional_success"
	OutcomeStateInconclusive       OutcomeState = "inconclusive"
	OutcomeStateVerifiedFailure    OutcomeState = "verified_failure"
)

type OutcomeEvidence struct {
	ID               EvidenceID       `json:"id"`
	ClientEvidenceID string           `json:"client_evidence_id"`
	Class            EvidenceClass    `json:"class"`
	Verdict          EvidenceVerdict  `json:"verdict"`
	PredicateID      string           `json:"predicate_id"`
	VerifierID       string           `json:"verifier_id"`
	VerifierVersion  string           `json:"verifier_version,omitempty"`
	ObservedAt       string           `json:"observed_at"`
	Fields           []CanonicalField `json:"fields,omitempty"`
}

type CanonicalOutcome struct {
	SchemaVersion       string            `json:"schema_version"`
	ID                  OutcomeID         `json:"id"`
	TenantID            TenantID          `json:"tenant_id"`
	TraceID             TraceID           `json:"trace_id"`
	ExecutionID         string            `json:"execution_id,omitempty"`
	SelectionID         string            `json:"selection_id,omitempty"`
	InjectionID         string            `json:"injection_id,omitempty"`
	TaskExecutionID     string            `json:"task_execution_id,omitempty"`
	SupersedesOutcomeID OutcomeID         `json:"supersedes_outcome_id,omitempty"`
	CorrectionReason    string            `json:"correction_reason,omitempty"`
	Evidence            []OutcomeEvidence `json:"evidence"`
	Hash                string            `json:"-"`
	CanonicalJSON       []byte            `json:"-"`
}
