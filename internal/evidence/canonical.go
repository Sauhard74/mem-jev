package evidence

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"github.com/sauhard74/mem-jev/internal/domain"
	"golang.org/x/text/unicode/norm"
)

var ErrInvalidCanonicalOutcome = fmt.Errorf("invalid canonical outcome")

const outcomeSchemaVersion = "outcome.v1"

type evidenceIdentity struct {
	TenantID domain.TenantID        `json:"tenant_id"`
	TraceID  domain.TraceID         `json:"trace_id"`
	Evidence domain.OutcomeEvidence `json:"evidence"`
}

type outcomeIdentity struct {
	TenantID            domain.TenantID          `json:"tenant_id"`
	TraceID             domain.TraceID           `json:"trace_id"`
	ExecutionID         string                   `json:"execution_id,omitempty"`
	SelectionID         string                   `json:"selection_id,omitempty"`
	SupersedesOutcomeID domain.OutcomeID         `json:"supersedes_outcome_id,omitempty"`
	CorrectionReason    string                   `json:"correction_reason,omitempty"`
	Evidence            []domain.OutcomeEvidence `json:"evidence"`
}

func Canonicalize(tenantID domain.TenantID, request *memjevv1.RecordOutcomeRequest) (domain.CanonicalOutcome, error) {
	if tenantID == "" {
		return domain.CanonicalOutcome{}, fmt.Errorf("tenant_id: required")
	}
	if err := contracts.ValidateOutcome(request); err != nil {
		return domain.CanonicalOutcome{}, err
	}

	traceID := domain.TraceID(normalizeToken(request.GetTraceId()))
	facts := make([]domain.OutcomeEvidence, len(request.GetEvidence()))
	for index, source := range request.GetEvidence() {
		if err := source.GetObservedAt().CheckValid(); err != nil {
			return domain.CanonicalOutcome{}, fmt.Errorf("evidence[%d].observed_at: %w", index, err)
		}
		fields, err := normalizeFields(index, source.GetFields())
		if err != nil {
			return domain.CanonicalOutcome{}, err
		}
		fact := domain.OutcomeEvidence{
			ClientEvidenceID: normalizeToken(source.GetClientEvidenceId()),
			Class:            evidenceClass(source.GetClass()),
			Verdict:          evidenceVerdict(source.GetVerdict()),
			PredicateID:      normalizeToken(source.GetPredicateId()),
			VerifierID:       normalizeToken(source.GetVerifierId()),
			VerifierVersion:  normalizeToken(source.GetVerifierVersion()),
			ObservedAt:       source.GetObservedAt().AsTime().UTC().Format(time.RFC3339Nano),
			Fields:           fields,
		}
		_, hash, err := canonical.MarshalAndHash(evidenceIdentity{TenantID: tenantID, TraceID: traceID, Evidence: fact})
		if err != nil {
			return domain.CanonicalOutcome{}, fmt.Errorf("derive evidence ID: %w", err)
		}
		fact.ID = domain.EvidenceID("oe_" + hash)
		facts[index] = fact
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].ID < facts[j].ID })

	identity := outcomeIdentity{
		TenantID:            tenantID,
		TraceID:             traceID,
		ExecutionID:         normalizeToken(request.GetExecutionId()),
		SelectionID:         normalizeToken(request.GetSelectionId()),
		SupersedesOutcomeID: domain.OutcomeID(normalizeToken(request.GetSupersedesOutcomeId())),
		CorrectionReason:    normalizeText(request.GetCorrectionReason()),
		Evidence:            facts,
	}
	_, identityHash, err := canonical.MarshalAndHash(identity)
	if err != nil {
		return domain.CanonicalOutcome{}, fmt.Errorf("derive outcome ID: %w", err)
	}
	outcome := domain.CanonicalOutcome{
		SchemaVersion:       outcomeSchemaVersion,
		ID:                  domain.OutcomeID("out_" + identityHash),
		TenantID:            tenantID,
		TraceID:             traceID,
		ExecutionID:         identity.ExecutionID,
		SelectionID:         identity.SelectionID,
		SupersedesOutcomeID: identity.SupersedesOutcomeID,
		CorrectionReason:    identity.CorrectionReason,
		Evidence:            facts,
	}
	encoded, hash, err := canonical.MarshalAndHash(outcome)
	if err != nil {
		return domain.CanonicalOutcome{}, err
	}
	outcome.CanonicalJSON = encoded
	outcome.Hash = hash
	return outcome, nil
}

// VerifyCanonical independently recomputes every content-derived identity and
// the canonical encoding before a storage adapter accepts an outcome.
func VerifyCanonical(outcome domain.CanonicalOutcome) error {
	if outcome.SchemaVersion != outcomeSchemaVersion || outcome.TenantID == "" || outcome.TraceID == "" || len(outcome.Evidence) == 0 {
		return ErrInvalidCanonicalOutcome
	}
	facts := make([]domain.OutcomeEvidence, len(outcome.Evidence))
	for index, source := range outcome.Evidence {
		fact := source
		fact.ID = ""
		_, hash, err := canonical.MarshalAndHash(evidenceIdentity{TenantID: outcome.TenantID, TraceID: outcome.TraceID, Evidence: fact})
		if err != nil || source.ID != domain.EvidenceID("oe_"+hash) {
			return ErrInvalidCanonicalOutcome
		}
		facts[index] = source
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].ID < facts[j].ID })
	for index := range facts {
		if facts[index].ID != outcome.Evidence[index].ID {
			return ErrInvalidCanonicalOutcome
		}
	}
	identity := outcomeIdentity{
		TenantID: outcome.TenantID, TraceID: outcome.TraceID, ExecutionID: outcome.ExecutionID,
		SelectionID: outcome.SelectionID, SupersedesOutcomeID: outcome.SupersedesOutcomeID,
		CorrectionReason: outcome.CorrectionReason, Evidence: facts,
	}
	_, identityHash, err := canonical.MarshalAndHash(identity)
	if err != nil || outcome.ID != domain.OutcomeID("out_"+identityHash) {
		return ErrInvalidCanonicalOutcome
	}
	copyOfOutcome := outcome
	copyOfOutcome.Hash = ""
	copyOfOutcome.CanonicalJSON = nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfOutcome)
	if err != nil || hash != outcome.Hash || !bytes.Equal(encoded, outcome.CanonicalJSON) {
		return ErrInvalidCanonicalOutcome
	}
	return nil
}

func normalizeFields(evidenceIndex int, source []*memjevv1.Field) ([]domain.CanonicalField, error) {
	if len(source) == 0 {
		return nil, nil
	}
	result := make([]domain.CanonicalField, 0, len(source))
	seen := make(map[string]struct{}, len(source))
	for index, field := range source {
		if field == nil {
			return nil, fmt.Errorf("evidence[%d].fields[%d]: required", evidenceIndex, index)
		}
		name := normalizeToken(field.GetName())
		if name == "" {
			return nil, fmt.Errorf("evidence[%d].fields[%d].name: must not be empty after normalization", evidenceIndex, index)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("evidence[%d].fields.%s: duplicate canonical field", evidenceIndex, name)
		}
		seen[name] = struct{}{}
		result = append(result, domain.CanonicalField{Name: name, Value: norm.NFC.String(field.GetStringValue())})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return result[i].Value < result[j].Value
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func evidenceClass(value memjevv1.EvidenceClass) domain.EvidenceClass {
	switch value {
	case memjevv1.EvidenceClass_EVIDENCE_CLASS_INDEPENDENT_VERIFIER:
		return domain.EvidenceClassIndependentVerifier
	case memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE:
		return domain.EvidenceClassGoalPredicate
	case memjevv1.EvidenceClass_EVIDENCE_CLASS_HARNESS_ASSERTION:
		return domain.EvidenceClassHarnessAssertion
	case memjevv1.EvidenceClass_EVIDENCE_CLASS_TOOL_POSTCONDITION:
		return domain.EvidenceClassToolPostcondition
	case memjevv1.EvidenceClass_EVIDENCE_CLASS_EXIT_STATUS_OR_SELF_REPORT:
		return domain.EvidenceClassExitStatusOrSelfReport
	default:
		return ""
	}
}

func evidenceVerdict(value memjevv1.EvidenceVerdict) domain.EvidenceVerdict {
	switch value {
	case memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED:
		return domain.EvidenceVerdictSatisfied
	case memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED:
		return domain.EvidenceVerdictFailed
	case memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_UNKNOWN:
		return domain.EvidenceVerdictUnknown
	default:
		return ""
	}
}

func normalizeToken(value string) string { return norm.NFC.String(strings.TrimSpace(value)) }

func normalizeText(value string) string {
	return norm.NFC.String(strings.TrimSpace(strings.ReplaceAll(value, "\r\n", "\n")))
}
