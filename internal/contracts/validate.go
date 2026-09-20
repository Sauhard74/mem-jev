package contracts

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/protobuf/proto"
)

const (
	maxIngestRequestBytes    = 1 << 20
	maxOutcomeRequestBytes   = 1 << 20
	maxRetrievalRequestBytes = 2 << 20
)

type ViolationError struct {
	Field string
	Rule  string
}

func ValidateRetrieval(request *memjevv1.RetrieveRequest) error {
	if request == nil {
		return &ViolationError{Field: "request", Rule: "required"}
	}
	if proto.Size(request) > maxRetrievalRequestBytes {
		return &ViolationError{Field: "request_size", Rule: "must not exceed 2097152 bytes"}
	}
	if err := protovalidate.Validate(request); err != nil {
		return fmt.Errorf("validate retrieval request: %w", err)
	}
	if canonicalToken(request.GetTask()) == "" {
		return &ViolationError{Field: "task", Rule: "must not be empty after normalization"}
	}
	if request.GetMaxCandidates() < 1 || request.GetMaxCandidates() > 100 {
		return &ViolationError{Field: "max_candidates", Rule: "must be between 1 and 100"}
	}
	seenTools := make(map[string]struct{}, len(request.GetTools()))
	for index, tool := range request.GetTools() {
		if tool == nil {
			return &ViolationError{Field: fmt.Sprintf("tools[%d]", index), Rule: "required"}
		}
		key := canonicalToken(tool.GetName()) + "\x00" + canonicalToken(tool.GetContractVersionId())
		if _, exists := seenTools[key]; exists {
			return &ViolationError{Field: fmt.Sprintf("tools[%d]", index), Rule: "duplicate canonical tool and contract version"}
		}
		seenTools[key] = struct{}{}
	}
	if err := validateUniqueFacts("environment", request.GetEnvironment()); err != nil {
		return err
	}
	if err := validateUniqueFacts("constraints", request.GetConstraints()); err != nil {
		return err
	}
	return nil
}

func validateUniqueFacts(field string, facts []*memjevv1.QueryFact) error {
	seen := make(map[string]struct{}, len(facts))
	for index, fact := range facts {
		if fact == nil {
			return &ViolationError{Field: fmt.Sprintf("%s[%d]", field, index), Rule: "required"}
		}
		name := canonicalToken(fact.GetName())
		if name == "" {
			return &ViolationError{Field: fmt.Sprintf("%s[%d].name", field, index), Rule: "must not be empty after normalization"}
		}
		if _, exists := seen[name]; exists {
			return &ViolationError{Field: fmt.Sprintf("%s[%d]", field, index), Rule: "duplicate canonical fact name"}
		}
		seen[name] = struct{}{}
	}
	return nil
}

func (e *ViolationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Rule)
}

func ValidateIngest(request *memjevv1.IngestTraceRequest) error {
	if request == nil {
		return &ViolationError{Field: "request", Rule: "required"}
	}
	if proto.Size(request) > maxIngestRequestBytes {
		return &ViolationError{Field: "request_size", Rule: "must not exceed 1048576 bytes"}
	}
	if err := protovalidate.Validate(request); err != nil {
		return fmt.Errorf("validate ingest request: %w", err)
	}
	for _, required := range []struct{ field, value string }{
		{field: "client_trace_id", value: request.GetClientTraceId()},
		{field: "harness", value: request.GetHarness()},
	} {
		if canonicalToken(required.value) == "" {
			return &ViolationError{Field: required.field, Rule: "must not be empty after normalization"}
		}
	}

	seen := make(map[string]struct{}, len(request.GetEvents()))
	for index, event := range request.GetEvents() {
		id := canonicalToken(event.GetClientEventId())
		if id == "" {
			return &ViolationError{Field: fmt.Sprintf("events[%d].client_event_id", index), Rule: "must not be empty after normalization"}
		}
		if canonicalToken(event.GetToolName()) == "" {
			return &ViolationError{Field: fmt.Sprintf("events[%d].tool_name", index), Rule: "must not be empty after normalization"}
		}
		if _, ok := seen[id]; ok {
			return &ViolationError{
				Field: fmt.Sprintf("events[%d].client_event_id", index),
				Rule:  "must be unique within the trace",
			}
		}
		seen[id] = struct{}{}
	}
	return nil
}

func ValidateOutcome(request *memjevv1.RecordOutcomeRequest) error {
	if request == nil {
		return &ViolationError{Field: "request", Rule: "required"}
	}
	if proto.Size(request) > maxOutcomeRequestBytes {
		return &ViolationError{Field: "request_size", Rule: "must not exceed 1048576 bytes"}
	}
	if err := protovalidate.Validate(request); err != nil {
		return fmt.Errorf("validate outcome request: %w", err)
	}
	if canonicalToken(request.GetTraceId()) == "" {
		return &ViolationError{Field: "trace_id", Rule: "must not be empty after normalization"}
	}
	if request.GetSupersedesOutcomeId() != "" && canonicalToken(request.GetCorrectionReason()) == "" {
		return &ViolationError{Field: "correction_reason", Rule: "required when superseding an outcome"}
	}
	if request.GetSupersedesOutcomeId() == "" && canonicalToken(request.GetCorrectionReason()) != "" {
		return &ViolationError{Field: "supersedes_outcome_id", Rule: "required when a correction reason is supplied"}
	}

	seen := make(map[string]struct{}, len(request.GetEvidence()))
	for index, evidence := range request.GetEvidence() {
		id := canonicalToken(evidence.GetClientEvidenceId())
		if id == "" {
			return &ViolationError{Field: fmt.Sprintf("evidence[%d].client_evidence_id", index), Rule: "must not be empty after normalization"}
		}
		if _, ok := seen[id]; ok {
			return &ViolationError{Field: fmt.Sprintf("evidence[%d].client_evidence_id", index), Rule: "must be unique within the outcome"}
		}
		seen[id] = struct{}{}
		if canonicalToken(evidence.GetPredicateId()) == "" {
			return &ViolationError{Field: fmt.Sprintf("evidence[%d].predicate_id", index), Rule: "must not be empty after normalization"}
		}
		if canonicalToken(evidence.GetVerifierId()) == "" {
			return &ViolationError{Field: fmt.Sprintf("evidence[%d].verifier_id", index), Rule: "must not be empty after normalization"}
		}
	}
	return nil
}

func canonicalToken(value string) string {
	return norm.NFC.String(strings.TrimSpace(value))
}
