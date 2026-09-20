package retrieval

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

const documentSchemaVersion = "retrieval-document.v1"

var ErrInvalidDocument = errors.New("invalid retrieval document")
var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type ToolRequirement struct {
	Name              string `json:"name"`
	ContractVersionID string `json:"contract_version_id"`
}

type ResourceRequirement struct {
	Type          string `json:"type"`
	Namespace     string `json:"namespace,omitempty"`
	IdentityHash  string `json:"identity_hash"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

type DocumentInput struct {
	TenantID                 domain.TenantID
	ProcedureVersionID       string
	ProcedureID              string
	TaskText                 string
	IntentHash               string
	EffectSignatureHash      string
	Tools                    []ToolRequirement
	OrderedStepContractIDs   []string
	Resources                []ResourceRequirement
	Effects                  []string
	EnvironmentScopeHash     string
	Harness                  Harness
	Environment              []Fact
	Lifecycle                string
	ObservedEndToEnd         bool
	VerificationStrength     uint8
	VerifiedSuccessCount     uint64
	UnsafeOutcomeCount       uint64
	ValidatedAt              time.Time
	ValidationPolicyVersion  string
	LearnedWithRecallConsent bool
	ResidencyRegion          string
	RiskClass                string
}

type Document struct {
	SchemaVersion            string                `json:"schema_version"`
	TenantID                 domain.TenantID       `json:"tenant_id"`
	ProcedureVersionID       string                `json:"procedure_version_id"`
	ProcedureID              string                `json:"procedure_id"`
	TaskText                 string                `json:"task_text"`
	IntentHash               string                `json:"intent_hash"`
	EffectSignatureHash      string                `json:"effect_signature_hash"`
	Tools                    []ToolRequirement     `json:"tools"`
	Resources                []ResourceRequirement `json:"resources,omitempty"`
	Effects                  []string              `json:"effects,omitempty"`
	EnvironmentScopeHash     string                `json:"environment_scope_hash"`
	Harness                  Harness               `json:"harness"`
	Environment              []Fact                `json:"environment,omitempty"`
	Lifecycle                string                `json:"lifecycle"`
	ObservedEndToEnd         bool                  `json:"observed_end_to_end"`
	VerificationStrength     uint8                 `json:"verification_strength"`
	VerifiedSuccessCount     uint64                `json:"verified_success_count"`
	UnsafeOutcomeCount       uint64                `json:"unsafe_outcome_count"`
	ValidatedAt              string                `json:"validated_at"`
	ValidationPolicyVersion  string                `json:"validation_policy_version"`
	LearnedWithRecallConsent bool                  `json:"learned_with_recall_consent"`
	ResidencyRegion          string                `json:"residency_region"`
	RiskClass                string                `json:"risk_class"`
	PrefixHashes             []string              `json:"prefix_hashes"`
	ID                       string                `json:"-"`
	ContentHash              string                `json:"-"`
	CanonicalJSON            []byte                `json:"-"`
}

func BuildDocument(input DocumentInput) (Document, error) {
	document := Document{
		SchemaVersion: documentSchemaVersion, TenantID: input.TenantID,
		ProcedureVersionID: token(input.ProcedureVersionID), ProcedureID: token(input.ProcedureID),
		TaskText: text(input.TaskText), IntentHash: token(input.IntentHash), EffectSignatureHash: token(input.EffectSignatureHash),
		EnvironmentScopeHash: token(input.EnvironmentScopeHash),
		Harness:              Harness{Name: token(input.Harness.Name), Version: token(input.Harness.Version)},
		Lifecycle:            token(input.Lifecycle), ObservedEndToEnd: input.ObservedEndToEnd,
		VerificationStrength: input.VerificationStrength, VerifiedSuccessCount: input.VerifiedSuccessCount,
		UnsafeOutcomeCount: input.UnsafeOutcomeCount, ValidationPolicyVersion: token(input.ValidationPolicyVersion),
		LearnedWithRecallConsent: input.LearnedWithRecallConsent, ResidencyRegion: token(input.ResidencyRegion), RiskClass: token(input.RiskClass),
	}
	if input.ValidatedAt.IsZero() {
		return Document{}, fmt.Errorf("%w: validation time required", ErrInvalidDocument)
	}
	document.ValidatedAt = input.ValidatedAt.UTC().Format(time.RFC3339Nano)
	if document.TenantID == "" || document.ProcedureVersionID == "" || document.ProcedureID == "" || document.TaskText == "" ||
		!sha256Pattern.MatchString(document.IntentHash) || !sha256Pattern.MatchString(document.EffectSignatureHash) ||
		!sha256Pattern.MatchString(document.EnvironmentScopeHash) || document.Harness.Name == "" ||
		document.VerificationStrength == 0 || document.VerifiedSuccessCount == 0 || document.ValidationPolicyVersion == "" ||
		!document.LearnedWithRecallConsent || document.ResidencyRegion == "" || !validDocumentRisk(document.RiskClass) ||
		!validLifecycle(document.Lifecycle) {
		return Document{}, fmt.Errorf("%w: required field missing or invalid", ErrInvalidDocument)
	}
	var err error
	document.Tools, err = normalizeToolRequirements(input.Tools)
	if err != nil {
		return Document{}, err
	}
	document.Resources, err = normalizeResourceRequirements(input.Resources)
	if err != nil {
		return Document{}, err
	}
	document.Effects, err = normalizeSet("effects", input.Effects)
	if err != nil {
		return Document{}, err
	}
	document.Environment, err = normalizeFacts("document environment", input.Environment)
	if err != nil {
		return Document{}, err
	}
	document.PrefixHashes, err = prefixHashes(input.OrderedStepContractIDs)
	if err != nil {
		return Document{}, err
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(document)
	if err != nil {
		return Document{}, fmt.Errorf("canonicalize retrieval document: %w", err)
	}
	document.ID, document.ContentHash, document.CanonicalJSON = "rdoc_"+hash, hash, canonicalJSON
	return document, nil
}

func ValidateDocument(document Document) error {
	copyOfDocument := document
	copyOfDocument.ID, copyOfDocument.ContentHash, copyOfDocument.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(copyOfDocument)
	if err != nil || document.ID != "rdoc_"+hash || document.ContentHash != hash || !bytes.Equal(document.CanonicalJSON, canonicalJSON) {
		return ErrInvalidDocument
	}
	return nil
}

func ReviseEvidence(document Document, verifiedSuccessCount, unsafeOutcomeCount uint64, validatedAt time.Time) (Document, error) {
	if err := ValidateDocument(document); err != nil || verifiedSuccessCount == 0 || validatedAt.IsZero() {
		return Document{}, ErrInvalidDocument
	}
	document.VerifiedSuccessCount = verifiedSuccessCount
	document.UnsafeOutcomeCount = unsafeOutcomeCount
	document.ValidatedAt = validatedAt.UTC().Format(time.RFC3339Nano)
	document.ID, document.ContentHash, document.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(document)
	if err != nil {
		return Document{}, err
	}
	document.ID, document.ContentHash, document.CanonicalJSON = "rdoc_"+hash, hash, canonicalJSON
	return document, nil
}

func normalizeToolRequirements(source []ToolRequirement) ([]ToolRequirement, error) {
	if len(source) == 0 {
		return nil, fmt.Errorf("%w: tools required", ErrInvalidDocument)
	}
	byName := make(map[string]string, len(source))
	for _, item := range source {
		name, version := token(item.Name), token(item.ContractVersionID)
		if name == "" || version == "" {
			return nil, fmt.Errorf("%w: tool identity required", ErrInvalidDocument)
		}
		if prior, exists := byName[name]; exists && prior != version {
			return nil, fmt.Errorf("%w: conflicting tool contract", ErrInvalidDocument)
		}
		byName[name] = version
	}
	result := make([]ToolRequirement, 0, len(byName))
	for name, version := range byName {
		result = append(result, ToolRequirement{Name: name, ContractVersionID: version})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func normalizeResourceRequirements(source []ResourceRequirement) ([]ResourceRequirement, error) {
	seen := make(map[string]ResourceRequirement, len(source))
	for _, item := range source {
		item.Type, item.Namespace, item.IdentityHash, item.SchemaVersion = token(item.Type), token(item.Namespace), token(item.IdentityHash), token(item.SchemaVersion)
		if item.Type == "" || !sha256Pattern.MatchString(item.IdentityHash) {
			return nil, fmt.Errorf("%w: invalid resource requirement", ErrInvalidDocument)
		}
		key := strings.Join([]string{item.Type, item.Namespace, item.IdentityHash}, "\x00")
		if prior, exists := seen[key]; exists && prior.SchemaVersion != item.SchemaVersion {
			return nil, fmt.Errorf("%w: conflicting resource schema", ErrInvalidDocument)
		}
		seen[key] = item
	}
	result := make([]ResourceRequirement, 0, len(seen))
	for _, item := range seen {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		left := strings.Join([]string{result[i].Type, result[i].Namespace, result[i].IdentityHash}, "\x00")
		right := strings.Join([]string{result[j].Type, result[j].Namespace, result[j].IdentityHash}, "\x00")
		return left < right
	})
	return result, nil
}

func prefixHashes(source []string) ([]string, error) {
	if len(source) == 0 {
		return nil, fmt.Errorf("%w: ordered steps required", ErrInvalidDocument)
	}
	limit := min(len(source), 8)
	result := make([]string, limit)
	canonicalIDs := make([]string, limit)
	for index := range limit {
		canonicalIDs[index] = token(source[index])
		if canonicalIDs[index] == "" {
			return nil, fmt.Errorf("%w: step contract required", ErrInvalidDocument)
		}
		_, hash, err := canonical.MarshalAndHash(canonicalIDs[:index+1])
		if err != nil {
			return nil, err
		}
		result[index] = hash
	}
	return result, nil
}

func validDocumentRisk(value string) bool {
	return value == "low" || value == "medium" || value == "high" || value == "critical"
}
func validLifecycle(value string) bool {
	return value == "candidate" || value == "trial" || value == "active"
}
