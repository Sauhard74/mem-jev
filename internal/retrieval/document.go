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

const (
	documentSchemaVersion       = "retrieval-document.v2"
	legacyDocumentSchemaVersion = "retrieval-document.v1"
)

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
	Interface                *domain.ProcedureInterface
}

type Document struct {
	SchemaVersion            string                     `json:"schema_version"`
	TenantID                 domain.TenantID            `json:"tenant_id"`
	ProcedureVersionID       string                     `json:"procedure_version_id"`
	ProcedureID              string                     `json:"procedure_id"`
	TaskText                 string                     `json:"task_text"`
	IntentHash               string                     `json:"intent_hash"`
	EffectSignatureHash      string                     `json:"effect_signature_hash"`
	Tools                    []ToolRequirement          `json:"tools"`
	Resources                []ResourceRequirement      `json:"resources,omitempty"`
	Effects                  []string                   `json:"effects,omitempty"`
	EnvironmentScopeHash     string                     `json:"environment_scope_hash"`
	Harness                  Harness                    `json:"harness"`
	Environment              []Fact                     `json:"environment,omitempty"`
	Lifecycle                string                     `json:"lifecycle"`
	ObservedEndToEnd         bool                       `json:"observed_end_to_end"`
	VerificationStrength     uint8                      `json:"verification_strength"`
	VerifiedSuccessCount     uint64                     `json:"verified_success_count"`
	UnsafeOutcomeCount       uint64                     `json:"unsafe_outcome_count"`
	ValidatedAt              string                     `json:"validated_at"`
	ValidationPolicyVersion  string                     `json:"validation_policy_version"`
	LearnedWithRecallConsent bool                       `json:"learned_with_recall_consent"`
	ResidencyRegion          string                     `json:"residency_region"`
	RiskClass                string                     `json:"risk_class"`
	Interface                *domain.ProcedureInterface `json:"interface,omitempty"`
	PrefixHashes             []string                   `json:"prefix_hashes"`
	ID                       string                     `json:"-"`
	ContentHash              string                     `json:"-"`
	CanonicalJSON            []byte                     `json:"-"`
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
		!validInitialLifecycle(document.Lifecycle) || input.Interface == nil || ValidateProcedureInterface(*input.Interface) != nil {
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
	procedureInterface := *input.Interface
	procedureInterface.Requirements = append([]domain.ProcedureRequirement(nil), input.Interface.Requirements...)
	procedureInterface.Provisions = append([]domain.ProcedureProvision(nil), input.Interface.Provisions...)
	for index := range procedureInterface.Requirements {
		procedureInterface.Requirements[index].PredicateIDs = append([]string(nil), input.Interface.Requirements[index].PredicateIDs...)
	}
	for index := range procedureInterface.Provisions {
		procedureInterface.Provisions[index].ProducedEffects = append([]string(nil), input.Interface.Provisions[index].ProducedEffects...)
		procedureInterface.Provisions[index].SuccessPredicateIDs = append([]string(nil), input.Interface.Provisions[index].SuccessPredicateIDs...)
	}
	document.Interface = &procedureInterface
	canonicalJSON, hash, err := canonical.MarshalAndHash(document)
	if err != nil {
		return Document{}, fmt.Errorf("canonicalize retrieval document: %w", err)
	}
	document.ID, document.ContentHash, document.CanonicalJSON = "rdoc_"+hash, hash, canonicalJSON
	return document, nil
}

func ValidateDocument(document Document) error {
	if document.SchemaVersion != documentSchemaVersion && document.SchemaVersion != legacyDocumentSchemaVersion {
		return ErrInvalidDocument
	}
	if document.SchemaVersion == documentSchemaVersion && (document.Interface == nil || ValidateProcedureInterface(*document.Interface) != nil) {
		return ErrInvalidDocument
	}
	if document.SchemaVersion == legacyDocumentSchemaVersion && document.Interface != nil {
		return ErrInvalidDocument
	}
	copyOfDocument := document
	copyOfDocument.ID, copyOfDocument.ContentHash, copyOfDocument.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(copyOfDocument)
	if err != nil || document.ID != "rdoc_"+hash || document.ContentHash != hash || !bytes.Equal(document.CanonicalJSON, canonicalJSON) {
		return ErrInvalidDocument
	}
	return nil
}

func ReviseEvidence(document Document, verifiedSuccessCount, unsafeOutcomeCount uint64, validatedAt time.Time) (Document, error) {
	if err := ValidateDocument(document); err != nil || document.SchemaVersion != documentSchemaVersion || verifiedSuccessCount == 0 || validatedAt.IsZero() {
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

func ReviseLifecycle(document Document, lifecycle string, verifiedSuccessCount, unsafeOutcomeCount uint64, validatedAt time.Time, validationPolicyVersion string) (Document, error) {
	if err := ValidateDocument(document); err != nil || document.SchemaVersion != documentSchemaVersion || validatedAt.IsZero() || !validLifecycle(token(lifecycle)) || token(validationPolicyVersion) == "" {
		return Document{}, ErrInvalidDocument
	}
	document.Lifecycle = token(lifecycle)
	document.VerifiedSuccessCount = verifiedSuccessCount
	document.UnsafeOutcomeCount = unsafeOutcomeCount
	document.ValidatedAt = validatedAt.UTC().Format(time.RFC3339Nano)
	document.ValidationPolicyVersion = token(validationPolicyVersion)
	document.ID, document.ContentHash, document.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(document)
	if err != nil {
		return Document{}, err
	}
	document.ID, document.ContentHash, document.CanonicalJSON = "rdoc_"+hash, hash, canonicalJSON
	return document, nil
}

func ValidateProcedureInterface(value domain.ProcedureInterface) error {
	if value.SchemaVersion != "procedure-interface.v1" || value.ID == "" || !sha256Pattern.MatchString(value.ContentHash) || value.ID != "pif_"+value.ContentHash {
		return ErrInvalidDocument
	}
	requirementKeys := make(map[string]struct{}, len(value.Requirements))
	for index, requirement := range value.Requirements {
		identity := requirement
		identity.ID = ""
		_, hash, err := canonical.MarshalAndHash(identity)
		key := strings.Join([]string{requirement.ResourceType, requirement.Namespace, requirement.IdentityHash, requirement.SchemaVersion, requirement.AccessMode}, "\x00")
		_, duplicate := requirementKeys[key]
		if err != nil || duplicate || requirement.ID != "req_"+hash || requirement.ResourceType == "" || requirement.Namespace == "" || !sha256Pattern.MatchString(requirement.IdentityHash) || requirement.AccessMode != "read" || !strictDocumentStrings(requirement.PredicateIDs) || index > 0 && value.Requirements[index-1].ID >= requirement.ID {
			return ErrInvalidDocument
		}
		requirementKeys[key] = struct{}{}
	}
	provisionKeys := make(map[string]struct{}, len(value.Provisions))
	for index, provision := range value.Provisions {
		identity := provision
		identity.ID = ""
		_, hash, err := canonical.MarshalAndHash(identity)
		key := strings.Join([]string{provision.ResourceType, provision.Namespace, provision.IdentityHash, provision.SchemaVersion}, "\x00")
		_, duplicate := provisionKeys[key]
		if err != nil || duplicate || provision.ID != "prov_"+hash || provision.ResourceType == "" || provision.Namespace == "" || !sha256Pattern.MatchString(provision.IdentityHash) || !strictDocumentStrings(provision.ProducedEffects) || !strictDocumentStrings(provision.SuccessPredicateIDs) || index > 0 && value.Provisions[index-1].ID >= provision.ID {
			return ErrInvalidDocument
		}
		provisionKeys[key] = struct{}{}
	}
	identity := value
	identity.ID, identity.ContentHash = "", ""
	_, hash, err := canonical.MarshalAndHash(identity)
	if err != nil || hash != value.ContentHash {
		return ErrInvalidDocument
	}
	return nil
}

func NewProcedureInterface(requirements []domain.ProcedureRequirement, provisions []domain.ProcedureProvision) (domain.ProcedureInterface, error) {
	result := domain.ProcedureInterface{SchemaVersion: "procedure-interface.v1"}
	seenRequirements := make(map[string]struct{}, len(requirements))
	for _, source := range requirements {
		value := source
		value.ID = ""
		value.ResourceType, value.Namespace, value.IdentityHash, value.SchemaVersion, value.AccessMode = token(value.ResourceType), token(value.Namespace), token(value.IdentityHash), token(value.SchemaVersion), token(value.AccessMode)
		var err error
		value.PredicateIDs, err = normalizeSet("requirement predicates", value.PredicateIDs)
		key := strings.Join([]string{value.ResourceType, value.Namespace, value.IdentityHash, value.SchemaVersion, value.AccessMode}, "\x00")
		if err != nil || value.ResourceType == "" || value.Namespace == "" || !sha256Pattern.MatchString(value.IdentityHash) || value.AccessMode != "read" {
			return domain.ProcedureInterface{}, ErrInvalidDocument
		}
		if _, duplicate := seenRequirements[key]; duplicate {
			return domain.ProcedureInterface{}, ErrInvalidDocument
		}
		seenRequirements[key] = struct{}{}
		_, hash, hashErr := canonical.MarshalAndHash(value)
		if hashErr != nil {
			return domain.ProcedureInterface{}, hashErr
		}
		value.ID = "req_" + hash
		result.Requirements = append(result.Requirements, value)
	}
	seenProvisions := make(map[string]struct{}, len(provisions))
	for _, source := range provisions {
		value := source
		value.ID = ""
		value.ResourceType, value.Namespace, value.IdentityHash, value.SchemaVersion = token(value.ResourceType), token(value.Namespace), token(value.IdentityHash), token(value.SchemaVersion)
		var err error
		value.ProducedEffects, err = normalizeSet("provision effects", value.ProducedEffects)
		if err == nil {
			value.SuccessPredicateIDs, err = normalizeSet("provision predicates", value.SuccessPredicateIDs)
		}
		key := strings.Join([]string{value.ResourceType, value.Namespace, value.IdentityHash, value.SchemaVersion}, "\x00")
		if err != nil || value.ResourceType == "" || value.Namespace == "" || !sha256Pattern.MatchString(value.IdentityHash) {
			return domain.ProcedureInterface{}, ErrInvalidDocument
		}
		if _, duplicate := seenProvisions[key]; duplicate {
			return domain.ProcedureInterface{}, ErrInvalidDocument
		}
		seenProvisions[key] = struct{}{}
		_, hash, hashErr := canonical.MarshalAndHash(value)
		if hashErr != nil {
			return domain.ProcedureInterface{}, hashErr
		}
		value.ID = "prov_" + hash
		result.Provisions = append(result.Provisions, value)
	}
	sort.Slice(result.Requirements, func(i, j int) bool { return result.Requirements[i].ID < result.Requirements[j].ID })
	sort.Slice(result.Provisions, func(i, j int) bool { return result.Provisions[i].ID < result.Provisions[j].ID })
	identity := result
	_, hash, err := canonical.MarshalAndHash(identity)
	if err != nil {
		return domain.ProcedureInterface{}, err
	}
	result.ID, result.ContentHash = "pif_"+hash, hash
	return result, nil
}

func strictDocumentStrings(values []string) bool {
	for index, value := range values {
		if token(value) == "" || token(value) != value || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
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
	return value == "candidate" || value == "trial" || value == "active" || value == "retired" || value == "quarantined"
}

func validInitialLifecycle(value string) bool {
	return value == "candidate" || value == "trial" || value == "active"
}
