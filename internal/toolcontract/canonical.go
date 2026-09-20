package toolcontract

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"golang.org/x/mod/semver"
	"golang.org/x/text/unicode/norm"
)

var ErrInvalidManifest = errors.New("invalid tool contract manifest")

func Canonicalize(source Manifest) (Manifest, error) {
	manifest := source
	manifest.ID = ""
	manifest.ContentHash = ""
	manifest.SchemaVersion = token(source.SchemaVersion)
	manifest.ToolID = token(source.ToolID)
	manifest.Version = canonicalVersion(source.Version)
	if manifest.SchemaVersion == "" || manifest.ToolID == "" || manifest.Version == "" {
		return Manifest{}, invalid("schema_version, tool_id, and semantic version are required")
	}

	var err error
	if manifest.Aliases, err = canonicalStrings(source.Aliases, manifest.ToolID); err != nil {
		return Manifest{}, err
	}
	if manifest.Inputs, err = canonicalFields(source.Inputs); err != nil {
		return Manifest{}, fmt.Errorf("%w: inputs: %w", ErrInvalidManifest, err)
	}
	if manifest.Outputs, err = canonicalFields(source.Outputs); err != nil {
		return Manifest{}, fmt.Errorf("%w: outputs: %w", ErrInvalidManifest, err)
	}
	fields := make(map[string]struct{}, len(manifest.Inputs)+len(manifest.Outputs))
	for _, field := range append(append([]FieldSpec(nil), manifest.Inputs...), manifest.Outputs...) {
		fields[field.Name] = struct{}{}
	}
	if manifest.Reads, err = canonicalResources(source.Reads, fields); err != nil {
		return Manifest{}, fmt.Errorf("%w: reads: %w", ErrInvalidManifest, err)
	}
	if manifest.Writes, err = canonicalResources(source.Writes, fields); err != nil {
		return Manifest{}, fmt.Errorf("%w: writes: %w", ErrInvalidManifest, err)
	}
	resources := make(map[string]struct{}, len(manifest.Reads)+len(manifest.Writes))
	for _, resource := range append(append([]ResourceSpec(nil), manifest.Reads...), manifest.Writes...) {
		resources[resource.Name] = struct{}{}
	}
	if manifest.Preconditions, err = canonicalPredicates(source.Preconditions, fields, resources); err != nil {
		return Manifest{}, fmt.Errorf("%w: preconditions: %w", ErrInvalidManifest, err)
	}
	if manifest.SuccessPredicates, err = canonicalPredicates(source.SuccessPredicates, fields, resources); err != nil {
		return Manifest{}, fmt.Errorf("%w: success predicates: %w", ErrInvalidManifest, err)
	}
	if manifest.VerificationMethods, err = canonicalVerifiers(source.VerificationMethods); err != nil {
		return Manifest{}, err
	}
	if manifest.Compatibility, err = canonicalRanges(source.Compatibility); err != nil {
		return Manifest{}, err
	}
	if !validSideEffect(manifest.SideEffect) || !validRisk(manifest.Risk) || !validIdempotency(manifest.Idempotency, fields) || !validRetry(manifest.Retry) {
		return Manifest{}, invalid("invalid effect, risk, idempotency, or retry semantics")
	}
	if source.Compensation != nil {
		manifest.Compensation = &CompensationSpec{ToolID: token(source.Compensation.ToolID), PredicateID: token(source.Compensation.PredicateID)}
		if manifest.Compensation.ToolID == "" || manifest.Compensation.PredicateID == "" {
			return Manifest{}, invalid("compensation tool and predicate are required")
		}
	}

	_, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return Manifest{}, fmt.Errorf("canonicalize tool contract: %w", err)
	}
	manifest.ID = "tcv_" + hash
	manifest.ContentHash = hash
	return manifest, nil
}

func canonicalFields(source []FieldSpec) ([]FieldSpec, error) {
	result := make([]FieldSpec, len(source))
	seen := make(map[string]struct{}, len(source))
	for index, field := range source {
		result[index] = FieldSpec{Name: token(field.Name), Type: token(field.Type), Required: field.Required, Sanitizer: Sanitizer(token(string(field.Sanitizer)))}
		if result[index].Name == "" || result[index].Type == "" || !validSanitizer(result[index].Sanitizer) {
			return nil, errors.New("each accepted field requires name, type, and a supported sanitizer")
		}
		if _, duplicate := seen[result[index].Name]; duplicate {
			return nil, fmt.Errorf("duplicate field %q", result[index].Name)
		}
		seen[result[index].Name] = struct{}{}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func canonicalResources(source []ResourceSpec, fields map[string]struct{}) ([]ResourceSpec, error) {
	result := make([]ResourceSpec, len(source))
	seen := make(map[string]struct{}, len(source))
	for index, resource := range source {
		result[index] = ResourceSpec{Name: token(resource.Name), Type: token(resource.Type), Namespace: token(resource.Namespace), Field: token(resource.Field)}
		item := result[index]
		if item.Name == "" || item.Type == "" || item.Namespace == "" || item.Field == "" {
			return nil, errors.New("resource name, type, namespace, and identity field are required")
		}
		if _, exists := fields[item.Field]; !exists {
			return nil, fmt.Errorf("resource %q references unknown field %q", item.Name, item.Field)
		}
		key := item.Namespace + "\x00" + item.Name
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("duplicate resource %q", item.Name)
		}
		seen[key] = struct{}{}
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].Namespace+"\x00"+result[i].Name, result[j].Namespace+"\x00"+result[j].Name
		return left < right
	})
	return result, nil
}

func canonicalPredicates(source []PredicateSpec, fields, resources map[string]struct{}) ([]PredicateSpec, error) {
	result := make([]PredicateSpec, len(source))
	seen := make(map[string]struct{}, len(source))
	for index, predicate := range source {
		result[index] = PredicateSpec{ID: token(predicate.ID), Resource: token(predicate.Resource), Field: token(predicate.Field), Operator: token(predicate.Operator), Value: norm.NFC.String(predicate.Value)}
		item := result[index]
		if item.ID == "" || (item.Resource == "" && item.Field == "") {
			return nil, errors.New("predicate ID and a resource or field are required")
		}
		if item.Resource != "" {
			if _, exists := resources[item.Resource]; !exists {
				return nil, fmt.Errorf("predicate %q references unknown resource %q", item.ID, item.Resource)
			}
		}
		if item.Field != "" {
			if _, exists := fields[item.Field]; !exists {
				return nil, fmt.Errorf("predicate %q references unknown field %q", item.ID, item.Field)
			}
		}
		if _, duplicate := seen[item.ID]; duplicate {
			return nil, fmt.Errorf("duplicate predicate %q", item.ID)
		}
		seen[item.ID] = struct{}{}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func canonicalVerifiers(source []VerificationMethod) ([]VerificationMethod, error) {
	result := make([]VerificationMethod, len(source))
	seen := make(map[string]struct{}, len(source))
	validClasses := map[string]struct{}{"independent_verifier": {}, "goal_predicate": {}, "harness_assertion": {}, "tool_postcondition": {}, "exit_status_or_self_report": {}}
	for index, method := range source {
		result[index] = VerificationMethod{ID: token(method.ID), EvidenceClass: token(method.EvidenceClass)}
		if result[index].ID == "" {
			return nil, invalid("verification method ID is required")
		}
		if _, valid := validClasses[result[index].EvidenceClass]; !valid {
			return nil, invalid("verification method evidence class is invalid")
		}
		if _, duplicate := seen[result[index].ID]; duplicate {
			return nil, invalid("duplicate verification method")
		}
		seen[result[index].ID] = struct{}{}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func canonicalRanges(source []CompatibilityRange) ([]CompatibilityRange, error) {
	result := make([]CompatibilityRange, len(source))
	for index, item := range source {
		result[index] = CompatibilityRange{MinimumInclusive: canonicalVersion(item.MinimumInclusive), MaximumExclusive: canonicalVersion(item.MaximumExclusive)}
		if result[index].MinimumInclusive == "" || result[index].MaximumExclusive == "" || semver.Compare("v"+result[index].MinimumInclusive, "v"+result[index].MaximumExclusive) >= 0 {
			return nil, invalid("compatibility ranges require ordered semantic versions")
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].MinimumInclusive == result[j].MinimumInclusive {
			return semver.Compare("v"+result[i].MaximumExclusive, "v"+result[j].MaximumExclusive) < 0
		}
		return semver.Compare("v"+result[i].MinimumInclusive, "v"+result[j].MinimumInclusive) < 0
	})
	return result, nil
}

func canonicalStrings(source []string, forbidden string) ([]string, error) {
	result := make([]string, 0, len(source))
	seen := map[string]struct{}{forbidden: {}}
	for _, value := range source {
		value = token(value)
		if value == "" {
			return nil, invalid("aliases must not be empty")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, invalid("aliases must be unique and differ from tool ID")
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func canonicalVersion(value string) string {
	value = strings.TrimSpace(value)
	withPrefix := value
	if !strings.HasPrefix(withPrefix, "v") {
		withPrefix = "v" + withPrefix
	}
	if !semver.IsValid(withPrefix) {
		return ""
	}
	return strings.TrimPrefix(semver.Canonical(withPrefix), "v")
}

func token(value string) string { return norm.NFC.String(strings.TrimSpace(value)) }

func invalid(message string) error { return fmt.Errorf("%w: %s", ErrInvalidManifest, message) }

func validSanitizer(value Sanitizer) bool {
	return value == SanitizerToken || value == SanitizerText || value == SanitizerRelativePath || value == SanitizerRedactSecrets || value == SanitizerInteger || value == SanitizerDrop
}

func validSideEffect(value SideEffectClass) bool {
	return value == SideEffectNone || value == SideEffectRead || value == SideEffectWrite || value == SideEffectExternal || value == SideEffectIrreversible
}

func validRisk(value RiskClass) bool {
	return value == RiskLow || value == RiskMedium || value == RiskHigh || value == RiskCritical
}

func validIdempotency(value IdempotencySpec, fields map[string]struct{}) bool {
	if value.Mode != IdempotencyGuaranteed && value.Mode != IdempotencyConditional && value.Mode != IdempotencyNone {
		return false
	}
	if value.Mode == IdempotencyConditional {
		_, exists := fields[token(value.KeyField)]
		return exists
	}
	return token(value.KeyField) == ""
}

func validRetry(value RetrySpec) bool {
	return (value.Mode == RetryNever && value.MaximumAttempts == 1) || (value.Mode == RetryOnDeclaredTransient && value.MaximumAttempts > 0 && value.MaximumAttempts <= 20)
}
