package eligibility

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"golang.org/x/text/unicode/norm"
)

var ErrInvalidPolicy = errors.New("invalid eligibility policy")

type Lifecycle string

const (
	LifecycleCandidate   Lifecycle = "candidate"
	LifecycleTrial       Lifecycle = "trial"
	LifecycleActive      Lifecycle = "active"
	LifecycleStale       Lifecycle = "stale"
	LifecycleSuperseded  Lifecycle = "superseded"
	LifecycleQuarantined Lifecycle = "quarantined"
	LifecycleRetired     Lifecycle = "retired"
)

type Risk uint8

const (
	RiskLow Risk = iota + 1
	RiskMedium
	RiskHigh
	RiskCritical
)

type PolicySpec struct {
	Version                      string      `json:"version"`
	AllowedLifecycle             []Lifecycle `json:"allowed_lifecycle"`
	AllowAdvisoryCandidates      bool        `json:"allow_advisory_candidates"`
	MaximumRisk                  Risk        `json:"maximum_risk"`
	MaximumValidationAgeSeconds  int64       `json:"maximum_validation_age_seconds"`
	CompatibleValidationPolicies []string    `json:"compatible_validation_policies"`
	AllowedResidencyRegions      []string    `json:"allowed_residency_regions"`
}

type Policy struct {
	PolicySpec
	ID            string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NewPolicy(source PolicySpec) (Policy, error) {
	spec := source
	spec.Version = token(source.Version)
	var err error
	spec.AllowedLifecycle, err = normalizeLifecycles(source.AllowedLifecycle)
	if err != nil {
		return Policy{}, err
	}
	spec.CompatibleValidationPolicies, err = normalizeStrings("validation policy", source.CompatibleValidationPolicies)
	if err != nil {
		return Policy{}, err
	}
	spec.AllowedResidencyRegions, err = normalizeStrings("residency region", source.AllowedResidencyRegions)
	if err != nil {
		return Policy{}, err
	}
	if spec.Version == "" || !validRisk(spec.MaximumRisk) || spec.MaximumValidationAgeSeconds <= 0 || len(spec.AllowedLifecycle) == 0 || len(spec.CompatibleValidationPolicies) == 0 || len(spec.AllowedResidencyRegions) == 0 {
		return Policy{}, fmt.Errorf("%w: required field missing or invalid", ErrInvalidPolicy)
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(spec)
	if err != nil {
		return Policy{}, fmt.Errorf("canonicalize eligibility policy: %w", err)
	}
	return Policy{PolicySpec: spec, ID: "egp_" + hash, CanonicalJSON: canonicalJSON}, nil
}

func normalizeLifecycles(source []Lifecycle) ([]Lifecycle, error) {
	seen := make(map[Lifecycle]struct{}, len(source))
	for _, value := range source {
		if value != LifecycleCandidate && value != LifecycleTrial && value != LifecycleActive {
			return nil, fmt.Errorf("%w: lifecycle %q cannot be eligible", ErrInvalidPolicy, value)
		}
		seen[value] = struct{}{}
	}
	result := make([]Lifecycle, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func normalizeStrings(kind string, source []string) ([]string, error) {
	seen := make(map[string]struct{}, len(source))
	for _, raw := range source {
		value := token(raw)
		if value == "" {
			return nil, fmt.Errorf("%w: empty %s", ErrInvalidPolicy, kind)
		}
		seen[value] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func token(value string) string { return norm.NFC.String(strings.TrimSpace(value)) }

func validRisk(value Risk) bool { return value >= RiskLow && value <= RiskCritical }
