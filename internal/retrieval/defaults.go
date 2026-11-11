package retrieval

import (
	"errors"
	"strings"

	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/ranking"
)

// Versions of the built-in serving manifests. Ranker versions carrying this
// prefix are treated as memJev-managed defaults and may be replaced by a newer
// default (for example when Jev is enabled later). Operator-published manifests
// never use this prefix and are never replaced automatically.
const (
	DefaultManifestPrefix     = "memjev-default"
	defaultPolicyVersion      = DefaultManifestPrefix + ".v1"
	defaultRankerVersion      = DefaultManifestPrefix + ".v1"
	defaultJevRankerVersion   = DefaultManifestPrefix + "-jev.v1"
	defaultValidationPolicy   = "outcome-policy.v1"
	defaultMaximumValidityAge = 90 * 24 * 60 * 60
)

// DefaultServingInput describes the deployment-specific parts of the default
// serving configuration.
type DefaultServingInput struct {
	// AllowedResidencyRegions limits which stored procedures may be recalled.
	AllowedResidencyRegions []string
	// Semantic adds the five Jev ranking features so Jev judgments can affect ranking.
	Semantic bool
}

// DefaultServing is a ready-to-activate policy and ranker pair.
type DefaultServing struct {
	Policy eligibility.Policy
	Ranker ranking.Manifest
}

// BuildDefaultServing returns the policy and ranker memJev activates when an
// operator has not published their own. New routines are learned as
// "candidate" procedures, so the policy admits them as advisory guidance.
func BuildDefaultServing(input DefaultServingInput) (DefaultServing, error) {
	regions := make([]string, 0, len(input.AllowedResidencyRegions))
	for _, region := range input.AllowedResidencyRegions {
		if region = strings.TrimSpace(region); region != "" {
			regions = append(regions, region)
		}
	}
	if len(regions) == 0 {
		return DefaultServing{}, errors.New("default serving requires at least one residency region")
	}
	policy, err := eligibility.NewPolicy(eligibility.PolicySpec{
		Version:                      defaultPolicyVersion,
		AllowedLifecycle:             []eligibility.Lifecycle{eligibility.LifecycleActive, eligibility.LifecycleTrial},
		AllowAdvisoryCandidates:      true,
		MaximumRisk:                  eligibility.RiskMedium,
		MaximumValidationAgeSeconds:  defaultMaximumValidityAge,
		CompatibleValidationPolicies: []string{defaultValidationPolicy},
		AllowedResidencyRegions:      regions,
	})
	if err != nil {
		return DefaultServing{}, err
	}
	spec := ranking.ManifestSpec{
		Version: defaultRankerVersion, RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100,
		Channels: []ranking.ChannelWeight{
			{Channel: string(ChannelExact), WeightMicros: 1_000_000},
			{Channel: string(ChannelLexical), WeightMicros: 700_000},
			{Channel: string(ChannelFacet), WeightMicros: 500_000},
			{Channel: string(ChannelGraph), WeightMicros: 300_000},
			{Channel: string(ChannelVector), WeightMicros: 600_000},
		},
		Features: []ranking.FeatureSpec{{Name: "verification_strength", Minimum: 0, Maximum: 10, Missing: 0, Coefficient: 1000}},
	}
	if input.Semantic {
		// Conservative starting coefficients from docs/runbooks/jev.md. Missing
		// values are zero, so a Jev outage ranks exactly like the no-Jev ranker.
		spec.Version = defaultJevRankerVersion
		spec.Features = append(spec.Features,
			ranking.FeatureSpec{Name: "jev_intent_fit_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 100_000},
			ranking.FeatureSpec{Name: "jev_non_contradiction_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 100_000},
			ranking.FeatureSpec{Name: "jev_partial_plan_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 25_000},
			ranking.FeatureSpec{Name: "jev_preconditions_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 75_000},
			ranking.FeatureSpec{Name: "jev_task_coverage_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 100_000},
		)
	}
	ranker, err := ranking.NewManifest(spec)
	if err != nil {
		return DefaultServing{}, err
	}
	return DefaultServing{Policy: policy, Ranker: ranker}, nil
}

// IsDefaultManifestVersion reports whether a manifest version is memJev-managed.
func IsDefaultManifestVersion(version string) bool {
	return strings.HasPrefix(version, DefaultManifestPrefix)
}
