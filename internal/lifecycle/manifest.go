package lifecycle

import (
	"bytes"
	"errors"
	"regexp"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

const PartsPerMillion uint32 = 1_000_000

var (
	ErrInvalidLifecycle = errors.New("invalid lifecycle value")
	hashPattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
	tokenPattern        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)
)

type State string

const (
	Candidate   State = "candidate"
	Trial       State = "trial"
	Active      State = "active"
	Retired     State = "retired"
	Quarantined State = "quarantined"
)

type PolicySpec struct {
	Version                          string `json:"version"`
	WilsonZSquaredPPM                uint32 `json:"wilson_z_squared_ppm"`
	TrialMinimumCausalSamples        uint64 `json:"trial_minimum_causal_samples"`
	TrialMinimumWilsonLowerBoundPPM  uint32 `json:"trial_minimum_wilson_lower_bound_ppm"`
	ActiveMinimumCausalSamples       uint64 `json:"active_minimum_causal_samples"`
	ActiveMinimumWilsonLowerBoundPPM uint32 `json:"active_minimum_wilson_lower_bound_ppm"`
	TrialUnsafeOutcomeCeiling        uint64 `json:"trial_unsafe_outcome_ceiling"`
	ActiveUnsafeOutcomeCeiling       uint64 `json:"active_unsafe_outcome_ceiling"`
	ImmediateQuarantineUnsafeCount   uint64 `json:"immediate_quarantine_unsafe_count"`
	EvidenceFreshnessSeconds         uint64 `json:"evidence_freshness_seconds"`
	MinimumTrialSeconds              uint64 `json:"minimum_trial_seconds"`
}

type PolicyManifest struct {
	SchemaVersion string `json:"schema_version"`
	PolicySpec
	ID            string `json:"-"`
	ContentHash   string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NewPolicyManifest(spec PolicySpec) (PolicyManifest, error) {
	manifest := PolicyManifest{SchemaVersion: "lifecycle-policy-manifest.v1", PolicySpec: spec}
	if !validPolicy(manifest) {
		return PolicyManifest{}, ErrInvalidLifecycle
	}
	encoded, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return PolicyManifest{}, err
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = "lcman_"+hash, hash, encoded
	return manifest, nil
}

func ValidatePolicyManifest(manifest PolicyManifest) error {
	rebuilt, err := NewPolicyManifest(manifest.PolicySpec)
	if err != nil || manifest.SchemaVersion != rebuilt.SchemaVersion || manifest.ID != rebuilt.ID || manifest.ContentHash != rebuilt.ContentHash || !bytes.Equal(manifest.CanonicalJSON, rebuilt.CanonicalJSON) {
		return ErrInvalidLifecycle
	}
	return nil
}

func validPolicy(manifest PolicyManifest) bool {
	const maximumPolicyWindowSeconds = 10 * 366 * 24 * 60 * 60
	return tokenPattern.MatchString(manifest.Version) &&
		manifest.WilsonZSquaredPPM > 0 && manifest.WilsonZSquaredPPM <= 25*PartsPerMillion &&
		manifest.TrialMinimumCausalSamples > 0 && manifest.ActiveMinimumCausalSamples >= manifest.TrialMinimumCausalSamples &&
		manifest.TrialMinimumWilsonLowerBoundPPM <= PartsPerMillion && manifest.ActiveMinimumWilsonLowerBoundPPM <= PartsPerMillion &&
		manifest.ActiveMinimumWilsonLowerBoundPPM >= manifest.TrialMinimumWilsonLowerBoundPPM &&
		manifest.ImmediateQuarantineUnsafeCount > 0 &&
		manifest.TrialUnsafeOutcomeCeiling < manifest.ImmediateQuarantineUnsafeCount && manifest.ActiveUnsafeOutcomeCeiling < manifest.ImmediateQuarantineUnsafeCount &&
		manifest.EvidenceFreshnessSeconds > 0 && manifest.EvidenceFreshnessSeconds <= maximumPolicyWindowSeconds &&
		manifest.MinimumTrialSeconds > 0 && manifest.MinimumTrialSeconds <= maximumPolicyWindowSeconds
}

func validState(value State) bool {
	return value == Candidate || value == Trial || value == Active || value == Retired || value == Quarantined
}
