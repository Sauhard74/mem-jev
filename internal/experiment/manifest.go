package experiment

import (
	"bytes"
	"errors"
	"regexp"
	"sort"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

const BucketCount uint32 = 1_000_000

var (
	ErrInvalidExperiment = errors.New("invalid experiment value")
	hashPattern          = regexp.MustCompile(`^[0-9a-f]{64}$`)
	tokenPattern         = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)
)

type ManifestSpec struct {
	Version                    string          `json:"version"`
	TenantID                   domain.TenantID `json:"tenant_id"`
	ProcedureID                string          `json:"procedure_id"`
	ChampionVersionID          string          `json:"champion_version_id"`
	ChallengerVersionIDs       []string        `json:"challenger_version_ids"`
	ExperimentEpoch            uint64          `json:"experiment_epoch"`
	ChallengerExposurePPM      uint32          `json:"challenger_exposure_ppm"`
	MaximumConcurrentTrials    uint32          `json:"maximum_concurrent_trials"`
	TenantExposureLimit        uint64          `json:"tenant_exposure_limit"`
	GlobalExposureLimit        uint64          `json:"global_exposure_limit"`
	TenantUnsafeOutcomeCeiling uint64          `json:"tenant_unsafe_outcome_ceiling"`
	GlobalUnsafeOutcomeCeiling uint64          `json:"global_unsafe_outcome_ceiling"`
	ExposureWindowSeconds      uint64          `json:"exposure_window_seconds"`
	AssignmentTTLSeconds       uint64          `json:"assignment_ttl_seconds"`
	TenantHighRiskOptIn        bool            `json:"tenant_high_risk_opt_in"`
}

type Manifest struct {
	SchemaVersion string `json:"schema_version"`
	ManifestSpec
	ID            string `json:"-"`
	ContentHash   string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NewManifest(spec ManifestSpec) (Manifest, error) {
	manifest := Manifest{SchemaVersion: "experiment-manifest.v1", ManifestSpec: spec}
	manifest.Version = token(manifest.Version)
	manifest.ProcedureID = token(manifest.ProcedureID)
	manifest.ChampionVersionID = token(manifest.ChampionVersionID)
	manifest.ChallengerVersionIDs = normalizeVersions(manifest.ChallengerVersionIDs)
	if !validManifest(manifest) {
		return Manifest{}, ErrInvalidExperiment
	}
	encoded, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return Manifest{}, err
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = "expman_"+hash, hash, encoded
	return manifest, nil
}

func ValidateManifest(manifest Manifest) error {
	rebuilt, err := NewManifest(manifest.ManifestSpec)
	if err != nil || manifest.SchemaVersion != rebuilt.SchemaVersion || manifest.ID != rebuilt.ID || manifest.ContentHash != rebuilt.ContentHash || !bytes.Equal(manifest.CanonicalJSON, rebuilt.CanonicalJSON) {
		return ErrInvalidExperiment
	}
	return nil
}

func validManifest(value Manifest) bool {
	const maximumWindow = 31 * 24 * 60 * 60
	if !tokenPattern.MatchString(value.Version) || value.TenantID == "" || !tokenPattern.MatchString(value.ProcedureID) || !tokenPattern.MatchString(value.ChampionVersionID) || value.ExperimentEpoch == 0 || value.ChallengerExposurePPM > BucketCount || value.ChallengerExposurePPM == 0 || value.MaximumConcurrentTrials == 0 || len(value.ChallengerVersionIDs) == 0 || len(value.ChallengerVersionIDs) > int(value.MaximumConcurrentTrials) || value.TenantExposureLimit == 0 || value.GlobalExposureLimit < value.TenantExposureLimit || value.ExposureWindowSeconds == 0 || value.ExposureWindowSeconds > maximumWindow || value.AssignmentTTLSeconds == 0 || value.AssignmentTTLSeconds > value.ExposureWindowSeconds {
		return false
	}
	for index, versionID := range value.ChallengerVersionIDs {
		if !tokenPattern.MatchString(versionID) || versionID == value.ChampionVersionID || index > 0 && value.ChallengerVersionIDs[index-1] >= versionID {
			return false
		}
	}
	return true
}

func normalizeVersions(source []string) []string {
	result := make([]string, len(source))
	for index, value := range source {
		result[index] = token(value)
	}
	sort.Strings(result)
	return result
}

func token(value string) string {
	for len(value) > 0 && (value[0] == ' ' || value[0] == '\t' || value[0] == '\n' || value[0] == '\r') {
		value = value[1:]
	}
	for len(value) > 0 && (value[len(value)-1] == ' ' || value[len(value)-1] == '\t' || value[len(value)-1] == '\n' || value[len(value)-1] == '\r') {
		value = value[:len(value)-1]
	}
	return value
}
