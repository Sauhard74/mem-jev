package experiment

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

type Risk string

const (
	RiskLow      Risk = "low"
	RiskMedium   Risk = "medium"
	RiskHigh     Risk = "high"
	RiskCritical Risk = "critical"
)

type ExposureSnapshot struct {
	WindowStart          time.Time `json:"window_start"`
	TenantExposureCount  uint64    `json:"tenant_exposure_count"`
	GlobalExposureCount  uint64    `json:"global_exposure_count"`
	TenantUnsafeCount    uint64    `json:"tenant_unsafe_count"`
	GlobalUnsafeCount    uint64    `json:"global_unsafe_count"`
	ConcurrentTrialCount uint32    `json:"concurrent_trial_count"`
}

type AssignmentRequest struct {
	Manifest        Manifest
	QueryBucketHash string
	Risk            Risk
	Exposure        ExposureSnapshot
	AssignedAt      time.Time
}

type Assignment struct {
	SchemaVersion              string          `json:"schema_version"`
	ID                         string          `json:"experiment_assignment_id"`
	TenantID                   domain.TenantID `json:"tenant_id"`
	ManifestID                 string          `json:"experiment_manifest_id"`
	AssignmentKeyHash          string          `json:"assignment_key_hash"`
	QueryBucketHash            string          `json:"query_bucket_hash"`
	AssignedProcedureVersionID string          `json:"assigned_procedure_version_id"`
	Bucket                     uint32          `json:"bucket"`
	Challenger                 bool            `json:"challenger"`
	Risk                       Risk            `json:"risk"`
	ReasonCodes                []string        `json:"reason_codes"`
	WindowStart                time.Time       `json:"window_start"`
	CreatedAt                  time.Time       `json:"created_at"`
	ExpiresAt                  time.Time       `json:"expires_at"`
	ContentHash                string          `json:"content_hash,omitempty"`
	CanonicalJSON              []byte          `json:"-"`
}

const (
	ReasonChallengerBucket      = "challenger_bucket"
	ReasonChampionBucket        = "champion_bucket"
	ReasonCriticalRisk          = "critical_risk_champion_only"
	ReasonHighRiskOptInRequired = "high_risk_opt_in_required"
	ReasonConcurrentTrialCap    = "concurrent_trial_cap_reached"
	ReasonTenantExposureCap     = "tenant_exposure_cap_reached"
	ReasonGlobalExposureCap     = "global_exposure_cap_reached"
	ReasonTenantUnsafeCeiling   = "tenant_unsafe_ceiling_exceeded"
	ReasonGlobalUnsafeCeiling   = "global_unsafe_ceiling_exceeded"
)

func Assign(request AssignmentRequest) (Assignment, error) {
	if ValidateManifest(request.Manifest) != nil || !hashPattern.MatchString(request.QueryBucketHash) || !validRisk(request.Risk) {
		return Assignment{}, ErrInvalidExperiment
	}
	request.Exposure.WindowStart = CanonicalWindowStart(request.Manifest, request.AssignedAt)
	if !validExposure(request) {
		return Assignment{}, ErrInvalidExperiment
	}
	_, keyHash, err := canonical.MarshalAndHash(struct {
		TenantID        domain.TenantID `json:"tenant_id"`
		ProcedureID     string          `json:"procedure_id"`
		QueryBucketHash string          `json:"query_bucket_hash"`
		ExperimentEpoch uint64          `json:"experiment_epoch"`
	}{request.Manifest.TenantID, request.Manifest.ProcedureID, request.QueryBucketHash, request.Manifest.ExperimentEpoch})
	if err != nil {
		return Assignment{}, err
	}
	digest, err := hex.DecodeString(keyHash)
	if err != nil || len(digest) != 32 {
		return Assignment{}, ErrInvalidExperiment
	}
	bucket := uint32(binary.BigEndian.Uint64(digest[:8]) % uint64(BucketCount))
	versionID := request.Manifest.ChampionVersionID
	reasons := make([]string, 0, 6)
	challenger := bucket < request.Manifest.ChallengerExposurePPM
	if !challenger {
		reasons = append(reasons, ReasonChampionBucket)
	}
	if request.Risk == RiskCritical {
		challenger = false
		reasons = append(reasons, ReasonCriticalRisk)
	}
	if request.Risk == RiskHigh && !request.Manifest.TenantHighRiskOptIn {
		challenger = false
		reasons = append(reasons, ReasonHighRiskOptInRequired)
	}
	if request.Exposure.ConcurrentTrialCount > request.Manifest.MaximumConcurrentTrials {
		challenger = false
		reasons = append(reasons, ReasonConcurrentTrialCap)
	}
	if request.Exposure.TenantExposureCount >= request.Manifest.TenantExposureLimit {
		challenger = false
		reasons = append(reasons, ReasonTenantExposureCap)
	}
	if request.Exposure.GlobalExposureCount >= request.Manifest.GlobalExposureLimit {
		challenger = false
		reasons = append(reasons, ReasonGlobalExposureCap)
	}
	if request.Exposure.TenantUnsafeCount > request.Manifest.TenantUnsafeOutcomeCeiling {
		challenger = false
		reasons = append(reasons, ReasonTenantUnsafeCeiling)
	}
	if request.Exposure.GlobalUnsafeCount > request.Manifest.GlobalUnsafeOutcomeCeiling {
		challenger = false
		reasons = append(reasons, ReasonGlobalUnsafeCeiling)
	}
	if challenger {
		versionID = request.Manifest.ChallengerVersionIDs[int(binary.BigEndian.Uint64(digest[8:16])%uint64(len(request.Manifest.ChallengerVersionIDs)))]
		reasons = append(reasons, ReasonChallengerBucket)
	}
	expiresAt := request.AssignedAt.Add(time.Duration(request.Manifest.AssignmentTTLSeconds) * time.Second)
	if windowEnd := request.Exposure.WindowStart.Add(time.Duration(request.Manifest.ExposureWindowSeconds) * time.Second); expiresAt.After(windowEnd) {
		expiresAt = windowEnd
	}
	assignment := Assignment{SchemaVersion: "experiment-assignment.v1", TenantID: request.Manifest.TenantID, ManifestID: request.Manifest.ID, AssignmentKeyHash: keyHash, QueryBucketHash: request.QueryBucketHash, AssignedProcedureVersionID: versionID, Bucket: bucket, Challenger: challenger, Risk: request.Risk, ReasonCodes: reasons, WindowStart: request.Exposure.WindowStart, CreatedAt: request.AssignedAt, ExpiresAt: expiresAt}
	return sealAssignment(assignment)
}

func sealAssignment(value Assignment) (Assignment, error) {
	value.ID, value.ContentHash, value.CanonicalJSON = "", "", nil
	sort.Strings(value.ReasonCodes)
	encoded, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Assignment{}, err
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "expasn_"+hash, hash, encoded
	return value, nil
}

func ValidateAssignment(value Assignment) error {
	if value.SchemaVersion != "experiment-assignment.v1" || value.ID != "expasn_"+value.ContentHash || !hashPattern.MatchString(value.ContentHash) || value.TenantID == "" || !tokenPattern.MatchString(value.ManifestID) || !hashPattern.MatchString(value.AssignmentKeyHash) || !hashPattern.MatchString(value.QueryBucketHash) || !tokenPattern.MatchString(value.AssignedProcedureVersionID) || value.Bucket >= BucketCount || !validRisk(value.Risk) || value.CreatedAt.IsZero() || value.ExpiresAt.IsZero() || !value.CreatedAt.Equal(value.CreatedAt.UTC()) || !value.ExpiresAt.Equal(value.ExpiresAt.UTC()) || !value.ExpiresAt.After(value.CreatedAt) || value.WindowStart.IsZero() || !value.WindowStart.Equal(value.WindowStart.UTC()) || len(value.ReasonCodes) == 0 {
		return ErrInvalidExperiment
	}
	for index, code := range value.ReasonCodes {
		if !tokenPattern.MatchString(code) || index > 0 && value.ReasonCodes[index-1] >= code {
			return ErrInvalidExperiment
		}
	}
	copyOf := value
	copyOf.ID, copyOf.ContentHash, copyOf.CanonicalJSON = "", "", nil
	rebuilt, err := sealAssignment(copyOf)
	if err != nil || rebuilt.ID != value.ID || !bytes.Equal(rebuilt.CanonicalJSON, value.CanonicalJSON) {
		return ErrInvalidExperiment
	}
	return nil
}

func validExposure(request AssignmentRequest) bool {
	window := time.Duration(request.Manifest.ExposureWindowSeconds) * time.Second
	return !request.AssignedAt.IsZero() && request.AssignedAt.Equal(request.AssignedAt.UTC()) && !request.Exposure.WindowStart.IsZero() && request.Exposure.WindowStart.Equal(request.Exposure.WindowStart.UTC()) && !request.Exposure.WindowStart.After(request.AssignedAt) && request.AssignedAt.Before(request.Exposure.WindowStart.Add(window))
}

func CanonicalWindowStart(manifest Manifest, assignedAt time.Time) time.Time {
	if assignedAt.IsZero() || manifest.ExposureWindowSeconds == 0 {
		return time.Time{}
	}
	return assignedAt.UTC().Truncate(time.Duration(manifest.ExposureWindowSeconds) * time.Second)
}

func validRisk(value Risk) bool {
	return value == RiskLow || value == RiskMedium || value == RiskHigh || value == RiskCritical
}
