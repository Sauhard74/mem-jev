package lifecycle

import (
	"bytes"
	"math/big"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

type Evidence struct {
	CausalSuccessCount     uint64    `json:"causal_success_count"`
	CausalFailureCount     uint64    `json:"causal_failure_count"`
	AssociatedSuccessCount uint64    `json:"associated_success_count"`
	AssociatedFailureCount uint64    `json:"associated_failure_count"`
	UnsafeOutcomeCount     uint64    `json:"unsafe_outcome_count"`
	LatestCausalAt         time.Time `json:"latest_causal_at"`
	TrialStartedAt         time.Time `json:"trial_started_at,omitempty"`
	QuarantineTriggers     []string  `json:"quarantine_triggers,omitempty"`
}

type EvaluationRequest struct {
	TenantID           domain.TenantID
	ProcedureID        string
	ProcedureVersionID string
	PriorState         State
	Manifest           PolicyManifest
	Evidence           Evidence
	EvidenceCutoffAt   time.Time
	EvaluatedAt        time.Time
}

type Decision struct {
	SchemaVersion          string          `json:"schema_version"`
	ID                     string          `json:"lifecycle_decision_id"`
	TenantID               domain.TenantID `json:"tenant_id"`
	ProcedureID            string          `json:"procedure_id"`
	ProcedureVersionID     string          `json:"procedure_version_id"`
	PolicyManifestID       string          `json:"lifecycle_policy_manifest_id"`
	PriorState             State           `json:"prior_state"`
	NextState              State           `json:"next_state"`
	CausalSuccessCount     uint64          `json:"causal_success_count"`
	CausalFailureCount     uint64          `json:"causal_failure_count"`
	AssociatedSuccessCount uint64          `json:"associated_success_count"`
	AssociatedFailureCount uint64          `json:"associated_failure_count"`
	UnsafeOutcomeCount     uint64          `json:"unsafe_outcome_count"`
	WilsonLowerBoundPPM    uint32          `json:"wilson_lower_bound_ppm"`
	ReasonCodes            []string        `json:"reason_codes"`
	EvidenceCutoffAt       time.Time       `json:"evidence_cutoff_at"`
	EvaluatedAt            time.Time       `json:"evaluated_at"`
	ContentHash            string          `json:"content_hash,omitempty"`
	CanonicalJSON          []byte          `json:"-"`
}

const (
	ReasonCandidatePromoted       = "candidate_promoted_to_trial"
	ReasonTrialPromoted           = "trial_promoted_to_active"
	ReasonInsufficientSamples     = "insufficient_causal_samples"
	ReasonWilsonBelowThreshold    = "wilson_lower_bound_below_threshold"
	ReasonTrialDurationIncomplete = "minimum_trial_duration_incomplete"
	ReasonEvidenceStale           = "causal_evidence_stale"
	ReasonUnsafeCeilingExceeded   = "unsafe_outcome_ceiling_exceeded"
	ReasonImmediateQuarantine     = "immediate_quarantine_triggered"
	ReasonActiveRollback          = "active_version_rolled_back"
	ReasonTerminalState           = "terminal_state_unchanged"
	ReasonStateMaintained         = "state_maintained"
)

func Evaluate(request EvaluationRequest) (Decision, error) {
	if ValidatePolicyManifest(request.Manifest) != nil || request.TenantID == "" || !tokenPattern.MatchString(request.ProcedureID) || !tokenPattern.MatchString(request.ProcedureVersionID) || !validState(request.PriorState) || request.EvidenceCutoffAt.IsZero() || request.EvaluatedAt.IsZero() || !request.EvidenceCutoffAt.Equal(request.EvidenceCutoffAt.UTC()) || !request.EvaluatedAt.Equal(request.EvaluatedAt.UTC()) || request.EvidenceCutoffAt.After(request.EvaluatedAt) || !validEvidence(request.Evidence, request.EvidenceCutoffAt) {
		return Decision{}, ErrInvalidLifecycle
	}
	samples := request.Evidence.CausalSuccessCount + request.Evidence.CausalFailureCount
	if samples < request.Evidence.CausalSuccessCount {
		return Decision{}, ErrInvalidLifecycle
	}
	bound := WilsonLowerBoundPPM(request.Evidence.CausalSuccessCount, samples, request.Manifest.WilsonZSquaredPPM)
	next, reasons := transition(request, samples, bound)
	decision := Decision{
		SchemaVersion: "lifecycle-decision.v1", TenantID: request.TenantID, ProcedureID: request.ProcedureID,
		ProcedureVersionID: request.ProcedureVersionID, PolicyManifestID: request.Manifest.ID,
		PriorState: request.PriorState, NextState: next,
		CausalSuccessCount: request.Evidence.CausalSuccessCount, CausalFailureCount: request.Evidence.CausalFailureCount,
		AssociatedSuccessCount: request.Evidence.AssociatedSuccessCount, AssociatedFailureCount: request.Evidence.AssociatedFailureCount,
		UnsafeOutcomeCount: request.Evidence.UnsafeOutcomeCount, WilsonLowerBoundPPM: bound,
		ReasonCodes: reasons, EvidenceCutoffAt: request.EvidenceCutoffAt, EvaluatedAt: request.EvaluatedAt,
	}
	return sealDecision(decision)
}

func transition(request EvaluationRequest, samples uint64, bound uint32) (State, []string) {
	e := request.Evidence
	p := request.Manifest
	if request.PriorState == Retired || request.PriorState == Quarantined {
		return request.PriorState, []string{ReasonTerminalState}
	}
	if len(e.QuarantineTriggers) > 0 || e.UnsafeOutcomeCount >= p.ImmediateQuarantineUnsafeCount {
		reasons := []string{ReasonImmediateQuarantine}
		for _, trigger := range e.QuarantineTriggers {
			reasons = append(reasons, "quarantine_trigger:"+trigger)
		}
		return Quarantined, reasons
	}
	fresh := !e.LatestCausalAt.IsZero() && !e.LatestCausalAt.Before(request.EvidenceCutoffAt.Add(-time.Duration(p.EvidenceFreshnessSeconds)*time.Second))
	if request.PriorState == Active {
		if !fresh || samples < p.ActiveMinimumCausalSamples || bound < p.ActiveMinimumWilsonLowerBoundPPM || e.UnsafeOutcomeCount > p.ActiveUnsafeOutcomeCeiling {
			reasons := []string{ReasonActiveRollback}
			if !fresh {
				reasons = append(reasons, ReasonEvidenceStale)
			}
			if samples < p.ActiveMinimumCausalSamples {
				reasons = append(reasons, ReasonInsufficientSamples)
			}
			if bound < p.ActiveMinimumWilsonLowerBoundPPM {
				reasons = append(reasons, ReasonWilsonBelowThreshold)
			}
			if e.UnsafeOutcomeCount > p.ActiveUnsafeOutcomeCeiling {
				reasons = append(reasons, ReasonUnsafeCeilingExceeded)
			}
			return Retired, reasons
		}
		return Active, []string{ReasonStateMaintained}
	}
	if !fresh {
		return request.PriorState, []string{ReasonEvidenceStale}
	}
	if request.PriorState == Candidate {
		reasons := thresholdReasons(samples, p.TrialMinimumCausalSamples, bound, p.TrialMinimumWilsonLowerBoundPPM)
		if e.UnsafeOutcomeCount > p.TrialUnsafeOutcomeCeiling {
			reasons = append(reasons, ReasonUnsafeCeilingExceeded)
		}
		if len(reasons) > 0 {
			return Candidate, reasons
		}
		return Trial, []string{ReasonCandidatePromoted}
	}
	reasons := thresholdReasons(samples, p.ActiveMinimumCausalSamples, bound, p.ActiveMinimumWilsonLowerBoundPPM)
	if e.UnsafeOutcomeCount > p.ActiveUnsafeOutcomeCeiling {
		return Candidate, []string{ReasonUnsafeCeilingExceeded}
	}
	if e.TrialStartedAt.IsZero() || e.TrialStartedAt.After(request.EvidenceCutoffAt.Add(-time.Duration(p.MinimumTrialSeconds)*time.Second)) {
		reasons = append(reasons, ReasonTrialDurationIncomplete)
	}
	if len(reasons) > 0 {
		return Trial, reasons
	}
	return Active, []string{ReasonTrialPromoted}
}

func thresholdReasons(samples, minimum uint64, bound, threshold uint32) []string {
	result := make([]string, 0, 2)
	if samples < minimum {
		result = append(result, ReasonInsufficientSamples)
	}
	if bound < threshold {
		result = append(result, ReasonWilsonBelowThreshold)
	}
	return result
}

// WilsonLowerBoundPPM returns the largest whole PPM value no greater than the
// exact Wilson lower confidence bound. All comparisons use integers.
func WilsonLowerBoundPPM(successes, samples uint64, zSquaredPPM uint32) uint32 {
	if samples == 0 || successes > samples || zSquaredPPM == 0 {
		return 0
	}
	lo, hi := uint32(0), PartsPerMillion
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if atOrBelowWilsonLower(mid, successes, samples, zSquaredPPM) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

func atOrBelowWilsonLower(ppm uint32, successes, samples uint64, zSquaredPPM uint32) bool {
	// The smaller root of (n+z²)x²-(2s+z²)x+s²/n = 0 is the lower bound.
	// Restrict to the left of the vertex, then test the quadratic as >= 0.
	values := ints(samples, successes, uint64(PartsPerMillion), uint64(zSquaredPPM), uint64(ppm))
	n, s, q, z, m := values[0], values[1], values[2], values[3], values[4]
	nqPlusZ := new(big.Int).Add(new(big.Int).Mul(n, q), z)
	leftCenter := new(big.Int).Mul(big.NewInt(2), new(big.Int).Mul(m, nqPlusZ))
	rightCenter := new(big.Int).Mul(q, new(big.Int).Add(new(big.Int).Mul(big.NewInt(2), new(big.Int).Mul(s, q)), z))
	if leftCenter.Cmp(rightCenter) > 0 {
		return false
	}
	term1 := new(big.Int).Mul(n, new(big.Int).Mul(nqPlusZ, new(big.Int).Mul(m, m)))
	twoSQPlusZ := new(big.Int).Add(new(big.Int).Mul(big.NewInt(2), new(big.Int).Mul(s, q)), z)
	term2 := new(big.Int).Mul(n, new(big.Int).Mul(twoSQPlusZ, new(big.Int).Mul(m, q)))
	term3 := new(big.Int).Mul(new(big.Int).Mul(s, s), new(big.Int).Mul(q, new(big.Int).Mul(q, q)))
	return new(big.Int).Add(new(big.Int).Sub(term1, term2), term3).Sign() >= 0
}

func ints(values ...uint64) []*big.Int {
	result := make([]*big.Int, len(values))
	for i, value := range values {
		result[i] = new(big.Int).SetUint64(value)
	}
	return result
}

func validEvidence(value Evidence, cutoff time.Time) bool {
	if !value.LatestCausalAt.IsZero() && (!value.LatestCausalAt.Equal(value.LatestCausalAt.UTC()) || value.LatestCausalAt.After(cutoff)) {
		return false
	}
	if !value.TrialStartedAt.IsZero() && (!value.TrialStartedAt.Equal(value.TrialStartedAt.UTC()) || value.TrialStartedAt.After(cutoff)) {
		return false
	}
	for i, code := range value.QuarantineTriggers {
		if !tokenPattern.MatchString(code) || i > 0 && value.QuarantineTriggers[i-1] >= code {
			return false
		}
	}
	return true
}

func sealDecision(value Decision) (Decision, error) {
	value.ID, value.ContentHash, value.CanonicalJSON = "", "", nil
	sort.Strings(value.ReasonCodes)
	encoded, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Decision{}, err
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "lcd_"+hash, hash, encoded
	return value, nil
}

func ValidateDecision(value Decision) error {
	if value.SchemaVersion != "lifecycle-decision.v1" || value.ID != "lcd_"+value.ContentHash || !hashPattern.MatchString(value.ContentHash) || value.TenantID == "" || !tokenPattern.MatchString(value.ProcedureID) || !tokenPattern.MatchString(value.ProcedureVersionID) || !tokenPattern.MatchString(value.PolicyManifestID) || !validState(value.PriorState) || !validState(value.NextState) || value.WilsonLowerBoundPPM > PartsPerMillion || value.EvidenceCutoffAt.IsZero() || value.EvaluatedAt.IsZero() || !value.EvidenceCutoffAt.Equal(value.EvidenceCutoffAt.UTC()) || !value.EvaluatedAt.Equal(value.EvaluatedAt.UTC()) || value.EvidenceCutoffAt.After(value.EvaluatedAt) || len(value.ReasonCodes) == 0 || len(value.ReasonCodes) > 32 {
		return ErrInvalidLifecycle
	}
	for index, code := range value.ReasonCodes {
		if !tokenPattern.MatchString(code) || index > 0 && value.ReasonCodes[index-1] >= code {
			return ErrInvalidLifecycle
		}
	}
	copyOf := value
	copyOf.ID, copyOf.ContentHash, copyOf.CanonicalJSON = "", "", nil
	rebuilt, err := sealDecision(copyOf)
	if err != nil || rebuilt.ID != value.ID || !bytes.Equal(rebuilt.CanonicalJSON, value.CanonicalJSON) {
		return ErrInvalidLifecycle
	}
	return nil
}
