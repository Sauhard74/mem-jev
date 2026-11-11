package lifecycle

// DefaultPolicyVersion names the promotion policy memJev applies when
// automatic promotion is on and no operator policy is configured.
const DefaultPolicyVersion = "memjev-default-lifecycle.v1"

// DefaultPolicy returns the default promotion policy. All counts are verified
// outcomes of runs that were given the procedure's plan (causal credit);
// provisional or unattributed outcomes never count. Bounds are 95% Wilson
// lower bounds on the success rate.
//
//   - candidate -> trial: at least 3 causal outcomes, lower bound >= 40%
//     (3 of 3 succeeds).
//   - trial -> active: at least 10 causal outcomes, lower bound >= 60%, and
//     at least one hour in trial.
//   - active -> retired: lower bound below 60%, or no causal evidence in the
//     last 90 days.
func DefaultPolicy() (PolicyManifest, error) {
	return NewPolicyManifest(PolicySpec{
		Version:                          DefaultPolicyVersion,
		WilsonZSquaredPPM:                3_841_600,
		TrialMinimumCausalSamples:        3,
		TrialMinimumWilsonLowerBoundPPM:  400_000,
		ActiveMinimumCausalSamples:       10,
		ActiveMinimumWilsonLowerBoundPPM: 600_000,
		TrialUnsafeOutcomeCeiling:        0,
		ActiveUnsafeOutcomeCeiling:       0,
		ImmediateQuarantineUnsafeCount:   1,
		EvidenceFreshnessSeconds:         90 * 24 * 60 * 60,
		MinimumTrialSeconds:              60 * 60,
	})
}
