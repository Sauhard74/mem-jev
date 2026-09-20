package lifecycle_test

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/lifecycle"
)

func TestWilsonLowerBoundExactVectors(t *testing.T) {
	for _, test := range []struct {
		successes, samples uint64
		want               uint32
	}{
		{0, 10, 0}, {1, 1, 206_549}, {5, 10, 236_593}, {50, 100, 403_831}, {95, 100, 888_249}, {100, 100, 963_006},
	} {
		if got := lifecycle.WilsonLowerBoundPPM(test.successes, test.samples, 3_841_459); got != test.want {
			t.Fatalf("Wilson(%d/%d) = %d, want %d", test.successes, test.samples, got, test.want)
		}
	}
	if got := lifecycle.WilsonLowerBoundPPM(math.MaxUint64, math.MaxUint64, 3_841_459); got != 999_999 {
		t.Fatalf("large-count Wilson bound = %d", got)
	}
}

func TestLifecycleBoundariesAndAssociatedEvidenceExclusion(t *testing.T) {
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	manifest := policy(t)
	base := lifecycle.EvaluationRequest{TenantID: "tenant_a", ProcedureID: "proc_a", ProcedureVersionID: "pv_a", PriorState: lifecycle.Candidate, Manifest: manifest, EvidenceCutoffAt: now, EvaluatedAt: now,
		Evidence: lifecycle.Evidence{CausalSuccessCount: 10, AssociatedSuccessCount: 1_000_000, LatestCausalAt: now}}
	decision, err := lifecycle.Evaluate(base)
	if err != nil || decision.NextState != lifecycle.Candidate || !contains(decision.ReasonCodes, lifecycle.ReasonWilsonBelowThreshold) {
		t.Fatalf("below threshold = %#v, %v", decision, err)
	}
	base.Evidence.CausalSuccessCount = 20
	decision, err = lifecycle.Evaluate(base)
	if err != nil || decision.NextState != lifecycle.Trial || decision.AssociatedSuccessCount != 1_000_000 {
		t.Fatalf("boundary promotion = %#v, %v", decision, err)
	}
	base.PriorState = lifecycle.Trial
	base.Evidence.CausalSuccessCount = 50
	base.Evidence.TrialStartedAt = now.Add(-24 * time.Hour)
	decision, err = lifecycle.Evaluate(base)
	if err != nil || decision.NextState != lifecycle.Active {
		t.Fatalf("trial promotion = %#v, %v", decision, err)
	}
}

func TestLifecycleRollbackQuarantineFreshnessAndTerminalStates(t *testing.T) {
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	base := lifecycle.EvaluationRequest{TenantID: "tenant_a", ProcedureID: "proc_a", ProcedureVersionID: "pv_a", PriorState: lifecycle.Active, Manifest: policy(t), EvidenceCutoffAt: now, EvaluatedAt: now,
		Evidence: lifecycle.Evidence{CausalSuccessCount: 49, CausalFailureCount: 1, LatestCausalAt: now}}
	for name, test := range map[string]struct {
		mutate func(*lifecycle.EvaluationRequest)
		want   lifecycle.State
		reason string
	}{
		"unsafe rollback":            {func(r *lifecycle.EvaluationRequest) { r.Evidence.UnsafeOutcomeCount = 1 }, lifecycle.Retired, lifecycle.ReasonUnsafeCeilingExceeded},
		"stale rollback":             {func(r *lifecycle.EvaluationRequest) { r.Evidence.LatestCausalAt = now.Add(-49 * time.Hour) }, lifecycle.Retired, lifecycle.ReasonEvidenceStale},
		"immediate quarantine count": {func(r *lifecycle.EvaluationRequest) { r.Evidence.UnsafeOutcomeCount = 2 }, lifecycle.Quarantined, lifecycle.ReasonImmediateQuarantine},
		"immediate quarantine trigger": {func(r *lifecycle.EvaluationRequest) {
			r.Evidence.QuarantineTriggers = []string{"credential_exfiltration"}
		}, lifecycle.Quarantined, lifecycle.ReasonImmediateQuarantine},
		"retired terminal": {func(r *lifecycle.EvaluationRequest) { r.PriorState = lifecycle.Retired }, lifecycle.Retired, lifecycle.ReasonTerminalState},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			test.mutate(&request)
			got, err := lifecycle.Evaluate(request)
			if err != nil || got.NextState != test.want || !contains(got.ReasonCodes, test.reason) {
				t.Fatalf("got %#v, %v", got, err)
			}
		})
	}
}

func TestLifecycleInsufficientSamplesAndTrialDurationBoundaries(t *testing.T) {
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	manifest := policy(t)
	candidate := lifecycle.EvaluationRequest{TenantID: "tenant_a", ProcedureID: "proc_a", ProcedureVersionID: "pv_a", PriorState: lifecycle.Candidate, Manifest: manifest, EvidenceCutoffAt: now, EvaluatedAt: now,
		Evidence: lifecycle.Evidence{CausalSuccessCount: 19, LatestCausalAt: now}}
	decision, err := lifecycle.Evaluate(candidate)
	if err != nil || decision.NextState != lifecycle.Candidate || !contains(decision.ReasonCodes, lifecycle.ReasonInsufficientSamples) {
		t.Fatalf("sample boundary = %#v, %v", decision, err)
	}
	trial := candidate
	trial.PriorState = lifecycle.Trial
	trial.Evidence.CausalSuccessCount = 50
	trial.Evidence.TrialStartedAt = now.Add(-24*time.Hour + time.Nanosecond)
	decision, err = lifecycle.Evaluate(trial)
	if err != nil || decision.NextState != lifecycle.Trial || !contains(decision.ReasonCodes, lifecycle.ReasonTrialDurationIncomplete) {
		t.Fatalf("duration below boundary = %#v, %v", decision, err)
	}
	trial.Evidence.TrialStartedAt = now.Add(-24 * time.Hour)
	decision, err = lifecycle.Evaluate(trial)
	if err != nil || decision.NextState != lifecycle.Active {
		t.Fatalf("duration exact boundary = %#v, %v", decision, err)
	}
	trial.Evidence.UnsafeOutcomeCount = 1
	decision, err = lifecycle.Evaluate(trial)
	if err != nil || decision.NextState != lifecycle.Candidate || !contains(decision.ReasonCodes, lifecycle.ReasonUnsafeCeilingExceeded) {
		t.Fatalf("trial rollback = %#v, %v", decision, err)
	}
}

func TestLifecycleDeterminismAndManifestAuthentication(t *testing.T) {
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	request := lifecycle.EvaluationRequest{TenantID: "tenant_a", ProcedureID: "proc_a", ProcedureVersionID: "pv_a", PriorState: lifecycle.Trial, Manifest: policy(t), EvidenceCutoffAt: now, EvaluatedAt: now,
		Evidence: lifecycle.Evidence{CausalSuccessCount: 49, CausalFailureCount: 1, AssociatedFailureCount: 77, LatestCausalAt: now, TrialStartedAt: now.Add(-24 * time.Hour)}}
	want, err := lifecycle.Evaluate(request)
	if err != nil || lifecycle.ValidateDecision(want) != nil {
		t.Fatal(err)
	}
	for seed := int64(0); seed < 100; seed++ {
		random := rand.New(rand.NewSource(seed))
		_ = random.Uint64()
		got, gotErr := lifecycle.Evaluate(request)
		if gotErr != nil || !bytes.Equal(got.CanonicalJSON, want.CanonicalJSON) {
			t.Fatalf("seed %d changed decision", seed)
		}
	}
	tampered := request.Manifest
	tampered.ActiveMinimumCausalSamples++
	request.Manifest = tampered
	if _, err := lifecycle.Evaluate(request); err == nil {
		t.Fatal("tampered manifest accepted")
	}
}

func policy(t *testing.T) lifecycle.PolicyManifest {
	t.Helper()
	value, err := lifecycle.NewPolicyManifest(lifecycle.PolicySpec{Version: "production-2026-09", WilsonZSquaredPPM: 3_841_459, TrialMinimumCausalSamples: 20, TrialMinimumWilsonLowerBoundPPM: 800_000, ActiveMinimumCausalSamples: 50, ActiveMinimumWilsonLowerBoundPPM: 850_000, TrialUnsafeOutcomeCeiling: 0, ActiveUnsafeOutcomeCeiling: 0, ImmediateQuarantineUnsafeCount: 2, EvidenceFreshnessSeconds: 48 * 60 * 60, MinimumTrialSeconds: 24 * 60 * 60})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
