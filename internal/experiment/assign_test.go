package experiment_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/experiment"
)

func TestAssignmentIsStableAcrossRetriesAndEpochChanges(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	manifest := validManifest(t, 1, 250_000, false)
	request := experiment.AssignmentRequest{Manifest: manifest, QueryBucketHash: fmt.Sprintf("%064x", 42), Risk: experiment.RiskLow, Exposure: experiment.ExposureSnapshot{WindowStart: now.Truncate(time.Hour)}, AssignedAt: now}
	first, err := experiment.Assign(request)
	if err != nil || experiment.ValidateAssignment(first) != nil {
		t.Fatal(err)
	}
	second, err := experiment.Assign(request)
	if err != nil || !bytes.Equal(first.CanonicalJSON, second.CanonicalJSON) {
		t.Fatalf("retry changed assignment: %v", err)
	}
	request.Manifest = validManifest(t, 2, 250_000, false)
	third, err := experiment.Assign(request)
	if err != nil || third.AssignmentKeyHash == first.AssignmentKeyHash {
		t.Fatalf("epoch failed to rotate assignment: %#v %v", third, err)
	}
}

func TestAssignmentInfersCanonicalExposureWindow(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 37, 0, 0, time.UTC)
	request := experiment.AssignmentRequest{Manifest: validManifest(t, 1, 250_000, false), QueryBucketHash: fmt.Sprintf("%064x", 43), Risk: experiment.RiskLow, Exposure: experiment.ExposureSnapshot{WindowStart: now.Add(-17 * time.Hour)}, AssignedAt: now}
	assignment, err := experiment.Assign(request)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC); !assignment.WindowStart.Equal(want) {
		t.Fatalf("window=%s want=%s", assignment.WindowStart, want)
	}
}

func TestRiskAndBudgetGatesFailToChampion(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	for name, test := range rangeCases() {
		t.Run(name, func(t *testing.T) {
			request := experiment.AssignmentRequest{Manifest: validManifest(t, 1, experiment.BucketCount, false), QueryBucketHash: fmt.Sprintf("%064x", 7), Risk: test.risk, Exposure: experiment.ExposureSnapshot{WindowStart: now.Truncate(time.Hour)}, AssignedAt: now}
			test.mutate(&request)
			assignment, err := experiment.Assign(request)
			if err != nil || assignment.Challenger || assignment.AssignedProcedureVersionID != request.Manifest.ChampionVersionID || !contains(assignment.ReasonCodes, test.reason) {
				t.Fatalf("assignment=%#v err=%v", assignment, err)
			}
		})
	}
}

func TestHighRiskRequiresManifestedTenantOptInAndCriticalNeverExplores(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	request := experiment.AssignmentRequest{Manifest: validManifest(t, 1, experiment.BucketCount, true), QueryBucketHash: fmt.Sprintf("%064x", 9), Risk: experiment.RiskHigh, Exposure: experiment.ExposureSnapshot{WindowStart: now.Truncate(time.Hour)}, AssignedAt: now}
	high, err := experiment.Assign(request)
	if err != nil || !high.Challenger {
		t.Fatalf("opted-in high risk did not explore: %#v %v", high, err)
	}
	request.Risk = experiment.RiskCritical
	critical, err := experiment.Assign(request)
	if err != nil || critical.Challenger || !contains(critical.ReasonCodes, experiment.ReasonCriticalRisk) {
		t.Fatalf("critical assignment=%#v err=%v", critical, err)
	}
}

func TestDistributionFixtureIsDeterministic(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	manifest := validManifest(t, 1, 100_000, false)
	challengers := 0
	for index := 0; index < 10_000; index++ {
		assignment, err := experiment.Assign(experiment.AssignmentRequest{Manifest: manifest, QueryBucketHash: fmt.Sprintf("%064x", index), Risk: experiment.RiskLow, Exposure: experiment.ExposureSnapshot{WindowStart: now.Truncate(time.Hour)}, AssignedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		if assignment.Challenger {
			challengers++
		}
	}
	if challengers != 1_062 {
		t.Fatalf("challenger fixture count=%d want=1062", challengers)
	}
}

func validManifest(t *testing.T, epoch uint64, ppm uint32, highRisk bool) experiment.Manifest {
	t.Helper()
	manifest, err := experiment.NewManifest(experiment.ManifestSpec{Version: "experiment.v1", TenantID: "tenant_a", ProcedureID: "proc_a", ChampionVersionID: "pv_champion", ChallengerVersionIDs: []string{"pv_b", "pv_a"}, ExperimentEpoch: epoch, ChallengerExposurePPM: ppm, MaximumConcurrentTrials: 2, TenantExposureLimit: 100, GlobalExposureLimit: 1000, TenantUnsafeOutcomeCeiling: 0, GlobalUnsafeOutcomeCeiling: 1, ExposureWindowSeconds: 3600, AssignmentTTLSeconds: 600, TenantHighRiskOptIn: highRisk})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func rangeCases() map[string]struct {
	risk   experiment.Risk
	mutate func(*experiment.AssignmentRequest)
	reason string
} {
	return map[string]struct {
		risk   experiment.Risk
		mutate func(*experiment.AssignmentRequest)
		reason string
	}{
		"high risk opt in": {experiment.RiskHigh, func(*experiment.AssignmentRequest) {}, experiment.ReasonHighRiskOptInRequired},
		"tenant exposure":  {experiment.RiskLow, func(r *experiment.AssignmentRequest) { r.Exposure.TenantExposureCount = r.Manifest.TenantExposureLimit }, experiment.ReasonTenantExposureCap},
		"global exposure":  {experiment.RiskLow, func(r *experiment.AssignmentRequest) { r.Exposure.GlobalExposureCount = r.Manifest.GlobalExposureLimit }, experiment.ReasonGlobalExposureCap},
		"tenant unsafe": {experiment.RiskLow, func(r *experiment.AssignmentRequest) {
			r.Exposure.TenantUnsafeCount = r.Manifest.TenantUnsafeOutcomeCeiling + 1
		}, experiment.ReasonTenantUnsafeCeiling},
		"global unsafe": {experiment.RiskLow, func(r *experiment.AssignmentRequest) {
			r.Exposure.GlobalUnsafeCount = r.Manifest.GlobalUnsafeOutcomeCeiling + 1
		}, experiment.ReasonGlobalUnsafeCeiling},
		"trial cap": {experiment.RiskLow, func(r *experiment.AssignmentRequest) {
			r.Exposure.ConcurrentTrialCount = r.Manifest.MaximumConcurrentTrials + 1
		}, experiment.ReasonConcurrentTrialCap},
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
