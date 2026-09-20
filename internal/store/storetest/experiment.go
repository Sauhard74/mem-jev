package storetest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/experiment"
)

type ExperimentFactory func(*testing.T) experiment.Repository

func RunExperimentContract(t *testing.T, factory ExperimentFactory) {
	t.Helper()
	t.Run("reservation is persisted before return and retries are stable", func(t *testing.T) {
		repository := factory(t)
		request := ValidExperimentRequest(t, 1, 100_000)
		first, err := repository.Reserve(context.Background(), request)
		if err != nil || first.Disposition != experiment.DispositionReserved {
			t.Fatalf("first=%#v err=%v", first, err)
		}
		request.AssignedAt = request.AssignedAt.Add(time.Second)
		second, err := repository.Reserve(context.Background(), request)
		if err != nil || second.Disposition != experiment.DispositionDuplicate || !experiment.EquivalentRouting(first.Assignment, second.Assignment) || second.Assignment.ID != first.Assignment.ID {
			t.Fatalf("second=%#v err=%v", second, err)
		}
		stored, err := repository.Find(context.Background(), string(first.Assignment.TenantID), first.Assignment.ManifestID, first.Assignment.AssignmentKeyHash)
		if err != nil || stored.ID != first.Assignment.ID {
			t.Fatalf("stored=%#v err=%v", stored, err)
		}
	})

	t.Run("concurrent reservations cannot overshoot tenant exposure", func(t *testing.T) {
		repository := factory(t)
		const requests = 32
		start := make(chan struct{})
		results := make(chan experiment.Reservation, requests)
		errorsSeen := make(chan error, requests)
		var wait sync.WaitGroup
		for index := 0; index < requests; index++ {
			request := ValidExperimentRequest(t, 1, experiment.BucketCount)
			request.Manifest, _ = experiment.NewManifest(experiment.ManifestSpec{Version: "contract.v1", TenantID: "tenant_a", ProcedureID: "proc_a", ChampionVersionID: "pv_champion", ChallengerVersionIDs: []string{"pv_challenger"}, ExperimentEpoch: 1, ChallengerExposurePPM: experiment.BucketCount, MaximumConcurrentTrials: 1, TenantExposureLimit: 5, GlobalExposureLimit: 100, TenantUnsafeOutcomeCeiling: 0, GlobalUnsafeOutcomeCeiling: 0, ExposureWindowSeconds: 3600, AssignmentTTLSeconds: 600})
			request.QueryBucketHash = fmt.Sprintf("%064x", index+1)
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				reservation, err := repository.Reserve(context.Background(), request)
				results <- reservation
				errorsSeen <- err
			}()
		}
		close(start)
		wait.Wait()
		close(results)
		close(errorsSeen)
		for err := range errorsSeen {
			if err != nil {
				t.Fatal(err)
			}
		}
		challengers := 0
		for result := range results {
			if result.Assignment.Challenger {
				challengers++
			}
		}
		if challengers != 5 {
			t.Fatalf("challenger reservations=%d want=5", challengers)
		}
	})

	t.Run("unsafe snapshot closes exploration immediately", func(t *testing.T) {
		repository := factory(t)
		request := ValidExperimentRequest(t, 1, experiment.BucketCount)
		request.Exposure.TenantUnsafeCount = request.Manifest.TenantUnsafeOutcomeCeiling + 1
		reservation, err := repository.Reserve(context.Background(), request)
		if err != nil || reservation.Assignment.Challenger || !experimentReason(reservation.Assignment.ReasonCodes, experiment.ReasonTenantUnsafeCeiling) {
			t.Fatalf("reservation=%#v err=%v", reservation, err)
		}
	})
}

func ValidExperimentRequest(t *testing.T, epoch uint64, ppm uint32) experiment.AssignmentRequest {
	t.Helper()
	manifest, err := experiment.NewManifest(experiment.ManifestSpec{Version: "contract.v1", TenantID: "tenant_a", ProcedureID: "proc_a", ChampionVersionID: "pv_champion", ChallengerVersionIDs: []string{"pv_challenger"}, ExperimentEpoch: epoch, ChallengerExposurePPM: ppm, MaximumConcurrentTrials: 1, TenantExposureLimit: 100, GlobalExposureLimit: 1000, TenantUnsafeOutcomeCeiling: 0, GlobalUnsafeOutcomeCeiling: 0, ExposureWindowSeconds: 3600, AssignmentTTLSeconds: 600})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	return experiment.AssignmentRequest{Manifest: manifest, QueryBucketHash: fmt.Sprintf("%064x", 42), Risk: experiment.RiskLow, Exposure: experiment.ExposureSnapshot{WindowStart: now}, AssignedAt: now.Add(time.Minute)}
}

func experimentReason(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
