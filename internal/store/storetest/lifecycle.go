package storetest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/lifecycle"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

type LifecycleProjectionRepository interface {
	projection.Repository
	RetrievalDocument(context.Context, domain.TenantID, string, uint64) (retrieval.Document, error)
}

type LifecycleFixture struct {
	Lifecycle  lifecycle.Repository
	Projection LifecycleProjectionRepository
}

type LifecycleFactory func(*testing.T) LifecycleFixture

func RunLifecycleContract(t *testing.T, factory LifecycleFactory) {
	t.Helper()
	t.Run("append-only publication and duplicate", func(t *testing.T) {
		fixture := factory(t)
		value := ValidProjection(t, 1, false)
		published, err := fixture.Projection.Publish(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		manifest := ValidLifecyclePolicy(t)
		publication := lifecyclePublication(t, fixture.Projection, value, published.ProjectionEpoch, manifest, lifecycle.Candidate, lifecycle.Trial, value.CreatedAt.Add(time.Minute), 1, 0)
		receipt, err := fixture.Lifecycle.Commit(context.Background(), publication)
		if err != nil || receipt.ProjectionEpoch <= published.ProjectionEpoch || receipt.Disposition != lifecycle.DispositionPublished {
			t.Fatalf("receipt=%#v err=%v", receipt, err)
		}
		oldDocument, oldErr := fixture.Projection.RetrievalDocument(context.Background(), value.TenantID, value.Version.ID, published.ProjectionEpoch)
		newDocument, newErr := fixture.Projection.RetrievalDocument(context.Background(), value.TenantID, value.Version.ID, receipt.ProjectionEpoch)
		if oldErr != nil || newErr != nil || oldDocument.Lifecycle != "candidate" || newDocument.Lifecycle != "trial" || oldDocument.ID == newDocument.ID {
			t.Fatalf("old=%#v new=%#v errors=%v/%v", oldDocument, newDocument, oldErr, newErr)
		}
		duplicate, err := fixture.Lifecycle.Commit(context.Background(), publication)
		if err != nil || duplicate.Disposition != lifecycle.DispositionDuplicate || duplicate.ProjectionEpoch != receipt.ProjectionEpoch {
			t.Fatalf("duplicate=%#v err=%v", duplicate, err)
		}
		stale := publication
		stale.Decision, err = lifecycle.Evaluate(lifecycle.EvaluationRequest{TenantID: value.TenantID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, PriorState: lifecycle.Candidate, Manifest: manifest, Evidence: lifecycle.Evidence{CausalSuccessCount: 1, LatestCausalAt: value.CreatedAt.Add(2 * time.Minute)}, EvidenceCutoffAt: value.CreatedAt.Add(2 * time.Minute), EvaluatedAt: value.CreatedAt.Add(2 * time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Lifecycle.Commit(context.Background(), stale); !errors.Is(err, lifecycle.ErrLifecycleConflict) {
			t.Fatalf("stale source accepted: %v", err)
		}
	})

	t.Run("one champion under concurrent contention and rollback", func(t *testing.T) {
		fixture := factory(t)
		manifest := ValidLifecyclePolicy(t)
		values := []projection.Projection{ValidProjection(t, 1, false), ValidProjection(t, 2, true)}
		trialPublications := make([]lifecycle.Publication, 2)
		for index, value := range values {
			published, err := fixture.Projection.Publish(context.Background(), value)
			if err != nil {
				t.Fatal(err)
			}
			candidate := lifecyclePublication(t, fixture.Projection, value, published.ProjectionEpoch, manifest, lifecycle.Candidate, lifecycle.Trial, value.CreatedAt.Add(time.Minute), 1, 0)
			trialReceipt, err := fixture.Lifecycle.Commit(context.Background(), candidate)
			if err != nil {
				t.Fatal(err)
			}
			trialPublications[index] = lifecyclePublication(t, fixture.Projection, value, trialReceipt.ProjectionEpoch, manifest, lifecycle.Trial, lifecycle.Active, value.CreatedAt.Add(2*time.Minute), 10, 0)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		var wait sync.WaitGroup
		for _, publication := range trialPublications {
			wait.Add(1)
			go func(value lifecycle.Publication) {
				defer wait.Done()
				<-start
				_, err := fixture.Lifecycle.Commit(context.Background(), value)
				results <- err
			}(publication)
		}
		close(start)
		wait.Wait()
		close(results)
		conflicts := 0
		for err := range results {
			if err == nil {
				continue
			} else if errors.Is(err, lifecycle.ErrChampionConflict) {
				conflicts++
			} else {
				t.Fatal(err)
			}
		}
		champion, err := fixture.Lifecycle.Champion(context.Background(), values[0].TenantID, values[0].Family.ID, manifest.ID)
		if err != nil || conflicts != 1 || champion.ProcedureVersionID == "" {
			t.Fatalf("champion=%#v conflicts=%d err=%v", champion, conflicts, err)
		}
		var winnerValue projection.Projection
		for _, value := range values {
			if value.Version.ID == champion.ProcedureVersionID {
				winnerValue = value
			}
		}
		head, err := fixture.Lifecycle.VersionHead(context.Background(), champion.TenantID, champion.ProcedureVersionID, manifest.ID)
		if err != nil {
			t.Fatal(err)
		}
		rollback := lifecyclePublication(t, fixture.Projection, winnerValue, head.ProjectionEpoch, manifest, lifecycle.Active, lifecycle.Retired, winnerValue.CreatedAt.Add(3*time.Minute), 10, 1)
		if _, err := fixture.Lifecycle.Commit(context.Background(), rollback); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.Lifecycle.Champion(context.Background(), champion.TenantID, champion.ProcedureID, manifest.ID); !errors.Is(err, lifecycle.ErrLifecycleNotFound) {
			t.Fatalf("champion survived rollback: %v", err)
		}
	})

	t.Run("deterministic rebuild equivalence", func(t *testing.T) {
		left, right := factory(t), factory(t)
		value := ValidProjection(t, 1, false)
		manifest := ValidLifecyclePolicy(t)
		var receipts []lifecycle.Receipt
		for _, fixture := range []LifecycleFixture{left, right} {
			published, err := fixture.Projection.Publish(context.Background(), value)
			if err != nil {
				t.Fatal(err)
			}
			publication := lifecyclePublication(t, fixture.Projection, value, published.ProjectionEpoch, manifest, lifecycle.Candidate, lifecycle.Trial, value.CreatedAt.Add(time.Minute), 1, 0)
			receipt, err := fixture.Lifecycle.Commit(context.Background(), publication)
			if err != nil {
				t.Fatal(err)
			}
			receipts = append(receipts, receipt)
		}
		if receipts[0] != receipts[1] {
			t.Fatalf("rebuild receipts differ: %#v %#v", receipts[0], receipts[1])
		}
		leftDocument, leftErr := left.Projection.RetrievalDocument(context.Background(), value.TenantID, value.Version.ID, receipts[0].ProjectionEpoch)
		rightDocument, rightErr := right.Projection.RetrievalDocument(context.Background(), value.TenantID, value.Version.ID, receipts[1].ProjectionEpoch)
		if leftErr != nil || rightErr != nil || !bytes.Equal(leftDocument.CanonicalJSON, rightDocument.CanonicalJSON) {
			t.Fatalf("rebuilt documents differ: %v %v", leftErr, rightErr)
		}
	})

	t.Run("policy rollover bootstraps from current immutable document", func(t *testing.T) {
		fixture := factory(t)
		value := ValidProjection(t, 1, false)
		published, err := fixture.Projection.Publish(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		firstPolicy := ValidLifecyclePolicy(t)
		trial := lifecyclePublication(t, fixture.Projection, value, published.ProjectionEpoch, firstPolicy, lifecycle.Candidate, lifecycle.Trial, value.CreatedAt.Add(time.Minute), 1, 0)
		trialReceipt, err := fixture.Lifecycle.Commit(context.Background(), trial)
		if err != nil {
			t.Fatal(err)
		}
		secondPolicy, err := lifecycle.NewPolicyManifest(lifecycle.PolicySpec{Version: "production-test-v2", WilsonZSquaredPPM: 1, TrialMinimumCausalSamples: 1, TrialMinimumWilsonLowerBoundPPM: 0, ActiveMinimumCausalSamples: 1, ActiveMinimumWilsonLowerBoundPPM: 0, TrialUnsafeOutcomeCeiling: 0, ActiveUnsafeOutcomeCeiling: 0, ImmediateQuarantineUnsafeCount: 2, EvidenceFreshnessSeconds: 3600, MinimumTrialSeconds: 1})
		if err != nil {
			t.Fatal(err)
		}
		activate := lifecyclePublication(t, fixture.Projection, value, trialReceipt.ProjectionEpoch, secondPolicy, lifecycle.Trial, lifecycle.Active, value.CreatedAt.Add(2*time.Minute), 10, 0)
		if _, err := fixture.Lifecycle.Commit(context.Background(), activate); err != nil {
			t.Fatal(err)
		}
		champion, err := fixture.Lifecycle.Champion(context.Background(), value.TenantID, value.Family.ID, secondPolicy.ID)
		if err != nil || champion.ProcedureVersionID != value.Version.ID {
			t.Fatalf("rollover champion=%#v err=%v", champion, err)
		}
	})
}

func ValidLifecyclePolicy(t *testing.T) lifecycle.PolicyManifest {
	t.Helper()
	manifest, err := lifecycle.NewPolicyManifest(lifecycle.PolicySpec{Version: "production-test", WilsonZSquaredPPM: 1, TrialMinimumCausalSamples: 1, TrialMinimumWilsonLowerBoundPPM: 0, ActiveMinimumCausalSamples: 1, ActiveMinimumWilsonLowerBoundPPM: 0, TrialUnsafeOutcomeCeiling: 0, ActiveUnsafeOutcomeCeiling: 0, ImmediateQuarantineUnsafeCount: 2, EvidenceFreshnessSeconds: 3600, MinimumTrialSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func lifecyclePublication(t *testing.T, repository LifecycleProjectionRepository, value projection.Projection, sourceEpoch uint64, manifest lifecycle.PolicyManifest, prior, next lifecycle.State, at time.Time, successes, unsafe uint64) lifecycle.Publication {
	t.Helper()
	document, err := repository.RetrievalDocument(context.Background(), value.TenantID, value.Version.ID, sourceEpoch)
	if err != nil {
		t.Fatal(err)
	}
	evidence := lifecycle.Evidence{CausalSuccessCount: successes, UnsafeOutcomeCount: unsafe, LatestCausalAt: at}
	if prior == lifecycle.Trial {
		evidence.TrialStartedAt = at.Add(-time.Minute)
	}
	request := lifecycle.EvaluationRequest{TenantID: value.TenantID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, PriorState: prior, Manifest: manifest, Evidence: evidence, EvidenceCutoffAt: at, EvaluatedAt: at}
	decision, err := lifecycle.Evaluate(request)
	if err != nil || decision.NextState != next {
		t.Fatalf("decision=%#v want=%s err=%v", decision, next, err)
	}
	return lifecycle.Publication{Manifest: manifest, Decision: decision, SourceDocument: document, SourceEpoch: sourceEpoch}
}
