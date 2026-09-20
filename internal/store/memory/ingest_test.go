package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestCommitContract(t *testing.T) {
	storetest.RunIngestContract(t, func(*testing.T) storetest.Repository {
		return NewIngestRepository()
	})
}

func TestOutcomeCommitContract(t *testing.T) {
	storetest.RunOutcomeContract(t, func(*testing.T) (store.IngestRepository, storetest.OutcomeRepository) {
		repository := NewIngestRepository()
		return repository, repository
	})
}

func TestProjectionContract(t *testing.T) {
	storetest.RunProjectionContract(t, func(*testing.T) storetest.ProjectionRepository {
		return NewProjectionRepository()
	})
}

func TestCommitRollsBackAfterEventFailure(t *testing.T) {
	repository := newIngestRepository(failureAfterEvents)
	_, err := repository.Commit(context.Background(), storetest.ValidCommitRequest(t))
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("error = %v, want %v", err, errInjectedFailure)
	}
	storetest.AssertCounts(t, repository, store.AggregateCounts{})
}
