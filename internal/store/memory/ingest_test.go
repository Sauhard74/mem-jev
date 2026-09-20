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

func TestCommitRollsBackAfterEventFailure(t *testing.T) {
	repository := newIngestRepository(failureAfterEvents)
	_, err := repository.Commit(context.Background(), storetest.ValidCommitRequest(t))
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("error = %v, want %v", err, errInjectedFailure)
	}
	storetest.AssertCounts(t, repository, store.AggregateCounts{})
}
