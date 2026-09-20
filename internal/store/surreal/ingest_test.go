//go:build integration

package surreal

import (
	"context"
	"errors"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/testinfra"
)

func TestCommitContract(t *testing.T) {
	storetest.RunIngestContract(t, func(t *testing.T) storetest.Repository {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		return NewIngestRepository(db)
	})
}

func TestCommitRollsBackAfterEventFailure(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	repository := newIngestRepository(db, failureAfterEvents)
	_, err := repository.Commit(context.Background(), storetest.ValidCommitRequest(t))
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("error = %v, want %v", err, errInjectedFailure)
	}
	storetest.AssertCounts(t, repository, store.AggregateCounts{})
}
