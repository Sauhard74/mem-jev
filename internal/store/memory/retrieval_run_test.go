package memory_test

import (
	"context"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestMemoryRetrievalRunRepositoryContract(t *testing.T) {
	storetest.RunRetrievalRunContract(t, func(t *testing.T) store.RetrievalRunRepository {
		return newMemoryRunRepository(t)
	})
}

func newMemoryRunRepository(t *testing.T) *memory.RetrievalRunRepository {
	t.Helper()
	projection := memory.NewProjectionRepository()
	if _, err := projection.Publish(context.Background(), storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	return memory.NewRetrievalRunRepository(projection)
}

func TestMemoryRetrievalRunAcceptsCapturedHistoricalSnapshot(t *testing.T) {
	projection := memory.NewProjectionRepository()
	if _, err := projection.Publish(context.Background(), storetest.ValidProjection(t, 1, false)); err != nil {
		t.Fatal(err)
	}
	repository := memory.NewRetrievalRunRepository(projection)
	config := storetest.ValidServingConfig(t, "tenant_a")
	if err := repository.ActivateServingConfig(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.AcquireServingSnapshot(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = projection.Publish(context.Background(), storetest.ValidProjection(t, 2, false)); err != nil {
		t.Fatal(err)
	}
	if err = repository.SaveRetrievalRun(context.Background(), storetest.ValidRetrievalRun(t, snapshot, 'e')); err != nil {
		t.Fatalf("save against captured snapshot: %v", err)
	}
}
