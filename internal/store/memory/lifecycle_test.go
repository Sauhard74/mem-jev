package memory_test

import (
	"testing"

	"github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestLifecycleRepositoryContract(t *testing.T) {
	storetest.RunLifecycleContract(t, func(*testing.T) storetest.LifecycleFixture {
		projection := memory.NewProjectionRepository()
		return storetest.LifecycleFixture{Lifecycle: memory.NewLifecycleRepository(projection), Projection: projection}
	})
}
