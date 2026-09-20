//go:build integration

package surreal

import (
	"context"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/testinfra"
)

func TestLifecycleRepositoryContract(t *testing.T) {
	storetest.RunLifecycleContract(t, func(t *testing.T) storetest.LifecycleFixture {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		seedProjectionOutcomes(t, db)
		projection := NewProjectionRepository(db)
		return storetest.LifecycleFixture{Lifecycle: NewLifecycleRepository(db), Projection: projection}
	})
}
