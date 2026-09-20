//go:build integration

package surreal

import (
	"context"
	"testing"

	"github.com/sauhard74/mem-jev/internal/experiment"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/testinfra"
)

func TestExperimentRepositoryContract(t *testing.T) {
	storetest.RunExperimentContract(t, func(t *testing.T) experiment.Repository {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		return NewExperimentRepository(db)
	})
}
