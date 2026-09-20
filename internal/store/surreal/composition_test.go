//go:build integration

package surreal

import (
	"context"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store/storetest"
	"github.com/sauhard74/mem-jev/internal/testinfra"
)

func TestCompositionRepositoryContract(t *testing.T) {
	storetest.RunCompositionContract(t, func(t *testing.T) storetest.CompositionRepository {
		db := testinfra.StartSurreal(t, surrealImage)
		if err := NewMigrator(db).Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		return NewCompositionRepository(db)
	})
}
