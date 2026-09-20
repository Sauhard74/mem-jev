package memory_test

import (
	"testing"

	"github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestCompositionRepositoryContract(t *testing.T) {
	storetest.RunCompositionContract(t, func(*testing.T) storetest.CompositionRepository {
		return memory.NewCompositionRepository()
	})
}
