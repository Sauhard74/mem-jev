package memory_test

import (
	"testing"

	"github.com/sauhard74/mem-jev/internal/experiment"
	"github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestExperimentRepositoryContract(t *testing.T) {
	storetest.RunExperimentContract(t, func(*testing.T) experiment.Repository { return memory.NewExperimentRepository() })
}
