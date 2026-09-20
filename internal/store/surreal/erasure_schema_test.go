package surreal

import (
	"strings"
	"testing"

	dbmigrations "github.com/sauhard74/mem-jev/db/migrations"
)

func TestEveryErasedTenantTableHasWriteFence(t *testing.T) {
	migration, err := dbmigrations.Files.ReadFile("0014_tenant_erasure_fence.surql")
	if err != nil {
		t.Fatal(err)
	}
	definition := string(migration)
	for _, table := range tenantErasureTables {
		want := "ON TABLE " + table + " WHEN"
		if !strings.Contains(definition, want) {
			t.Errorf("tenant table %q has no erasure write fence", table)
		}
	}
}
