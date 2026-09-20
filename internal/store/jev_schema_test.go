package store_test

import (
	"strings"
	"testing"

	dbmigrations "github.com/sauhard74/mem-jev/db/migrations"
)

func TestJevMigrationDefinesImmutableTenantScopedLedger(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0010_jev_judgment_ledger.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, required := range []string{
		"DEFINE TABLE IF NOT EXISTS jev_rubric_manifest SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS jev_judgment SCHEMAFULL PERMISSIONS NONE",
		"DEFINE INDEX IF NOT EXISTS jev_judgment_tenant_key_unique",
		"FIELDS tenant_id, judgment_key UNIQUE",
		"DEFINE INDEX IF NOT EXISTS jev_judgment_expiry",
		"canonical_judgment",
		"TYPE string READONLY",
	} {
		if !strings.Contains(schema, required) {
			t.Errorf("migration missing %q", required)
		}
	}
}
