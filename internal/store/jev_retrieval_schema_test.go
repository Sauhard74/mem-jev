package store_test

import (
	"strings"
	"testing"

	dbmigrations "github.com/sauhard74/mem-jev/db/migrations"
)

func TestJevRetrievalProvenanceMigrationIsTenantBoundAndImmutable(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0011_retrieval_semantic_provenance.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, required := range []string{
		"DEFINE TABLE IF NOT EXISTS retrieval_semantic_judgment SCHEMAFULL PERMISSIONS NONE",
		"judgment_key",
		"judgment_content_hash",
		"rubric_manifest_id",
		"feature_values",
		"FIELDS tenant_id, retrieval_run_id, procedure_version_id UNIQUE",
		"TYPE string READONLY",
	} {
		if !strings.Contains(schema, required) {
			t.Errorf("migration missing %q", required)
		}
	}
}
