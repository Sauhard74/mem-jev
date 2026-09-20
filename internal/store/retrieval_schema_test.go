package store_test

import (
	"strings"
	"testing"

	dbmigrations "github.com/sauhard74/mem-jev/db/migrations"
)

func TestRetrievalMigrationDefinesProductionInvariants(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0003_core_retrieval.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	required := []string{
		"DEFINE TABLE IF NOT EXISTS projection_epoch SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS projection_epoch_head SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS retrieval_document SCHEMAFULL",
		"DEFINE ANALYZER IF NOT EXISTS retrieval_text_analyzer",
		"FULLTEXT ANALYZER retrieval_text_analyzer BM25",
		"DEFINE TABLE IF NOT EXISTS embedding_manifest SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS embedding_value SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS embedding_index_generation SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS ranker_manifest SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS eligibility_policy_manifest SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS retrieval_run SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS retrieval_channel_hit SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS retrieval_gate_decision SCHEMAFULL",
		"DEFINE TABLE IF NOT EXISTS retrieval_ranked_candidate SCHEMAFULL",
		"ASSERT $value = in.tenant_id AND $value = out.tenant_id",
		"DEFINE INDEX IF NOT EXISTS retrieval_run_expiry",
	}
	for _, fragment := range required {
		if !strings.Contains(schema, fragment) {
			t.Errorf("migration missing %q", fragment)
		}
	}
	if strings.Contains(schema, " HNSW ") {
		t.Fatal("production schema must not use an in-memory HNSW index at target scale")
	}
}
