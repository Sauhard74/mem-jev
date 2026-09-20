package store_test

import (
	"strings"
	"testing"

	dbmigrations "github.com/sauhard74/mem-jev/db/migrations"
)

func TestCompositionMigrationDefinesProductionInvariants(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0004_composition_revisions.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	required := []string{
		"DEFINE TABLE IF NOT EXISTS planner_manifest SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS planner_serving_head SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS procedure_compatibility_edge SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS selection_record SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS selection_plan_node SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS selection_plan_edge SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS selection_plan_gap SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS lifecycle_policy_manifest SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS lifecycle_decision SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS experiment_manifest SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS experiment_assignment SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS experiment_exposure_counter SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS outcome_credit SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS promotion_evaluation SCHEMAFULL PERMISSIONS NONE",
		"DEFINE INDEX IF NOT EXISTS procedure_compatibility_edge_tenant_epoch_unique",
		"DEFINE INDEX IF NOT EXISTS selection_record_tenant_injection_unique",
		"DEFINE INDEX IF NOT EXISTS selection_record_tenant_idempotency_unique",
		"DEFINE INDEX IF NOT EXISTS selection_record_expiry",
		"DEFINE INDEX IF NOT EXISTS experiment_assignment_expiry",
		"DEFINE INDEX IF NOT EXISTS outcome_credit_expiry",
		"ASSERT string::len($value) = 64",
	}
	for _, fragment := range required {
		if !strings.Contains(schema, fragment) {
			t.Errorf("migration missing %q", fragment)
		}
	}
	for _, durable := range []string{"lifecycle_decision", "promotion_evaluation"} {
		section := tableSection(schema, durable)
		if strings.Contains(section, "expires_at") {
			t.Errorf("durable table %s must not expire", durable)
		}
		for _, mutable := range []string{"evidence_count TYPE int;", "success_count TYPE int;", "unsafe_count TYPE int;"} {
			if strings.Contains(section, mutable) {
				t.Errorf("durable table %s contains mutable evidence field %q", durable, mutable)
			}
		}
	}
}

func TestCompositionMigrationScopesEveryIdentityByTenant(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0004_composition_revisions.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, table := range []string{
		"planner_manifest", "planner_serving_head", "procedure_compatibility_edge", "selection_record",
		"selection_plan_node", "selection_plan_edge", "selection_plan_gap", "lifecycle_policy_manifest",
		"lifecycle_decision", "experiment_manifest", "experiment_assignment", "experiment_exposure_counter",
		"outcome_credit", "promotion_evaluation",
	} {
		section := tableSection(schema, table)
		if !strings.Contains(section, "tenant_id ON TABLE "+table+" TYPE string READONLY") {
			t.Errorf("table %s lacks immutable tenant key", table)
		}
		if !strings.Contains(section, "FIELDS tenant_id,") && !strings.Contains(section, "FIELDS tenant_id UNIQUE") {
			t.Errorf("table %s lacks a tenant-leading index", table)
		}
	}
}

func TestOutcomeCreditLinkageMigrationPreventsMultipleClaims(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0006_outcome_credit_linkage.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, fragment := range []string{"injection_id ON TABLE outcome_evidence", "task_execution_id ON TABLE outcome_evidence", "selection_hash ON TABLE outcome_credit", "outcome_credit_injection_unique", "FIELDS tenant_id, injection_id UNIQUE"} {
		if !strings.Contains(schema, fragment) {
			t.Errorf("migration missing %q", fragment)
		}
	}
}

func TestLifecycleHeadsEnforceSingleChampionAndImmutablePublicationResult(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0007_lifecycle_heads.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, fragment := range []string{
		"retrieval_document_id ON TABLE lifecycle_decision TYPE option<string> READONLY",
		"projection_epoch ON TABLE lifecycle_decision TYPE option<int> READONLY",
		"DEFINE TABLE IF NOT EXISTS lifecycle_version_head SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS lifecycle_champion_head SCHEMAFULL PERMISSIONS NONE",
		"lifecycle_version_head_unique",
		"lifecycle_champion_head_unique",
		"FIELDS tenant_id, procedure_id, lifecycle_policy_manifest_id UNIQUE",
	} {
		if !strings.Contains(schema, fragment) {
			t.Errorf("migration missing %q", fragment)
		}
	}
}

func TestExperimentReservationMigrationSerializesBudgetChecks(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0008_experiment_reservations.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, fragment := range []string{
		"challenger ON TABLE experiment_assignment TYPE option<bool> READONLY",
		"window_start ON TABLE experiment_assignment TYPE option<datetime> READONLY",
		"DEFINE TABLE IF NOT EXISTS experiment_tenant_budget_head SCHEMAFULL PERMISSIONS NONE",
		"DEFINE TABLE IF NOT EXISTS experiment_global_budget_head SCHEMAFULL PERMISSIONS NONE",
		"experiment_tenant_budget_head_unique",
		"experiment_global_budget_head_unique",
		"exposure_count ON TABLE experiment_tenant_budget_head TYPE int ASSERT $value >= 0",
		"unsafe_count ON TABLE experiment_global_budget_head TYPE int ASSERT $value >= 0",
	} {
		if !strings.Contains(schema, fragment) {
			t.Errorf("migration missing %q", fragment)
		}
	}
}

func TestCompatibilityAndMaintenanceMigrationIsDurableAndFenced(t *testing.T) {
	body, err := dbmigrations.Files.ReadFile("0009_compatibility_jobs.surql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(body)
	for _, fragment := range []string{
		"DEFINE TABLE IF NOT EXISTS compatibility_graph_projection SCHEMAFULL PERMISSIONS NONE",
		"compatibility_graph_projection_key_unique",
		"canonical_graph ON TABLE compatibility_graph_projection TYPE string READONLY",
		"DEFINE TABLE IF NOT EXISTS maintenance_job SCHEMAFULL PERMISSIONS NONE",
		"fencing_token ON TABLE maintenance_job TYPE int ASSERT $value >= 0",
		"maintenance_job_idempotency_unique",
		"maintenance_job_ready",
		"maintenance_job_lease",
		"DEFINE TABLE IF NOT EXISTS derived_projection_snapshot SCHEMAFULL PERMISSIONS NONE",
		"derived_projection_snapshot_epoch",
		"DEFINE TABLE IF NOT EXISTS derived_activation_permit SCHEMAFULL PERMISSIONS NONE",
		"derived_activation_permit_epoch_unique",
	} {
		if !strings.Contains(schema, fragment) {
			t.Errorf("migration missing %q", fragment)
		}
	}
}

func tableSection(schema, table string) string {
	start := strings.Index(schema, "DEFINE TABLE IF NOT EXISTS "+table+" ")
	if start < 0 {
		return ""
	}
	rest := schema[start:]
	if end := strings.Index(rest[len("DEFINE TABLE"):], "\nDEFINE TABLE IF NOT EXISTS "); end >= 0 {
		return rest[:len("DEFINE TABLE")+end]
	}
	return rest
}
