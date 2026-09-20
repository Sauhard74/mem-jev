-- New assignment fields are optional only for compatibility with pre-v8 rows.
-- The v1 reservation repository writes every field atomically.
DEFINE FIELD IF NOT EXISTS procedure_id ON TABLE experiment_assignment TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS experiment_epoch ON TABLE experiment_assignment TYPE option<int> READONLY ASSERT $value = NONE OR $value > 0;
DEFINE FIELD IF NOT EXISTS query_bucket_hash ON TABLE experiment_assignment TYPE option<string> READONLY ASSERT $value = NONE OR string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS challenger ON TABLE experiment_assignment TYPE option<bool> READONLY;
DEFINE FIELD IF NOT EXISTS risk ON TABLE experiment_assignment TYPE option<string> READONLY ASSERT $value = NONE OR $value IN ["low", "medium", "high", "critical"];
DEFINE FIELD IF NOT EXISTS reason_codes ON TABLE experiment_assignment TYPE option<array<string>> READONLY;
DEFINE FIELD IF NOT EXISTS window_start ON TABLE experiment_assignment TYPE option<datetime> READONLY;
DEFINE INDEX IF NOT EXISTS experiment_assignment_tenant_window ON TABLE experiment_assignment FIELDS tenant_id, window_start, challenger;
DEFINE INDEX IF NOT EXISTS experiment_assignment_global_window ON TABLE experiment_assignment FIELDS window_start, challenger;

-- Mutable reservation heads serialize budget checks. They are reconstructable
-- from immutable assignments and aggregated unsafe outcomes.
DEFINE TABLE IF NOT EXISTS experiment_tenant_budget_head SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE experiment_tenant_budget_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS window_start ON TABLE experiment_tenant_budget_head TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS exposure_count ON TABLE experiment_tenant_budget_head TYPE int ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS unsafe_count ON TABLE experiment_tenant_budget_head TYPE int ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE experiment_tenant_budget_head TYPE datetime;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE experiment_tenant_budget_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE experiment_tenant_budget_head TYPE string ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS experiment_tenant_budget_head_unique ON TABLE experiment_tenant_budget_head FIELDS tenant_id, window_start UNIQUE;

DEFINE TABLE IF NOT EXISTS experiment_global_budget_head SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE experiment_global_budget_head TYPE string READONLY ASSERT $value = "_global";
DEFINE FIELD IF NOT EXISTS window_start ON TABLE experiment_global_budget_head TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS exposure_count ON TABLE experiment_global_budget_head TYPE int ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS unsafe_count ON TABLE experiment_global_budget_head TYPE int ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE experiment_global_budget_head TYPE datetime;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE experiment_global_budget_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE experiment_global_budget_head TYPE string ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS experiment_global_budget_head_unique ON TABLE experiment_global_budget_head FIELDS tenant_id, window_start UNIQUE;
