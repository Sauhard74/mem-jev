-- Lifecycle heads are mutable serving projections over immutable decisions.
DEFINE FIELD IF NOT EXISTS retrieval_document_id ON TABLE lifecycle_decision TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE lifecycle_decision TYPE option<int> READONLY ASSERT $value = NONE OR $value > 0;

DEFINE TABLE IF NOT EXISTS lifecycle_version_head SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE lifecycle_version_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS procedure_id ON TABLE lifecycle_version_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS procedure_version_id ON TABLE lifecycle_version_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS lifecycle_policy_manifest_id ON TABLE lifecycle_version_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS lifecycle_state ON TABLE lifecycle_version_head TYPE string ASSERT $value IN ["candidate", "trial", "active", "retired", "quarantined"];
DEFINE FIELD IF NOT EXISTS lifecycle_decision_id ON TABLE lifecycle_version_head TYPE string;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE lifecycle_version_head TYPE int ASSERT $value > 0;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE lifecycle_version_head TYPE datetime;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE lifecycle_version_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE lifecycle_version_head TYPE string ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS lifecycle_version_head_unique ON TABLE lifecycle_version_head FIELDS tenant_id, procedure_version_id, lifecycle_policy_manifest_id UNIQUE;

DEFINE TABLE IF NOT EXISTS lifecycle_champion_head SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE lifecycle_champion_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS procedure_id ON TABLE lifecycle_champion_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS lifecycle_policy_manifest_id ON TABLE lifecycle_champion_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS procedure_version_id ON TABLE lifecycle_champion_head TYPE string;
DEFINE FIELD IF NOT EXISTS lifecycle_decision_id ON TABLE lifecycle_champion_head TYPE string;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE lifecycle_champion_head TYPE int ASSERT $value > 0;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE lifecycle_champion_head TYPE datetime;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE lifecycle_champion_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE lifecycle_champion_head TYPE string ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS lifecycle_champion_head_unique ON TABLE lifecycle_champion_head FIELDS tenant_id, procedure_id, lifecycle_policy_manifest_id UNIQUE;
