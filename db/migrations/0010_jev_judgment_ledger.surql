-- Immutable TypeSafe Jev rubric and judgment ledger.

DEFINE TABLE IF NOT EXISTS jev_rubric_manifest SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS rubric_manifest_id ON TABLE jev_rubric_manifest TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS provider ON TABLE jev_rubric_manifest TYPE string READONLY ASSERT $value = "typesafe";
DEFINE FIELD IF NOT EXISTS model ON TABLE jev_rubric_manifest TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS version ON TABLE jev_rubric_manifest TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS admission_version ON TABLE jev_rubric_manifest TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS canonical_rubric ON TABLE jev_rubric_manifest TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE jev_rubric_manifest TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE jev_rubric_manifest TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE jev_rubric_manifest TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS jev_rubric_manifest_id_unique ON TABLE jev_rubric_manifest FIELDS rubric_manifest_id UNIQUE;
DEFINE INDEX IF NOT EXISTS jev_rubric_manifest_model ON TABLE jev_rubric_manifest FIELDS provider, model, created_at;

DEFINE TABLE IF NOT EXISTS jev_judgment SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS judgment_key ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS query_hash ON TABLE jev_judgment TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS procedure_version_id ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS document_hash ON TABLE jev_judgment TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS environment_hash ON TABLE jev_judgment TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS policy_manifest_id ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS rubric_manifest_id ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS provider ON TABLE jev_judgment TYPE string READONLY ASSERT $value = "typesafe";
DEFINE FIELD IF NOT EXISTS model ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS canonical_judgment ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE jev_judgment TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS reusable_until ON TABLE jev_judgment TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE jev_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE jev_judgment TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS jev_judgment_tenant_key_unique ON TABLE jev_judgment FIELDS tenant_id, judgment_key UNIQUE;
DEFINE INDEX IF NOT EXISTS jev_judgment_expiry ON TABLE jev_judgment FIELDS tenant_id, reusable_until;
DEFINE INDEX IF NOT EXISTS jev_judgment_model_rubric ON TABLE jev_judgment FIELDS tenant_id, model, rubric_manifest_id, created_at;
