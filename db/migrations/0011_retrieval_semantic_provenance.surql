-- Per-retrieval Jev provenance. The retrieval run remains the canonical replay record.

DEFINE TABLE IF NOT EXISTS retrieval_semantic_judgment SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS retrieval_run_id ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS procedure_version_id ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS judgment_key ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS disposition ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS judgment_content_hash ON TABLE retrieval_semantic_judgment TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS provider ON TABLE retrieval_semantic_judgment TYPE string READONLY ASSERT $value = "typesafe";
DEFINE FIELD IF NOT EXISTS model ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS rubric_manifest_id ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS feature_values ON TABLE retrieval_semantic_judgment TYPE object FLEXIBLE READONLY;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE retrieval_semantic_judgment TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS expires_at ON TABLE retrieval_semantic_judgment TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE retrieval_semantic_judgment TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE retrieval_semantic_judgment TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS retrieval_semantic_judgment_unique ON TABLE retrieval_semantic_judgment FIELDS tenant_id, retrieval_run_id, procedure_version_id UNIQUE;
DEFINE INDEX IF NOT EXISTS retrieval_semantic_judgment_key ON TABLE retrieval_semantic_judgment FIELDS tenant_id, judgment_key, created_at;
DEFINE INDEX IF NOT EXISTS retrieval_semantic_judgment_expiry ON TABLE retrieval_semantic_judgment FIELDS expires_at;
