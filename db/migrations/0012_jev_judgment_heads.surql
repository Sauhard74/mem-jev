-- Mutable CAS heads over the immutable Jev judgment successor chain.

DEFINE FIELD IF NOT EXISTS base_key ON TABLE jev_judgment TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS predecessor_hash ON TABLE jev_judgment TYPE option<string> READONLY;
DEFINE INDEX IF NOT EXISTS jev_judgment_base_created ON TABLE jev_judgment FIELDS tenant_id, base_key, created_at;

DEFINE TABLE IF NOT EXISTS jev_judgment_head SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE jev_judgment_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS base_key ON TABLE jev_judgment_head TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS current_judgment_key ON TABLE jev_judgment_head TYPE string;
DEFINE FIELD IF NOT EXISTS current_content_hash ON TABLE jev_judgment_head TYPE string ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE jev_judgment_head TYPE datetime;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE jev_judgment_head TYPE string READONLY;
DEFINE INDEX IF NOT EXISTS jev_judgment_head_tenant_base_unique ON TABLE jev_judgment_head FIELDS tenant_id, base_key UNIQUE;
