-- Non-content tombstone proving completion of an operator-authorized tenant erasure.

DEFINE TABLE IF NOT EXISTS tenant_erasure_receipt SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS request_id ON TABLE tenant_erasure_receipt TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS tenant_hash ON TABLE tenant_erasure_receipt TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS archive_objects_deleted ON TABLE tenant_erasure_receipt TYPE int READONLY ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS completed_at ON TABLE tenant_erasure_receipt TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS canonical_receipt ON TABLE tenant_erasure_receipt TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE tenant_erasure_receipt TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE tenant_erasure_receipt TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS tenant_erasure_receipt_request_unique ON TABLE tenant_erasure_receipt FIELDS request_id UNIQUE;
DEFINE INDEX IF NOT EXISTS tenant_erasure_receipt_tenant_hash ON TABLE tenant_erasure_receipt FIELDS tenant_hash, completed_at;
