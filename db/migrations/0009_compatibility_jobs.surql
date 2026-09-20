-- Immutable compatibility graph headers authenticate the complete edge set,
-- including an intentionally empty graph.
DEFINE TABLE IF NOT EXISTS compatibility_graph_projection SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE compatibility_graph_projection TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS compatibility_graph_id ON TABLE compatibility_graph_projection TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE compatibility_graph_projection TYPE int READONLY ASSERT $value > 0;
DEFINE FIELD IF NOT EXISTS planner_manifest_id ON TABLE compatibility_graph_projection TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS policy_manifest_id ON TABLE compatibility_graph_projection TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS matrix_hash ON TABLE compatibility_graph_projection TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS candidate_set_hash ON TABLE compatibility_graph_projection TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS edge_count ON TABLE compatibility_graph_projection TYPE int READONLY ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS canonical_graph ON TABLE compatibility_graph_projection TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE compatibility_graph_projection TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE compatibility_graph_projection TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE compatibility_graph_projection TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS compatibility_graph_projection_id_unique ON TABLE compatibility_graph_projection FIELDS tenant_id, compatibility_graph_id UNIQUE;
DEFINE INDEX IF NOT EXISTS compatibility_graph_projection_key_unique ON TABLE compatibility_graph_projection FIELDS tenant_id, projection_epoch, planner_manifest_id, policy_manifest_id UNIQUE;

-- Durable operational work. Payload and identity are immutable; lease fields
-- are fenced mutable delivery state.
DEFINE TABLE IF NOT EXISTS maintenance_job SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE maintenance_job TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS maintenance_job_id ON TABLE maintenance_job TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS job_kind ON TABLE maintenance_job TYPE string READONLY ASSERT $value IN ["compatibility_projection", "lifecycle_evaluation", "exposure_aggregation", "transient_expiry", "lifecycle_rollback", "projection_rebuild"];
DEFINE FIELD IF NOT EXISTS idempotency_key_hash ON TABLE maintenance_job TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS canonical_payload ON TABLE maintenance_job TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS canonical_job ON TABLE maintenance_job TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS state ON TABLE maintenance_job TYPE string ASSERT $value IN ["pending", "leased", "completed", "dead_letter"];
DEFINE FIELD IF NOT EXISTS attempt_count ON TABLE maintenance_job TYPE int ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS fencing_token ON TABLE maintenance_job TYPE int ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS lease_owner ON TABLE maintenance_job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS lease_expires_at ON TABLE maintenance_job TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS available_at ON TABLE maintenance_job TYPE datetime;
DEFINE FIELD IF NOT EXISTS last_error_code ON TABLE maintenance_job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE maintenance_job TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE maintenance_job TYPE datetime;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE maintenance_job TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE maintenance_job TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS maintenance_job_tenant_id_unique ON TABLE maintenance_job FIELDS tenant_id, maintenance_job_id UNIQUE;
DEFINE INDEX IF NOT EXISTS maintenance_job_idempotency_unique ON TABLE maintenance_job FIELDS tenant_id, job_kind, idempotency_key_hash UNIQUE;
DEFINE INDEX IF NOT EXISTS maintenance_job_ready ON TABLE maintenance_job FIELDS state, available_at;
DEFINE INDEX IF NOT EXISTS maintenance_job_lease ON TABLE maintenance_job FIELDS state, lease_expires_at;

-- Rebuild snapshots and permits are immutable audit evidence. A serving head
-- may advance only when a permit authenticates byte-identical snapshots.
DEFINE TABLE IF NOT EXISTS derived_projection_snapshot SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE derived_projection_snapshot TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS derived_snapshot_id ON TABLE derived_projection_snapshot TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE derived_projection_snapshot TYPE int READONLY ASSERT $value > 0;
DEFINE FIELD IF NOT EXISTS role ON TABLE derived_projection_snapshot TYPE string READONLY ASSERT $value IN ["stored", "rebuilt"];
DEFINE FIELD IF NOT EXISTS canonical_snapshot ON TABLE derived_projection_snapshot TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE derived_projection_snapshot TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE derived_projection_snapshot TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE derived_projection_snapshot TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS derived_projection_snapshot_tenant_id_unique ON TABLE derived_projection_snapshot FIELDS tenant_id, derived_snapshot_id, role UNIQUE;
DEFINE INDEX IF NOT EXISTS derived_projection_snapshot_epoch ON TABLE derived_projection_snapshot FIELDS tenant_id, projection_epoch, role;

DEFINE TABLE IF NOT EXISTS derived_activation_permit SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE derived_activation_permit TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS activation_permit_id ON TABLE derived_activation_permit TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE derived_activation_permit TYPE int READONLY ASSERT $value > 0;
DEFINE FIELD IF NOT EXISTS stored_snapshot_id ON TABLE derived_activation_permit TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS rebuilt_snapshot_id ON TABLE derived_activation_permit TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS canonical_permit ON TABLE derived_activation_permit TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS authorized_at ON TABLE derived_activation_permit TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE derived_activation_permit TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE derived_activation_permit TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS derived_activation_permit_tenant_id_unique ON TABLE derived_activation_permit FIELDS tenant_id, activation_permit_id UNIQUE;
DEFINE INDEX IF NOT EXISTS derived_activation_permit_epoch_unique ON TABLE derived_activation_permit FIELDS tenant_id, projection_epoch, stored_snapshot_id, rebuilt_snapshot_id UNIQUE;
