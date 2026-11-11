-- A correction may repeat a fact an earlier outcome on the same trace already
-- reported (worked -> didn't work -> worked). Evidence identity hashes only the
-- tenant, trace and fact, so evidence is unique per outcome, not per tenant.
REMOVE INDEX IF EXISTS verification_result_tenant_evidence_unique ON TABLE verification_result;
DEFINE INDEX IF NOT EXISTS verification_result_outcome_evidence_unique ON TABLE verification_result FIELDS tenant_id, outcome_id, evidence_id UNIQUE;

-- An outcome can report that the run of a recalled plan was unsafe. It raises
-- the plan's procedure_version.unsafe_outcome_count, which quarantines it.
DEFINE FIELD IF NOT EXISTS unsafe ON TABLE outcome_evidence TYPE option<bool> READONLY;
