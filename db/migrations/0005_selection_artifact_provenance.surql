-- Selection replay compares the immutable draft hash while the full record hash
-- also binds repository-assigned creation and expiry times.
DEFINE FIELD IF NOT EXISTS draft_hash ON TABLE selection_record TYPE option<string> READONLY;

-- Partial-plan gaps preserve both requirement and requested-goal provenance.
DEFINE FIELD IF NOT EXISTS procedure_version_id ON TABLE selection_plan_gap TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS goal_predicate_id ON TABLE selection_plan_gap TYPE option<string> READONLY;
