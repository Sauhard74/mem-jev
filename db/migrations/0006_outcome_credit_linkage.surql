DEFINE FIELD IF NOT EXISTS injection_id ON TABLE outcome_evidence TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS task_execution_id ON TABLE outcome_evidence TYPE option<string> READONLY;

DEFINE FIELD IF NOT EXISTS outcome_credit_id ON TABLE outcome_receipt TYPE option<string> READONLY;
DEFINE FIELD IF NOT EXISTS credit_class ON TABLE outcome_receipt TYPE option<string> READONLY ASSERT $value = NONE OR $value IN ["causal_success", "associated_success", "causal_failure", "associated_failure", "unattributable"];

DEFINE FIELD IF NOT EXISTS selection_hash ON TABLE outcome_credit TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE INDEX IF NOT EXISTS outcome_credit_injection_unique ON TABLE outcome_credit FIELDS tenant_id, injection_id UNIQUE;
