package surreal

const exactCandidateQuery = `
SELECT procedure_version_id, projection_epoch, 1.0 AS score
FROM retrieval_document
WHERE tenant_id = $tenant_id AND projection_epoch <= $epoch AND intent_hash = $intent_hash AND effect_signature_hash = $effect_hash
ORDER BY projection_epoch DESC, procedure_version_id ASC
LIMIT $scan_limit`

const exactEffectInferenceQuery = `
SELECT effect_signature_hash
FROM retrieval_document
WHERE tenant_id = $tenant_id AND projection_epoch <= $epoch AND intent_hash = $intent_hash
GROUP BY effect_signature_hash
ORDER BY effect_signature_hash ASC
LIMIT 2`

const lexicalCandidateQuery = `
SELECT procedure_version_id, projection_epoch, search::score(0) AS score
FROM retrieval_document
WHERE tenant_id = $tenant_id AND projection_epoch <= $epoch AND task_text @0@ $task
ORDER BY score DESC, projection_epoch DESC, procedure_version_id ASC
LIMIT $scan_limit`

const facetCandidateQuery = `
SELECT procedure_version_id, projection_epoch,
       type::float(if environment_scope_hash = $environment_hash { 4 } else { 0 } +
        if harness_name = $harness { 2 } else { 0 } +
        if tool_contract_version_ids CONTAINSANY $contracts { 1 } else { 0 }) AS score
FROM retrieval_document
WHERE tenant_id = $tenant_id AND projection_epoch <= $epoch AND
      (environment_scope_hash = $environment_hash OR harness_name = $harness OR tool_contract_version_ids CONTAINSANY $contracts)
ORDER BY score DESC, projection_epoch DESC, procedure_version_id ASC
LIMIT $scan_limit`

const graphCandidateQuery = `
SELECT in.procedure_version_id AS procedure_version_id, in.projection_epoch AS projection_epoch, type::float(count()) AS score
FROM retrieval_uses_tool
WHERE tenant_id = $tenant_id AND in.projection_epoch <= $epoch AND out.contract_version_id IN $contracts
GROUP BY procedure_version_id, projection_epoch
ORDER BY score DESC, projection_epoch DESC, procedure_version_id ASC
LIMIT $scan_limit`
