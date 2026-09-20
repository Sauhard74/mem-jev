# Evidence pipeline operations

The evidence pipeline is fail closed. A trace is never promoted from tool exit status alone. The API stores a canonical encrypted archive and an outbox intent; the worker validates the archive, independently reevaluates outcome evidence, resolves the tenant's immutable tool-contract snapshot, builds the causal graph, synthesizes a procedure or explicit abstention, and atomically publishes a projection.

## Health and service objectives

Check API `/readyz`, worker `/readyz`, SurrealDB, object storage, and Temporal before investigating individual workflows. The workflow ID is deterministic: `synthesis/<tenant>/trace/<trace>` for trace validation and `synthesis/<tenant>/trace/<trace>/outcome/<outcome>` for promotion. Starting an existing ID is a successful duplicate delivery.

Alert on these signals:

- `memjev.outcome.conflicts`: page only for a sustained tenant-specific surge; individual conflicts are expected and safely become `inconclusive`.
- `memjev.archive.corruptions`: page immediately. The affected workflow fails non-retryably and an immutable `pipeline_quarantine` audit event is written.
- `memjev.synthesis.abstentions{reason.code}`: ticket on a sustained rate increase; page if a previously stable tenant or tool moves sharply to `opaque_tool` or `uncovered_goal`.
- `memjev.outbox.lease_age`: warn when p95 exceeds 30 seconds for 10 minutes and page when p99 exceeds 120 seconds.
- `memjev.outbox.retries` and `memjev.outbox.dead_letters`: page on any dead letter or a sustained retry increase.
- `memjev.projection.lag`: warn when p95 exceeds 60 seconds; page when p99 exceeds 5 minutes.
- `memjev.projection.rebuild_mismatches`: page on any nonzero value and stop serving the affected version.

## Triage

1. Identify tenant, deterministic workflow ID, trace ID, outcome ID, and the last persisted `pipeline_stage_artifact`.
2. Confirm the outbox state. `completed` means Temporal accepted the deterministic workflow start, not that synthesis succeeded.
3. Inspect Temporal activity failure code. Retry only `archive_unavailable` and provider failures classified retryable. Do not retry `archive_corrupt`, `evidence_mismatch`, `tenant_mismatch`, or artifact conflicts until the source fault is understood.
4. For an abstention, inspect the immutable `synthesis_manifest.abstention_code`; abstention is a successful safety decision, not a partial procedure.
5. Never edit a procedure version, manifest, stage artifact, or audit event in place. Corrections create new outcome evidence and therefore a new deterministic workflow.

## Safe validation

Run `./scripts/integration.sh` from a clean checkout. It starts the pinned stack, verifies migrations and encrypted archives, restarts the worker, and exercises verified promotion, contradictory evidence, unknown-tool abstention, scoped negative paths, duplicate delivery, tenant denial, and corruption quarantine. `./scripts/smoke.sh` checks the public ingest and outcome APIs.
