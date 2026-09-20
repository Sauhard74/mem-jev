# Composition, lifecycle, and experiment operations

This runbook covers the derived compatibility graph, append-only lifecycle decisions, champion heads, deterministic experiment assignments, and their durable maintenance jobs. None of these paths may depend on Jev availability.

## Invariants to check first

- Every agent-facing plan has a committed `selection_record` and opaque injection ID.
- A compatibility edge exists only when exact typed interfaces satisfy the configured schema matrix.
- A lifecycle state change has an immutable `lifecycle_decision` and a newer `retrieval_document` projection epoch. Historical documents are never updated.
- There is at most one `lifecycle_champion_head` per tenant, procedure family, and lifecycle policy.
- Every challenger response has a committed `experiment_assignment`; tenant and global budget heads are updated in the same transaction.
- Critical-risk traffic always resolves to the champion. High-risk traffic requires `tenant_high_risk_opt_in` in the immutable experiment manifest.

## Backlog and worker recovery

1. Inspect pending and expired leased rows in `maintenance_job`, grouped by `job_kind` and oldest `available_at`.
2. Confirm worker database and Temporal health before changing a lease. An expired lease is recoverable automatically; a live lease must not be reassigned manually.
3. Restart or replace the worker. The next claim increments `fencing_token`; completion from the old worker must fail with `stale maintenance job lease`.
   Claimed batches execute concurrently and each handler is bounded by its own lease deadline; a handler must never continue unbounded after its fencing authority expires.
4. For repeated failures, inspect `last_error_code`, the authenticated `canonical_job`, and the source projection epoch. Do not edit the payload or reset `attempt_count` in place. Enqueue a corrected job with a new idempotency identity.
5. A `dead_letter` job requires operator review. Preserve it for audit and enqueue a replacement only after the cause is understood.

## Unsafe rollback

1. Confirm the outcome is causally linked to the exact injection and procedure version. Associated evidence cannot trigger automatic rollback.
2. Recompute the lifecycle decision with the active policy manifest and the evidence cutoff recorded by the worker.
3. Verify the decision is `active -> retired` or `active -> quarantined` and includes the unsafe/stale/Wilson reason code.
4. Commit the decision through the lifecycle repository. The transaction must publish the new retrieval document epoch and remove the champion head together.
5. Pin retrieval to the new epoch and verify the old version is rejected by lifecycle eligibility. Old injections continue replaying their original snapshot.
6. Promote a challenger only through a separate lifecycle decision. Never overwrite the retired document or point the old decision at a new version.

## Rebuild and regional recovery

1. Rebuild into an isolated database or generation. Replay canonical records in stable tenant/time/ID order.
2. Rebuild compatibility graphs from typed interfaces and the recorded matrix, selection credit from immutable selections and outcomes, and lifecycle heads from ordered lifecycle decisions.
3. Produce a `derived-projection-snapshot.v1` for stored and rebuilt state. Compare all record IDs and hashes with `rebuild.CompareDerivedSnapshots`.
4. Activate only when an authenticated `derived-activation-permit.v1` is produced. Any mismatch leaves the old generation active and pages the owner.
5. Keep the prior projection epoch and planner serving head available for rollback. Activation and rollback move heads; they do not mutate evidence, plans, assignments, or decisions.

## Capacity qualification

Before production promotion, run `scripts/retrieval-release-qualification.sh` against at least 10 million canonical events and one million retrieval document revisions for at least 30 minutes at 100 RPS. Record p50/p95/p99 latency, API CPU/RSS, SurrealDB CPU/RSS, projection snapshot age, maintenance backlog by kind, dead letters, and transaction retry rate. A run is invalid if the corpus minima, tenant isolation checks, vector-generation checks, or rebuild equivalence checks are skipped.
