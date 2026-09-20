# Outbox recovery

Outbox delivery is at least once. Correctness comes from database-time leases, monotonic fencing tokens, deterministic Temporal workflow IDs, and immutable stage artifacts.

## Diagnose

Query by tenant and workflow ID. Record `state`, `attempt_count`, `lease_owner`, `lease_generation`, `lease_expires_at`, `available_at`, and `last_error_code`. Compare database time—not operator workstation time—to lease expiry. Confirm whether Temporal already contains the deterministic workflow ID.

## Recovery rules

- `pending`: leave it for normal polling unless `available_at` is unexpectedly old; then investigate worker readiness and Temporal connectivity.
- `leased` with an unexpired lease: do not intervene. A second worker must not bypass the fencing token.
- `leased` with an expired lease: normal claiming safely increments `lease_generation`; restart or scale a healthy worker.
- `completed`: delivery to Temporal succeeded. Inspect Temporal and stage artifacts for workflow completion; do not reset the row merely because synthesis later abstained or failed.
- `dead_letter`: classify `last_error_code`, repair the external/configuration cause, and create an audited replay intent. Do not edit attempt counts or reuse a stale fencing token.

Duplicate starts are expected and treated as success when the workflow ID matches. A duplicate with different immutable input is a conflict and must be quarantined, not forced through.

## Verification after recovery

Watch `memjev.outbox.lease_age`, retries, dead letters, and projection lag return to baseline. Confirm that each recovered outcome has exactly one synthesis manifest, at most one immutable procedure version for the same identity, and byte-identical canonical projection on replay.
