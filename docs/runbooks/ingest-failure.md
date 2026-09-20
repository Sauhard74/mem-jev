# Ingest failure response

| Failure | Observable symptom | Safe retry | Operator action |
|---|---|---|---|
| Archive unavailable | `archive_unavailable`, retryable; no ledger receipt | Retry the identical body and idempotency key with backoff | Investigate S3 health, encryption policy, credentials, and latency if sustained |
| Archive succeeds, database fails | Retryable database/internal failure; one unreferenced content-addressed object may exist | Retry identically; conditional archive creation reuses the object | No immediate action; alert and sweep only objects older than the documented reconciliation window |
| Transaction conflict | Increased `memjev.transaction.retries`; request normally succeeds internally | Client retries only if the final response is retryable | Investigate hot tenant/idempotency contention when retries exhaust |
| Idempotency conflict | `idempotency_conflict`, non-retryable | Do not retry with that key; compare caller intent and issue a new key only for a genuinely new operation | Investigate buggy or replaying clients; never mutate the prior receipt |
| Database unavailable | Readiness is `NOT_SERVING`; ingest returns unavailable/internal without a receipt | Retry identically after readiness recovers | Restore SurrealDB quorum/connectivity; do not delete archive objects during recovery |
| Outbox backlog | Accepted receipts continue while pending outbox age/count rises | Do not replay accepted ingest solely to restart synthesis | Restore workers/workflow service, inspect deterministic workflow IDs, and replay pending jobs idempotently |

Never log or paste submitted task text, commands, bearer tokens, raw canonical objects, or tenant IDs into incident channels. Correlate with request ID, receipt ID, trace ID, bounded tenant hash, reason code, and timestamps.
