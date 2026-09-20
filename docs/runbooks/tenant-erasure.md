# Tenant erasure

Tenant erasure is an operator-only workflow. First revoke the tenant credential from the mounted credential document and roll the API replicas so no new request can authenticate. Keep the worker running: the durable database fence created by the command rejects every create or update for that tenant, including writes from already-running workflows.

Set `MEMJEV_ERASURE_LEDGER_DIR` to a mode-0700 directory on an independently backed-up operator volume. Before destructive work, the command writes one mode-0600 intent file per request using fsync plus an atomic link and directory fsync. The ledger intentionally contains tenant identifiers and exact confirmations, so it is sensitive recovery material: encrypt it at rest, restrict it to the erasure operator, retain it longer than every data backup, and never include it inside a recovery snapshot.

Send the request on standard input so the tenant identifier is never exposed in process arguments:

```sh
printf '%s\n' '{"tenant_id":"tenant_a","request_id":"erase_01JABCDE1234567890","confirmation":"erase:tenant_a:erase_01JABCDE1234567890"}' | memjev-admin erase-tenant
```

The command validates the complete JSON document and ledger before external work, creates a durable database write fence, removes every S3 object version and delete marker under the hashed tenant prefix, verifies the prefix is empty, deletes tenant-owned online rows transactionally, and records a tenant-hash-only receipt. A failed archive deletion leaves the fence active and does not delete database rows; repeat the identical request after correcting the archive failure. A different request ID for an already fenced tenant is rejected.

Every restore requires the current independent ledger and replays it before health verification. This prevents an older snapshot from resurrecting erased data. Do not route traffic if ledger replay or the final health check fails.
