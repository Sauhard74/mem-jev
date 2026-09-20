# Retrieval replay and audit

Clients must send a 16–128 byte printable `Idempotency-Key` on Retrieve. The server hashes it and derives a tenant-scoped run ID. The raw key is never stored. Reusing the key with the same canonical query returns the persisted decision without executing channels again; reusing it with a different query returns `idempotency_conflict` and increments the replay-mismatch alert.

Use `ExplainRetrieval` with the run ID and an owning-tenant credential. Verify:

- query hash and projection epoch match the original response;
- policy, ranker, and index manifest IDs still resolve to the immutable stored bytes;
- channel hits, gate facts, integer fused score, final score, and total-order rank match the run record;
- selected version IDs exist in the captured document epoch;
- the encrypted query envelope starts with the expected key ID and contains no plaintext.

Do not replay by rerunning current indexes. For incident reconstruction, load the stored run and captured retrieval documents, validate every content hash, and regenerate the response from the persisted ranks. A missing historical document, manifest mutation, duplicate document, or hash mismatch is corruption: stop serving that tenant and preserve the records for investigation.
