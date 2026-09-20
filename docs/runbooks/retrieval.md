# Retrieval operations

The retrieval response is valid only after its decision record commits. A selected response identifies the immutable projection epoch, eligibility policy, ranker, and channel manifests used to produce it. Never reconstruct a historical answer from current indexes.

## Triage order

1. Check API readiness, SurrealDB quorum, and `memjev.retrieval.persistence_failures`.
2. Split latency by channel and result code. Required channels are exact, lexical, facet, and graph; any sustained failure is an availability incident. Vector and Jev are optional and must degrade without changing hard gates.
3. Inspect the run through `ExplainRetrieval` using a credential for the owning tenant. The explanation contains hashes, manifest IDs, channel membership, gate facts, scores, and ranks; it never returns the raw query.
4. Confirm the serving head references immutable manifests that exist for the same tenant. Do not edit a manifest in place. Publish a new content-addressed manifest and atomically activate a new serving configuration.

## Safe rollback

Move the tenant serving head to the last known-good immutable serving configuration. Existing runs remain replayable against their captured epoch and manifests. If the underlying epoch is corrupt, stop retrieval for the affected tenant, quarantine the epoch, rebuild from canonical projections, and compare document-set hashes before activation.

## Key rotation and retention

`MEMJEV_RETRIEVAL_QUERY_KEY_ID` names an exact 256-bit query-envelope key version. Rotate by deploying a new key ID and key together; retain old key material until every run encrypted with it has expired. Retrieval runs are transient and may not live longer than 30 days. Deleting expired runs must include channel hits, gate decisions, and ranks in the same maintenance operation.

## Jev boundary

Jev is not required for the core path. If it is disabled or unavailable, the exact-effect route is omitted and deterministic lexical, facet, and graph retrieval continues. A future Jev judgment may add a recorded pointwise feature only after eligibility; it cannot change a rejection or select a procedure by itself.
