# Retrieval operations

The retrieval response is valid only after its decision record commits. A selected response identifies the immutable projection epoch, eligibility policy, ranker, and channel manifests used to produce it. Never reconstruct a historical answer from current indexes. Idempotency binds both the canonical query and the server-owned authorization context (tenant, recall consent, and allowed residency regions); a key reused under different context must return a conflict.

The request never supplies an effect-signature hash. For the exact-intent channel, the server examines the tenant's immutable documents at the captured projection epoch and binds the sole effect signature only when that corpus inference is unambiguous. Zero or multiple signatures produce no exact hit; lexical, facet, graph, and vector retrieval continue, followed by the same hard gates. This prevents the server from guessing an effect or trusting a client-controlled routing key.

## Triage order

1. Check API readiness, SurrealDB quorum, and `memjev.retrieval.persistence_failures`.
2. Check `memjev.retrieval.snapshot_age`, `memjev.retrieval.vector_staleness`, and the active `ranker.manifest_id` series before changing serving heads.
3. Split latency by channel and result code. Required channels are exact, lexical, facet, and graph; any sustained failure is an availability incident. Vector and Jev are optional and must degrade without changing hard gates.
4. Inspect the run through `ExplainRetrieval` using a credential for the owning tenant. The explanation contains hashes, manifest IDs, channel membership, gate facts, scores, and ranks; it never returns the raw query.
5. Confirm the serving head references immutable manifests that exist for the same tenant. Do not edit a manifest in place. Publish a new content-addressed manifest and atomically activate a new serving configuration.

## Safe rollback

Move the tenant serving head to the last known-good immutable serving configuration. Existing runs remain replayable against their captured epoch and manifests. If the underlying epoch is corrupt, stop retrieval for the affected tenant, quarantine the epoch, rebuild from canonical projections, and compare document-set hashes before activation.

## Key rotation and retention

`MEMJEV_RETRIEVAL_QUERY_KEY_ID` names an exact 256-bit query-envelope key version. Rotate by deploying a new key ID and key together; retain old key material until every run encrypted with it has expired. Retrieval runs are transient and may not live longer than 30 days. Deleting expired runs must include channel hits, gate decisions, and ranks in the same maintenance operation.

## Jev boundary

Jev is not required for the core path. It evaluates only eligible candidates in a deterministic ambiguity band, and its quantized features feed the same immutable integer ranker used by the preliminary pass. It cannot change an eligibility rejection. If TypeSafe is disabled or unavailable, explicit zero missing-values reproduce the no-Jev ordering. See [Jev ranking enhancement operations](jev.md) for secret mounting, consent, manifests, alerts, and rollback.

## Default serving configuration

A tenant needs an active serving configuration (eligibility policy, ranker, and index manifests) before recall can return anything. With `MEMJEV_RETRIEVAL_AUTO_ACTIVATE=true` (the default when `MEMJEV_ENVIRONMENT=development`), the API activates memJev's default configuration on a tenant's first recall:

- The policy admits `active` and `trial` procedures, plus new `candidate` procedures as advisory guidance, up to medium risk, validated by `outcome-policy.v1` within 90 days, in the regions listed in `MEMJEV_RETRIEVAL_DEFAULT_REGIONS` (default `local` in development; required when auto-activation is on elsewhere).
- The ranker uses the exact, lexical, facet, graph, and vector channels. When Jev is enabled it also declares the five Jev features.
- Missing index manifests for the configured channels are created. A vector index is included only if its manifest was already provisioned.

Default manifests have versions starting with `memjev-default`. The API replaces an active configuration only when both its policy and ranker are such defaults, for example to switch to the Jev ranker after Jev is enabled. A configuration an operator published is never replaced.

Each per-person scope (a credential with `subject_id`, or a `Memjev-Subject` header) is its own tenant scope for storage, so it gets its own serving configuration on first recall.

The worker stamps each learned procedure with `MEMJEV_PROCEDURE_REGION` (default `local` in development, otherwise the archive region). Recall admits a procedure only if that region is allowed by both the policy and the caller's credential `region`, so keep the three in agreement. Procedures learned before this setting existed carry the archive region and are not recalled by a `local` policy.

