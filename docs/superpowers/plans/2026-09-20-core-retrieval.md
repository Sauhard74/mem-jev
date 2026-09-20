# Core Retrieval Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Every behavior change follows test-driven development: observe the focused test fail, implement the minimum production behavior, then run the focused and surrounding suites.

**Goal:** Serve safe, explainable procedure candidates from immutable projections through deterministic multi-channel retrieval, eligibility gates, versioned integer ranking, abstention, and replayable retrieval records. Jev plugs into a later bounded semantic-judgment stage; the core path remains complete and available without it.

**Architecture:** SurrealDB remains the hybrid graph, BM25, typed-facet, and DiskANN store. The server constructs and hashes a canonical query, captures the tenant's logical projection epoch and immutable serving manifests, executes independent candidate channels concurrently, persists their raw ordered results, gates the union before any semantic model, fuses eligible candidates with fixed-width integer arithmetic, and persists the exact decision record. Approximate vector generation is explicitly recorded and its returned pool is reranked by exact distance. A replay reads the stored candidate snapshot and manifests rather than querying today's corpus.

**Tech Stack:** Go 1.23, Connect/Protobuf, SurrealDB 3.2.4, OpenTelemetry, Docker Compose, and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md`, sections 10 and 14.

## Non-negotiable invariants

- Tenant, policy, sharing, residency, and snapshot identity are server-derived; the request cannot select or override them.
- Canonicalization is Unicode-normalized, alias-aware, path-normalized, field-order independent, set-order independent, inspectable, and versioned.
- A retrieval decision is reproducible for a fixed canonical query, captured candidate pool, policy/ranker/index manifests, and committed semantic judgments.
- Candidate channels never silently substitute for one another. Each records rank, raw features, latency, index manifest, completeness, and approximation provenance.
- Approximate vector search uses deterministic facet filters, an explicitly bounded overfetch pool, and exact-distance reranking of that returned pool.
- The union key is immutable procedure-version ID. No mutable family alias participates in final identity or tie-breaking.
- All hard eligibility gates run before Jev and cannot be overridden by Jev or score fusion.
- Each rejection has a stable code and canonical supporting facts. Gates default closed for absent required facts or unknown compatibility.
- Scores from heterogeneous systems are never added raw. Channel ranks enter weighted reciprocal-rank fusion; other features enter a versioned calibrated linear model.
- Serving arithmetic is fixed-width integer arithmetic with validated overflow bounds, explicit missing-value behavior, and deterministic ties.
- The final total order is score, verification strength, observed end-to-end status, then procedure-version ID.
- An empty or insufficient result abstains with a stable reason and evidence; it never manufactures a plan.
- Jev is an optional pointwise judgment provider for eligible ambiguous candidates. Failure, timeout, circuit-open, or capacity exhaustion selects the recorded no-Jev ranker.
- Retrieval-run and explanation records are transient operational data with a policy-defined expiry, but immutable while retained.
- Raw task text, procedure text, resource values, and semantic payloads never enter logs, metrics, workflow metadata, or error strings.

## Production decisions

- **Snapshot:** reuse the monotonic tenant projection checkpoint generation as the logical epoch. Persist the exact candidate IDs and channel results so replay does not depend on database MVCC history.
- **Lexical:** use a SurrealDB 3 full-text analyzer and BM25 index, but persist only ordered results and quantized scores. Application code owns cross-channel fusion.
- **Vector:** use one physical embedding table/index per immutable embedding manifest and dimension. The first manifest is explicitly configured; changing model or dimension creates a new table/index generation. DiskANN is the production default at target scale; an exact in-memory adapter is the test oracle.
- **Graph:** expand only typed tool-contract, resource-type, effect, and procedure-prefix relationships. Text similarity cannot create a graph edge.
- **Lifecycle:** `active` and `trial` are execution-eligible; `candidate` may be returned only as advisory when policy explicitly allows it. `stale`, `superseded`, `quarantined`, and `retired` always fail closed.
- **Ranking:** use checked `int64` accumulation over `int32` quantized features and coefficients. Weighted RRF is computed as integer micros divided by `k + rank`; manifests are rejected when worst-case accumulation could overflow.
- **Availability:** exact, lexical, facet, and graph channels form the required core. Vector and Jev have independent deadlines and degradation flags. A degraded response is allowed only when the policy's minimum channel set is satisfied.

## Task 1: Public retrieval contract and canonical query

**Files:**
- Create: `proto/memjev/v1/retrieval.proto`
- Modify: `proto/memjev/v1/common.proto`
- Modify: `internal/contracts/validate.go`
- Modify: `internal/contracts/contracts_test.go`
- Create: `internal/retrieval/query.go`
- Create: `internal/retrieval/query_test.go`
- Generate: `gen/memjev/v1/retrieval.pb.go`
- Generate: `gen/memjev/v1/memjevv1connect/retrieval.connect.go`

**Produces:** `RetrievalService/Retrieve` and `ExplainRetrieval`; typed tools, harness, environment, resources, constraints, risk/latency budgets, candidates, abstention, provenance, and stable explanations; plus a pure canonical query constructor and hash.

- [ ] Test descriptors: no tenant/snapshot/ranker/policy overrides; bounded request fields; strict enums; no maps; no raw explanation payloads.
- [ ] Test canonicalization: Unicode, whitespace, aliases, path forms, duplicate/set order, tool-version order, and semantically meaningful changes.
- [ ] Observe focused RED tests, then define/generate the contract and implement canonical query construction.
- [ ] Run `make generate && go test -race ./internal/contracts ./internal/retrieval && ./scripts/check-generated.sh`.
- [ ] Commit as `feat(retrieval): define canonical retrieval contract`.

## Task 2: Versioned ranker manifest and deterministic integer fusion

**Files:**
- Create: `internal/ranking/manifest.go`
- Create: `internal/ranking/fusion.go`
- Create: `internal/ranking/fusion_test.go`
- Create: `internal/ranking/testdata/ranker.golden.json`

**Produces:** validated immutable ranker manifests, weighted RRF, calibrated fixed-width features, checked linear scoring, stable missing values, and a total candidate ordering.

- [ ] Test manifest hashing, coefficient bounds, overflow rejection, channel-order independence, missing channels, ties, negative coefficients, extrema, and golden byte stability.
- [ ] Property-test permutation invariance and total-order transitivity.
- [ ] Observe RED, implement pure integer ranking, and prohibit float input at the fusion boundary.
- [ ] Run `go test -race ./internal/ranking`.
- [ ] Commit as `feat(ranking): add deterministic integer score fusion`.

## Task 3: Fail-closed eligibility engine

**Files:**
- Create: `internal/eligibility/policy.go`
- Create: `internal/eligibility/engine.go`
- Create: `internal/eligibility/engine_test.go`

**Produces:** stable eligibility decisions and facts for tenant/sharing, lifecycle, supersession, tools/contracts, schemas, resources, effects, environment/harness, freshness/validation, policy, consent, and residency.

- [ ] Test every gate alone and in combination, unknown facts, advisory candidates, deterministic reason ordering, supporting-fact redaction, policy-version changes, and Jev inability to override rejection.
- [ ] Observe RED and implement a pure engine with closed enums and canonical fact ordering.
- [ ] Run `go test -race ./internal/eligibility`.
- [ ] Commit as `feat(retrieval): gate candidates before semantic judgment`.

## Task 4: Retrieval schema, manifests, and logical epochs

**Files:**
- Create: `db/migrations/0003_core_retrieval.surql`
- Modify: `db/migrations/embed.go`
- Create: `internal/store/retrieval_schema_test.go`
- Modify: `internal/testinfra/surreal.go`

**Produces:** strict tables/indexes for serving documents, typed graph relations, projection epochs, embedding/index/ranker/policy manifests, retrieval runs, channel hits, gate decisions, ranked candidates, and retention metadata.

- [ ] Test schema/index presence, tenant isolation, immutable manifests/runs, composite uniqueness, lifecycle constraints, epoch monotonicity, relation endpoints, full-text index use, vector dimension isolation, and migration replay.
- [ ] Observe integration RED against pinned SurrealDB.
- [ ] Add schema-full records, full-text analyzer/BM25, manifest-specific DiskANN generation, and transactionally guarded epoch records.
- [ ] Verify query plans with `EXPLAIN FULL`; run migrations twice.
- [ ] Commit as `feat(store): add deterministic retrieval schema`.

## Task 5: Immutable retrieval serving projection

**Files:**
- Modify: `internal/projection/model.go`
- Modify: `internal/store/projection.go`
- Modify: `internal/store/memory/projection.go`
- Modify: `internal/store/surreal/projection.go`
- Modify: `internal/projection/service_test.go`
- Create: `internal/retrieval/document.go`
- Create: `internal/retrieval/document_test.go`

**Produces:** a denormalized immutable serving document and typed relations published atomically with each procedure version and tenant epoch.

- [ ] Test normalized task text, exact signatures, facets, tool contracts, resources/effects, environment, lifecycle, evidence features, immutable identity, and canonical bytes.
- [ ] Test publish/checkpoint/document atomicity, duplicate convergence, challenger epoch advance, and rebuild byte identity.
- [ ] Observe RED and extend projection publication without mutable aliases.
- [ ] Run unit/race and Surreal adapter tests twice.
- [ ] Commit as `feat(projection): publish immutable retrieval documents`.

## Task 6: Candidate channel interfaces and deterministic memory oracle

**Files:**
- Create: `internal/retrieval/channel.go`
- Create: `internal/retrieval/candidates.go`
- Create: `internal/retrieval/candidates_test.go`
- Create: `internal/store/memory/retrieval.go`
- Create: `internal/store/storetest/retrieval.go`

**Produces:** independent exact, lexical, facet, graph, and vector channel contracts; bounded deadlines; canonical hit ordering; stable union/deduplication; and an exact reference implementation.

- [ ] Test duplicate candidates, missing/failed/late channels, deterministic deadline outcomes, malformed hits, score quantization, result bounds, and input permutation.
- [ ] Observe RED and implement channel orchestration without ranking or eligibility policy leakage.
- [ ] Run `go test -race ./internal/retrieval ./internal/store/memory`.
- [ ] Commit as `feat(retrieval): orchestrate independent candidate channels`.

## Task 7: Surreal exact, BM25, facet, and graph channels

**Files:**
- Create: `internal/store/surreal/retrieval.go`
- Create: `internal/store/surreal/retrieval_test.go`
- Create: `internal/store/surreal/retrieval_queries.go`

**Produces:** tenant/epoch-bound candidate queries with strict bounds, deterministic secondary ordering, exact task/effect matching, BM25, typed facets, and typed graph expansion.

- [ ] Contract-test all channels against the memory oracle, tenant isolation, epoch cutoffs, lifecycle prefiltering, index-plan use, equal-score ties, cancellation, and adversarial strings.
- [ ] Observe integration RED and implement parameterized SurrealQL only.
- [ ] Assert full-text and graph indexes with `EXPLAIN FULL` in CI.
- [ ] Run adapter tests twice against a clean pinned database.
- [ ] Commit as `feat(store): query deterministic retrieval channels`.

## Task 8: Versioned embedding pipeline and exact pool rerank

**Files:**
- Create: `internal/embedding/provider.go`
- Create: `internal/embedding/canonical.go`
- Create: `internal/embedding/service.go`
- Create: `internal/embedding/service_test.go`
- Create: `internal/store/surreal/vector.go`
- Create: `internal/store/surreal/vector_test.go`

**Produces:** a provider-neutral pinned embedding manifest, content-addressed embeddings, durable generation state, deterministic facet filtering, DiskANN overfetch, and exact-distance reranking of the returned pool.

- [ ] Test manifest/dimension mismatch, normalization, NaN/Inf rejection, duplicate generation, retry/fencing, stale embeddings, ANN provenance, exact rerank ties, timeouts, and no-vector degradation.
- [ ] Observe RED; implement a deterministic test provider and production provider interface without alias-based model selection.
- [ ] Prove the database vector index is used and exact reranking is application-verifiable.
- [ ] Run race and pinned integration tests.
- [ ] Commit as `feat(retrieval): add versioned vector candidate generation`.

## Task 9: Immutable snapshot and retrieval-run repository

**Files:**
- Create: `internal/store/retrieval.go`
- Create: `internal/store/memory/retrieval_run.go`
- Create: `internal/store/surreal/retrieval_run.go`
- Create: `internal/store/storetest/retrieval_run.go`

**Produces:** atomic snapshot acquisition and immutable persistence of canonical query, epoch, manifests, channel hits, approximation flags, gates, features, ranking, abstention, degradation, expiry, and selection linkage.

- [ ] Test concurrent projection writes, snapshot consistency, duplicate run IDs, append-only enforcement, replay reads, partial-write rollback, TTL policy, tenant isolation, and canonical export stability.
- [ ] Observe RED and implement a single transaction per completed run plus an explicit failed-run audit record.
- [ ] Run memory contract, race, and Surreal tests twice.
- [ ] Commit as `feat(store): persist replayable retrieval decisions`.

## Task 10: Retrieval service, deterministic degradation, and abstention

**Files:**
- Create: `internal/retrieval/service.go`
- Create: `internal/retrieval/service_test.go`
- Create: `internal/retrieval/explain.go`
- Create: `internal/retrieval/explain_test.go`

**Produces:** end-to-end canonicalize/snapshot/retrieve/union/gate/fuse/persist behavior, policy-defined degraded modes, confidence thresholds, stable abstentions, and redacted explanations. Includes a dormant pointwise `SemanticJudge` interface for the later Jev slice.

- [ ] Test exact wins, multi-channel fusion, all gate failures, below-threshold abstention, vector loss, required-core loss, cancellation, persistence failure, replay, semantic-judge bypass, and no-Jev determinism.
- [ ] Observe RED and implement bounded structured concurrency with no goroutine leaks.
- [ ] Run `go test -race ./internal/retrieval` and repeat deterministic golden cases.
- [ ] Commit as `feat(retrieval): serve deterministic procedure candidates`.

## Task 11: Authenticated API and explanation endpoint

**Files:**
- Create: `internal/api/retrieval_handler.go`
- Create: `internal/api/retrieval_handler_test.go`
- Modify: `internal/api/server.go`
- Modify: `cmd/api/main.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

**Produces:** authenticated Retrieve and ExplainRetrieval endpoints with credential-bound tenant, exact `retrievals:read` scope, rate/size limits, safe errors, observability, and graceful degradation metadata.

- [ ] Test auth/scope, cross-tenant run access, request bounds, strict JSON/protobuf, timeout budgets, idempotent retry identity, response redaction, and safe metrics/logs.
- [ ] Observe RED and wire the service with independent channel budgets and health reporting.
- [ ] Run API/race tests and the generated-contract gate.
- [ ] Commit as `feat(api): expose deterministic retrieval and explanations`.

## Task 12: Production qualification and operations

**Files:**
- Create: `tests/e2e/retrieval_test.go`
- Create: `tests/load/retrieval.go`
- Create: `scripts/retrieval-integration.sh`
- Modify: `scripts/integration.sh`
- Modify: `.github/workflows/ci.yml`
- Create: `ops/alerts/retrieval.yaml`
- Create: `docs/runbooks/retrieval.md`
- Create: `docs/runbooks/vector-degradation.md`
- Create: `docs/runbooks/retrieval-replay.md`
- Modify: `docs/runbooks/local-development.md`

**Produces:** repeatable proof of deterministic safe retrieval at the target 10M-event design point and 100 mixed RPS operating envelope.

- [ ] E2E exact/BM25/facet/graph/vector cases, cross-tenant denial, every hard gate, stable ties, abstention, vector outage, run replay, concurrent projection, and migration replay.
- [ ] Load-test 100 mixed RPS with a production-shaped corpus and prove no-Jev p95 <=250ms, bounded saturation, stable memory, and zero incorrect-tenant/incorrect-snapshot responses.
- [ ] Add metrics/alerts for per-channel latency/error/degradation, candidate counts, gate rejection codes, abstention, snapshot age, ranker version, vector staleness, run persistence failure, and replay mismatch.
- [ ] Exercise clean-stack startup, index build/rebuild, failover, vector corruption quarantine, credential rotation, backup/restore, and byte-identical decision replay.
- [ ] Run full unit/race/lint/vet/generated/migration/integration/load gates, request independent review, resolve all Critical/Important findings, and rerun from a clean stack.
- [ ] Commit as `test: qualify deterministic production retrieval`.

## Completion gate

Core retrieval is complete only when:

- the five channels have explicit and persisted provenance;
- every candidate is gated before semantic judgment;
- ranking is integer-only, versioned, and total;
- exact decisions replay byte-identically from persisted runs;
- approximate and degraded behavior is visible and policy-bounded;
- API explanations identify why a version was selected or rejected without exposing sensitive payloads;
- a no-Jev deployment meets the stated availability and latency envelope;
- the Jev slice can attach through `SemanticJudge` without changing hard gates, channel results, or deterministic fallback behavior.
