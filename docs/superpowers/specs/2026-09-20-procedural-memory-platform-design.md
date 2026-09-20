# Production Procedural Memory Platform Design

**Status:** Proposed for review  
**Date:** 2026-09-20  
**Target:** Hosted, multi-tenant procedural memory for agents  
**Initial capacity:** 10 million canonical events and 100 retrieval requests per second

## 1. Purpose

Build a generic hosted service that learns bounded, reusable procedures from agent traces and returns safe, inspectable plans for later tasks. The service does not auto-execute recalled procedures. It gives an agent an advisory plan containing steps, prerequisites, verification requirements, uncovered work, confidence, and provenance.

The product is not described as general learning. Its honest category is **evidence-backed procedural reuse with bounded composition and outcome-based revision selection**.

The design must outperform a simple fuzzy cache in ways that can be measured:

- distinguish task success from command completion;
- preserve failures as contextual negative evidence;
- reject incompatible procedures before semantic ranking;
- compose compatible stored procedures without inventing hidden steps;
- explain and reproduce every committed retrieval decision;
- abstain when evidence is weak;
- remain available when embeddings or Jev are unavailable;
- support tenant isolation, erasure, audit, recovery, and regional hosting from the beginning.

## 2. Non-goals

- Autonomous execution of returned procedures.
- Generating action primitives that do not exist in stored procedures or declared tool contracts.
- Claiming mathematically deterministic model inference.
- Treating an embedding match as proof that a procedure applies.
- Active-active cross-region writes in the first production topology.
- Global cross-tenant learning from customer content without explicit, revocable participation.
- Silently promoting a procedure from weak or ambiguous outcome evidence.

## 3. Design principles

1. **Evidence before compression.** The durable canonical ledger precedes derived procedures.
2. **Compatibility before relevance.** Hard gates run before semantic scoring.
3. **Abstention is a valid result.** A miss is safer than an attractive but incompatible plan.
4. **Models advise; code decides.** Jev produces typed probabilities. Versioned application code performs admission, fusion, ordering, and thresholds.
5. **Committed decisions are reproducible.** A cold model call may vary; an accepted judgment is immutable and reusable.
6. **Every projection is rebuildable.** Procedures, indexes, embeddings, graphs, and rankings derive from versioned canonical evidence.
7. **Composition is lower-confidence than observation.** A valid chain is not equivalent to a procedure observed succeeding end to end.
8. **Tenancy is an execution boundary.** Tenant identity comes from credentials, never from request data.
9. **The core path degrades deterministically.** Capture and retrieval must work without Jev and tolerate vector degradation.
10. **No production claim without evidence.** Accuracy, latency, safety, and scale claims require a frozen evaluation or load result.

## 4. System boundaries

The platform has four logical planes.

### 4.1 Edge and client plane

- Agent SDKs and adapters normalize harness events into the public trace protocol.
- A local privacy filter allow-lists fields and redacts common credential forms before transmission.
- Consent has three fail-closed modes: `deny`, `recall_only`, and `learn_and_recall`.
- Capture is asynchronous from the agent's task after a durable local handoff; recall has a hard deadline.
- Returned procedures are rendered as inert reference data, never executable prompt instructions.

The first supported integrations are one deep coding-agent adapter and a generic HTTP/SDK integration. Browser and computer-use adapters are separate modules because DOM identity, screen-state diffs, and semantic target resolution require modality-specific contracts.

### 4.2 Online data plane

- **Ingest API:** authenticates, applies consent, validates envelopes, sanitizes raw events, canonicalizes them, writes the archive object, and commits the online ledger plus outbox record.
- **Retrieval API:** creates a corpus snapshot, runs versioned candidate generation and deterministic gates, optionally obtains Jev judgments under budget, composes plans, fuses scores, and returns a procedure or abstention.
- **Outcome API:** receives explicit execution links, verification evidence, and agent feedback. It does not accept an unverified boolean as authoritative success.
- **Explanation API:** returns candidate sources, rejection reasons, score components, model/policy versions, and composition provenance.

### 4.3 Asynchronous learning plane

- Canonical-event projector.
- Outcome evaluator.
- Causal graph builder.
- Procedure synthesizer.
- Embedding worker.
- Revision evaluator.
- Index and projection rebuilder.
- Erasure and retention worker.

Workflows are durable, at least once, and idempotent. A transactional outbox in SurrealDB starts workflows with deterministic workflow IDs. Duplicate delivery is expected.

### 4.4 Control plane

- Tenant, region, plan, quota, consent, policy, and key management.
- Tool-contract registry and schema lifecycle.
- Model, rubric, embedding, ranker, and synthesis manifests.
- Feature rollout, kill switches, evaluation registry, and audit export.

Control-plane failure must not invalidate cached data-plane decisions. The data plane uses signed, versioned policy bundles with bounded validity.

## 5. Chosen technology architecture

### 5.1 Application runtime

Use **Go** for APIs, workers, canonicalization, graph planning, and policy execution. A single runtime keeps deterministic transformations, concurrency behavior, deployment, and observability consistent. Services are separate deployable binaries over shared internal libraries, not a distributed microservice per concept.

Interfaces are defined in Protocol Buffers. Public APIs expose JSON over HTTP with generated SDK types; internal calls use Connect RPC. All public mutations require an idempotency key.

### 5.2 Online database

Use **SurrealDB** as the primary online graph/document/search database because procedures, typed steps, evidence, resources, edges, BM25 indexes, and vector indexes share one transactional model.

The design does not depend on SurrealDB being the permanent canonical store. A storage adapter isolates query construction, and the canonical archive supports a future migration or complete rebuild.

Deployment model:

- SurrealDB managed multi-node service for the primary region after workload qualification;
- schema-full tables;
- shared database for standard tenants with mandatory credential-bound tenant scoping;
- dedicated database routing for regulated or very large tenants;
- no cross-tenant graph edge is representable through the application API;
- automatic transaction retry for write conflicts;
- unique indexes or named invariant records for deduplication and authoritative judgment selection.

### 5.3 Canonical archive

Use a versioned, encrypted, S3-compatible object store as the portable append-only archive for sanitized canonical event batches and manifests.

The write protocol avoids a distributed transaction:

1. Stream raw input through sanitization without logging or durable raw storage.
2. Canonicalize and hash the sanitized batch.
3. Write the immutable archive object under a content-addressed key.
4. Commit the archive reference, canonical online records, and outbox job in one SurrealDB transaction.
5. Garbage-collect unreferenced archive objects after a safety window.

The API acknowledges durable ingestion only after steps 3 and 4 succeed. Retrying the same idempotency key reuses the content-addressed object and committed ingest receipt.

### 5.4 Durable workflow execution

Use a managed durable workflow service behind a narrow adapter. The initial implementation uses Temporal-compatible workflow semantics: deterministic workflow IDs, activity retries, heartbeats, deadlines, and visibility. SurrealDB remains the source of job intent through its outbox; the workflow system is not the source of business truth.

### 5.5 Deployment and observability

Run stateless APIs and workers as OCI containers in a managed container platform. The first production region uses AWS ECS/Fargate, an application load balancer, WAF, private subnets, KMS-backed secrets, and autoscaling. Avoid Kubernetes until a demonstrated scheduling or tenancy requirement justifies its operational cost.

Use OpenTelemetry for traces, metrics, and structured logs. Logs contain identifiers, versions, timings, sizes, and result codes—not prompts, raw trace fields, procedure text, secrets, or Jev state.

### 5.6 Deployment modes

The same logical contracts support three isolation tiers without changing procedure semantics:

- shared hosted data plane with credential-bound tenant isolation;
- dedicated hosted database and worker pools for large or regulated tenants;
- customer-managed data plane with the hosted control plane limited to signed manifests and explicitly permitted model calls.

The first release is hosted, but storage, workflow, embedding, and Jev clients are capability interfaces rather than global singletons. This preserves a future VPC, residency, offline, or customer-key deployment without forking the data model or ranking contract.

## 6. Durable data model

All tenant-owned durable records include `tenant_id`, `created_at`, schema version, provenance, and an immutable content hash where applicable. Global signed manifests use an explicit system scope and cannot contain tenant content.

Core records:

- `tenant`, `credential`, `consent_policy`, `quota_policy`;
- `harness`, `tool_contract`, `tool_contract_version`;
- `trace_run`, `canonical_event`, `resource_ref`, `artifact_ref`;
- `outcome_evidence`, `verification_result`;
- `procedure`, `procedure_version`, `step`, `negative_path`;
- `policy_bundle`, `synthesis_manifest`, `embedding_manifest`, `ranker_manifest`;
- `embedding`, `jev_judgment`;
- `retrieval_run`, `retrieval_candidate`, `selection_record`;
- `outbox_job`, `projection_checkpoint`, `erasure_job`.

Core edges:

- `procedure_version -> contains -> step`;
- `step -> next | depends_on -> step`;
- `step -> consumes | produces -> resource_ref`;
- `step -> verified_by -> verification_result`;
- `step -> failed_under -> negative_path`;
- `procedure_version -> supersedes | derived_from -> procedure_version`;
- `selection_record -> selected -> procedure_version`;
- `selection_record -> resulted_in -> outcome_evidence`.

Canonical events and outcome evidence are append-only except for legally required erasure. Corrections are new records that supersede earlier evidence. Procedure versions are immutable. A projection may be deleted and rebuilt.

## 7. Tool contracts and resource identity

A tool-contract version declares:

- canonical tool identity and aliases;
- typed inputs and outputs;
- read and write resource sets;
- side-effect class and risk level;
- idempotency and retry semantics;
- preconditions and success predicates;
- compensation behavior;
- verification methods;
- compatibility range across tool versions;
- sanitizer rules for every accepted field.

Unknown tools may be stored as opaque evidence but cannot produce a promoted procedure unless a tenant policy explicitly permits an opaque, advisory-only step. Opaque steps never justify inferred resource dependencies.

Resource identity is typed and namespaced. Canonicalization must distinguish logical resources from environment-specific instances. For example, a repository-relative file identity may generalize within one repository family, while an absolute local path remains instance-specific.

## 8. Capture and outcome inference

### 8.1 Outcome states

- `verified_success`: goal-level evidence satisfies the active policy.
- `provisional_success`: local steps succeeded, but goal proof is incomplete.
- `inconclusive`: evidence is contradictory, missing, or unverifiable.
- `verified_failure`: a required predicate failed or a declared failure condition was observed.

Only `verified_success` promotes a procedure by default. Other states remain available for evaluation, negative evidence, and later reconciliation.

### 8.2 Evidence hierarchy

From strongest to weakest:

1. Independent verifier tied to the original task and execution ID.
2. Declared goal predicate over artifacts or external state.
3. Harness-native test or assertion with authenticated result.
4. Tool-level postcondition.
5. Exit status or agent self-report.

Weak evidence cannot be silently upgraded. Conflicting strong evidence produces `inconclusive` and an audit event.

### 8.3 Causal graph construction

The graph builder links steps only through declared resource flow, explicit control dependencies, or verifier dependencies. Temporal adjacency alone does not establish causality.

For a verified run, synthesis computes the smallest subgraph that:

- reaches every required goal predicate;
- retains prerequisite and verification nodes;
- preserves required side effects and ordering;
- retains compensation boundaries;
- excludes branches proven irrelevant to the successful outcome.

If necessity cannot be established, the step remains with an `uncertain_necessity` annotation. Failed branches become contextual negative paths keyed by environment, tool versions, resource state, and failure predicate. Negative evidence is never applied outside its compatibility scope.

Every synthesis output records the canonical event range plus sanitizer, registry, policy, graph-builder, and synthesizer versions.

## 9. Procedure identity, revisions, and lifecycle

A procedure family represents one task intent and compatible effect signature. A version is a particular step graph, precondition set, verifier set, and environment scope.

- Identical canonical step graphs refresh evidence for the existing version.
- Different graphs create challenger versions.
- Versions are never overwritten.
- Stale, unsafe, and superseded versions are excluded from serving but retained according to policy.

Lifecycle states are `candidate`, `trial`, `active`, `stale`, `superseded`, `quarantined`, and `retired`.

Promotion requires minimum verified evidence, bounded unsafe outcomes, compatible tool contracts, and a completed evaluation manifest. Automatic retirement requires stronger evidence than admission and remains reversible during a retention window.

## 10. Retrieval and ranking

### 10.1 Canonical query

The server constructs a canonical query from:

- task text;
- available tools and exact contract versions;
- harness and environment identity;
- accessible resources;
- explicit constraints and forbidden effects;
- tenant and policy bundle;
- requested latency and risk class.

Canonicalization fixes Unicode form, whitespace, field order, identifier aliases, path forms, and set ordering. It emits a hash and a human-inspectable normalized representation.

### 10.2 Corpus snapshot

Each retrieval obtains a logical projection epoch. Candidate IDs, index manifests, policy versions, and approximate-search flags are persisted in `retrieval_run`. Reproduction means running against this recorded snapshot or its archived projection, not against whatever corpus exists later.

### 10.3 Candidate channels

Run independently and in parallel:

1. exact task/effect signature;
2. BM25 lexical search;
3. typed facet matching;
4. graph expansion from tools, resources, and procedure prefixes;
5. vector similarity.

Vector retrieval uses deterministic facet filtering followed by ANN overfetch and exact distance reranking for the returned pool. HNSW or DiskANN generation is marked approximate in provenance. Exact full-corpus vector scans are not assumed at the target size.

Candidate lists are unioned and deduplicated by immutable procedure-version ID. Per-channel ranks and raw features are persisted before reranking.

### 10.4 Eligibility gates

Candidates are rejected before Jev when any hard condition fails:

- tenant or sharing scope;
- lifecycle or supersession state;
- required tool availability;
- tool and schema version compatibility;
- hard resource preconditions;
- forbidden or excessive side effects;
- environment and harness compatibility;
- freshness, validation, or policy requirements;
- consent or data-residency policy.

Every rejection has a stable reason code and supporting fact.

### 10.5 Jev judgment

Jev is a bounded semantic enhancement, not a serving dependency. Only ambiguous top candidates that pass all gates are admitted under a tenant and global token bucket.

Each candidate is judged independently against the same canonical query using typed outputs:

- `intent_fit: Score`;
- `preconditions_likely_satisfied: Noul`;
- `task_coverage: Score`;
- `contradicts_request: Noul`;
- `useful_as_partial_plan: Noul`.

The prompt contains only the request, sanitized environment facts, and one candidate. Model and rubric versions are pinned. No prior candidate or ordering is included.

The cache key is the hash of canonical query, procedure version, environment facts, rubric, model, and policy. Concurrent misses are single-flighted. A unique database invariant chooses one authoritative immutable judgment. A model upgrade creates a new judgment namespace.

Hard deadlines, circuit breakers, concurrency limits, and quota admission protect the core path. Timeout, capacity exhaustion, 429, 529, malformed output, or circuit-open state selects the deterministic no-Jev ranker. Launch requires either contracted enterprise capacity or a measured admission rate that meets the service SLO without Jev.

### 10.6 Stable fusion

Raw scores from unrelated retrieval systems are not added directly.

1. Fuse channel ranks using versioned weighted reciprocal-rank fusion.
2. Convert compatibility, verification strength, evidence freshness, observed success, negative evidence, and Jev probabilities into calibrated features.
3. Apply a versioned linear ranker whose coefficients are trained offline on a frozen labeled corpus.
4. Quantize features and coefficients to fixed-width integers before serving.
5. Sort by final rank, verification strength, observed-versus-composed status, procedure-version ID.

All thresholds, coefficients, normalizers, missing-value behavior, and tie-breaks live in the ranker manifest.

The externally defensible guarantee is: **for a fixed snapshot, canonical query, policy/ranker versions, exact candidate pool, and committed judgments, the result is reproducible.** Approximate candidate generation and first-time Jev inference are disclosed exceptions.

## 11. Composition planner

Composition operates after individual eligibility and initial ranking.

### 11.1 Compatibility graph

A directed edge exists only when an output resource/effect from procedure A satisfies a typed input or precondition of procedure B under compatible schemas, environments, and policies. String similarity cannot create an edge.

The planner may insert an unrequested bridge procedure only when:

- the bridge satisfies a declared missing prerequisite;
- every bridge step independently passes eligibility;
- its side effects fit the request's risk budget;
- its addition is explicit in the returned plan.

### 11.2 Search objective

Use deterministic bounded beam search over immutable procedure versions. Optimize, in order:

1. hard validity;
2. verified subgoal coverage;
3. evidence strength;
4. observed end-to-end success;
5. precondition satisfaction;
6. risk and side-effect cost;
7. plan length and redundancy.

The manifest fixes beam width, expansion depth, maximum procedures, maximum bridge count, and total estimated tool cost. Search exhaustion returns the best partial chain plus explicit gaps.

Cycles are forbidden unless represented by a contract-defined bounded retry construct. A step may run in parallel only when there is no dependency path, write-set overlap, exclusive resource, compensation conflict, or policy prohibition.

An observed end-to-end procedure outranks an otherwise equal unseen composition. Each unobserved edge and bridge applies a calibrated confidence penalty. The response distinguishes observed, previously verified composition, and novel composition.

## 12. Revision trials and outcome credit

Every served plan creates a `selection_record` containing query hash, candidate snapshot, selected versions or chain, ranker/policy manifests, exploration reason, and injection ID.

The agent includes the injection ID in subsequent events. Credit is assigned only when outcome evidence can be linked to the same task execution and goal predicates.

Credit states are:

- `causal_success`;
- `associated_success`;
- `causal_failure`;
- `associated_failure`;
- `unattributable`.

Only causal evidence changes automatic promotion by default. Associated evidence informs evaluation but cannot retire a champion.

Challenger exploration is deterministic and auditable: eligible traffic assignment is derived from a hash of tenant, procedure family, query bucket, and experiment epoch. Policies cap exposure by risk class and tenant. High-risk procedures require explicit tenant opt-in and cannot be automatically explored.

Promotion uses a conservative champion/challenger rule with minimum samples, lower confidence bounds, unsafe-outcome ceilings, freshness weighting, and rollback triggers. Exact statistical policy is versioned and established through offline simulation before activation.

## 13. API contract

Initial versioned endpoints:

- `POST /v1/traces`: ingest one sanitized trace envelope or batch.
- `POST /v1/retrievals`: return a plan, partial plan, or abstention.
- `POST /v1/outcomes`: attach verification evidence to a selection or trace.
- `GET /v1/retrievals/{id}/explanation`: inspect ranking and rejection evidence.
- `GET /v1/procedures/{id}`: inspect an authorized immutable version.
- `POST /v1/procedures/{id}:quarantine`: emergency disable.
- `POST /v1/exports`: create an auditable tenant export.
- `POST /v1/erasures`: create an erasure workflow.
- `GET /v1/status`: tenant-visible health and quota state.

Retrieval returns:

- `result_type`: `procedure`, `chain`, `partial`, or `abstain`;
- ordered steps and safe parallel groups;
- preconditions and required tools;
- verification and compensation instructions;
- covered and uncovered subgoals;
- confidence class and evidence class;
- observed/composed/novel-composition marker;
- score decomposition and rejection summary;
- procedure, model, policy, ranker, and snapshot versions;
- injection ID and expiry.

All list APIs use opaque cursors. Errors use stable machine codes, retry classification, request ID, and safe detail. No error echoes sensitive submitted data.

## 14. Security, privacy, and tenant isolation

- Credentials bind tenant, environment, scopes, region, and consent before request parsing reaches storage code.
- The storage layer automatically injects tenant predicates; callers cannot supply or override them.
- SurrealDB record permissions provide defense in depth, not the sole boundary.
- Edge creation verifies both endpoints belong to the same tenant and compatible sharing scope.
- Per-tenant envelope encryption keys protect archives; dedicated tenants may use customer-managed keys.
- TLS is mandatory in transit; service-to-service identity uses short-lived credentials.
- Raw trace bodies, prompts, procedure text, and Jev state are excluded from logs and metrics.
- Secret detection runs client-side and server-side. Unknown fields are rejected, not ignored.
- Returned text is length-limited, control-character stripped, structurally delimited, and labeled untrusted reference data.
- Procedures cannot contain executable control instructions for the host agent.
- Tenant and harness kill switches stop learning, recall, or both without deployment.
- Erasure propagates through online records, indexes, caches, archives, exports, backups, and derived judgments according to a documented timeline.
- Cross-tenant noninterference tests cover every query, traversal, cache, export, explanation, and failure path.

## 15. Reliability and regional recovery

The initial topology is tenant-home-region with active/passive disaster recovery, not active/active writes.

- Stateless APIs run across at least two availability zones.
- SurrealDB uses the qualified multi-node deployment in the home region.
- Canonical archives use versioning, retention locks where required, and asynchronous cross-region replication.
- Projection checkpoints and manifests are replicated independently.
- A standby region can rebuild online projections from the canonical archive.
- DNS or edge routing moves a tenant only through an explicit failover workflow.

Initial service objectives:

- core retrieval and ingest availability: 99.95% monthly, excluding documented tenant-caused rejection;
- deterministic no-Jev retrieval p95: 250 ms at the qualified workload;
- Jev-enhanced retrieval p95: 900 ms under admitted capacity;
- durable ingest p95: 500 ms for a standard batch;
- regional-loss RPO: at most 5 minutes for accepted canonical events;
- regional-loss RTO: at most 60 minutes for core recall in degraded mode.

These are release gates, not marketing claims, until demonstrated by load and recovery exercises.

Degradation order:

1. omit Jev and use the no-Jev ranker;
2. omit vector retrieval and use exact/BM25/facets/graph;
3. serve cached eligible plans only when the primary data store is read-only;
4. abstain rather than use an unverifiable or policy-stale procedure.

Capture may queue sanitized local envelopes when the service is unavailable, subject to consent, encryption, size, and TTL limits.

## 16. Capacity and performance design

Ten million canonical events is not treated as ten million total records. Capacity tests include event fan-out into resources, edges, evidence, procedure versions, candidates, and judgments.

Key controls:

- tenant and time locality in primary indexes;
- bounded graph traversals and composition expansion;
- ANN overfetch followed by exact reranking of the returned pool;
- TTL and compaction for retrieval snapshots, candidates, and obsolete projections;
- cold archival of inactive canonical evidence while retaining rebuild manifests;
- request-level candidate, token, graph, and Jev budgets;
- admission control before expensive work;
- asynchronous embedding and synthesis, never on the ingest acknowledgment path;
- Jev concurrency isolated from core API worker pools.

A representative load model mixes retrieval, ingestion, outcomes, explanation, background synthesis, embedding, compaction, export, and erasure. A read-only 100-RPS benchmark is insufficient.

## 17. Evaluation strategy

### 17.1 Offline retrieval corpus

Freeze tenant-safe labeled datasets covering:

- exact repeats;
- paraphrases;
- partial repeats;
- valid new compositions;
- misleading semantic similarity;
- stale environments and changed tools;
- conflicting constraints;
- unseen task families;
- adversarial prompt injection and secret-bearing traces.

Metrics include Recall@K, MRR, nDCG, top-1 correctness, unsafe-hit rate, abstention calibration, false promotion, required-step retention, composition validity, revision regret, and verified end-task success.

Every ranker, rubric, model, embedding, gate, and synthesis change runs against the frozen corpus and records a signed evaluation manifest. No alias-based model change enters production.

### 17.2 Determinism tests

- Repeated canonicalization produces byte-identical output.
- Rebuilding a projection from the same archive and manifests produces identical IDs and graph structure.
- Fusion and tie-breaking are property-tested across randomized candidate ordering.
- A committed Jev judgment always yields the same downstream rank.
- Cold Jev variability is measured and disclosed, not mislabeled deterministic.

### 17.3 Safety and isolation tests

- Cross-tenant traversal and cache-key fuzzing.
- Edge-endpoint tenant mismatch rejection.
- Prompt-injection and control-character corpora.
- Secret redaction and forbidden-field rejection.
- Tool-version and resource-precondition incompatibility.
- Erasure completeness and tombstone behavior.

### 17.4 Scale and resilience tests

At the full 10-million-event data shape and 100 mixed RPS, measure p50/p95/p99 latency, throughput, ANN recall, transaction conflicts, Jev cold/warm/degraded states, workflow lag, cost per useful recall, index rebuild time, regional rebuild, backup restoration, and node/provider failure behavior.

Release requires successful chaos exercises for SurrealDB unavailability, object-store delay, workflow replay, embedding outage, Jev throttling, corrupt projection, policy expiry, and regional loss.

## 18. Operational surfaces

Operators and tenants receive:

- health, quota, backlog, model/ranker, and projection status;
- explain and refusal-reason views;
- quarantine and rollback controls;
- retry and dead-letter inspection;
- projection rebuild and consistency audit;
- export, erasure, retention, and residency controls;
- per-procedure evidence history and revision comparison;
- alerts for unsafe outcome, cross-tenant invariant violation, stale policy, archive lag, and rank drift.

Operational repair never edits immutable evidence. It appends corrective state or rebuilds a projection.

## 19. Delivery decomposition

This architecture is implemented as production slices, each preserving the final interfaces rather than as throwaway MVP work:

1. **Foundation:** repository, contracts, identity, tenancy, policy bundles, canonicalization, archive protocol, SurrealDB schema, outbox, observability.
2. **Evidence pipeline:** ingest, outcome inference, causal graph, synthesis, negative paths, projection rebuild.
3. **Core retrieval:** exact/BM25/facets/graph/vector, eligibility gates, snapshots, deterministic fusion, abstention, explanations.
4. **Composition and revisions:** typed chain planner, bridges, parallel groups, selection records, trials, credit, lifecycle.
5. **Jev enhancement:** pointwise rubric, cache, single-flight, capacity controls, calibrated ranker, degradation.
6. **Production hardening:** adapters, consent, security, erasure, regional recovery, load/chaos qualification, runbooks.

Each slice must pass its own contract, migration, security, rebuild, and failure tests before the next slice depends on it.

## 20. Acceptance criteria

The system is production-ready for the stated target only when:

- a sanitized trace can be rebuilt from archive into byte-identical canonical IDs and equivalent projections;
- success promotion requires goal-level verified evidence;
- incompatible candidates never reach Jev;
- every result or abstention is explainable from persisted inputs and manifests;
- the no-Jev path meets the availability and latency objectives;
- composition exposes bridges, gaps, parallelism, and novelty penalties;
- revision promotion is linked to attributable outcomes and is reversible;
- cross-tenant tests find no readable, traversable, cached, logged, or exported leakage;
- erasure and regional-rebuild drills complete within policy;
- the mixed 10-million-event, 100-RPS workload meets the qualified SLOs;
- product language states the exact determinism boundary and avoids claims of general learning.
