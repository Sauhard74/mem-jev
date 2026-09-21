# Composition, Revision, and Lifecycle Implementation Plan

> **Execution contract:** implement this plan task-by-task with test-first changes, one reviewable commit per task, and a clean verification gate after every schema or API boundary.

**Goal:** Turn individually eligible procedures into deterministic typed plans, persist every injection, attribute later outcomes to the exact plan served, and promote or roll back immutable procedure versions through auditable champion/challenger policy.

**Safety target:** tenant, consent, residency, policy, and risk boundaries remain hard gates; no composition or lifecycle decision depends on Jev.

**Non-negotiable invariants**

- A composition edge exists only because a typed output/write contract satisfies a typed input/read/precondition contract. Text or embedding similarity cannot create an edge.
- The planner consumes the already eligible, snapshot-pinned candidate set and may only add bridges that independently pass the same eligibility policy.
- Search uses integer scores, immutable manifests, explicit limits, a total tie order, and no map iteration in decision order.
- Cycles are rejected unless a future contract introduces an explicit bounded-retry node; this slice does not infer loops.
- Parallel groups require no dependency path, write-set overlap, exclusive resource overlap, compensation conflict, or policy prohibition.
- Every response that can influence an agent has a committed selection record and opaque injection ID before it is returned.
- Outcome credit requires tenant-bound linkage to the injection and task execution. Associated evidence never promotes or retires automatically.
- Lifecycle is append-only state. Promotion and rollback create new decisions and projection epochs; immutable evidence and procedure versions are never edited.
- Existing single-procedure retrieval remains a valid plan of length one and is the deterministic fallback if composition is exhausted or disabled.

## Task 1: Preserve typed procedure interfaces through synthesis

**Modify:** `internal/domain/procedure.go`, `internal/synthesis/synthesizer.go`, `internal/synthesis/synthesizer_test.go`

Add canonical procedure-step reads, writes, effects, success predicates, risk, and compensation metadata derived from retained causal-graph nodes. Bump the synthesis schema version because this changes the canonical result. Reject a synthesized result when retained metadata is incomplete, duplicated inconsistently, or refers to a resource outside the retained graph.

**Proof:** golden tests show dead branches cannot leak capabilities into a synthesized interface and identical causal inputs produce byte-identical v2 synthesis.

**Commit:** `feat(synthesis): preserve typed procedure interfaces`

## Task 2: Publish immutable procedure input/output contracts

**Modify:** `internal/projection/model.go`, `internal/projection/service_test.go`, `internal/retrieval/document.go`, `internal/retrieval/document_test.go`

Create `ProcedureInterface` with typed requirements and provisions. A requirement records resource type, namespace, logical identity hash, schema version, access mode, and predicate IDs. A provision records the same identity plus produced effects and success predicates. Derive external requirements as retained reads with no retained predecessor; derive provisions as terminal retained writes and verified effects. Bump retrieval documents to v2 and keep v1 decoding read-only for historical runs.

**Proof:** interface derivation is independent of event order where no causal edge exists, schema mismatches remain distinct, and a procedure cannot claim an output that was only read.

**Commit:** `feat(projection): publish typed procedure interfaces`

## Task 3: Add immutable composition and lifecycle schema

**Create:** `db/migrations/0004_composition_revisions.surql`, `internal/store/composition_schema_test.go`

Add strict tenant-scoped tables and indexes for:

- planner manifests and serving heads;
- procedure compatibility edges tied to projection epoch and interface hashes;
- selection records, selected plan nodes/edges/gaps, and injection IDs;
- lifecycle policy manifests and append-only lifecycle decisions;
- experiment manifests, deterministic assignments, and exposure counters;
- outcome credit records and immutable promotion evaluations.

Unique constraints must make duplicate publication idempotent and conflicting content fail closed. Selection and credit rows are transient under policy; lifecycle decisions and promotion evidence are durable.

**Proof:** migration apply/replay/concurrency tests, schema assertions, tenant-key assertions, expiry indexes, and no mutable evidence fields.

**Commit:** `feat(store): add composition and lifecycle schema`

## Task 4: Build the typed compatibility graph

**Create:** `internal/composition/contract.go`, `internal/composition/compatibility.go`, `internal/composition/compatibility_test.go`, `internal/store/memory/composition.go`, `internal/store/storetest/composition.go`

Build edges only for exact resource type/namespace/identity and declared schema compatibility. The edge stores the satisfied requirement IDs, source provision IDs, interface hashes, projection epoch, and policy manifest. Reject wildcard identity, unknown schema compatibility, effect-only guesses, cross-tenant input, and superseded/quarantined versions.

Use a bounded explicit compatibility matrix in the planner manifest; do not infer semver compatibility for resource schemas.

**Proof:** contract tests cover exact matches, compatible upgrades, incompatible schemas, multiple requirements, cross-tenant denial, duplicate edges, and stable ordering.

**Commit:** `feat(composition): build typed compatibility graph`

## Task 5: Implement deterministic bounded plan search

**Create:** `internal/composition/manifest.go`, `internal/composition/planner.go`, `internal/composition/planner_test.go`

Implement bounded beam search over immutable versions. The manifest fixes beam width, maximum depth, maximum procedures, bridge count, total tool cost, novelty penalty, unobserved-edge penalty, and every integer coefficient. State identity is a canonical hash of ordered nodes, satisfied requirements, produced resources, covered goals, and gaps.

Ordering is lexicographic by: hard validity, covered goals, evidence strength, observed end-to-end status, satisfied preconditions, negative risk cost, negative tool cost, negative length, then canonical state hash. Deduplicate by state hash and retain the better total-order state. Search exhaustion returns the best partial plan and stable gap codes.

**Proof:** golden fixtures cover bridge insertion, partial plans, beam truncation, cycles, cost limits, novelty penalties, and shuffled-input determinism.

**Commit:** `feat(composition): plan deterministic procedure chains`

## Task 6: Derive safe parallel groups

**Create:** `internal/composition/parallel.go`, `internal/composition/parallel_test.go`

After selecting a DAG, compute stable parallel groups. Two nodes can share a group only if neither reaches the other and their transitive reads/writes, exclusive resources, compensation boundaries, and policy constraints do not conflict. Group IDs and order are content-addressed.

**Proof:** tests cover read/read allowance, write/read and write/write exclusion, indirect dependency exclusion, compensation exclusion, and deterministic grouping across shuffled input.

**Commit:** `feat(composition): derive conflict-free parallel groups`

## Task 7: Persist plans and issue injection IDs atomically

**Create:** `internal/selection/model.go`, `internal/selection/repository.go`, `internal/store/memory/selection.go`, `internal/store/surreal/selection.go`, `internal/store/storetest/selection.go`

Define a canonical selection record containing query and request-context hashes, retrieval run ID, snapshot/manifests, plan nodes/edges/groups/gaps, novelty class, experiment assignment, and expiry. Generate the injection ID from tenant plus an idempotency identity, and commit the complete record before returning it. Concurrent duplicate requests converge on one record; changed context conflicts.

**Proof:** memory and SurrealDB contract suites cover atomicity, rollback after injected failures, replay, concurrent duplicates, tenant isolation, corruption detection, and expiry.

**Commit:** `feat(selection): persist plans before agent injection`

## Task 8: Extend retrieval API with executable plans

**Modify:** `proto/memjev/v1/retrieval.proto`, generated code, `internal/retrieval/service.go`, `internal/api/retrieval_handler.go`, associated tests

Return a plan envelope with injection ID, ordered procedure nodes, explicit bridges, dependencies, parallel groups, gaps, novelty class, and provenance. Preserve candidates for explanation but make the plan the agent-facing artifact. A length-one observed plan remains backward compatible. Add an explanation view for composition edge facts and search-limit gaps.

**Proof:** handler tests require scope/tenant binding, committed selection before response, no raw resource identities, stable protobuf bytes on replay, and safe error mapping.

**Commit:** `feat(api): return persisted executable plans`

## Task 9: Link outcomes to selections and assign conservative credit

**Create:** `internal/credit/model.go`, `internal/credit/assign.go`, `internal/credit/assign_test.go`; **modify:** outcome contract/service and repositories

Accept injection ID and task-execution identity on outcome submission. Assign exactly one of `causal_success`, `associated_success`, `causal_failure`, `associated_failure`, or `unattributable` using versioned rules. Causal credit requires matching tenant, execution identity, goal predicates, temporal window, and selected node or plan. Reject forged, expired, cross-tenant, or multiply claimed linkage.

**Proof:** tests cover delayed outcomes, retries, partial plans, mixed node outcomes, duplicate receipts, foreign injection IDs, and evidence that is valid but only associated.

**Commit:** `feat(credit): attribute outcomes to injected plans`

## Task 10: Implement append-only lifecycle evaluation

**Create:** `internal/lifecycle/manifest.go`, `internal/lifecycle/evaluate.go`, `internal/lifecycle/evaluate_test.go`, repository adapters and contracts

Define states `candidate`, `trial`, `active`, `retired`, and `quarantined`. Use a versioned conservative policy with minimum causal samples, integer fixed-point Wilson lower bounds, unsafe-outcome ceilings, evidence freshness, trial duration, and immediate quarantine triggers. Associated evidence is reported but excluded from automatic transitions. Evaluation emits an immutable decision with all counts, bounds, reason codes, and prior state.

Activation and rollback publish a new retrieval-document projection epoch; they never mutate historical documents. At most one active champion exists per procedure family under a lifecycle policy.

**Proof:** exact boundary vectors, insufficient evidence, unsafe rollback, stale evidence, deterministic integer arithmetic, one-champion invariant, and rebuild equivalence.

**Commit:** `feat(lifecycle): evaluate auditable procedure promotion`

## Task 11: Add deterministic champion/challenger assignment

**Create:** `internal/experiment/manifest.go`, `internal/experiment/assign.go`, `internal/experiment/assign_test.go`

Assign only eligible low/medium-risk traffic using a hash of tenant, family, canonical query bucket, and experiment epoch. Manifests cap challenger exposure, total concurrent trials, and tenant/global unsafe budgets. High-risk exploration requires explicit tenant opt-in; critical risk is never automatically explored. Persist assignment before serving and make retries stable.

**Proof:** distribution fixtures, exact boundary hashes, epoch rollover, opt-out, risk gates, exposure caps, and deterministic replay.

**Commit:** `feat(experiment): assign deterministic challenger traffic`

## Task 12: Workers, rebuild, and operational qualification

**Modify/Create:** worker workflows, rebuild service, E2E/load/chaos tests, alerts, dashboards, and runbooks for composition/lifecycle

Add durable jobs for compatibility projection, lifecycle evaluation, exposure aggregation, expiry, and rollback. Rebuild compatibility, selection-derived credit, and lifecycle projections from canonical records and compare hashes before activation.

Qualification must include:

- hosted bridge, partial-plan, stable-tie, and parallel-group cases;
- concurrent selection persistence and byte-identical replay;
- projection advance while an old injection replays;
- duplicate/delayed/foreign outcome credit;
- champion promotion, unsafe rollback, and one-champion contention;
- planner-manifest rollback and compatibility corruption quarantine;
- worker crash/restart, SurrealDB failover, backlog recovery, and regional rebuild;
- mixed retrieval/composition/outcome traffic with server CPU/RSS, snapshot age, queue depth, and latency recorded.

**Commit:** `test: qualify composition and lifecycle production path`

## Completion gate

This slice is complete only when all twelve tasks are committed; migrations replay safely; race, generation, vet, lint, security, and full-stack checks pass; every agent-facing plan has a committed injection ID; outcome credit is tenant-bound and reproducible; rebuilds are byte-identical; and lifecycle rollback is proven.
