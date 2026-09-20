# Evidence Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Every behavior change follows test-driven development: observe the focused test fail, implement the minimum production behavior, then run the focused and surrounding suites.

**Goal:** Turn authenticated outcome evidence and archived canonical traces into deterministic, evidence-backed, immutable procedure versions while preserving complete provenance and rebuildability.

**Architecture:** The API appends authenticated outcome evidence to SurrealDB. A leased outbox relay starts one deterministic Temporal workflow per trace. Workflow activities reconstruct the canonical archive, evaluate evidence under a versioned policy, resolve versioned tool contracts, build causal dependencies only from declared resource/control/verifier relationships, synthesize the smallest justified successful subgraph, and atomically publish an immutable procedure projection plus a byte-verifiable manifest. SurrealDB remains business truth; Temporal coordinates retries and deadlines but owns no domain state.

**Tech Stack:** Go 1.23, Connect/Protobuf, SurrealDB 3.2.4, S3-compatible canonical archives, Temporal Go SDK 1.41.1 (last line supporting Go 1.23), OpenTelemetry, Docker Compose, and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md`

## Non-negotiable invariants

- Only `verified_success` is eligible for automatic procedure projection.
- Evidence strength is ordered: independent verifier, goal predicate, authenticated harness assertion, tool postcondition, exit status/self-report.
- Strong contradictory evidence yields `inconclusive` and an append-only audit event.
- Outcome evidence is append-only; correction is a new record that supersedes an earlier record.
- Outcome identity is derived from canonical content; transport idempotency is tenant-scoped and conflicts on different content.
- Outcome links name an authenticated tenant's trace and optionally a selection/execution; linkage is verified in storage.
- Canonical archives are re-read, decrypted, hash-verified, schema-validated, and their trace/event IDs recomputed before synthesis.
- Temporal adjacency never creates a causal edge.
- Unknown tools are opaque evidence. They cannot infer resources or produce an automatically promoted procedure.
- Failed branches become negative paths only with an exact compatibility scope and are never globally generalized.
- Procedure versions and synthesis manifests are immutable. An identical graph refreshes evidence for the same version; a different graph is a challenger version.
- Projection replacement is atomic and rebuildable from archives, outcomes, contracts, and signed/versioned manifests.
- Every background mutation is idempotent and safe under duplicate delivery, lease expiry, worker crash, and activity retry.
- No logs, metrics, workflow search attributes, errors, or Temporal payload metadata contain prompt, task, event fields, procedure text, secrets, or raw evidence.

## Task 1: Public outcome contract and strict boundary validation

**Files:**
- Create: `proto/memjev/v1/outcome.proto`
- Modify: `proto/memjev/v1/common.proto`
- Modify: `internal/contracts/validate.go`
- Modify: `internal/contracts/contracts_test.go`
- Generate: `gen/memjev/v1/outcome.pb.go`
- Generate: `gen/memjev/v1/memjevv1connect/outcome.connect.go`

**Produces:** `OutcomeService/RecordOutcome`, four explicit outcome states, five evidence classes, typed trace/selection/execution linkage, supersession, predicate identity, verifier provenance, and safe structured evidence fields. Tenant and idempotency remain transport metadata.

- [ ] Add descriptor and validation tests proving no tenant/idempotency fields, required trace linkage, bounded evidence, strict enums, and rejection of unknown JSON/protobuf fields.
- [ ] Run `go test ./internal/contracts -run Outcome -v` and observe RED.
- [ ] Define the contract, generate committed code, and implement transport-independent validation.
- [ ] Run `make generate && go test ./internal/contracts -v && ./scripts/check-generated.sh`.
- [ ] Commit as `feat(api): define authenticated outcome evidence contract`.

## Task 2: Canonical outcome domain and evidence evaluator

**Files:**
- Create: `internal/domain/outcome.go`
- Create: `internal/evidence/canonical.go`
- Create: `internal/evidence/evaluator.go`
- Create: `internal/evidence/evaluator_test.go`
- Modify: `internal/domain/ids.go`

**Produces:** canonical outcome/evidence IDs; `Evaluator` returning state, strongest satisfied/failed class, promotion eligibility, policy version, and conflict reasons.

- [ ] Write table tests for deterministic IDs, input-order independence, all hierarchy transitions, weak-vs-strong conflicts, strong-vs-strong contradictions, superseded evidence, and default fail-closed behavior.
- [ ] Observe focused RED tests.
- [ ] Implement canonical sorting/hashing and pure deterministic evaluation. No wall clock, maps with unstable iteration, or I/O may influence output.
- [ ] Run `go test -race ./internal/evidence ./internal/domain`.
- [ ] Commit as `feat(evidence): evaluate canonical outcome evidence deterministically`.

## Task 3: Immutable tool-contract registry

**Files:**
- Create: `internal/toolcontract/contract.go`
- Create: `internal/toolcontract/registry.go`
- Create: `internal/toolcontract/registry_test.go`
- Create: `internal/toolcontract/canonical.go`

**Produces:** versioned contract manifests covering canonical identity/aliases, typed resource inputs/outputs, effects, risk, retry/idempotency, pre/postconditions, verification, compensation, compatibility, and per-field sanitizers.

- [ ] Test exact/alias resolution, version compatibility, immutable hashes, duplicate aliases, unsafe sanitizer gaps, and opaque unknown-tool fallback.
- [ ] Observe RED, then implement an in-memory capability interface and deterministic manifest validator.
- [ ] Ensure opaque contracts expose no inferred resources and are never auto-promotable.
- [ ] Run `go test -race ./internal/toolcontract`.
- [ ] Commit as `feat(contracts): add immutable tool contract registry`.

## Task 4: Evidence and projection schema

**Files:**
- Create: `db/migrations/0002_evidence_pipeline.surql`
- Modify: `db/migrations/embed.go`
- Create: `internal/store/evidence_schema_test.go`
- Modify: `internal/testinfra/surreal.go`

**Produces:** strict tenant-scoped tables and indexes for outcome evidence, verification results, tool contracts/versions, procedure families/versions, steps, typed dependencies, resource links, negative paths, synthesis manifests, projection checkpoints, and audit events.

- [ ] Write migration tests for table/index presence, immutable field enforcement, uniqueness, cross-tenant edge rejection, and migration replay.
- [ ] Observe integration RED against pinned SurrealDB.
- [ ] Implement schema-full records, composite tenant keys, relationship endpoint checks, and migration checksum embedding.
- [ ] Run the focused integration suite twice to prove replay stability.
- [ ] Commit as `feat(store): add evidence and procedure projection schema`.

## Task 5: Transactional outcome append service and API

**Files:**
- Create: `internal/store/outcome.go`
- Create: `internal/store/outcome_surreal.go`
- Create: `internal/store/outcome_surreal_test.go`
- Create: `internal/outcome/service.go`
- Create: `internal/outcome/service_test.go`
- Create: `internal/api/outcome_handler.go`
- Create: `internal/api/outcome_handler_test.go`
- Modify: `internal/api/server.go`
- Modify: `cmd/api/main.go`

**Produces:** authenticated `POST /v1/outcomes` semantics that append evidence and an evaluation outbox intent atomically.

- [ ] Test credential-bound tenant isolation, consent, trace/selection linkage, idempotent replay, content conflict, supersession validation, secret rejection, concurrency, safe errors, and retry classification.
- [ ] Observe service and adapter RED tests.
- [ ] Implement service and Surreal transaction. Never read tenant identity from the request.
- [ ] Wire strict Connect codecs, bounded bodies, structured metrics, and identifier-only logs.
- [ ] Run unit, race, and pinned Surreal integration tests.
- [ ] Commit as `feat(outcomes): append verified evidence transactionally`.

## Task 6: Verified canonical archive reconstruction

**Files:**
- Modify: `internal/archive/archive.go`
- Modify: `internal/archive/s3.go`
- Modify: `internal/archive/memory.go`
- Create: `internal/rebuild/loader.go`
- Create: `internal/rebuild/loader_test.go`

**Produces:** a streaming archive reader that validates key, encryption envelope, byte count, SHA-256, canonical JSON, schema version, tenant/trace identity, event order, and recomputed IDs before exposing a batch.

- [ ] Test valid round-trip, truncation, tampering, wrong tenant/key/hash, reordered events, forged IDs, unknown schema, and cancellation/size limits.
- [ ] Observe RED and implement bounded streaming reads plus full canonical reconstruction.
- [ ] Run `go test -race ./internal/archive ./internal/rebuild`.
- [ ] Commit as `feat(rebuild): verify canonical archives before synthesis`.

## Task 7: Typed causal graph construction

**Files:**
- Create: `internal/synthesis/graph.go`
- Create: `internal/synthesis/builder.go`
- Create: `internal/synthesis/builder_test.go`

**Produces:** a stable DAG/multigraph representation with explicit resource-flow, control, verifier, ordering, effect, and compensation-boundary edges.

- [ ] Test that declared resource flow/control/verifier dependencies create edges, temporal adjacency alone does not, cycles fail closed, opaque tools infer none, incompatible resources do not link, and output is permutation-stable.
- [ ] Observe RED and implement deterministic node/edge ordering and cycle diagnostics.
- [ ] Run `go test -race ./internal/synthesis -run Builder`.
- [ ] Commit as `feat(synthesis): build causal graphs from declared dependencies`.

## Task 8: Deterministic minimal successful subgraph and negative paths

**Files:**
- Create: `internal/synthesis/synthesizer.go`
- Create: `internal/synthesis/synthesizer_test.go`
- Create: `internal/domain/procedure.go`
- Create: `internal/domain/negative_path.go`

**Produces:** a versioned synthesis result containing retained steps, prerequisites, verification nodes, effects/order/compensation boundaries, uncertain-necessity annotations, scoped negative paths, uncovered predicates, and exact input/version provenance.

- [ ] Test minimality, multiple goals, mandatory effects, uncertain necessity, irrelevant dead-end removal, failed-branch scoping, scope incompatibility, deterministic ties, and abstention for opaque/insufficient evidence.
- [ ] Observe RED and implement deterministic backward reachability plus conservative annotations.
- [ ] Property-test permutation invariance and idempotence.
- [ ] Run `go test -race ./internal/synthesis ./internal/domain`.
- [ ] Commit as `feat(synthesis): derive evidence-backed procedure subgraphs`.

## Task 9: Immutable projection writer and byte-identical rebuild

**Files:**
- Create: `internal/store/projection.go`
- Create: `internal/store/projection_surreal.go`
- Create: `internal/store/projection_surreal_test.go`
- Create: `internal/projection/service.go`
- Create: `internal/projection/service_test.go`

**Produces:** atomic publication of procedure family/version, ordered steps/edges, negative paths, evidence links, synthesis manifest, and checkpoint.

- [ ] Test same graph refreshes evidence without a new version, different graph creates a challenger, immutable records cannot be overwritten, partial failure publishes nothing, duplicate delivery converges, tenant isolation, and projection deletion/rebuild yields byte-identical canonical records.
- [ ] Observe RED, implement canonical projection hashes and a single Surreal transaction.
- [ ] Run focused tests twice and compare exported canonical projection bytes.
- [ ] Commit as `feat(projection): publish immutable rebuildable procedures`.

## Task 10: Leased outbox and Temporal workflow adapter

**Files:**
- Create: `internal/store/outbox.go`
- Create: `internal/store/outbox_surreal.go`
- Create: `internal/store/outbox_surreal_test.go`
- Create: `internal/workflow/client.go`
- Create: `internal/workflow/temporal.go`
- Create: `internal/workflow/temporal_test.go`
- Modify: `go.mod`
- Modify: `go.sum`

**Produces:** compare-and-claim leases, fencing tokens, retry/dead-letter state, deterministic workflow IDs, and a Temporal adapter pinned to SDK 1.41.1.

- [ ] Test competing claimers, lease expiry, stale fencing rejection, completion/retry idempotency, deterministic IDs, already-started handling, sanitized workflow inputs, and provider error classification.
- [ ] Observe RED and implement leasing against database time.
- [ ] Add Temporal SDK 1.41.1; configure TLS/API-key/mTLS capability without global clients or secrets in workflow inputs.
- [ ] Run `go test -race ./internal/store ./internal/workflow` plus pinned integration tests.
- [ ] Commit as `feat(workflow): relay synthesis intents durably`.

## Task 11: Deterministic synthesis workflow and worker

**Files:**
- Create: `internal/workflow/synthesis.go`
- Create: `internal/workflow/synthesis_test.go`
- Create: `internal/workflow/activities.go`
- Create: `internal/workflow/activities_test.go`
- Create: `cmd/worker/main.go`
- Create: `cmd/worker/main_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `compose.yaml`
- Modify: `Dockerfile`

**Produces:** one deterministic workflow per trace; bounded activities for load, evaluate, build, synthesize, and project; retries, heartbeats, timeouts, cancellation, graceful shutdown, health, and telemetry.

- [ ] Use Temporal's deterministic test environment to test replay, retry, cancellation, timeout, duplicate starts, non-retryable corruption, and payload privacy.
- [ ] Observe RED and implement workflows with no non-deterministic I/O inside workflow code.
- [ ] Implement a worker binary and outbox relay with separate readiness/liveness and graceful drain.
- [ ] Run race tests and the Temporal workflow test suite.
- [ ] Commit as `feat(worker): run deterministic evidence synthesis workflows`.

## Task 12: Production qualification and operations

**Files:**
- Create: `tests/e2e/evidence_pipeline_test.go`
- Create: `scripts/evidence-integration.sh`
- Modify: `scripts/integration.sh`
- Modify: `scripts/smoke.sh`
- Modify: `.github/workflows/ci.yml`
- Create: `docs/runbooks/evidence-pipeline.md`
- Create: `docs/runbooks/projection-rebuild.md`
- Create: `docs/runbooks/outbox-recovery.md`
- Modify: `docs/runbooks/local-development.md`

**Produces:** reproducible proof that a real authenticated trace plus outcome becomes exactly one verified immutable procedure and rebuilds identically after projection deletion.

- [ ] Add E2E cases for verified success, contradictory evidence, unknown tool abstention, negative-path scoping, cross-tenant denial, duplicate delivery, worker restart, and corrupted archive quarantine.
- [ ] Add metrics/alerts for outcome conflicts, archive corruption, synthesis abstention, lease age, retry/dead-letter, projection lag, and rebuild mismatch.
- [ ] Exercise clean-stack startup, API/worker health, migration replay, encrypted archive, Surreal records, workflow completion, and byte-identical rebuild.
- [ ] Run `make verify && ./scripts/integration.sh && ./scripts/smoke.sh` twice from a clean stack.
- [ ] Request an independent production review; resolve every Critical/Important finding with focused RED/GREEN tests.
- [ ] Commit as `test: qualify the production evidence pipeline`.

## Completion gate

The evidence slice is complete only when all twelve tasks are committed, the worktree is clean, generated code is reproducible, race/lint/vet pass, the pinned full stack passes twice, archive corruption fails closed, cross-tenant tests pass, deterministic workflow replay passes, and a projection rebuilt from canonical inputs is byte-identical to the original. Later retrieval work may depend only on the immutable projection and manifest contracts established here.
