# Jev Judgment Ledger Implementation Plan

**Goal:** Add direct TypeSafe Jev judgments as a bounded, tenant-isolated, replayable ranking enhancement while retaining deterministic no-Jev availability.

**Architecture:** Eligible candidates are preliminarily ranked, deterministically admitted, and evaluated pointwise through a hardened TypeSafe adapter. Canonical typed results are committed once to a tenant-scoped ledger and converted to bounded integer features. The retrieval run persists judgment provenance and exact derived features. All provider and capacity failures degrade to the existing ranker.

**Provider:** `POST https://api.typesafe.ai/v1/systemone`, bearer key from a mounted file, pinned model, no redirects, one interactive attempt.

## Task 1: Canonical rubric and typed judgments

**Create:** `internal/jev/model.go`, `internal/jev/rubric.go`, `internal/jev/quantize.go` and tests/golden data.

- Define immutable rubric manifests and typed Score/Noul answers.
- Canonicalize manifests and judgment keys with stable hashes.
- Quantize decimal probabilities to micros without binary-float input at the persistence/ranking boundary.
- Validate exact answer sets, distributions, usage, and model identity.
- Prove identical input produces byte-identical manifests, keys, and derived features.

## Task 2: Secret-file resolver and direct TypeSafe adapter

**Create:** `internal/jev/secret.go`, `internal/jev/client.go` and tests.

- Load only a bounded, regular, restrictively-permissioned secret file.
- Implement the official request/response contract with strict JSON decoding and bounded bodies.
- Disable redirects; require HTTPS and the configured allowlisted host.
- Classify timeout, cancellation, authentication, throttling, server, transport, oversized, and invalid-response failures into stable codes.
- Verify no error contains the authorization value, raw request state, or response body.

## Task 3: Immutable judgment repository contracts

**Create:** `internal/jev/repository.go`, store contract tests, memory repository.

- Implement tenant-bound lookup and create-if-absent.
- Make identical writes idempotent and conflicting writes fail closed.
- Preserve expired judgments as immutable audit records while excluding them from reusable hits.
- Test cross-tenant denial, concurrent winners, canonical validation, and cancellation.

## Task 4: SurrealDB schema and repository

**Create:** the next ordered migration and `internal/store/surreal/jev.go` with integration tests.

- Add schema-full rubric and judgment tables with unique tenant/key indexes.
- Commit or read the authoritative winner transactionally.
- Add expiry and model/rubric lookup indexes.
- Prove migration replay and repository parity with the memory implementation.

## Task 5: Admission, single-flight, quotas, and circuit breaker

**Create:** `internal/jev/admission.go`, `internal/jev/service.go` and tests.

- Deterministically choose the ambiguity band from preliminary ranks.
- Enforce request candidate/token budgets, per-tenant/global rates, bounded concurrency, and minimum remaining deadline.
- Collapse same-key misses with cancellation-safe single-flight.
- Implement a state-machine circuit breaker with monotonic-clock durations and authentication fast-open behavior.
- Return stable degradation codes; never fail core retrieval because Jev is unavailable.

## Task 6: Configuration and production wiring

**Modify:** `internal/config`, `cmd/api`, Compose/Kubernetes/runtime examples and tests.

- Add fail-closed Jev configuration and unknown-variable coverage.
- Require pinned model and secret file outside development when enabled.
- Construct dedicated transport, repository, service, and shutdown lifecycle.
- Keep disabled configuration behavior byte-compatible with current no-Jev retrieval.

## Task 7: Retrieval integration and stable fusion

**Modify:** retrieval and ranking models/services/tests.

- Run preliminary rank only after all eligibility gates.
- Judge only deterministically admitted candidates.
- Append bounded Jev feature values and rerun the same manifest-driven integer ranker.
- Define explicit manifest missing values for every Jev feature.
- Prove Jev cannot override rejections and every failure mode yields the expected no-Jev ordering.

## Task 8: Provenance, replay, and explanations

**Modify:** retrieval run model, repositories, schema, explanations, APIs, and tests.

- Persist judgment key, disposition, content hash, provider/model/rubric IDs, and exact derived features per candidate.
- Make idempotent retrieval replay perform zero provider calls.
- Redact state and answer payloads from explanations while exposing safe provenance and degradation codes.
- Prove historical replay after model/rubric/config changes.

## Task 9: Observability and security qualification

**Modify:** observability package, security tests, dashboards/alerts, and runbooks.

- Add bounded-cardinality metrics for admission, hits, commits, degradation, latency, circuit state, and usage.
- Verify logs/traces exclude task text, procedure text, API key, request state, and provider body.
- Add hostile server, redirect, oversized-body, malformed JSON, cancellation, race, and secret-rotation tests.

## Task 10: End-to-end and provider qualification

**Create/modify:** integration harnesses, Make targets, CI gates, and operator documentation.

- Exercise cold miss, warm hit, concurrent miss, 429, timeout, 5xx, corrupt response, circuit open/recovery, and no-Jev fallback.
- Add an opt-in live TypeSafe smoke test that reads the mounted key file and never prints it.
- Measure provider-disabled and enabled paths at 100 mixed RPS; prove provider saturation does not reduce core availability.
- Run race, lint, generation, migration replay, vulnerability, image, and full-stack gates.

Implementation is complete only when all ten tasks are committed, disabled-mode compatibility is proven, live-provider smoke passes with an operator-supplied secret, and documented load/chaos evidence supports the production claims.
