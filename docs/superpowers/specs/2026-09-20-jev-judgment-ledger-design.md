# Jev Judgment Ledger Design

**Status:** approved 2026-09-20

## 1. Outcome

Add TypeSafe Jev as a bounded semantic enhancement to the hosted retrieval path without making model inference a correctness or availability dependency. Hard eligibility remains deterministic code. Jev evaluates only already-eligible, ambiguous procedure versions. A committed typed judgment becomes immutable input to the versioned integer ranker; provider failure selects the existing no-Jev ranker.

The production guarantee is deliberately narrow: for a fixed serving snapshot, canonical query, candidate pool, policy and ranker manifests, and committed judgment set, ranking and replay are byte-reproducible. A first uncached inference is probabilistic and is never described as deterministic.

## 2. Provider contract and semantic rubric

The adapter calls TypeSafe directly:

- `POST https://api.typesafe.ai/v1/systemone`;
- bearer authentication loaded from a mounted secret file;
- explicit model version in production, never the moving `jev-latest` alias;
- structured `state` containing only the canonical task, sanitized compatibility facts, and one procedure version;
- a versioned map of independent typed questions.

Rubric v1 uses five atomic questions:

- `intent_fit`: Score with five ordered semantic-fit levels;
- `preconditions_likely_satisfied`: Noul;
- `task_coverage`: Score with five ordered coverage levels;
- `contradicts_request`: Noul;
- `useful_as_partial_plan`: Noul.

The adapter validates the response model, exact answer set, answer types, finite probabilities, probability ranges and sums, level counts, choice keys, usage bounds, and maximum body size. It stores provider probabilities as decimal micros using a specified round-half-away-from-zero conversion. The application derives ranker features and performs all arithmetic, thresholds, weighting, and tie-breaking. Provider prose is neither requested nor stored.

## 3. Admission and request path

The retrieval service first runs all existing tenant, consent, residency, lifecycle, compatibility, resource, policy, freshness, and risk gates. Jev cannot admit a rejected candidate or modify a rejection.

The no-Jev ranker produces a preliminary total order. A versioned admission manifest selects an ambiguity band from the eligible top pool using only deterministic properties: rank distance, maximum candidate count, risk class, tenant policy, latency class, and request budget. Exact high-confidence winners bypass Jev. Candidates are judged independently so their input cannot leak the current ordering or competing procedures.

For each admitted candidate the service computes a judgment key from:

- tenant privacy namespace;
- canonical query hash;
- immutable procedure-version ID and semantic document hash;
- sanitized environment-fact hash;
- eligibility-policy ID;
- rubric manifest ID;
- pinned provider and model version.

The service checks the durable ledger first. Cache misses pass through process-local single-flight and then distributed create-if-absent semantics. Only one valid immutable record wins. A concurrent loser reads the winner. An already-committed record is never overwritten; model or rubric changes create a new namespace.

## 4. Capacity and failure isolation

Jev uses a dedicated HTTP transport, semaphore, and admission budget rather than the core retrieval worker pool. Controls are applied in this order:

1. feature and tenant policy;
2. request deadline and minimum remaining-time guard;
3. per-tenant token bucket;
4. global token bucket;
5. bounded concurrency semaphore;
6. circuit breaker;
7. one provider attempt within a hard sub-deadline.

Interactive retrieval never retries Jev. Retries multiply tail latency and provider load; asynchronous warming may retry with bounded exponential backoff and jitter. Timeout, cancellation, 401/403, 429, 5xx, oversized or malformed output, capacity exhaustion, budget rejection, or circuit-open state records a stable degradation code and continues with the no-Jev ranker. Authentication failures open the circuit immediately and alert operators, but do not fail retrieval.

The initial production deadline is configurable and capped below the retrieval request deadline. Rate and concurrency settings are conservative defaults that must be raised only from measured TypeSafe capacity. At 100 mixed RPS, the core no-Jev path remains independently qualified.

## 5. Persistence and replay

SurrealDB gains two schema-full records:

- `jev_rubric_manifest`: immutable canonical rubric, model pin, quantization contract, admission version, and content hash;
- `jev_judgment`: immutable tenant-scoped key, input hashes, provider/model/rubric identity, quantized typed answers, usage, timestamps, content hash, and expiry policy.

The unique judgment key enforces one authoritative record. Raw API keys are never persisted. Raw provider requests and canonical task/procedure text are not placed in logs, metrics, or judgment rows. The encrypted canonical query remains governed by the existing retrieval-run retention contract.

Each retrieval run records, per eligible candidate, the judgment key, disposition (`hit`, `committed`, or a degradation code), judgment content hash when present, and the exact derived feature values. Replay never calls TypeSafe: it uses the persisted ranked features and committed judgment references captured by the original run.

Judgment expiry controls reuse, not mutation. Expired entries remain immutable audit evidence until their retention policy removes them. Legal erasure deletes tenant-owned judgment rows and encrypted content through the platform erasure workflow, while non-content audit tombstones retain only permitted identifiers and hashes.

## 6. Ranking integration

The existing ranker manifest is extended only through named bounded features. Rubric v1 derives:

- `jev_intent_fit_micros`;
- `jev_preconditions_micros`;
- `jev_task_coverage_micros`;
- `jev_non_contradiction_micros` as `1_000_000 - contradicts_request`;
- `jev_partial_plan_micros`;
- optional confidence features derived from returned distributions.

Missing Jev features use explicit no-Jev values from the ranker manifest. They never default implicitly to zero. The same checked fixed-width integer scoring, deterministic total ordering, and stable tie-breaks remain authoritative. Jev cannot override hard gates, and a response is not accepted merely because Jev favors it: the final score and selection threshold stay in versioned code and manifests.

## 7. Configuration and secrets

Production configuration includes an enable flag, endpoint allowlist, pinned model, rubric file, secret-file path, hard timeout, response-size limit, candidate and token budgets, concurrency, tenant/global rates and bursts, circuit thresholds, and judgment reuse lifetime. Unknown configuration remains fail-closed.

The API key is read from `MEMJEV_JEV_API_KEY_FILE`. Direct key environment variables are intentionally unsupported. The file must be a regular file with restrictive permissions, a bounded size, and a single trimmed token. The client supports atomic secret rotation by re-reading on a bounded schedule or filesystem identity change without logging either old or new material.

## 8. Observability and security

Metrics expose admission, cache hit, committed judgment, fallback reason, latency, circuit state, provider status class, token usage, and validation failure. Tenant labels follow the existing opt-in cardinality policy. Traces contain hashes, manifest IDs, durations, and result codes only. No task text, procedure text, state, answers containing content, authorization headers, or secret paths are recorded.

Outbound access is restricted to the configured HTTPS TypeSafe host, with redirects disabled, TLS verification required, connection limits bounded, and proxy behavior explicit. Request bodies are size-bounded before send and response bodies before decode. Tests include malicious/oversized JSON, NaN-like values, wrong answer types, incomplete answer maps, status handling, redirect rejection, cancellation, secret redaction, tenant isolation, and race conditions.

## 9. Delivery and qualification

Implementation proceeds test-first in these increments:

1. typed domain model, canonical rubric, quantization, and golden vectors;
2. hardened direct TypeSafe HTTP adapter and secret-file resolver;
3. memory and SurrealDB judgment-ledger repositories with contract tests;
4. single-flight, quotas, semaphore, circuit breaker, and deterministic degradation;
5. preliminary-rank admission and Jev feature fusion in retrieval;
6. persisted provenance, replay, explanation redaction, and observability;
7. live-provider smoke test using an operator-supplied mounted secret;
8. cold, warm, throttled, timeout, corrupt-response, and provider-outage load/chaos qualification.

Release requires the full no-Jev verification suite, deterministic golden replay, race tests, migration replay, vulnerability scans, and a provider-enabled qualification showing that Jev saturation never violates the core retrieval availability target.
