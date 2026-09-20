# Jev ranking enhancement operations

Jev is a post-eligibility, pointwise ranking enhancement. Exact, lexical, facet, graph, policy gates, and the deterministic integer ranker remain authoritative. Provider failure, quota exhaustion, a circuit-open state, or invalid output must preserve the preliminary no-Jev ordering.

## Enablement prerequisites

1. Pin an exact TypeSafe model matching `jev-X.Y.Z`; never use a floating alias.
2. Put the API key alone in a root/operator-managed regular file, remove the trailing newline if the provider key format requires it, and set mode `0600`. Never put the key in an environment variable, Compose file, credential document, log, trace, or ticket.
3. Set `allow_external_inference: true` only on tenant credentials whose data-processing agreement and residency policy permit TypeSafe inference. This server-owned flag cannot be supplied by a retrieval request.
4. Publish a new immutable ranker manifest that declares all five features below. Missing values are explicit zeroes so degradation is identical to the no-Jev ranker:

```json
[
  {"name":"jev_intent_fit_micros","minimum":0,"maximum":1000000,"missing":0,"coefficient":100000},
  {"name":"jev_non_contradiction_micros","minimum":0,"maximum":1000000,"missing":0,"coefficient":100000},
  {"name":"jev_partial_plan_micros","minimum":0,"maximum":1000000,"missing":0,"coefficient":25000},
  {"name":"jev_preconditions_micros","minimum":0,"maximum":1000000,"missing":0,"coefficient":75000},
  {"name":"jev_task_coverage_micros","minimum":0,"maximum":1000000,"missing":0,"coefficient":100000}
]
```

The coefficients are a conservative starting manifest, not mutable configuration. Calibrate them offline against a versioned evaluation set, publish a new manifest, canary by tenant serving head, and retain the prior manifest for instant rollback.

## Local encrypted-secret deployment

Set `MEMJEV_JEV_API_KEY_HOST_FILE` and `MEMJEV_JEV_MODEL` in the operator shell or ignored `.env.local`, then run:

```sh
docker compose --env-file .env.local -f compose.yaml -f compose.jev.yaml up --build -d
```

The one-shot initializer copies the key to a mode-0400 named volume owned by API UID 65532. The normal Compose profile remains provider-disabled and requires no Jev secret.

## Triage and safe rollback

- Page immediately on authentication failures; confirm the mounted file was rotated, then restart only the API replicas after the secret mount updates.
- For throttling, timeout, capacity, or circuit-open alerts, do not disable core retrieval. Reduce the admitted-candidate cap or token budgets only through reviewed configuration and observe no-Jev availability.
- Use `ExplainRetrieval` to inspect judgment key, provider/model/rubric IDs, content hash, exact integer features, and degradation code. Raw task state and TypeSafe answers are deliberately absent.
- Roll back by activating the prior immutable ranker/serving configuration or setting `MEMJEV_JEV_ENABLED=false`. Existing retrieval runs remain replayable and perform zero provider calls.
- Never delete a judgment to force reevaluation. Change the query/document/environment/policy/rubric/model identity or wait for its bounded reuse window; immutable rows remain audit evidence.

## Required release evidence

Run `make verify`, SurrealDB integration tests, the full Compose integration gate, and the opt-in live smoke test. Retain cold-miss, warm-hit, concurrent single-flight, timeout, 429, 5xx, malformed/oversized response, circuit recovery, secret rotation, 100-RPS fallback, and provider-enabled latency evidence with the release record. A successful live call is required before enabling a new model in production.

The live smoke command reads the same restricted file source and prints only the pinned model, token counts, and quantized feature values:

```sh
MEMJEV_JEV_API_KEY_FILE=/run/secrets/jev-api-key \
MEMJEV_JEV_MODEL=jev-1.13.0 \
make jev-smoke
```
