# Integrate memJev with an agent

memJev sits around an agent run. It does not execute tools and it does not replace the agent framework. The harness retrieves advisory guidance before execution, captures the real tool trace during execution, and records independently verified evidence afterward.

```text
task + capabilities
        │
        ▼
     Retrieve ── selected plan ──► agent context
        │                              │
        │ abstain                      ▼
        └──────────────────────► normal agent run
                                       │
                                       ▼
                              ordered tool events
                                       │
                                       ▼
                                  IngestTrace
                                       │
                              independent verifier
                                       │
                                       ▼
                                 RecordOutcome
```

## The integration contract

### 1. Retrieve before execution

Send the task together with the tools, tool-contract versions, accessible resources, environment facts, forbidden effects, and risk/latency classes that apply to this execution. Do not claim capabilities the agent does not have.

Treat the response as follows:

- `SELECTED`: give `plan` to the agent as advisory context. Preserve `plan.injectionId` and `plan.taskExecutionId`.
- `ABSTAINED`: run the agent normally. Abstention is an intentional safe result, not an exception.
- unavailable or timed out: follow your product's fallback policy. memJev is designed so the agent can continue without recalled guidance.

Never turn a procedure node directly into an unreviewed tool execution. The plan can contain gaps and constraints, and remains advisory.

### 2. Capture the real trace

Record tool events in execution order. Each event needs a stable client event ID, occurrence time, kind, tool identity/version, sanitized inputs, and the real tool result. Preserve failed attempts: synthesis decides which dead ends are relevant and which successful path can be promoted.

Do not send raw credentials. The service sanitizes known secret forms, but the harness should avoid collecting secrets in the first place.

Call `IngestTrace` after execution, or durably spool and retry it asynchronously. Keep its returned `traceId`; outcome evidence is bound to it.

### 3. Verify outside the model

Do not use “the model said it succeeded” as authoritative evidence. Run a verifier appropriate to the task: tests, an API read-back, a state assertion, a signed callback, or another deterministic goal predicate.

Call `RecordOutcome` with at least one evidence item. If the run used a recalled plan, include the original `injectionId` and `taskExecutionId`. That link is what allows memJev to distinguish causal use of a plan from an unrelated success.

### 4. Make retries deterministic

Every mutating call requires an `Idempotency-Key`. Generate one logical key per operation and persist it with your run. Retrying the same payload with the same key returns the original result; reusing a key for a different payload is rejected.

The bearer credential determines the tenant, region, scopes, and consent policy. A request body cannot select another tenant.

## Minimal harness shape

```text
plan = memjev.retrieve(task, tools, resources, constraints)
result, events = agent.run(task, advisory_plan=plan if selected else none)
trace = memjev.ingest(task, events)
evidence = verifier.check(task, result)
memjev.record_outcome(trace.id, evidence, plan.injection_id)
```

A complete, compiling implementation is available in [`examples/go-agent/main.go`](../examples/go-agent/main.go). Replace only its `executeAgent` function with the framework-specific invocation and event capture.

## Tool contracts and first-run behavior

Tool names alone are insufficient for safe replay. A tenant operator must provision immutable tool-contract versions describing inputs, outputs, effects, resources, and verification semantics. Retrieval is enabled only after compatible policy, ranker, index, and serving manifests are active.

Consequently, a clean self-hosted database will not immediately return a procedure. It will abstain or report unavailable retrieval configuration until the operator has provisioned those controls and enough verified evidence has produced an eligible procedure. This is fail-closed behavior, not a cache miss disguised as a match.

## Production checklist

- Persist idempotency keys and memJev IDs with the agent execution.
- Bound retrieval latency and continue safely on abstention or configured fallback errors.
- Pass only capabilities and resources available to this exact run.
- Keep the returned plan advisory; enforce your normal tool authorization checks.
- Capture failed and successful tool results without rewriting history.
- Verify outcomes independently and submit corrections as new evidence rather than mutating old evidence.
- Never log bearer credentials, raw secrets, or unencrypted task payloads.
- Use the explanation endpoint for audits, not as input to automatic execution.
