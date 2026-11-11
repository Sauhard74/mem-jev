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

Tool names alone are insufficient for safe replay. A tenant operator registers an immutable tool-contract version for each tool, describing its inputs, outputs, effects, resources, and verification semantics. A trace that uses a tool without a contract is recorded but never learned: the worker abstains with `opaque_tool`.

### Registering contracts

Put the contracts in a JSON file, `{"contracts": [ ... ]}`. [`examples/tool-contracts/quickstart.json`](../examples/tool-contracts/quickstart.json) is a complete example. The main fields are:

- `tool_id` and `version`: the `toolName` and `toolVersion` your traces report. `compatibility` lists the tool version ranges the contract covers.
- `inputs`: the fields the tool takes, each with a `sanitizer` (`token`, `text`, `relative_path`, `redact_secrets`, `integer`, or `drop`). Traces may only carry the fields `command`, `path`, `url_host`, `query_shape`, `resource_type`, `resource_id`, and `assertion`, so name inputs after these.
- `side_effect` (`none`, `read`, `write`, `external`, `irreversible`) and `risk` (`low`, `medium`, `high`, `critical`).
- `success_predicates` and `verification_methods`: a run is learned when its outcome evidence names one of these predicate IDs (`predicateId`) or verifier IDs (`verifierId`).

Register them with a running local stack:

```sh
scripts/register-tool-contracts.sh my-tools.json [--tenant tenant_local]
```

Elsewhere, pipe the same document with a `tenant_id` into the admin binary, which needs the API's database settings:

```sh
printf '%s' '{"tenant_id":"tenant_a","contracts":[...]}' | memjev-admin register-tool-contracts
```

Each registered contract prints one JSON line with its `contract_version_id` (`tcv_...`). Send that ID as the tool's `contractVersionId` in retrieval requests; a learned routine is recalled only when the request offers the same contract versions. Registering the same contract again does nothing. Changing a registered tool version is rejected, so publish a new `version` instead. Contracts registered for a tenant also apply to each of its people.

### First-run behavior

Retrieval also needs active policy, ranker, index, and serving manifests. In local development memJev activates a default configuration on the first recall; elsewhere an operator activates one (see the [retrieval runbook](runbooks/retrieval.md)).

Consequently, a clean database will not immediately return a procedure. It abstains until verified evidence for runs of contracted tools has produced an eligible procedure. This is fail-closed behavior, not a cache miss disguised as a match.

## One memory per person

A tenant's tokens share one memory unless they name a person. There are two ways to name one:

- A token issued with `subject_id` (for example `scripts/add-person.sh alice`) always acts for that person.
- A token issued with `may_assert_subject` (`scripts/add-person.sh --service`) names the person on each request with the `Memjev-Subject` header. Use this when one trusted service acts for many people, such as a chat platform's memory plugin. Requests without the header use the tenant's shared memory. Other tokens that send the header are rejected.

Person IDs are 1 to 256 printable characters. Each person's data is stored in the scope `<tenant>__<person>`; IDs longer than 63 characters or with characters other than letters, digits, and dashes are replaced by `h_` and the first 40 hex digits of SHA-256 over `memjev-subject.v1`, a zero byte, and the ID. `scripts/add-person.sh` prints the scope, which you need to [erase one person](runbooks/tenant-erasure.md).

## Production checklist

- Persist idempotency keys and memJev IDs with the agent execution.
- Bound retrieval latency and continue safely on abstention or configured fallback errors.
- Pass only capabilities and resources available to this exact run.
- Keep the returned plan advisory; enforce your normal tool authorization checks.
- Capture failed and successful tool results without rewriting history.
- Verify outcomes independently and submit corrections as new evidence rather than mutating old evidence.
- Never log bearer credentials, raw secrets, or unencrypted task payloads.
- Use the explanation endpoint for audits, not as input to automatic execution.
