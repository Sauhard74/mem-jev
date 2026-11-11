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

- `SELECTED`: give `plan` to the agent as advisory context. Preserve `plan.injectionId` and `plan.taskExecutionId`. Each `plan.nodes[].steps` lists the procedure's tool calls in order (tool name, version, and contract version; your harness supplies the arguments).
  - If `plan.advisoryOnly` is `true`, the plan comes from a routine that has not been promoted yet. See [unpromoted plans](#unpromoted-plans-and-promotion).
- `ABSTAINED`: run the agent normally. Abstention is an intentional safe result, not an exception. `abstentionCode` says why; `advisory_only` means only unpromoted routines matched and none could be handed out as a plan (see below), and `candidates` lists them for display only.
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

## Unpromoted plans and promotion

A newly learned routine starts as a `candidate`. memJev hands it out as a plan only when no promoted routine matches, and marks that plan `advisoryOnly: true` (as is the candidate entry). It is handed out only if it passes every check a promoted plan passes (residency, the request's forbidden effects, tool contract versions, and risk class), is low or medium risk, and no step's tool is irreversible or high or critical risk. Otherwise the response abstains with `advisory_only`.

What a harness should do with an unpromoted plan:

- Treat it as a suggestion to try, never as approval: keep your normal tool authorization and review, especially for writes.
- Run the task, verify it outside the model, and call `RecordOutcome` with the plan's `injectionId` and `taskExecutionId`.
- If you do not want to try unproven routines, ignore plans with `advisoryOnly: true` and run normally.

Promotion uses only verified outcomes of runs that were given the plan (linked by `injectionId` and `taskExecutionId`, with all goal evidence satisfied). A plan made of one learned routine covers the task however it was worded, so a person's verdict (`goal_predicate` or `independent_verifier` evidence) on a recalled run counts even when the request did not repeat the original task text. `provisional_success`, outcomes without the link, and runs that did not get the plan never count. Under the default policy (`memjev-default-lifecycle.v1`):

| Transition | When |
|---|---|
| candidate → trial | at least 3 such outcomes and a 95% Wilson lower bound on the success rate of at least 40% (3 of 3) |
| trial → active | at least 10 outcomes, lower bound at least 60%, and at least one hour in trial |
| trial → candidate | at least 3 outcomes and lower bound below 40% (verified failures) |
| active → retired | lower bound below 60% (verified failures), or no such outcome in 90 days |
| any → quarantined | an outcome reported `unsafe` (immediately; see below) |

### Corrections, unsafe runs, and feedback traces

- **Corrections.** To change an answer ("worked" to "didn't work"), record a new outcome with `supersedesOutcomeId` set to the outcome that holds the plan's credit, a `correctionReason`, and the same `injectionId`. The correction takes over the credit, and the earlier answer stops counting. Only the current holder can be superseded (`outcome_credit_claimed` otherwise). A correction may repeat facts of earlier outcomes.
- **Unsafe runs.** Set `unsafe: true` on an outcome with an `injectionId` when following the plan did something unsafe. If the run already has an outcome, send the unsafe report as a correction of it (`supersedesOutcomeId`). Every routine in that plan is quarantined before the call returns and is never served again. This works in every environment, whether or not automatic promotion is on.
- **Feedback traces.** If your harness ingests a separate trace for a person's feedback on a recalled run (the same steps plus a check), record its outcome with the plan's `injectionId`. memJev folds it into the recalled routine instead of learning a duplicate, as long as the plan had one routine and the extra steps only read (`side_effect` `none` or `read`).

Each person's scope (`tenant__person`) is evaluated on its own outcomes only. Every decision is stored in `lifecycle_decision` and logged by the worker as `lifecycle decision`. Run an evaluation immediately with:

```sh
printf '{"tenant_id":"tenant_local"}' | docker compose --env-file .env.local exec -T api /memjev-admin evaluate-lifecycle
```

It evaluates the tenant and each of its people and prints one JSON line per decision.

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

Or call the API from anywhere with a tenant credential that has the `contracts:write` scope (the local `MEMJEV_E2E_TOKEN` from `scripts/setup.sh` has it; per-person credentials never do). Send the same file as a string:

```sh
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data "$(python3 -c 'import json,sys; print(json.dumps({"contractsJson": open(sys.argv[1]).read()}))' my-tools.json)" \
  "$MEMJEV_URL/memjev.v1.ToolContractService/RegisterToolContracts"
```

The contracts are registered for the credential's tenant and listed in the response. Operators can also pipe the document with a `tenant_id` into the admin binary, which needs the API's database settings:

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

`scripts/add-person.sh` adds the token to the credential file and hands it to the running API, which re-reads its credential file every few seconds; the API is not restarted.

Person IDs are 1 to 256 printable characters. Tenant IDs may not contain `__`, and tenants with people may not end in `_`. Each person's data is stored in the scope `<tenant>__<person>`; IDs longer than 63 characters or with characters other than letters, digits, and dashes are replaced by `h_` and the first 40 hex digits of SHA-256 over `memjev-subject.v1`, a zero byte, and the ID. `scripts/add-person.sh` prints the scope, which you need to [erase one person](runbooks/tenant-erasure.md).

## Errors

Errors carry a reason code in the message (`request failed: outcome_credit_claimed`) and in `details[].debug.reasonCode`, with `retryable`. Retry only when `retryable` is true (`database_unavailable`, `snapshot_unavailable`, ...). Conflicts such as `outcome_credit_claimed`, `outcome_conflict`, `idempotency_conflict`, and `contract_version_conflict` are not retryable: fix the request instead. A retrieval for a person or tenant that has learned nothing yet fails with `nothing_learned_yet` and creates no state.

## Production checklist

- Persist idempotency keys and memJev IDs with the agent execution.
- Bound retrieval latency and continue safely on abstention or configured fallback errors.
- Pass only capabilities and resources available to this exact run.
- Keep the returned plan advisory; enforce your normal tool authorization checks.
- Capture failed and successful tool results without rewriting history.
- Verify outcomes independently and submit corrections as new evidence rather than mutating old evidence.
- Never log bearer credentials, raw secrets, or unencrypted task payloads.
- Use the explanation endpoint for audits, not as input to automatic execution.
