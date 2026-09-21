# memJev

**Deterministic procedural memory for agents.**

[![CI](https://github.com/Sauhard74/mem-jev/actions/workflows/ci.yml/badge.svg)](https://github.com/Sauhard74/mem-jev/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.25%2B-000000)](https://go.dev/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-000000.svg)](LICENSE)

memJev turns successful agent runs into versioned, evidence-backed procedures and retrieves the right procedure for future tasks. It is a memory layer around your agent—not another agent framework and not a model-training system.

```text
successful run → verified evidence → reusable procedure
new task       → deterministic retrieval → advisory plan
```

## Why memJev?

Agents repeatedly spend tokens and tool calls rediscovering workflows they have already completed. A fuzzy trace cache helps, but it can replay the wrong path when tools, resources, policies, or environments differ.

memJev makes reuse explicit and inspectable:

- Records sanitized, ordered tool traces.
- Requires verified outcome evidence before a run can become memory.
- Removes dead ends while preserving relevant negative paths.
- Retrieves with exact, lexical, graph, facet, and optional vector signals.
- Applies deterministic compatibility and policy gates before ranking.
- Returns an advisory plan with provenance—or safely abstains.
- Preserves immutable procedure revisions and replayable decisions.
- Isolates every tenant through credential-derived identity.

## Quickstart

You need Go 1.25+, Docker with Compose v2, OpenSSL, curl, and Python 3.

```sh
git clone https://github.com/Sauhard74/mem-jev.git
cd mem-jev
make quickstart
```

That command creates local credentials, starts the stack, waits for readiness, and demonstrates the complete public API flow:

1. Retrieve an advisory procedure.
2. Record the agent's tool trace.
3. Submit independently verified outcome evidence.

A new corpus has no reusable procedures yet, so the first retrieval may abstain. The run is still recorded and can become evidence for future procedures once its tool contracts and outcome are verified.

### Useful commands

```sh
make setup            # Start the local stack
make quickstart       # Run the retrieve → trace → outcome example
make onboarding-test  # Verify the setup and API lifecycle
make test             # Run the Go test suite with the race detector
make verify           # Run all repository quality checks
make dev-down         # Stop the stack and keep its data
```

## Add memJev to an agent

memJev wraps an agent execution in three calls:

```text
plan = memjev.retrieve(task, tools, resources, constraints)
result, events = agent.run(task, advisory_plan=plan)
trace = memjev.ingest(task, events)
evidence = verifier.check(task, result)
memjev.record_outcome(trace.id, evidence, plan.injection_id)
```

The plan is advisory. Your agent keeps its existing authorization, tool execution, and safety controls. If memJev abstains or retrieval is unavailable, the agent can continue normally.

See the [agent integration guide](docs/agent-integration.md) for the lifecycle and safety contract, or run the compiling Go example:

```sh
set -a
source .env.local
set +a

MEMJEV_URL="$MEMJEV_E2E_API_URL" \
MEMJEV_TOKEN="$MEMJEV_E2E_TOKEN" \
go run ./examples/go-agent "write a hello file"
```

## API

memJev exposes Connect/Protobuf services with JSON-compatible HTTP endpoints.

| Service | Method | Purpose |
| --- | --- | --- |
| `RetrievalService` | `Retrieve` | Find an eligible advisory plan for a task. |
| `RetrievalService` | `ExplainRetrieval` | Inspect candidates, gates, scores, and provenance. |
| `IngestService` | `IngestTrace` | Record the ordered events from an agent run. |
| `OutcomeService` | `RecordOutcome` | Attach independently verified evidence to a trace. |
| `HealthService` | `Check` | Report service readiness. |

Every mutating operation is idempotent. Authentication determines the tenant and allowed scopes; request bodies cannot choose a tenant.

Copy-paste requests are available in the [HTTP API walkthrough](docs/api-quickstart.md). The versioned contracts live in [`proto/memjev/v1`](proto/memjev/v1).

## How it works

```text
                     ┌──────────────────────────┐
agent / harness ────►│ retrieve advisory memory │
        │            └────────────┬─────────────┘
        │                         │
        ▼                         ▼
 ordered trace              deterministic gates
        │                  + stable rank fusion
        ▼                         │
 verified outcome                ▼
        │                   procedure or abstain
        ▼
 procedure synthesis
        │
        ▼
 immutable graph + retrieval indexes
```

The system stores canonical evidence and immutable procedure revisions. Retrieval takes a consistent snapshot, generates candidates, rejects incompatible options, applies stable integer scoring, and persists the complete decision before returning it.

SurrealDB provides graph, document, text, and vector storage. Temporal runs durable evidence and synthesis workflows. An S3-compatible archive stores canonical traces.

## Optional Jev judgments

[TypeSafe Jev](https://typesafe.ai/) can judge ambiguous candidates after deterministic eligibility gates have run. It is optional and never required for availability. Accepted judgments are recorded with their model, rubric, features, and content hash so replays do not call the provider again.

```sh
export MEMJEV_JEV_API_KEY_HOST_FILE=/absolute/path/to/jev-api-key
export MEMJEV_JEV_MODEL=jev-1.13.0
docker compose --env-file .env.local -f compose.yaml -f compose.jev.yaml up --build -d
```

Read the [Jev guide](docs/runbooks/jev.md) before enabling it.

## Documentation

- [Agent integration](docs/agent-integration.md)
- [HTTP API walkthrough](docs/api-quickstart.md)
- [Local development](docs/runbooks/local-development.md)
- [Architecture](docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md)
- [Retrieval behavior](docs/runbooks/retrieval.md)
- [Evidence pipeline](docs/runbooks/evidence-pipeline.md)
- [Composition and lifecycle](docs/runbooks/composition-lifecycle.md)

## Contributing

Issues and pull requests are welcome. Before opening a PR:

```sh
make onboarding-test
make verify
```

Please include tests for behavior changes and keep generated Protobuf files in sync. For larger changes, open an issue first so the API and evidence model can be discussed before implementation.

## Security

Do not open a public issue for a vulnerability. Use GitHub's private vulnerability reporting for this repository. Never commit API keys, bearer tokens, production credentials, or raw trace data containing secrets.

## License

memJev is open source under the [GNU Affero General Public License v3.0](LICENSE).
