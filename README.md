<div align="center">

<h1>memJev</h1>

<h3>Deterministic procedural memory for agents.</h3>

<p>
  <a href="https://memjev.getmetacognition.com">Website</a> ·
  <a href="#quickstart">Quickstart</a> ·
  <a href="#add-memjev-to-an-agent">Agent setup</a> ·
  <a href="#api">API</a> ·
  <a href="#how-it-works">Architecture</a>
</p>

<p>
  <a href="https://github.com/Sauhard74/mem-jev/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/Sauhard74/mem-jev/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://go.dev/"><img alt="Go 1.25+" src="https://img.shields.io/badge/Go-1.25%2B-000000"></a>
  <a href="LICENSE"><img alt="AGPL v3" src="https://img.shields.io/badge/License-AGPL%20v3-000000.svg"></a>
</p>

<br>

<h3>Successful runs should compound.</h3>

<p>
  Record what worked. Verify it. Give the next agent a deterministic head start.
</p>

</div>

---

memJev turns successful agent runs into versioned, evidence-backed procedures and retrieves the right procedure for future tasks. It sits around your existing agent stack—capturing tool traces after execution and returning advisory plans before the next run.

| Capture | Verify | Reuse |
| --- | --- | --- |
| Record sanitized tool calls in execution order. | Promote runs only after independent outcome evidence. | Retrieve a compatible procedure or safely abstain. |

Unlike a fuzzy trace cache, memJev checks tools, resources, policies, environment, freshness, and risk before a procedure can be returned. Every decision remains inspectable and replayable.

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
