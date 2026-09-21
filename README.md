# memJev

memJev is a hosted, multi-tenant procedural-memory service for agents. It records sanitized traces and verified outcomes, derives immutable evidence-backed procedures, retrieves compatible procedures through deterministic gates and rank fusion, and returns an advisory plan with replayable provenance.

The honest product boundary is deliberate: this is procedural reuse and bounded composition, not general learning. Compared with a fuzzy trace cache, memJev adds verified outcome evidence, negative paths, tool/resource compatibility, hard eligibility gates, multi-procedure composition, immutable revisions, abstention, exact decision replay, and tenant isolation.

## Five-minute quickstart

Requirements: Go 1.25 or newer, Docker with Compose v2, OpenSSL, curl, and Python 3.

```sh
git clone https://github.com/Sauhard74/mem-jev.git
cd mem-jev
make quickstart
```

`make quickstart` generates mode-0600 development credentials, starts the complete local stack, waits for it to become healthy, and demonstrates the agent-facing lifecycle: retrieve a plan, ingest the resulting trace, and submit independently verified outcome evidence. It preserves an existing `.env.local`; it does not rotate credentials on every run.

A brand-new corpus has no eligible procedure and may have no active retrieval configuration. In that state, retrieval safely abstains or is unavailable and the example continues without recalled guidance. Ingest and outcome evidence are still recorded. Production tenants need versioned tool contracts and retrieval manifests provisioned before recall is enabled.

Useful local commands:

```sh
make setup          # create credentials and start the stack
make quickstart     # run the complete public API lifecycle
make onboarding-test
make dev-down       # stop containers but keep local volumes
```

To run the compiling Go integration example:

```sh
set -a
source .env.local
set +a
MEMJEV_URL="$MEMJEV_E2E_API_URL" MEMJEV_TOKEN="$MEMJEV_E2E_TOKEN" \
  go run ./examples/go-agent "write a hello file"
```

The framework-specific seam is `executeAgent`: give the returned plan to the agent as advisory context, capture its ordered tool events, and replace the example verifier with an independent task verifier. See [Integrate an agent](docs/agent-integration.md) and the [HTTP API walkthrough](docs/api-quickstart.md).

## Architecture

```text
agent/harness
    │
    ├── Ingest API ── sanitize/canonicalize ── S3 archive + SurrealDB ledger
    │                                                │
    ├── Outcome API ── authenticated evidence ───────┤
    │                                                ▼
    │                                      Temporal worker
    │                               graph → synthesis → revisions
    │                                                │
    └── Retrieval API                                ▼
          snapshot → exact/BM25/facet/graph/vector candidates
                   → deterministic compatibility and policy gates
                   → integer preliminary rank
                   → bounded TypeSafe Jev judgment for ambiguous candidates
                   → same integer ranker → composition or abstention
                   → atomic retrieval run + explanation provenance
```

Jev is never an eligibility or availability dependency. It runs only after hard gates, only for a deterministic ambiguity band, and only when the tenant credential permits external inference. A cold TypeSafe call is probabilistic; once accepted, the typed judgment, quantized features, model, rubric, and content hash are immutable. Expiry appends a content-linked successor and advances a compare-and-swap head instead of mutating history. Replays make zero provider calls.

## Production properties

- Credential-derived tenant identity; request bodies cannot select a tenant.
- Fail-closed consent, residency, lifecycle, tool, resource, risk, and freshness gates.
- RFC 8785-style canonical records, content hashes, append-only evidence, and immutable manifests.
- Transactional outbox and deterministic Temporal workflow identities.
- SurrealDB graph/document/BM25/vector storage behind repository contracts; S3-compatible canonical archive.
- Fixed-width integer ranking with total ordering and no floating-point persistence boundary.
- Server-side exact-effect routing only when the captured tenant corpus has one unambiguous effect signature for the canonical intent; clients cannot inject the route key.
- Jev single-flight, per-tenant/global token buckets, bounded concurrency, circuit breaking, hard deadlines, and deterministic fallback.
- Connect/Protobuf public APIs, strict JSON, bounded bodies, idempotency, safe errors, OpenTelemetry, executable Prometheus alerts, and locked-down distroless containers.

## Development and qualification

Requirements: Go 1.26.8, Docker with Compose v2, OpenSSL, curl, and Python 3.

```sh
./scripts/integration.sh
./scripts/smoke.sh
make verify
```

The integration gate builds a clean stack, replays all migrations, exercises durable ingest and synthesis, tests deterministic five-channel retrieval and vector quarantine, restarts the worker to prove recovery, validates tenant isolation, and runs a mixed selected/abstained/explanation load probe at 100 scheduled RPS.

This is the release qualification path, not the normal installation command. Use `make setup` for everyday development.

Direct TypeSafe access is opt-in and uses a mounted mode-0600 file—never a key environment variable:

```sh
export MEMJEV_JEV_API_KEY_HOST_FILE=/absolute/path/to/jev-api-key
export MEMJEV_JEV_MODEL=jev-1.13.0
docker compose --env-file .env.local -f compose.yaml -f compose.jev.yaml up --build -d
```

See [Jev operations](docs/runbooks/jev.md) before enabling tenant traffic. The live provider smoke test is `make jev-smoke` with `MEMJEV_JEV_API_KEY_FILE` and `MEMJEV_JEV_MODEL` set in the operator shell.

## Documentation

- [Agent integration guide](docs/agent-integration.md)
- [HTTP API walkthrough](docs/api-quickstart.md)
- [Local development and release qualification](docs/runbooks/local-development.md)
- [System design](docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md)
- [Jev judgment-ledger design](docs/superpowers/specs/2026-09-20-jev-judgment-ledger-design.md)
- [Production deployment and rollback](docs/runbooks/production-deploy.md)
- [Encrypted backup and verified restore](docs/runbooks/backup-restore.md)
- [Tenant erasure](docs/runbooks/tenant-erasure.md)

## Controlled production launch

Before the first hosted deployment, configure immutable image digests and production dependencies, run `make production-preflight`, create an encrypted backup, and deploy with `scripts/deploy-production.sh`. Operator-only tenant erasure is available through `go run ./cmd/admin erase-tenant` with a strict JSON request on standard input; the tenant identifier is never placed in command arguments or the durable receipt. Keep the previous image digests available for `scripts/rollback-production.sh`.

- [Retrieval operations](docs/runbooks/retrieval.md)
- [Evidence-pipeline operations](docs/runbooks/evidence-pipeline.md)
- [Composition and lifecycle operations](docs/runbooks/composition-lifecycle.md)

The local 100-RPS gate is a smoke qualification, not proof of the documented 10-million-event production target. Release qualification requires the production-shaped corpus, 30-minute load window, failover, backup/restore, and retained evidence described in the runbook.

## License

memJev is licensed under the [GNU Affero General Public License v3.0](LICENSE).
