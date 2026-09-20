# MemJev

MemJev is a hosted, multi-tenant procedural-memory service for agents. It records sanitized traces and verified outcomes, derives immutable evidence-backed procedures, retrieves compatible procedures through deterministic gates and rank fusion, and returns an advisory plan with replayable provenance.

The honest product boundary is deliberate: this is procedural reuse and bounded composition, not general learning. Compared with a fuzzy trace cache, MemJev adds verified outcome evidence, negative paths, tool/resource compatibility, hard eligibility gates, multi-procedure composition, immutable revisions, abstention, exact decision replay, and tenant isolation.

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

## Local qualification

Requirements: Go 1.26.8, Docker with Compose v2, OpenSSL, curl, and Python 3.

```sh
./scripts/integration.sh
./scripts/smoke.sh
make verify
```

The integration gate builds a clean stack, replays all migrations, exercises durable ingest and synthesis, tests deterministic five-channel retrieval and vector quarantine, restarts the worker to prove recovery, validates tenant isolation, and runs a mixed selected/abstained/explanation load probe at 100 scheduled RPS.

Direct TypeSafe access is opt-in and uses a mounted mode-0600 file—never a key environment variable:

```sh
export MEMJEV_JEV_API_KEY_HOST_FILE=/absolute/path/to/jev-api-key
export MEMJEV_JEV_MODEL=jev-1.13.0
docker compose --env-file .env.local -f compose.yaml -f compose.jev.yaml up --build -d
```

See [Jev operations](docs/runbooks/jev.md) before enabling tenant traffic. The live provider smoke test is `make jev-smoke` with `MEMJEV_JEV_API_KEY_FILE` and `MEMJEV_JEV_MODEL` set in the operator shell.

## Documentation

- [System design](docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md)
- [Jev judgment-ledger design](docs/superpowers/specs/2026-09-20-jev-judgment-ledger-design.md)
- [Production deployment and rollback](docs/runbooks/production-deploy.md)
- [Encrypted backup and verified restore](docs/runbooks/backup-restore.md)
- [Tenant erasure](docs/runbooks/tenant-erasure.md)

## Controlled production launch

Before the first hosted deployment, configure immutable image digests and production dependencies, run `make production-preflight`, create an encrypted backup, and deploy with `scripts/deploy-production.sh`. Operator-only tenant erasure is available through `go run ./cmd/admin erase-tenant` with a strict JSON request on standard input; the tenant identifier is never placed in command arguments or the durable receipt. Keep the previous image digests available for `scripts/rollback-production.sh`.
- [Local development and release qualification](docs/runbooks/local-development.md)
- [Retrieval operations](docs/runbooks/retrieval.md)
- [Evidence-pipeline operations](docs/runbooks/evidence-pipeline.md)
- [Composition and lifecycle operations](docs/runbooks/composition-lifecycle.md)

The local 100-RPS gate is a smoke qualification, not proof of the documented 10-million-event production target. Release qualification requires the production-shaped corpus, 30-minute load window, failover, backup/restore, and retained evidence described in the runbook.
