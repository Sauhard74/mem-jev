# Local development

Prerequisites are Go 1.25 or newer (the release and CI toolchain is pinned to Go 1.26.8), Docker with Compose v2, OpenSSL, curl, and Python 3. No durable credential is committed to the repository.

For ordinary development, run `make setup`. It creates `.env.local` and `.runtime/credentials.json` with mode `0600`, starts the complete Compose stack, and waits for health checks. It preserves existing credentials. Run `make quickstart` to exercise retrieve, ingest, and outcome through the public API, and `make dev-down` to stop the containers without deleting local volumes.

The commands below run the complete integration suite. They are intentionally heavier than setup.

Run `./scripts/integration.sh`. It creates mode-0600 development credentials in `.env.local` and `.runtime/`, rebuilds a clean Compose stack, waits for readiness, then runs the SurrealDB integration and end-to-end tests. It also restarts the worker and checks durable recovery, verified synthesis, deterministic duplicate replay, negative-path scoping, abstention, tenant isolation, and archive-corruption quarantine. Run `./scripts/smoke.sh` for a second public-API check. Stop and erase the local state with `docker compose --env-file .env.local down --volumes`.

The integration suite also exercises the optional vector channel against SurrealDB and verifies selection, abstention, migration replay, projection-pinned replay, stable tie ordering, concurrent idempotency, encrypted query storage, explanations, authorization-context binding, cross-tenant denial, and vector degradation behavior.

The stack binds API, worker health, Temporal, SurrealDB, and MinIO only to loopback. Containers use an internal service network plus a host-access bridge solely for loopback port publication. MinIO receives an ephemeral development KMS key so the same mandatory server-side-encryption path is exercised locally. API and worker filesystems are read-only, run as numeric UID 65532, and have no Linux capabilities. Memory adapters are permitted only when `MEMJEV_ENVIRONMENT=development`.

Production uses `surreal-s3`, `wss://` for SurrealDB, `MEMJEV_SURREAL_AUTH_SCOPE=database` (or an explicitly justified namespace scope), HTTPS for any explicit S3/OTLP base endpoint, externally managed AWS credentials, and a credential file containing SHA-256 hashes rather than bearer tokens. Root-scoped SurrealDB authentication is rejected in production. `MEMJEV_REQUEST_TIMEOUT` defaults to 10 seconds and controls handler, body-read, and response-write deadlines.
