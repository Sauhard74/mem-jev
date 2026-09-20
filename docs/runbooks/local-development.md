# Local development

Prerequisites are Go 1.23, Docker with Compose v2, OpenSSL, curl, and Python 3. No durable credential is committed to the repository.

Run `./scripts/integration.sh`. It creates mode-0600 development credentials in `.env.local` and `.runtime/`, rebuilds a clean Compose stack, waits for readiness, then runs the SurrealDB integration and end-to-end tests. Run `./scripts/smoke.sh` for a second public-API check. Stop and erase the local state with `docker compose --env-file .env.local down --volumes`.

The stack binds API, SurrealDB, and MinIO only to loopback. Containers use an internal service network plus a host-access bridge solely for loopback port publication. MinIO receives an ephemeral development KMS key so the same mandatory server-side-encryption path is exercised locally. The API filesystem is read-only, runs as numeric UID 65532, and has no Linux capabilities. Memory adapters are permitted only when `MEMJEV_ENVIRONMENT=development`.

Production uses `surreal-s3`, `wss://` for SurrealDB, HTTPS for any explicit S3/OTLP endpoint, externally managed AWS credentials, and a credential file containing SHA-256 hashes rather than bearer tokens.
