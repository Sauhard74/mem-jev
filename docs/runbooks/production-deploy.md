# Production deployment and rollback

Production uses prebuilt API and worker images referenced by immutable SHA-256 digests. Populate a mode-0600 `.env.production` with the variables required by `compose.production.yaml`, including managed SurrealDB, S3 and Temporal endpoints, TLS domain, HTTPS OTLP endpoint, query-encryption key, a pinned Jev model, and paths to mode-0600 credential and Jev key files. The API receives those files as read-only mounts; keep provider and database secrets outside Git.

Run `make production-preflight`, take an encrypted backup, then run `scripts/deploy-production.sh`. The deployment pulls exact images, waits for container health and verifies the public TLS health endpoint. Preserve the deployment state file with mode 0600.

For rollback, load the last known-good `MEMJEV_PREVIOUS_API_IMAGE` and `MEMJEV_PREVIOUS_WORKER_IMAGE` digests and run `scripts/rollback-production.sh`. Rollback changes application images only; forward-only database migrations must remain backward-compatible with the previous release.
