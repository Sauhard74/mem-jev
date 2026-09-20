# Day-One Launch Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the minimum recoverability, tenant-erasure, and deployment controls required for a controlled early production launch.

**Architecture:** Keep destructive operations outside the public API. A dedicated operator binary performs fail-closed, tenant-scoped erasure across canonical archives and SurrealDB and emits a content-addressed receipt. Portable operator scripts create encrypted backup bundles, restore only into an explicitly empty target, verify manifests, and provide production preflight and image rollback controls.

**Tech Stack:** Go 1.26.8, SurrealDB 3.2.4, AWS S3 API, POSIX shell, Surreal CLI, MinIO `mc`, `age`, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md`

## Global Constraints

- Credentials and tenant identifiers must never appear in logs, command arguments, backup manifests, or receipts.
- Erasure is operator-only, requires an exact confirmation token, deletes archives before online records, and is idempotent.
- Backups are encrypted before leaving a mode-0700 temporary directory and are accompanied by a SHA-256 manifest.
- Restore refuses a non-empty target, verifies bundle integrity before import, and runs health verification after import.
- Production deploys use immutable image references, TLS at the edge, health-gated rollout, and an explicit previous-image rollback value.

## Review Focus

- Partial archive deletion must stop before database deletion and remain safely retryable.
- A tenant identifier containing control characters or path syntax must be rejected before any external operation.
- A tampered or wrong-key backup must fail before changing the restore target.
- Restore into a non-empty database or archive bucket must fail closed.
- Rollback without an immutable previous image digest must refuse to run.

---

### Task 1: Operator tenant erasure

**Files:**
- Create: `internal/erasure/model.go`
- Create: `internal/erasure/service.go`
- Create: `internal/erasure/service_test.go`
- Create: `internal/store/surreal/erasure.go`
- Create: `internal/store/surreal/erasure_test.go`
- Create: `cmd/admin/main.go`
- Create: `db/migrations/0013_erasure_receipts.surql`

**Interfaces:**
- Consumes: tenant-scoped S3 prefix deletion and parameterized SurrealDB transactions.
- Produces: `erasure.Service.Erase(context.Context, Request) (Receipt, error)` and `memjev-admin erase-tenant`.

- [ ] Write service tests for validation, confirmation, archive-first ordering, idempotency, and database suppression after archive failure.
- [ ] Run `go test ./internal/erasure -count=1` and observe failure because the package is absent.
- [ ] Implement canonical requests/receipts and the archive/database capability interfaces.
- [ ] Run `go test ./internal/erasure -count=1` and observe PASS.
- [ ] Add a schema-full hashed receipt table and a Surreal repository integration test proving all tenant-owned rows are deleted while global manifests and another tenant survive.
- [ ] Run `go test -tags=integration ./internal/store/surreal -run TestTenantErasure -count=1` and observe RED, then implement the transaction and observe PASS.
- [ ] Wire the operator command with secret-file/config loading and an exact `erase:<tenant>:<request-id>` confirmation.
- [ ] Run `go test ./cmd/admin ./internal/erasure -count=1` and observe PASS.
- [ ] Commit as `feat(ops): add tenant erasure workflow`.

### Task 2: Encrypted backup and verified restore

**Files:**
- Create: `scripts/backup.sh`
- Create: `scripts/restore.sh`
- Create: `scripts/verify-backup.sh`
- Create: `scripts/tests/backup_restore_test.sh`
- Create: `docs/runbooks/backup-restore.md`

**Interfaces:**
- Consumes: `surreal export/import`, `mc mirror`, `pg_dump/psql`, `age`, and SHA-256 tooling.
- Produces: encrypted `.tar.age` bundles plus a non-sensitive `.sha256` sidecar and a verified empty-target restore flow.

- [ ] Write a fake-command shell test proving secret-free argv, restrictive temporary permissions, archive/database/workflow inclusion, tamper rejection, empty-target enforcement, and cleanup after failure.
- [ ] Run `scripts/tests/backup_restore_test.sh` and observe RED because scripts are absent.
- [ ] Implement backup, verification, and restore scripts with required environment validation and traps.
- [ ] Run `scripts/tests/backup_restore_test.sh` and observe PASS.
- [ ] Document scheduled execution, retention, recovery verification, and the limitation that MinIO mirror captures current immutable canonical objects rather than version metadata.
- [ ] Commit as `feat(ops): add encrypted backup and verified restore`.

### Task 3: Production preflight, TLS deployment, and rollback

**Files:**
- Create: `compose.production.yaml`
- Create: `ops/Caddyfile`
- Create: `scripts/production-preflight.sh`
- Create: `scripts/deploy-production.sh`
- Create: `scripts/rollback-production.sh`
- Create: `scripts/tests/production_ops_test.sh`
- Create: `docs/runbooks/production-deploy.md`
- Modify: `Makefile`

**Interfaces:**
- Consumes: immutable API/worker image digest, domain, OTLP endpoint, mounted credential files, backup age recipient, and Compose health checks.
- Produces: `make production-preflight`, a health-gated deployment script, and an explicit digest rollback script.

- [ ] Write a shell test proving preflight rejects mutable tags, missing TLS/domain/telemetry/backup configuration, loose secret permissions, and absent rollback digest.
- [ ] Run `scripts/tests/production_ops_test.sh` and observe RED because scripts are absent.
- [ ] Implement the production overlay with Caddy TLS termination, production environment, no source builds, read-only services, bounded logs, and immutable images.
- [ ] Implement preflight, deploy, and rollback scripts; deploy records the previous digest only after health succeeds.
- [ ] Run `scripts/tests/production_ops_test.sh` and observe PASS.
- [ ] Run `docker compose -f compose.production.yaml config --quiet` with fixture environment and observe PASS.
- [ ] Document first deploy, health verification, backup prerequisite, rollback, and credential rotation.
- [ ] Commit as `ops: add production deploy and rollback controls`.

### Task 4: Whole-slice verification

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: all operator tests and existing verification gates.
- Produces: CI enforcement and an accurate launch-readiness statement.

- [ ] Add the operator shell tests and production Compose validation to CI without requiring production secrets.
- [ ] Run all operator tests, `make verify`, alert validation, and the full integration gate.
- [ ] Run a clean backup/restore drill and tenant-erasure integration test against the local Compose stack.
- [ ] Request one whole-branch review and resolve all Critical/Important findings test-first.
- [ ] Commit as `test: qualify day-one launch safety`.
