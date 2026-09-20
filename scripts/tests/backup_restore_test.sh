#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
for script in backup.sh restore.sh verify-backup.sh; do
  bash -n "$repo_dir/scripts/$script"
done

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
if "$repo_dir/scripts/backup.sh" >"$tmp/out" 2>"$tmp/err"; then
  echo "backup accepted missing configuration" >&2
  exit 1
fi
if ! grep -q 'MEMJEV_BACKUP_OUTPUT_DIR is required' "$tmp/err"; then
  echo "backup did not fail closed" >&2
  exit 1
fi

printf 'encrypted fixture' > "$tmp/backup.tar.age"
shasum -a 256 "$tmp/backup.tar.age" | awk '{print $1}' > "$tmp/backup.tar.age.sha256"
printf '#!/usr/bin/env sh\nexit 0\n' > "$tmp/memjev-admin"; chmod 700 "$tmp/memjev-admin"
: > "$tmp/erasure-ledger.jsonl"; chmod 600 "$tmp/erasure-ledger.jsonl"
"$repo_dir/scripts/verify-backup.sh" "$tmp/backup.tar.age"
printf 'tamper' >> "$tmp/backup.tar.age"
if "$repo_dir/scripts/verify-backup.sh" "$tmp/backup.tar.age" >/dev/null 2>&1; then
  echo "tampered backup verified" >&2
  exit 1
fi

if env MEMJEV_BACKUP_AGE_IDENTITY_FILE="$tmp/identity" MEMJEV_RESTORE_ARCHIVE_TARGET=fixture/archive MEMJEV_RESTORE_ARCHIVE_SSE=AES256 MEMJEV_SURREAL_ENDPOINT=wss://database.example.com MEMJEV_SURREAL_NAMESPACE=restore MEMJEV_SURREAL_DATABASE=restore MEMJEV_TEMPORAL_POSTGRES_DATABASE=postgres://restore MEMJEV_RESTORE_HEALTHCHECK_URL=https://restore.example.com/health MEMJEV_ERASURE_LEDGER_FILE="$tmp/erasure-ledger.jsonl" MEMJEV_ADMIN_BINARY="$tmp/memjev-admin" MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION=wrong "$repo_dir/scripts/restore.sh" "$tmp/backup.tar.age" >/dev/null 2>&1; then
  echo "restore accepted invalid empty-target attestation" >&2
  exit 1
fi

grep -q 'ON_ERROR_STOP=1' "$repo_dir/scripts/restore.sh" || { echo "restore does not fail on PostgreSQL errors" >&2; exit 1; }
grep -q 'SurrealDB restore target is not empty' "$repo_dir/scripts/restore.sh" || { echo "restore does not inspect SurrealDB emptiness" >&2; exit 1; }
grep -q 'archive restore target is not empty' "$repo_dir/scripts/restore.sh" || { echo "restore does not inspect archive emptiness" >&2; exit 1; }
grep -q 'PostgreSQL restore target is not empty' "$repo_dir/scripts/restore.sh" || { echo "restore does not inspect PostgreSQL emptiness" >&2; exit 1; }
grep -q 'content-sha256' "$repo_dir/scripts/restore.sh" || { echo "restore does not reconstruct canonical metadata" >&2; exit 1; }

echo "backup/restore operator tests: PASS"
