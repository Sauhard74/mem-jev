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
mkdir "$tmp/erasure-ledger"; chmod 700 "$tmp/erasure-ledger"
"$repo_dir/scripts/verify-backup.sh" "$tmp/backup.tar.age"
printf 'tamper' >> "$tmp/backup.tar.age"
if "$repo_dir/scripts/verify-backup.sh" "$tmp/backup.tar.age" >/dev/null 2>&1; then
  echo "tampered backup verified" >&2
  exit 1
fi

if env MEMJEV_BACKUP_AGE_IDENTITY_FILE="$tmp/identity" MEMJEV_RESTORE_ARCHIVE_TARGET=fixture/archive MEMJEV_RESTORE_ARCHIVE_ENDPOINT=https://archive.example.com MEMJEV_RESTORE_ARCHIVE_BUCKET=archive MEMJEV_RESTORE_ARCHIVE_REGION=us-east-1 MEMJEV_RESTORE_ARCHIVE_SSE=AES256 MEMJEV_SURREAL_ENDPOINT=wss://database.example.com MEMJEV_SURREAL_NAMESPACE=restore MEMJEV_SURREAL_DATABASE=restore MEMJEV_TEMPORAL_POSTGRES_DATABASE=postgres://restore MEMJEV_RESTORE_HEALTHCHECK_URL=https://restore.example.com/health MEMJEV_ERASURE_LEDGER_DIR="$tmp/erasure-ledger" MEMJEV_ADMIN_BINARY="$tmp/memjev-admin" MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION=wrong "$repo_dir/scripts/restore.sh" "$tmp/backup.tar.age" >"$tmp/attestation.out" 2>"$tmp/attestation.err"; then
  echo "restore accepted invalid empty-target attestation" >&2
  exit 1
fi
grep -q 'empty-target attestation rejected' "$tmp/attestation.err" || { echo "restore did not reach the attestation guard" >&2; exit 1; }

grep -q 'ON_ERROR_STOP=1' "$repo_dir/scripts/restore.sh" || { echo "restore does not fail on PostgreSQL errors" >&2; exit 1; }
grep -q 'SurrealDB restore target is not empty' "$repo_dir/scripts/restore.sh" || { echo "restore does not inspect SurrealDB emptiness" >&2; exit 1; }
grep -q 'archive restore target is not empty' "$repo_dir/scripts/restore.sh" || { echo "restore does not inspect archive emptiness" >&2; exit 1; }
grep -q 'PostgreSQL restore target is not empty' "$repo_dir/scripts/restore.sh" || { echo "restore does not inspect PostgreSQL emptiness" >&2; exit 1; }
grep -q 'content-sha256' "$repo_dir/scripts/restore.sh" || { echo "restore does not reconstruct canonical metadata" >&2; exit 1; }
printf '[{"tables":{}}]' | jq -e 'type == "array" and length == 1 and (.[0].tables | type == "object") and (.[0].tables | length) == 0' >/dev/null
if printf '[{"tables":{"existing":"DEFINE TABLE existing"}}]' | jq -e 'type == "array" and length == 1 and (.[0].tables | type == "object") and (.[0].tables | length) == 0' >/dev/null; then
  echo "SurrealDB nonempty result passed emptiness predicate" >&2; exit 1
fi
grep -q 'if ! archive_listing=.*--versions' "$repo_dir/scripts/restore.sh" || { echo "archive inspection is not fail-closed and version-aware" >&2; exit 1; }
grep -q 'verify-archive-object' "$repo_dir/scripts/restore.sh" || { echo "restore does not use the production archive reader" >&2; exit 1; }

fixture="$tmp/fixture"
mkdir -p "$fixture/archive/canonical/0123456789abcdef/canonical.v1"
printf '{"ok":true}' > "$tmp/object.json"
object_hash=$(shasum -a 256 "$tmp/object.json" | awk '{print $1}')
cp "$tmp/object.json" "$fixture/archive/canonical/0123456789abcdef/canonical.v1/$object_hash.json"
printf 'DEFINE TABLE restored SCHEMALESS;\n' > "$fixture/surreal.surql"
printf 'CREATE TABLE restored(id integer);\n' > "$fixture/temporal.sql"
(cd "$fixture" && shasum -a 256 surreal.surql temporal.sql "archive/canonical/0123456789abcdef/canonical.v1/$object_hash.json" > MANIFEST.sha256)
tar -C "$fixture" -cf "$tmp/fixture.tar" surreal.surql temporal.sql archive MANIFEST.sha256
printf 'encrypted fixture two' > "$tmp/restore.tar.age"
shasum -a 256 "$tmp/restore.tar.age" | awk '{print $1}' > "$tmp/restore.tar.age.sha256"

fakebin="$tmp/fakebin"; mkdir "$fakebin"
printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'if [[ "$1" == sql ]]; then if [[ -f "$FAKE_SURREAL_IMPORTED" ]]; then printf '\''[{"tables":{"restored":"DEFINE TABLE restored"}}]'\''; else printf '\''[{"tables":{}}]'\''; fi; elif [[ "$1" == import ]]; then : > "$FAKE_SURREAL_IMPORTED"; else exit 2; fi' > "$fakebin/surreal"
printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'case "$1" in alias) printf '\''{"url":"https://archive.example.com"}'\'' ;; ls) [[ "${FAKE_MC_LS_FAIL:-}" != 1 ]] || exit 23 ;; cp) printf '\''%s\n'\'' "$*" >> "$FAKE_CALL_LOG" ;; *) exit 2 ;; esac' > "$fakebin/mc"
printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'if [[ "$1" == -d ]]; then /bin/cat "$FAKE_RESTORE_TAR"; else exit 2; fi' > "$fakebin/age"
printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'if [[ " $* " == *" --file "* ]]; then : > "$FAKE_POSTGRES_IMPORTED"; else if [[ -f "$FAKE_POSTGRES_IMPORTED" ]]; then printf '\''1\n'\''; else printf '\''0\n'\''; fi; fi' > "$fakebin/psql"
printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'printf '\''%s\n'\'' "$1" >> "$FAKE_CALL_LOG"; while IFS= read -r _; do :; done' > "$fakebin/memjev-admin"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fakebin/curl"
printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'if [[ -n "${FAKE_FIND_FAIL_MATCH:-}" && " $* " == *"$FAKE_FIND_FAIL_MATCH"* ]]; then exit 23; fi' 'exec /usr/bin/find "$@"' > "$fakebin/find"
chmod 700 "$fakebin"/*

restore_env=(PATH="$fakebin:$PATH" FAKE_RESTORE_TAR="$tmp/fixture.tar" FAKE_SURREAL_IMPORTED="$tmp/surreal-imported" FAKE_POSTGRES_IMPORTED="$tmp/postgres-imported" FAKE_CALL_LOG="$tmp/calls.log" MEMJEV_BACKUP_AGE_IDENTITY_FILE="$tmp/identity" MEMJEV_RESTORE_ARCHIVE_TARGET=fixture/archive MEMJEV_RESTORE_ARCHIVE_ENDPOINT=https://archive.example.com MEMJEV_RESTORE_ARCHIVE_BUCKET=archive MEMJEV_RESTORE_ARCHIVE_REGION=us-east-1 MEMJEV_RESTORE_ARCHIVE_SSE=AES256 MEMJEV_SURREAL_ENDPOINT=wss://database.example.com MEMJEV_SURREAL_NAMESPACE=restore MEMJEV_SURREAL_DATABASE=restore MEMJEV_TEMPORAL_POSTGRES_DATABASE=postgres://restore MEMJEV_RESTORE_HEALTHCHECK_URL=https://restore.example.com/health MEMJEV_ERASURE_LEDGER_DIR="$tmp/erasure-ledger" MEMJEV_ADMIN_BINARY="$fakebin/memjev-admin" MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION=empty:restore:restore:fixture/archive:postgres://restore)
env "${restore_env[@]}" "$repo_dir/scripts/restore.sh" "$tmp/restore.tar.age" > "$tmp/restore.out"
grep -q 'restore completed and verified' "$tmp/restore.out" || { echo "complete restore fixture did not succeed" >&2; exit 1; }
grep -q 'verify-archive-object' "$tmp/calls.log" || { echo "archive verifier was not executed" >&2; exit 1; }

rm -f "$tmp/surreal-imported" "$tmp/postgres-imported"
if env "${restore_env[@]}" FAKE_FIND_FAIL_MATCH=erasure-ledger "$repo_dir/scripts/restore.sh" "$tmp/restore.tar.age" >"$tmp/enumeration.out" 2>"$tmp/enumeration.err"; then
  echo "restore accepted ledger enumeration failure" >&2; exit 1
fi
grep -q 'erasure ledger enumeration failed' "$tmp/enumeration.err" || { echo "restore did not report ledger enumeration failure" >&2; exit 1; }
[[ ! -e "$tmp/surreal-imported" ]] || { echo "restore mutated SurrealDB before ledger enumeration completed" >&2; exit 1; }

echo "backup/restore operator tests: PASS"
