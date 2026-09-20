#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: restore.sh BACKUP.tar.age" >&2
  exit 2
fi
backup=$1
required=(MEMJEV_BACKUP_AGE_IDENTITY_FILE MEMJEV_RESTORE_ARCHIVE_TARGET MEMJEV_RESTORE_ARCHIVE_SSE MEMJEV_SURREAL_ENDPOINT MEMJEV_SURREAL_NAMESPACE MEMJEV_SURREAL_DATABASE MEMJEV_TEMPORAL_POSTGRES_DATABASE MEMJEV_RESTORE_HEALTHCHECK_URL MEMJEV_ERASURE_LEDGER_FILE MEMJEV_ADMIN_BINARY)
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || { printf '%s is required\n' "$name" >&2; exit 2; }
done
[[ -x "$MEMJEV_ADMIN_BINARY" ]] || { echo "admin binary is not executable" >&2; exit 2; }
[[ -f "$MEMJEV_ERASURE_LEDGER_FILE" ]] || { echo "independent erasure ledger is missing" >&2; exit 2; }
ledger_mode=$(stat -f '%Lp' "$MEMJEV_ERASURE_LEDGER_FILE" 2>/dev/null || stat -c '%a' "$MEMJEV_ERASURE_LEDGER_FILE")
[[ "$ledger_mode" == 600 || "$ledger_mode" == 400 ]] || { echo "erasure ledger permissions must be 0600 or 0400" >&2; exit 2; }
attestation="empty:$MEMJEV_SURREAL_NAMESPACE:$MEMJEV_SURREAL_DATABASE:$MEMJEV_RESTORE_ARCHIVE_TARGET:$MEMJEV_TEMPORAL_POSTGRES_DATABASE"
if [[ "${MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION:-}" != "$attestation" ]]; then
  echo "empty-target attestation rejected" >&2
  exit 2
fi
for command in surreal mc psql age tar jq curl; do
  command -v "$command" >/dev/null || { printf '%s is required\n' "$command" >&2; exit 2; }
done
"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verify-backup.sh" "$backup" >/dev/null
work=$(mktemp -d "${TMPDIR:-/tmp}/memjev-restore.XXXXXX")
chmod 700 "$work"
trap 'rm -rf "$work"' EXIT
age -d -i "$MEMJEV_BACKUP_AGE_IDENTITY_FILE" "$backup" | tar -C "$work" -xf -
(cd "$work" && shasum -a 256 -c MANIFEST.sha256 >/dev/null)

info=$(printf 'INFO FOR DB;\n' | surreal sql --hide-welcome --json --endpoint "$MEMJEV_SURREAL_ENDPOINT" --namespace "$MEMJEV_SURREAL_NAMESPACE" --database "$MEMJEV_SURREAL_DATABASE")
printf '%s' "$info" | jq -e 'all(.[]; ((.result.tables // {}) | length) == 0)' >/dev/null || { echo "SurrealDB restore target is not empty" >&2; exit 1; }
[[ -z "$(mc ls --recursive "$MEMJEV_RESTORE_ARCHIVE_TARGET")" ]] || { echo "archive restore target is not empty" >&2; exit 1; }
table_count=$(psql -X -Atqc "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')" "$MEMJEV_TEMPORAL_POSTGRES_DATABASE")
[[ "$table_count" == 0 ]] || { echo "PostgreSQL restore target is not empty" >&2; exit 1; }

surreal import --log error --endpoint "$MEMJEV_SURREAL_ENDPOINT" --namespace "$MEMJEV_SURREAL_NAMESPACE" --database "$MEMJEV_SURREAL_DATABASE" "$work/surreal.surql"
while IFS= read -r -d '' object; do
  relative=${object#"$work/archive/"}
  schema=${relative%/*}; schema=${schema##*/}
  filename=${relative##*/}; hash=${filename%.json}
  [[ "$schema" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ && "$hash" =~ ^[0-9a-f]{64}$ ]] || { echo "invalid canonical archive key in backup" >&2; exit 1; }
  destination="$MEMJEV_RESTORE_ARCHIVE_TARGET/$relative"
  encryption=(--enc-s3 "$MEMJEV_RESTORE_ARCHIVE_TARGET")
  if [[ "$MEMJEV_RESTORE_ARCHIVE_SSE" == aws:kms ]]; then
    [[ -n "${MEMJEV_RESTORE_ARCHIVE_KMS_KEY_ID:-}" ]] || { echo "restore KMS key is required" >&2; exit 2; }
    encryption=(--enc-kms "$MEMJEV_RESTORE_ARCHIVE_TARGET=$MEMJEV_RESTORE_ARCHIVE_KMS_KEY_ID")
  elif [[ "$MEMJEV_RESTORE_ARCHIVE_SSE" != AES256 ]]; then
    echo "unsupported restore archive encryption" >&2; exit 2
  fi
  mc cp --quiet --attr "schema-version=$schema;content-sha256=$hash" "${encryption[@]}" "$object" "$destination"
  stat_json=$(mc stat --json "$destination")
  printf '%s' "$stat_json" | jq -e --arg schema "$schema" --arg hash "$hash" '(tostring | contains($schema)) and (tostring | contains($hash))' >/dev/null || { echo "restored archive metadata verification failed" >&2; exit 1; }
  restored_hash=$(mc cat "$destination" | shasum -a 256 | awk '{print $1}')
  [[ "$restored_hash" == "$hash" ]] || { echo "restored archive content verification failed" >&2; exit 1; }
done < <(find "$work/archive" -type f -print0)
psql -X -v ON_ERROR_STOP=1 --single-transaction --file "$work/temporal.sql" "$MEMJEV_TEMPORAL_POSTGRES_DATABASE"
while IFS= read -r erasure_request || [[ -n "$erasure_request" ]]; do
  [[ -z "$erasure_request" ]] && continue
  printf '%s\n' "$erasure_request" | env MEMJEV_ERASURE_LEDGER_REPLAY=true "$MEMJEV_ADMIN_BINARY" erase-tenant >/dev/null
done < "$MEMJEV_ERASURE_LEDGER_FILE"

post_info=$(printf 'INFO FOR DB;\n' | surreal sql --hide-welcome --json --endpoint "$MEMJEV_SURREAL_ENDPOINT" --namespace "$MEMJEV_SURREAL_NAMESPACE" --database "$MEMJEV_SURREAL_DATABASE")
printf '%s' "$post_info" | jq -e 'any(.[]; ((.result.tables // {}) | length) > 0)' >/dev/null || { echo "SurrealDB restore verification failed" >&2; exit 1; }
post_table_count=$(psql -X -Atqc "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')" "$MEMJEV_TEMPORAL_POSTGRES_DATABASE")
[[ "$post_table_count" =~ ^[1-9][0-9]*$ ]] || { echo "PostgreSQL restore verification failed" >&2; exit 1; }
curl --fail --silent --show-error "$MEMJEV_RESTORE_HEALTHCHECK_URL" -H 'content-type: application/json' --data '{}' >/dev/null
echo "restore completed and verified"
