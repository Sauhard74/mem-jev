#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: restore.sh BACKUP.tar.age" >&2
  exit 2
fi
backup=$1
required=(MEMJEV_BACKUP_AGE_IDENTITY_FILE MEMJEV_RESTORE_ARCHIVE_TARGET MEMJEV_SURREAL_ENDPOINT MEMJEV_SURREAL_NAMESPACE MEMJEV_SURREAL_DATABASE MEMJEV_TEMPORAL_POSTGRES_DATABASE)
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || { printf '%s is required\n' "$name" >&2; exit 2; }
done
attestation="empty:$MEMJEV_SURREAL_NAMESPACE:$MEMJEV_SURREAL_DATABASE:$MEMJEV_RESTORE_ARCHIVE_TARGET"
if [[ "${MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION:-}" != "$attestation" ]]; then
  echo "empty-target attestation rejected" >&2
  exit 2
fi
for command in surreal mc psql age tar; do
  command -v "$command" >/dev/null || { printf '%s is required\n' "$command" >&2; exit 2; }
done
"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verify-backup.sh" "$backup" >/dev/null
work=$(mktemp -d "${TMPDIR:-/tmp}/memjev-restore.XXXXXX")
chmod 700 "$work"
trap 'rm -rf "$work"' EXIT
age -d -i "$MEMJEV_BACKUP_AGE_IDENTITY_FILE" "$backup" | tar -C "$work" -xf -
(cd "$work" && shasum -a 256 -c MANIFEST.sha256 >/dev/null)
surreal import --log error --endpoint "$MEMJEV_SURREAL_ENDPOINT" --namespace "$MEMJEV_SURREAL_NAMESPACE" --database "$MEMJEV_SURREAL_DATABASE" "$work/surreal.surql"
mc mirror "$work/archive" "$MEMJEV_RESTORE_ARCHIVE_TARGET"
psql --file "$work/temporal.sql" "$MEMJEV_TEMPORAL_POSTGRES_DATABASE"
echo "restore completed; run API smoke verification before routing traffic"
