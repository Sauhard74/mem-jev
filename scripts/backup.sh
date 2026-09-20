#!/usr/bin/env bash
set -euo pipefail

required=(MEMJEV_BACKUP_OUTPUT_DIR MEMJEV_BACKUP_AGE_RECIPIENT MEMJEV_BACKUP_ARCHIVE_SOURCE MEMJEV_SURREAL_ENDPOINT MEMJEV_SURREAL_NAMESPACE MEMJEV_SURREAL_DATABASE MEMJEV_TEMPORAL_POSTGRES_DATABASE)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf '%s is required\n' "$name" >&2
    exit 2
  fi
done
for command in surreal mc pg_dump age tar; do
  command -v "$command" >/dev/null || { printf '%s is required\n' "$command" >&2; exit 2; }
done

mkdir -p "$MEMJEV_BACKUP_OUTPUT_DIR"
chmod 700 "$MEMJEV_BACKUP_OUTPUT_DIR"
work=$(mktemp -d "${TMPDIR:-/tmp}/memjev-backup.XXXXXX")
chmod 700 "$work"
trap 'rm -rf "$work"' EXIT

surreal export --log error --endpoint "$MEMJEV_SURREAL_ENDPOINT" --namespace "$MEMJEV_SURREAL_NAMESPACE" --database "$MEMJEV_SURREAL_DATABASE" "$work/surreal.surql"
mkdir -m 700 "$work/archive"
mc mirror "$MEMJEV_BACKUP_ARCHIVE_SOURCE" "$work/archive"
pg_dump --file "$work/temporal.sql" "$MEMJEV_TEMPORAL_POSTGRES_DATABASE"

(cd "$work" && shasum -a 256 surreal.surql temporal.sql > MANIFEST.sha256 && find archive -type f -print0 | sort -z | xargs -0 shasum -a 256 >> MANIFEST.sha256)
stamp=$(date -u +%Y%m%dT%H%M%SZ)
output="$MEMJEV_BACKUP_OUTPUT_DIR/memjev-$stamp.tar.age"
tar -C "$work" -cf - surreal.surql temporal.sql archive MANIFEST.sha256 | age -r "$MEMJEV_BACKUP_AGE_RECIPIENT" -o "$output"
chmod 600 "$output"
shasum -a 256 "$output" | awk '{print $1}' > "$output.sha256"
chmod 600 "$output.sha256"
printf '%s\n' "$output"
