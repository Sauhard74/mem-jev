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
"$repo_dir/scripts/verify-backup.sh" "$tmp/backup.tar.age"
printf 'tamper' >> "$tmp/backup.tar.age"
if "$repo_dir/scripts/verify-backup.sh" "$tmp/backup.tar.age" >/dev/null 2>&1; then
  echo "tampered backup verified" >&2
  exit 1
fi

if MEMJEV_RESTORE_EMPTY_TARGET_ATTESTATION=wrong "$repo_dir/scripts/restore.sh" "$tmp/backup.tar.age" >/dev/null 2>&1; then
  echo "restore accepted invalid empty-target attestation" >&2
  exit 1
fi

echo "backup/restore operator tests: PASS"
