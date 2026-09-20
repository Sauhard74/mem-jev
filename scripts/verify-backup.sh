#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" || ! -f "$1.sha256" ]]; then
  echo "usage: verify-backup.sh BACKUP.tar.age" >&2
  exit 2
fi
want=$(tr -d '[:space:]' < "$1.sha256")
got=$(shasum -a 256 "$1" | awk '{print $1}')
if [[ ! "$want" =~ ^[0-9a-f]{64}$ || "$want" != "$got" ]]; then
  echo "backup checksum mismatch" >&2
  exit 1
fi
echo "backup checksum: PASS"
