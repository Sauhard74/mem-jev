#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
for script in production-preflight.sh deploy-production.sh rollback-production.sh; do bash -n "$repo_dir/scripts/$script"; done
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
printf '{}' > "$tmp/credentials.json"; chmod 600 "$tmp/credentials.json"
digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
base_env=(MEMJEV_DOMAIN=memory.example.com MEMJEV_OTLP_ENDPOINT=https://otel.example.com MEMJEV_BACKUP_AGE_RECIPIENT=age1fixture MEMJEV_CREDENTIALS_FILE_HOST="$tmp/credentials.json" MEMJEV_CADDY_IMAGE="registry/caddy@$digest")
if env "${base_env[@]}" MEMJEV_API_IMAGE=registry/api:latest MEMJEV_WORKER_IMAGE=registry/worker:latest "$repo_dir/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo "preflight accepted mutable images" >&2; exit 1
fi
env "${base_env[@]}" MEMJEV_API_IMAGE="registry/api@$digest" MEMJEV_WORKER_IMAGE="registry/worker@$digest" MEMJEV_PREVIOUS_API_IMAGE="registry/api@$digest" MEMJEV_PREVIOUS_WORKER_IMAGE="registry/worker@$digest" "$repo_dir/scripts/production-preflight.sh"
chmod 644 "$tmp/credentials.json"
if env "${base_env[@]}" MEMJEV_API_IMAGE="registry/api@$digest" MEMJEV_WORKER_IMAGE="registry/worker@$digest" MEMJEV_PREVIOUS_API_IMAGE="registry/api@$digest" MEMJEV_PREVIOUS_WORKER_IMAGE="registry/worker@$digest" "$repo_dir/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo "preflight accepted loose secret permissions" >&2; exit 1
fi
echo "production operator tests: PASS"
