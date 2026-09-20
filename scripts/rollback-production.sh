#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
digest_pattern='@sha256:[0-9a-f]{64}$'
[[ "${MEMJEV_PREVIOUS_API_IMAGE:-}" =~ $digest_pattern && "${MEMJEV_PREVIOUS_WORKER_IMAGE:-}" =~ $digest_pattern ]] || { echo "immutable previous images are required" >&2; exit 2; }
export MEMJEV_API_IMAGE=$MEMJEV_PREVIOUS_API_IMAGE MEMJEV_WORKER_IMAGE=$MEMJEV_PREVIOUS_WORKER_IMAGE
docker compose --env-file "${MEMJEV_PRODUCTION_ENV_FILE:-.env.production}" -f "$repo_dir/compose.production.yaml" up -d --wait --remove-orphans api worker
curl --fail --silent --show-error "https://$MEMJEV_DOMAIN/memjev.v1.HealthService/Check" -H 'content-type: application/json' --data '{}'>/dev/null
echo "production rollback: PASS"
