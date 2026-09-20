#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
"$repo_dir/scripts/production-preflight.sh"
docker compose --env-file "${MEMJEV_PRODUCTION_ENV_FILE:-.env.production}" -f "$repo_dir/compose.production.yaml" pull
docker compose --env-file "${MEMJEV_PRODUCTION_ENV_FILE:-.env.production}" -f "$repo_dir/compose.production.yaml" up -d --wait --remove-orphans
curl --fail --silent --show-error "https://$MEMJEV_DOMAIN/memjev.v1.HealthService/Check" -H 'content-type: application/json' --data '{}'>/dev/null
state=${MEMJEV_DEPLOY_STATE_FILE:-.memjev-deploy-state}
umask 077
printf 'MEMJEV_PREVIOUS_API_IMAGE=%q\nMEMJEV_PREVIOUS_WORKER_IMAGE=%q\n' "$MEMJEV_API_IMAGE" "$MEMJEV_WORKER_IMAGE" > "$state"
echo "production deploy: PASS"
