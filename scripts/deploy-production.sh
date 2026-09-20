#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_dir/scripts/load-production-env.sh"
env_file=${MEMJEV_PRODUCTION_ENV_FILE:-.env.production}
load_production_env "$env_file"
"$repo_dir/scripts/production-preflight.sh"
"$repo_dir/scripts/verify-rollback-images.sh"
docker compose --env-file "$env_file" -f "$repo_dir/compose.production.yaml" pull
docker compose --env-file "$env_file" -f "$repo_dir/compose.production.yaml" up -d --wait --remove-orphans
curl --fail --silent --show-error "https://$MEMJEV_DOMAIN/memjev.v1.HealthService/Check" -H 'content-type: application/json' --data '{}'>/dev/null
state=${MEMJEV_DEPLOY_STATE_FILE:-.memjev-deploy-state}
umask 077
state_tmp="$state.tmp.$$"
trap 'rm -f "$state_tmp"' EXIT
printf 'MEMJEV_CURRENT_API_IMAGE=%s\nMEMJEV_CURRENT_WORKER_IMAGE=%s\nMEMJEV_PREVIOUS_API_IMAGE=%s\nMEMJEV_PREVIOUS_WORKER_IMAGE=%s\nMEMJEV_PREVIOUS_SCHEMA_MAX_VERSION=%s\n' \
  "$MEMJEV_API_IMAGE" "$MEMJEV_WORKER_IMAGE" "$MEMJEV_PREVIOUS_API_IMAGE" "$MEMJEV_PREVIOUS_WORKER_IMAGE" "$MEMJEV_PREVIOUS_SCHEMA_MAX_VERSION" > "$state_tmp"
chmod 600 "$state_tmp"
mv "$state_tmp" "$state"
trap - EXIT
echo "production deploy: PASS"
