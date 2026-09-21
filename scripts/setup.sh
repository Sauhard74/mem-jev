#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file=${MEMJEV_ENV_FILE:-$repo_dir/.env.local}
runtime_dir=${MEMJEV_RUNTIME_DIR:-$repo_dir/.runtime}
prepare_only=false

usage() {
  printf '%s\n' 'Usage: scripts/setup.sh [--prepare-only]'
  printf '%s\n' '  --prepare-only  Generate local credentials without starting Docker Compose.'
}

case "${1:-}" in
  "") ;;
  --prepare-only) prepare_only=true ;;
  -h|--help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

command -v openssl >/dev/null 2>&1 || { printf '%s\n' 'openssl is required' >&2; exit 1; }
mkdir -p "$runtime_dir" "$(dirname "$env_file")"
chmod 700 "$runtime_dir"

if [[ ! -f "$env_file" ]]; then
  umask 077
  token=$(openssl rand -hex 24)
  other_token=$(openssl rand -hex 24)
  other_region_token=$(openssl rand -hex 24)
  surreal_pass=$(openssl rand -hex 24)
  minio_pass=$(openssl rand -hex 24)
  minio_kms="memjev-local:$(openssl rand -base64 32 | tr -d '\n')"
  temporal_postgres_pass=$(openssl rand -hex 24)
  credential_path="$runtime_dir/credentials.json"

  token_hash=$(printf '%s' "$token" | shasum -a 256 | awk '{print $1}')
  other_token_hash=$(printf '%s' "$other_token" | shasum -a 256 | awk '{print $1}')
  other_region_token_hash=$(printf '%s' "$other_region_token" | shasum -a 256 | awk '{print $1}')
  printf '{"credentials":[{"credential_sha256":"%s","tenant_id":"tenant_local","region":"local","scopes":["ingest:write","outcomes:write","retrievals:read"],"consent":"learn_and_recall"},{"credential_sha256":"%s","tenant_id":"tenant_other","region":"local","scopes":["ingest:write","outcomes:write","retrievals:read"],"consent":"learn_and_recall"},{"credential_sha256":"%s","tenant_id":"tenant_local","region":"other","scopes":["retrievals:read"],"consent":"learn_and_recall"}]}' \
    "$token_hash" "$other_token_hash" "$other_region_token_hash" > "$credential_path"
  chmod 600 "$credential_path"

  {
    printf 'SURREAL_USER=root\n'
    printf 'SURREAL_PASS=%s\n' "$surreal_pass"
    printf 'SURREAL_PORT=18000\n'
    printf 'MINIO_ROOT_USER=memjevlocal\n'
    printf 'MINIO_ROOT_PASSWORD=%s\n' "$minio_pass"
    printf 'MINIO_KMS_SECRET_KEY=%s\n' "$minio_kms"
    printf 'MINIO_PORT=19000\n'
    printf 'TEMPORAL_POSTGRES_PASSWORD=%s\n' "$temporal_postgres_pass"
    printf 'TEMPORAL_PORT=17233\n'
    printf 'MEMJEV_ARCHIVE_BUCKET=memjev-canonical\n'
    printf 'MEMJEV_API_PORT=18080\n'
    printf 'MEMJEV_WORKER_HEALTH_PORT=18081\n'
    printf 'MEMJEV_CREDENTIALS_FILE_HOST=%s\n' "$credential_path"
    printf 'MEMJEV_E2E_TOKEN=%s\n' "$token"
    printf 'MEMJEV_E2E_OTHER_TOKEN=%s\n' "$other_token"
    printf 'MEMJEV_E2E_OTHER_REGION_TOKEN=%s\n' "$other_region_token"
    printf 'MEMJEV_E2E_API_URL=http://127.0.0.1:18080\n'
    printf 'MEMJEV_E2E_SURREAL_URL=ws://127.0.0.1:18000\n'
    printf 'MEMJEV_E2E_S3_URL=http://127.0.0.1:19000\n'
    printf 'MEMJEV_E2E_S3_BUCKET=memjev-canonical\n'
  } > "$env_file"
  chmod 600 "$env_file"
else
  chmod 600 "$env_file"
  # shellcheck disable=SC1090
  source "$env_file"
  credential_path=${MEMJEV_CREDENTIALS_FILE_HOST:?existing environment file is missing MEMJEV_CREDENTIALS_FILE_HOST}
  [[ -f "$credential_path" ]] || {
    printf 'credential file does not exist: %s\n' "$credential_path" >&2
    exit 1
  }
  chmod 600 "$credential_path"
fi

if [[ "$prepare_only" == true ]]; then
  printf 'Local environment ready: %s\n' "$env_file"
  exit 0
fi

command -v docker >/dev/null 2>&1 || { printf '%s\n' 'Docker with Compose v2 is required' >&2; exit 1; }
docker compose version >/dev/null
docker compose --env-file "$env_file" up --detach --build --wait

# shellcheck disable=SC1090
source "$env_file"
printf '\nmemJev is ready.\n'
printf 'API: %s\n' "$MEMJEV_E2E_API_URL"
printf 'Credentials: source %s\n' "$env_file"
printf 'Next: make quickstart\n'
