#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file=${MEMJEV_ENV_FILE:-$repo_dir/.env.local}
runtime_dir=${MEMJEV_RUNTIME_DIR:-$repo_dir/.runtime}
prepare_only=false
jev_key_file=${MEMJEV_JEV_API_KEY_HOST_FILE:-}
jev_model=${MEMJEV_JEV_MODEL:-}
project=${COMPOSE_PROJECT_NAME:-}
default_jev_model=jev-1.13.0

usage() {
  printf '%s\n' 'Usage: scripts/setup.sh [--prepare-only] [--jev-key-file PATH] [--jev-model jev-X.Y.Z] [--project NAME]'
  printf '%s\n' '  --prepare-only       Generate local credentials without starting Docker Compose.'
  printf '%s\n' '  --jev-key-file PATH  Enable Jev ranking with the TypeSafe API key stored in PATH.'
  printf '%s\n' "  --jev-model MODEL    Pinned Jev model (default $default_jev_model)."
  printf '%s\n' '  --project NAME       Docker Compose project name (default memjev). Saved in the'
  printf '%s\n' '                       env file so the other scripts use the same project.'
  printf '%s\n' 'MEMJEV_JEV_API_KEY_HOST_FILE, MEMJEV_JEV_MODEL and COMPOSE_PROJECT_NAME work the same way as the flags.'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prepare-only) prepare_only=true ;;
    --jev-key-file) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; jev_key_file=$2; shift ;;
    --jev-model) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; jev_model=$2; shift ;;
    --project) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; project=$2; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
  shift
done

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

# Set or replace KEY=VALUE in the environment file.
set_env() {
  local key=$1 value=$2 temporary
  temporary=$(mktemp "$env_file.XXXXXX")
  { grep -v "^$key=" "$env_file" || true; printf '%s=%s\n' "$key" "$value"; } > "$temporary"
  chmod 600 "$temporary"
  mv "$temporary" "$env_file"
}

# Compose reads COMPOSE_PROJECT_NAME and COMPOSE_FILE from the env file, so
# every script given --env-file targets this stack from any directory.
if [[ -n "$project" ]]; then
  [[ "$project" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || { printf 'invalid Compose project name: %s\n' "$project" >&2; exit 1; }
  set_env COMPOSE_PROJECT_NAME "$project"
fi

# A Jev key given now, or saved by an earlier run, turns Jev on.
if [[ -z "$jev_key_file" ]]; then
  jev_key_file=$(sed -n 's/^MEMJEV_JEV_API_KEY_HOST_FILE=//p' "$env_file")
fi
compose_files=(-f "$repo_dir/compose.yaml")
set_env COMPOSE_FILE "$repo_dir/compose.yaml"
if [[ -n "$jev_key_file" ]]; then
  [[ -f "$jev_key_file" ]] || { printf 'Jev key file does not exist: %s\n' "$jev_key_file" >&2; exit 1; }
  jev_key_file=$(cd "$(dirname "$jev_key_file")" && pwd)/$(basename "$jev_key_file")
  [[ -s "$jev_key_file" ]] || { printf 'Jev key file is empty: %s\n' "$jev_key_file" >&2; exit 1; }
  chmod 600 "$jev_key_file"
  if [[ -z "$jev_model" ]]; then
    jev_model=$(sed -n 's/^MEMJEV_JEV_MODEL=//p' "$env_file")
  fi
  jev_model=${jev_model:-$default_jev_model}
  [[ "$jev_model" =~ ^jev-[0-9]+\.[0-9]+\.[0-9]+$ ]] || { printf 'Jev model must be pinned as jev-X.Y.Z: %s\n' "$jev_model" >&2; exit 1; }
  command -v python3 >/dev/null 2>&1 || { printf '%s\n' 'python3 is required to enable Jev' >&2; exit 1; }
  set_env MEMJEV_JEV_API_KEY_HOST_FILE "$jev_key_file"
  set_env MEMJEV_JEV_MODEL "$jev_model"
  set_env COMPOSE_FILE "$repo_dir/compose.yaml:$repo_dir/compose.jev.yaml"
  # Jev only runs for credentials that allow external model calls. These are
  # local development credentials, so allow it for all of them.
  python3 - "$credential_path" <<'PY'
import json, os, sys
path = sys.argv[1]
with open(path) as handle:
    document = json.load(handle)
for entry in document["credentials"]:
    entry["allow_external_inference"] = True
temporary = path + ".tmp"
with open(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as handle:
    json.dump(document, handle, separators=(",", ":"))
os.replace(temporary, path)
PY
  compose_files+=(-f "$repo_dir/compose.jev.yaml")
  printf 'Jev enabled with model %s.\n' "$jev_model"
fi

if [[ "$prepare_only" == true ]]; then
  printf 'Local environment ready: %s\n' "$env_file"
  exit 0
fi

command -v docker >/dev/null 2>&1 || { printf '%s\n' 'Docker with Compose v2 is required' >&2; exit 1; }
docker compose version >/dev/null
docker compose --env-file "$env_file" "${compose_files[@]}" up --detach --build --wait

# The quickstart's demo tool needs a contract before its runs can be learned.
contract_id=$(MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/register-tool-contracts.sh" "$repo_dir/examples/tool-contracts/quickstart.json" |
  python3 -c 'import json,sys; print(json.loads(sys.stdin.readline())["contract_version_id"])')
set_env MEMJEV_QUICKSTART_CONTRACT_ID "$contract_id"

# shellcheck disable=SC1090
source "$env_file"
printf '\nmemJev is ready.\n'
printf 'API: %s\n' "$MEMJEV_E2E_API_URL"
printf 'Credentials: source %s\n' "$env_file"
printf 'Next: make quickstart\n'
