#!/usr/bin/env bash
# Issue a per-person API token. Routines recorded with the token are recalled
# only for that person; other people in the same tenant never see them.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file=${MEMJEV_ENV_FILE:-$repo_dir/.env.local}
tenant=tenant_local
region=local
restart=true

usage() {
  printf '%s\n' 'Usage: scripts/add-person.sh PERSON [--tenant TENANT] [--region REGION] [--no-restart]'
  printf '%s\n' '  PERSON        Letters, digits and dashes, for example alice or j-doe.'
  printf '%s\n' '  --tenant      Tenant the person belongs to (default tenant_local).'
  printf '%s\n' '  --region      Residency region (default local).'
  printf '%s\n' '  --no-restart  Do not restart the API to load the new token.'
}

person=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; tenant=$2; shift ;;
    --region) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; region=$2; shift ;;
    --no-restart) restart=false ;;
    -h|--help) usage; exit 0 ;;
    -*) usage >&2; exit 2 ;;
    *) [[ -z "$person" ]] || { usage >&2; exit 2; }; person=$1 ;;
  esac
  shift
done
[[ -n "$person" ]] || { usage >&2; exit 2; }
[[ "$person" =~ ^[A-Za-z0-9][A-Za-z0-9-]{0,62}$ ]] || { printf '%s\n' 'PERSON may contain only letters, digits and dashes' >&2; exit 2; }
[[ "$tenant" != *__* ]] || { printf '%s\n' 'tenant may not contain "__"' >&2; exit 2; }

for command_name in openssl python3 shasum; do
  command -v "$command_name" >/dev/null 2>&1 || { printf '%s is required\n' "$command_name" >&2; exit 1; }
done
[[ -f "$env_file" ]] || { printf 'environment file not found: %s (run scripts/setup.sh first)\n' "$env_file" >&2; exit 1; }
credential_path=$(sed -n 's/^MEMJEV_CREDENTIALS_FILE_HOST=//p' "$env_file")
[[ -f "$credential_path" ]] || { printf 'credential file not found: %s\n' "$credential_path" >&2; exit 1; }
allow_external=false
if grep -q '^MEMJEV_JEV_API_KEY_HOST_FILE=.' "$env_file"; then
  allow_external=true
fi

token=$(openssl rand -hex 24)
token_hash=$(printf '%s' "$token" | shasum -a 256 | awk '{print $1}')
python3 - "$credential_path" "$token_hash" "$tenant" "$person" "$region" "$allow_external" <<'PY'
import json, os, sys
path, token_hash, tenant, person, region, allow_external = sys.argv[1:]
with open(path) as handle:
    document = json.load(handle)
document["credentials"].append({
    "credential_sha256": token_hash,
    "tenant_id": tenant,
    "subject_id": person,
    "region": region,
    "scopes": ["ingest:write", "outcomes:write", "retrievals:read"],
    "consent": "learn_and_recall",
    "allow_external_inference": allow_external == "true",
})
temporary = path + ".tmp"
with open(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as handle:
    json.dump(document, handle, separators=(",", ":"))
os.replace(temporary, path)
PY

if [[ "$restart" == true ]] && command -v docker >/dev/null 2>&1 && docker compose --env-file "$env_file" ps --quiet api 2>/dev/null | grep -q .; then
  compose_files=(-f "$repo_dir/compose.yaml")
  if [[ "$allow_external" == true ]]; then
    compose_files+=(-f "$repo_dir/compose.jev.yaml")
  fi
  docker compose --env-file "$env_file" "${compose_files[@]}" up --detach --wait --force-recreate credential-init api >/dev/null
fi

printf 'Token for %s in %s (shown once, store it securely):\n%s\n' "$person" "$tenant" "$token"
