#!/usr/bin/env bash
# Register tool contracts with a running local memJev. Traces are learned only
# when every tool they use has a contract; see docs/agent-integration.md.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file=${MEMJEV_ENV_FILE:-$repo_dir/.env.local}
tenant=tenant_local
project=''

usage() {
  printf '%s\n' 'Usage: scripts/register-tool-contracts.sh FILE [--tenant TENANT] [--project NAME]'
  printf '%s\n' '  FILE       JSON file {"contracts": [ ... ]}; see examples/tool-contracts/.'
  printf '%s\n' '  --tenant   Tenant the contracts belong to (default tenant_local). They also'
  printf '%s\n' '             apply to every person in the tenant.'
  printf '%s\n' '  --project  Docker Compose project running memJev (default: COMPOSE_PROJECT_NAME'
  printf '%s\n' '             from the environment or the env file, else memjev).'
  printf '%s\n' 'Prints one JSON line per contract. Use its contract_version_id as the'
  printf '%s\n' 'contractVersionId of the tool in Retrieve requests.'
}

file=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; tenant=$2; shift ;;
    --project) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; project=$2; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) usage >&2; exit 2 ;;
    *) [[ -z "$file" ]] || { usage >&2; exit 2; }; file=$1 ;;
  esac
  shift
done
[[ -n "$file" && -f "$file" ]] || { usage >&2; exit 2; }
[[ -f "$env_file" ]] || { printf 'environment file not found: %s (run scripts/setup.sh first)\n' "$env_file" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { printf '%s\n' 'python3 is required' >&2; exit 1; }

project_args=()
if [[ -n "$project" ]]; then
  project_args=(-p "$project")
fi
python3 -c '
import json, sys
with open(sys.argv[1]) as handle:
    document = json.load(handle)
contracts = document["contracts"] if isinstance(document, dict) else document
print(json.dumps({"tenant_id": sys.argv[2], "contracts": contracts}))
' "$file" "$tenant" | docker compose --env-file "$env_file" ${project_args[@]+"${project_args[@]}"} exec -T api /memjev-admin register-tool-contracts
