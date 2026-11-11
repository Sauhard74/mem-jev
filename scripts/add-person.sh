#!/usr/bin/env bash
# Issue an API token for one person, or for a trusted service that names the
# person on each request. Routines recorded for a person are recalled only for
# that person; other people in the same tenant never see them.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file=${MEMJEV_ENV_FILE:-$repo_dir/.env.local}
tenant=tenant_local
region=local
reload=true
service=false
project=''

usage() {
  printf '%s\n' 'Usage: scripts/add-person.sh PERSON [options]'
  printf '%s\n' '       scripts/add-person.sh --service [options]'
  printf '%s\n' '  PERSON        Person ID, up to 256 characters, for example alice or team_j.doe.'
  printf '%s\n' '  --service     Issue a token for a trusted service instead. It may name the'
  printf '%s\n' '                person on each request with the Memjev-Subject header.'
  printf '%s\n' '  --tenant      Tenant the person belongs to (default tenant_local).'
  printf '%s\n' '  --region      Residency region (default local).'
  printf '%s\n' '  --project     Docker Compose project running memJev (default: COMPOSE_PROJECT_NAME'
  printf '%s\n' '                from the environment or the env file, else memjev).'
  printf '%s\n' '  --no-reload   Only update the credential file; do not hand it to the running API.'
}

person=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --service) service=true ;;
    --tenant) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; tenant=$2; shift ;;
    --region) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; region=$2; shift ;;
    --project) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; project=$2; shift ;;
    --no-reload|--no-restart) reload=false ;;
    -h|--help) usage; exit 0 ;;
    -*) usage >&2; exit 2 ;;
    *) [[ -z "$person" ]] || { usage >&2; exit 2; }; person=$1 ;;
  esac
  shift
done
if [[ "$service" == true ]]; then
  [[ -z "$person" ]] || { usage >&2; exit 2; }
else
  [[ -n "$person" ]] || { usage >&2; exit 2; }
fi
[[ "$tenant" != *__* && "$tenant" != *_ ]] || { printf '%s\n' 'tenant may not contain "__" or end with "_"' >&2; exit 2; }

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
# Validates the person ID the way the API does and prints its storage scope.
scope=$(python3 - "$credential_path" "$token_hash" "$tenant" "$person" "$region" "$allow_external" <<'PY'
import hashlib, json, os, re, sys, unicodedata
path, token_hash, tenant, person, region, allow_external = sys.argv[1:]
entry = {
    "credential_sha256": token_hash,
    "tenant_id": tenant,
    "region": region,
    "scopes": ["ingest:write", "outcomes:write", "retrievals:read"],
    "consent": "learn_and_recall",
    "allow_external_inference": allow_external == "true",
}
scope = tenant
if person:
    encoded = person.encode()
    if len(encoded) > 256 or person.strip() != person or any(unicodedata.category(c) == "Cc" for c in person):
        sys.exit("PERSON must be 1 to 256 printable characters without surrounding spaces")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,62}", person):
        key = person
    else:
        key = "h_" + hashlib.sha256(b"memjev-subject.v1\x00" + encoded).hexdigest()[:40]
    scope = tenant + "__" + key
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", scope):
        sys.exit("tenant is too long for a per-person scope")
    entry["subject_id"] = person
else:
    entry["may_assert_subject"] = True
with open(path) as handle:
    document = json.load(handle)
document["credentials"].append(entry)
temporary = path + ".tmp"
with open(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as handle:
    json.dump(document, handle, separators=(",", ":"))
os.replace(temporary, path)
print(scope)
PY
)

project_args=()
if [[ -n "$project" ]]; then
  project_args=(-p "$project")
fi
compose=(docker compose --env-file "$env_file" ${project_args[@]+"${project_args[@]}"})
# The API re-reads its credential file every few seconds, so a new person only
# needs the file copied into the API's secret volume; the API keeps running.
if [[ "$reload" == true ]] && command -v docker >/dev/null 2>&1; then
  if "${compose[@]}" ps --quiet api 2>/dev/null | grep -q .; then
    if ! output=$("${compose[@]}" run --rm --no-deps --quiet-pull credential-init 2>&1); then
      printf '%s\n' "$output" >&2
      printf '%s\n' 'Could not hand the new credential file to the API.' >&2
      exit 1
    fi
    # Wait until the API accepts the token (anything but 401 Unauthorized).
    api_port=$(sed -n 's/^MEMJEV_API_PORT=//p' "$env_file")
    accepted=false
    for _ in $(seq 1 20); do
      status=$(curl --silent --output /dev/null --write-out '%{http_code}' -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $token" --data '{}' "http://127.0.0.1:${api_port:-8080}/memjev.v1.RetrievalService/ExplainRetrieval" 2>/dev/null || true)
      if [[ "$status" != 401 && "$status" != 000 ]]; then
        accepted=true
        break
      fi
      sleep 0.5
    done
    if [[ "$accepted" != true ]]; then
      printf '%s\n' 'The API has not picked up the new token yet; it reloads its credential file every few seconds.' >&2
    fi
  else
    printf '%s\n' 'No running memJev API found for this project; start it (or pass --project) to load the token.' >&2
  fi
fi

if [[ "$service" == true ]]; then
  printf 'Service token for %s (shown once, store it securely).\n' "$tenant"
  printf 'Send "Memjev-Subject: <person id>" with each request to keep each person separate:\n%s\n' "$token"
else
  printf 'Token for %s in %s, stored as %s (shown once, store it securely):\n%s\n' "$person" "$tenant" "$scope" "$token"
fi
