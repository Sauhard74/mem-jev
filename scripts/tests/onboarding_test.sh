#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

env_file="$tmp_dir/dev.env"
runtime_dir="$tmp_dir/runtime"

MEMJEV_ENV_FILE="$env_file" \
MEMJEV_RUNTIME_DIR="$runtime_dir" \
  "$repo_dir/scripts/setup.sh" --prepare-only >/dev/null

[[ -s "$env_file" ]] || { printf '%s\n' 'setup did not create the environment file' >&2; exit 1; }
[[ -s "$runtime_dir/credentials.json" ]] || { printf '%s\n' 'setup did not create the credential file' >&2; exit 1; }
[[ "$(stat -c '%a' "$env_file" 2>/dev/null || stat -f '%Lp' "$env_file")" == "600" ]] || {
  printf '%s\n' 'environment file permissions are not 0600' >&2
  exit 1
}
[[ "$(stat -c '%a' "$runtime_dir/credentials.json" 2>/dev/null || stat -f '%Lp' "$runtime_dir/credentials.json")" == "600" ]] || {
  printf '%s\n' 'credential file permissions are not 0600' >&2
  exit 1
}

# shellcheck disable=SC1090
source "$env_file"
first_token=$MEMJEV_E2E_TOKEN
[[ "$MEMJEV_E2E_API_URL" == "http://127.0.0.1:18080" ]] || {
  printf 'unexpected API URL: %s\n' "$MEMJEV_E2E_API_URL" >&2
  exit 1
}
[[ "$MEMJEV_CREDENTIALS_FILE_HOST" == "$runtime_dir/credentials.json" ]] || {
  printf 'unexpected credential path: %s\n' "$MEMJEV_CREDENTIALS_FILE_HOST" >&2
  exit 1
}

MEMJEV_ENV_FILE="$env_file" \
MEMJEV_RUNTIME_DIR="$runtime_dir" \
  "$repo_dir/scripts/setup.sh" --prepare-only >/dev/null

# shellcheck disable=SC1090
source "$env_file"
[[ "$MEMJEV_E2E_TOKEN" == "$first_token" ]] || {
  printf '%s\n' 'setup replaced an existing development token' >&2
  exit 1
}

jev_key="$tmp_dir/jev-api-key"
printf '%s' 'test-key' > "$jev_key"
MEMJEV_ENV_FILE="$env_file" \
MEMJEV_RUNTIME_DIR="$runtime_dir" \
  "$repo_dir/scripts/setup.sh" --prepare-only --jev-key-file "$jev_key" >/dev/null
grep -q "^MEMJEV_JEV_API_KEY_HOST_FILE=$jev_key$" "$env_file" || { printf '%s\n' 'setup did not save the Jev key path' >&2; exit 1; }
grep -q '^MEMJEV_JEV_MODEL=jev-1.13.0$' "$env_file" || { printf '%s\n' 'setup did not save the default Jev model' >&2; exit 1; }
python3 -c 'import json,sys; assert all(c["allow_external_inference"] for c in json.load(open(sys.argv[1]))["credentials"])' "$runtime_dir/credentials.json" || {
  printf '%s\n' 'setup did not allow Jev on local credentials' >&2
  exit 1
}

MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/add-person.sh" alice --no-restart > "$tmp_dir/alice.out"
alice_token=$(tail -n 1 "$tmp_dir/alice.out")
alice_hash=$(printf '%s' "$alice_token" | shasum -a 256 | awk '{print $1}')
python3 - "$runtime_dir/credentials.json" "$alice_hash" <<'PY' || { printf '%s\n' 'add-person did not add a per-person credential' >&2; exit 1; }
import json, sys
entries = [c for c in json.load(open(sys.argv[1]))["credentials"] if c["credential_sha256"] == sys.argv[2]]
assert len(entries) == 1 and entries[0]["subject_id"] == "alice" and entries[0]["tenant_id"] == "tenant_local"
PY
if MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/add-person.sh" ' padded' --no-restart >/dev/null 2>&1; then
  printf '%s\n' 'add-person accepted an invalid name' >&2
  exit 1
fi

# Long or underscored person IDs are accepted and stored under a hashed scope.
long_person="milkyway_$(printf 'p%.0s' $(seq 1 100))"
MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/add-person.sh" "$long_person" --no-restart > "$tmp_dir/long.out"
expected_scope=$(python3 -c 'import hashlib,sys; print("tenant_local__h_" + hashlib.sha256(b"memjev-subject.v1\x00" + sys.argv[1].encode()).hexdigest()[:40])' "$long_person")
grep -q "stored as $expected_scope" "$tmp_dir/long.out" || { printf '%s\n' 'add-person printed the wrong scope for a long person ID' >&2; exit 1; }

MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/add-person.sh" --service --no-restart > "$tmp_dir/service.out"
service_hash=$(tail -n 1 "$tmp_dir/service.out" | tr -d '\n' | shasum -a 256 | awk '{print $1}')
python3 - "$runtime_dir/credentials.json" "$service_hash" <<'PY' || { printf '%s\n' 'add-person --service did not add a trusted service credential' >&2; exit 1; }
import json, sys
entries = [c for c in json.load(open(sys.argv[1]))["credentials"] if c["credential_sha256"] == sys.argv[2]]
assert len(entries) == 1 and entries[0].get("may_assert_subject") is True and "subject_id" not in entries[0]
PY

MEMJEV_ENV_FILE="$env_file" MEMJEV_RUNTIME_DIR="$runtime_dir" \
  "$repo_dir/scripts/setup.sh" --prepare-only --project memjev-test >/dev/null
grep -q '^COMPOSE_PROJECT_NAME=memjev-test$' "$env_file" || { printf '%s\n' 'setup did not save the Compose project' >&2; exit 1; }
grep -q "^COMPOSE_FILE=$repo_dir/compose.yaml:$repo_dir/compose.jev.yaml$" "$env_file" || { printf '%s\n' 'setup did not save the Compose files' >&2; exit 1; }

printf '%s\n' 'onboarding setup test: PASS'
