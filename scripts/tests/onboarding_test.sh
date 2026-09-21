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

printf '%s\n' 'onboarding setup test: PASS'
