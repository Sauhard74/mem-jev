#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp_dir=$(mktemp -d)
server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

port_file="$tmp_dir/port"
request_file="$tmp_dir/requests.jsonl"
python3 "$repo_dir/scripts/tests/fake_memjev_server.py" "$port_file" "$request_file" &
server_pid=$!
for _ in $(seq 1 50); do
  [[ -s "$port_file" ]] && break
  sleep 0.1
done
[[ -s "$port_file" ]] || { printf '%s\n' 'fake server did not start' >&2; exit 1; }

env_file="$tmp_dir/dev.env"
{
  printf 'MEMJEV_E2E_API_URL=http://127.0.0.1:%s\n' "$(cat "$port_file")"
  printf 'MEMJEV_E2E_TOKEN=local-test-token\n'
} > "$env_file"

MEMJEV_ENV_FILE="$env_file" MEMJEV_SKIP_SETUP=1 \
  "$repo_dir/scripts/quickstart.sh" "write a hello file" > "$tmp_dir/output"

python3 - "$request_file" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    requests = [json.loads(line) for line in source]

expected_paths = [
    "/memjev.v1.RetrievalService/Retrieve",
    "/memjev.v1.IngestService/IngestTrace",
    "/memjev.v1.OutcomeService/RecordOutcome",
]
assert [request["path"] for request in requests] == expected_paths
assert all(request["authorization"] == "Bearer local-test-token" for request in requests)
assert all(request["idempotency_key"] for request in requests)
assert requests[0]["body"]["task"] == "write a hello file"
assert requests[1]["body"]["task"] == "write a hello file"
assert requests[2]["body"]["traceId"] == "trace_demo"
assert requests[2]["body"]["injectionId"] == "injection_demo"
assert requests[2]["body"]["taskExecutionId"] == "execution_demo"
PY

grep -q 'Lifecycle complete' "$tmp_dir/output"
printf '%s\n' 'quickstart lifecycle test: PASS'
