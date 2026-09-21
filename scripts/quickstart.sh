#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file=${MEMJEV_ENV_FILE:-$repo_dir/.env.local}
task=${1:-"write a hello file"}

for command_name in curl openssl python3; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

if [[ "${MEMJEV_SKIP_SETUP:-0}" != "1" ]]; then
  MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/setup.sh"
fi
[[ -f "$env_file" ]] || { printf 'environment file not found: %s\n' "$env_file" >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a
: "${MEMJEV_E2E_API_URL:?environment file is missing MEMJEV_E2E_API_URL}"
: "${MEMJEV_E2E_TOKEN:?environment file is missing MEMJEV_E2E_TOKEN}"

run_id=$(openssl rand -hex 12)
occurred_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
headers=(
  -H 'Content-Type: application/json'
  -H "Authorization: Bearer $MEMJEV_E2E_TOKEN"
)

retrieve_payload=$(python3 - "$task" <<'PY'
import json
import sys
print(json.dumps({
    "task": sys.argv[1],
    "tools": [{"name": "shell", "contractVersionId": "shell.v1"}],
    "harness": {"name": "quickstart", "version": "1"},
    "riskClass": "RISK_CLASS_LOW",
    "latencyClass": "LATENCY_CLASS_INTERACTIVE",
    "maxCandidates": 5,
}, separators=(",", ":")))
PY
)
printf '%s\n' '1/3 Retrieve an advisory plan'
if retrieval=$(curl --fail-with-body --silent --show-error \
  "${headers[@]}" -H "Idempotency-Key: quickstart-retrieve-$run_id" \
  --data "$retrieve_payload" \
  "$MEMJEV_E2E_API_URL/memjev.v1.RetrievalService/Retrieve"); then
  python3 -m json.tool <<< "$retrieval"
  injection_id=$(python3 -c 'import json,sys; print((json.load(sys.stdin).get("plan") or {}).get("injectionId", ""))' <<< "$retrieval")
  task_execution_id=$(python3 -c 'import json,sys; print((json.load(sys.stdin).get("plan") or {}).get("taskExecutionId", ""))' <<< "$retrieval")
else
  printf '%s\n' 'No retrieval configuration is active yet; continuing without recalled guidance.'
  injection_id=''
  task_execution_id=''
fi

trace_payload=$(python3 - "$task" "$occurred_at" "$run_id" <<'PY'
import json
import sys
task, occurred_at, run_id = sys.argv[1:]
print(json.dumps({
    "clientTraceId": "quickstart-" + run_id,
    "harness": "quickstart",
    "harnessVersion": "1",
    "task": task,
    "events": [{
        "clientEventId": "event-1",
        "occurredAt": occurred_at,
        "kind": "EVENT_KIND_EXECUTE",
        "toolName": "shell",
        "toolVersion": "1",
        "fields": [{"name": "command", "stringValue": "printf hello"}],
        "result": {"state": "TOOL_RESULT_STATE_SUCCESS", "exitCode": 0},
    }],
}, separators=(",", ":")))
PY
)
printf '\n%s\n' '2/3 Record the agent trace'
ingest=$(curl --fail-with-body --silent --show-error \
  "${headers[@]}" -H "Idempotency-Key: quickstart-ingest-$run_id" \
  --data "$trace_payload" \
  "$MEMJEV_E2E_API_URL/memjev.v1.IngestService/IngestTrace")
python3 -m json.tool <<< "$ingest"
trace_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["traceId"])' <<< "$ingest")

outcome_payload=$(python3 - "$trace_id" "$occurred_at" "$run_id" "$injection_id" "$task_execution_id" <<'PY'
import json
import sys
trace_id, observed_at, run_id, injection_id, task_execution_id = sys.argv[1:]
payload = {
    "traceId": trace_id,
    "executionId": "quickstart-" + run_id,
    "evidence": [{
        "clientEvidenceId": "goal-" + run_id,
        "class": "EVIDENCE_CLASS_GOAL_PREDICATE",
        "verdict": "EVIDENCE_VERDICT_SATISFIED",
        "predicateId": "quickstart-goal",
        "verifierId": "quickstart-user",
        "verifierVersion": "1",
        "observedAt": observed_at,
    }],
}
if injection_id:
    payload["injectionId"] = injection_id
if task_execution_id:
    payload["taskExecutionId"] = task_execution_id
print(json.dumps(payload, separators=(",", ":")))
PY
)
printf '\n%s\n' '3/3 Submit verified outcome evidence'
outcome=$(curl --fail-with-body --silent --show-error \
  "${headers[@]}" -H "Idempotency-Key: quickstart-outcome-$run_id" \
  --data "$outcome_payload" \
  "$MEMJEV_E2E_API_URL/memjev.v1.OutcomeService/RecordOutcome")
python3 -m json.tool <<< "$outcome"

printf '\nLifecycle complete. memJev retrieved, captured, and evaluated one agent run.\n'
printf '%s\n' 'A new local corpus may abstain until a registered tool contract and verified procedure are available.'
