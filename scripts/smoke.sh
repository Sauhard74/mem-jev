#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"
if [[ ! -f .env.local ]]; then
  printf '%s\n' 'run scripts/integration.sh first' >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source .env.local
set +a

idempotency_key="smoke-$(openssl rand -hex 16)"
response=$(curl --fail-with-body --silent --show-error \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MEMJEV_E2E_TOKEN" \
  -H "Idempotency-Key: $idempotency_key" \
  --data '{"clientTraceId":"smoke-trace","harness":"smoke","events":[{"clientEventId":"event-1","occurredAt":"2026-09-20T00:00:00Z","kind":"EVENT_KIND_EXECUTE","toolName":"shell","fields":[{"name":"command","stringValue":"true"}],"result":{"state":"TOOL_RESULT_STATE_SUCCESS"}}]}' \
  "$MEMJEV_E2E_API_URL/memjev.v1.IngestService/IngestTrace")
python3 -c 'import json,sys; x=json.load(sys.stdin); print(json.dumps({"request_id":"smoke","receipt_id":x["receiptId"],"trace_id":x["traceId"],"result_code":x["disposition"]}, separators=(",",":")))' <<<"$response"
