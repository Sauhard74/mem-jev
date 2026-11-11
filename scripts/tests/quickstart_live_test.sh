#!/usr/bin/env bash
# Live check on a fresh stack: a routine learned from the quickstart comes
# back as an advisory plan with steps, verified runs of that plan promote it,
# and the promoted routine is then served as a normal plan.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp_dir=$(mktemp -d)
env_file="$tmp_dir/live.env"
project=${QUICKSTART_LIVE_PROJECT:-memjev-quickstart-live}
compose=(docker compose --env-file "$env_file")
cleanup() {
  if [[ "${QUICKSTART_LIVE_KEEP:-}" == 1 ]]; then
    printf 'kept stack %s; env file %s\n' "$project" "$env_file" >&2
    return
  fi
  if [[ -f "$env_file" ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

MEMJEV_ENV_FILE="$env_file" MEMJEV_RUNTIME_DIR="$tmp_dir/runtime" \
  "$repo_dir/scripts/setup.sh" --prepare-only --project "$project" >/dev/null
# Own ports, so the test can run next to a developer's stack.
sed -i.bak -e 's/^SURREAL_PORT=.*/SURREAL_PORT=28700/' -e 's/^MINIO_PORT=.*/MINIO_PORT=28701/' \
  -e 's/^TEMPORAL_PORT=.*/TEMPORAL_PORT=28702/' -e 's/^MEMJEV_API_PORT=.*/MEMJEV_API_PORT=28703/' \
  -e 's/^MEMJEV_WORKER_HEALTH_PORT=.*/MEMJEV_WORKER_HEALTH_PORT=28704/' \
  -e 's#^MEMJEV_E2E_API_URL=.*#MEMJEV_E2E_API_URL=http://127.0.0.1:28703#' "$env_file"
rm -f "$env_file.bak"
printf 'MEMJEV_LIFECYCLE_INTERVAL=2s\n' >> "$env_file"
MEMJEV_ENV_FILE="$env_file" MEMJEV_RUNTIME_DIR="$tmp_dir/runtime" "$repo_dir/scripts/setup.sh" >/dev/null

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

quickstart() {
  MEMJEV_ENV_FILE="$env_file" MEMJEV_SKIP_SETUP=1 "$repo_dir/scripts/quickstart.sh" > "$tmp_dir/quickstart.out" 2>&1
}

# Retrieve once and print the response JSON (or the error body).
retrieve() {
  curl --silent --show-error -H 'Content-Type: application/json' -H "Authorization: Bearer $MEMJEV_E2E_TOKEN" \
    -H "Idempotency-Key: live-retrieve-$(openssl rand -hex 6)" \
    --data '{"task":"write a hello file","tools":[{"name":"quickstart_print","contractVersionId":"'"$MEMJEV_QUICKSTART_CONTRACT_ID"'"}],"harness":{"name":"quickstart","version":"1"},"riskClass":"RISK_CLASS_LOW","latencyClass":"LATENCY_CLASS_INTERACTIVE","maxCandidates":5}' \
    "$MEMJEV_E2E_API_URL/memjev.v1.RetrievalService/Retrieve"
}

# plan_state prints "advisory", "promoted", or "none" for a Retrieve response,
# and fails if a plan lacks an injection ID, a task execution ID, or steps.
plan_state() {
  python3 -c '
import json, sys
response = json.loads(sys.argv[1])
plan = response.get("plan")
if response.get("disposition") != "RETRIEVAL_DISPOSITION_SELECTED" or not plan:
    print("none"); sys.exit(0)
assert plan.get("injectionId") and plan.get("taskExecutionId"), plan
steps = [step for node in plan["nodes"] for step in node.get("steps", [])]
assert steps and steps[0]["toolName"] == "quickstart_print", plan
print("advisory" if plan.get("advisoryOnly") else "promoted")
' "$1"
}

wait_for_plan() {
  local want=$1 state response
  for _ in $(seq 1 60); do
    response=$(retrieve)
    state=$(plan_state "$response")
    if [[ "$state" == "$want" ]]; then
      printf '%s' "$response"
      return 0
    fi
    sleep 2
  done
  printf 'timed out waiting for a %s plan; last response:\n%s\n' "$want" "$response" >&2
  exit 1
}

quickstart
grep -q 'Lifecycle complete' "$tmp_dir/quickstart.out" || { cat "$tmp_dir/quickstart.out" >&2; exit 1; }

# The learned routine comes back as an advisory plan with steps.
wait_for_plan advisory >/dev/null
quickstart
python3 - "$tmp_dir/quickstart.out" <<'PY'
import json, re, sys
text = open(sys.argv[1]).read()
retrieval = json.loads(re.search(r"1/3 Retrieve an advisory plan\n(\{.*?\n\})\n", text, re.S).group(1))
plan = retrieval["plan"]
assert retrieval["disposition"] == "RETRIEVAL_DISPOSITION_SELECTED", retrieval
assert plan["injectionId"] and plan["advisoryOnly"] is True, plan
assert plan["nodes"][0]["steps"][0]["toolName"] == "quickstart_print", plan
assert retrieval["candidates"][0]["advisoryOnly"] is True, retrieval
PY

# Two more verified runs, worded differently, give three causal successes,
# which the default policy promotes to trial. Natural wording reaches the
# routine through the lexical channels, not the exact one, and must still earn
# causal credit. The worker evaluates every 2 seconds.
quickstart 'please write the hello file again'
cp "$tmp_dir/quickstart.out" "$tmp_dir/reworded.out"
quickstart 'create a hello file'
wait_for_plan promoted >/dev/null

surreal_sql() {
  "${compose[@]}" exec -T surreal /surreal sql --hide-welcome --json \
    --endpoint http://127.0.0.1:8000 --namespace memjev --database memjev \
    --username "$SURREAL_USER" --password "$SURREAL_PASS" <<< "$1" 2>/dev/null || true
}

decisions=$(surreal_sql 'SELECT prior_state, next_state FROM lifecycle_decision;')
grep -q '"next_state":"trial"' <<< "${decisions// /}" || {
  printf 'no candidate -> trial lifecycle_decision row: %s\n' "$decisions" >&2
  exit 1
}
# The reworded runs were credited causally, and runs of the recalled plan
# were folded into it rather than learned as a second routine.
python3 - "$tmp_dir/reworded.out" <<'PY'
import json, re, sys
text = open(sys.argv[1]).read()
outcome = json.loads(re.search(r"3/3 Submit verified outcome evidence\n(\{.*?\n\})\n", text, re.S).group(1))
assert outcome.get("creditClass") == "OUTCOME_CREDIT_CLASS_CAUSAL_SUCCESS", outcome
PY
versions=$(surreal_sql 'SELECT count() AS n FROM procedure_version GROUP ALL;')
grep -q '"n":1' <<< "${versions// /}" || { printf 'expected one learned routine, got: %s\n' "$versions" >&2; exit 1; }

# The admin command runs evaluation on demand (nothing new to decide here).
printf '{"tenant_id":"tenant_local"}' | "${compose[@]}" exec -T api /memjev-admin evaluate-lifecycle >/dev/null

api() {
  curl --silent --show-error -H 'Content-Type: application/json' -H "Authorization: Bearer $MEMJEV_E2E_TOKEN" \
    -H "Idempotency-Key: live-$(openssl rand -hex 6)" --data "$2" "$MEMJEV_E2E_API_URL/memjev.v1.$1"
}
field() { python3 -c 'import json,sys; v=json.loads(sys.argv[1]); [v := v[k] for k in sys.argv[2:]]; print(v)' "$@"; }

# Corrections: worked -> didn't work -> worked on one recalled run. Each one
# takes over the plan's credit; repeating the first fact is not an outage.
quickstart 'write a hello file'
sections=$(python3 - "$tmp_dir/quickstart.out" <<'PY'
import json, re, sys
text = open(sys.argv[1]).read()
blocks = [json.loads(b) for b in re.findall(r"\n(\{.*?\n\})\n", "\n" + text, re.S)]
plan, ingest, outcome = blocks[0]["plan"], blocks[1], blocks[2]
print(plan["injectionId"], plan["taskExecutionId"], ingest["traceId"], outcome["outcomeId"])
PY
)
read -r injection_id task_execution_id trace_id first_outcome <<< "$sections"
now=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
outcome_body() { # verdict supersedes
  printf '{"traceId":"%s","injectionId":"%s","taskExecutionId":"%s","supersedesOutcomeId":"%s","correctionReason":"person changed the answer","evidence":[{"clientEvidenceId":"person","class":"EVIDENCE_CLASS_GOAL_PREDICATE","verdict":"%s","predicateId":"quickstart-goal","verifierId":"person","observedAt":"%s"}]}' \
    "$trace_id" "$injection_id" "$task_execution_id" "$2" "$1" "$now"
}
failed=$(api OutcomeService/RecordOutcome "$(outcome_body EVIDENCE_VERDICT_FAILED "$first_outcome")")
[[ $(field "$failed" creditClass) == OUTCOME_CREDIT_CLASS_CAUSAL_FAILURE ]] || { printf 'correction to failure: %s\n' "$failed" >&2; exit 1; }
worked=$(api OutcomeService/RecordOutcome "$(outcome_body EVIDENCE_VERDICT_SATISFIED "$(field "$failed" outcomeId)")")
[[ $(field "$worked" creditClass) == OUTCOME_CREDIT_CLASS_CAUSAL_SUCCESS ]] || { printf 'correction back to success: %s\n' "$worked" >&2; exit 1; }
stale=$(api OutcomeService/RecordOutcome "$(outcome_body EVIDENCE_VERDICT_FAILED "$first_outcome")")
grep -q 'outcome_credit_claimed' <<< "$stale" || { printf 'stale correction was not refused: %s\n' "$stale" >&2; exit 1; }

# An unsafe report quarantines the routine before the call returns.
quickstart 'write a hello file'
read -r injection_id task_execution_id trace_id recorded_outcome <<< "$(python3 - "$tmp_dir/quickstart.out" <<'PY'
import json, re, sys
text = open(sys.argv[1]).read()
blocks = [json.loads(b) for b in re.findall(r"\n(\{.*?\n\})\n", "\n" + text, re.S)]
print(blocks[0]["plan"]["injectionId"], blocks[0]["plan"]["taskExecutionId"], blocks[1]["traceId"], blocks[2]["outcomeId"])
PY
)"
# The run already has an outcome, so the unsafe report is a correction of it.
unsafe=$(api OutcomeService/RecordOutcome "$(printf '{"traceId":"%s","injectionId":"%s","taskExecutionId":"%s","supersedesOutcomeId":"%s","correctionReason":"the run was unsafe","unsafe":true,"evidence":[{"clientEvidenceId":"unsafe","class":"EVIDENCE_CLASS_GOAL_PREDICATE","verdict":"EVIDENCE_VERDICT_FAILED","predicateId":"quickstart-goal","verifierId":"person","observedAt":"%s"}]}' "$trace_id" "$injection_id" "$task_execution_id" "$recorded_outcome" "$now")")
[[ $(field "$unsafe" outcomeId) == out_* ]] || { printf 'unsafe outcome: %s\n' "$unsafe" >&2; exit 1; }
[[ $(plan_state "$(retrieve)") == none ]] || { printf '%s\n' 'a quarantined routine was still served' >&2; exit 1; }
grep -q '"next_state":"quarantined"' <<< "$(surreal_sql 'SELECT next_state FROM lifecycle_decision;' | tr -d ' ')" || {
  printf '%s\n' 'no quarantine lifecycle_decision row' >&2; exit 1; }

# A person with nothing learned gets nothing_learned_yet and creates no state.
person_token=$(MEMJEV_ENV_FILE="$env_file" "$repo_dir/scripts/add-person.sh" live_person --project "$project" | tail -1)
person=$(curl --silent -H 'Content-Type: application/json' -H "Authorization: Bearer $person_token" -H "Idempotency-Key: live-person-$(openssl rand -hex 6)" \
  --data '{"task":"write a hello file","tools":[{"name":"quickstart_print","contractVersionId":"'"$MEMJEV_QUICKSTART_CONTRACT_ID"'"}],"harness":{"name":"quickstart","version":"1"},"riskClass":"RISK_CLASS_LOW","latencyClass":"LATENCY_CLASS_INTERACTIVE","maxCandidates":5}' \
  "$MEMJEV_E2E_API_URL/memjev.v1.RetrievalService/Retrieve")
grep -q 'nothing_learned_yet' <<< "$person" || { printf 'new person retrieve: %s\n' "$person" >&2; exit 1; }
! grep -q 'tenant_local__' <<< "$(surreal_sql 'SELECT tenant_id FROM retrieval_serving_head;')" \
  || { printf '%s\n' 'a retrieve for a new person created a serving configuration' >&2; exit 1; }

printf '%s\n' 'live quickstart learning test: PASS'
