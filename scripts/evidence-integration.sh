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

curl --fail --silent --show-error "http://127.0.0.1:${MEMJEV_WORKER_HEALTH_PORT}/readyz" >/dev/null
docker compose --env-file .env.local stop worker >/dev/null
go test -race -tags=e2e ./tests/e2e -run TestPrepareExpiredLeaseForWorkerRecovery -count=1 -v
docker compose --env-file .env.local start worker >/dev/null
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${MEMJEV_WORKER_HEALTH_PORT}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --fail --silent --show-error "http://127.0.0.1:${MEMJEV_WORKER_HEALTH_PORT}/readyz" >/dev/null
go test -race -tags=e2e ./tests/e2e -run TestWorkerRecoversExpiredLeaseAfterRestart -count=1 -v
go test -race -tags=e2e ./tests/e2e -run 'Test(EvidencePipelineProductionCases|MaintenanceWorkerPublishesAuthenticatedCompatibilityGraph)$' -count=1 -v
printf '%s\n' 'evidence pipeline gate: PASS'
