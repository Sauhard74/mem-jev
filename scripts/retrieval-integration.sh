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

go test -race -tags=e2e ./tests/e2e -run 'Test(DeterministicHostedRetrieval|HostedFiveChannelRetrievalAndVectorQuarantine)' -count=1 -v
go run ./tests/load -endpoint "$MEMJEV_E2E_API_URL" -token "$MEMJEV_E2E_TOKEN" -other-token "$MEMJEV_E2E_OTHER_TOKEN" -rps 100 -duration 10s -concurrency 64
printf '%s\n' 'retrieval integration gate: PASS'
