#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"

: "${MEMJEV_E2E_API_URL:?required}"
: "${MEMJEV_E2E_SURREAL_URL:?required}"
: "${MEMJEV_E2E_TOKEN:?required}"
: "${MEMJEV_E2E_OTHER_TOKEN:?required}"
: "${SURREAL_USER:?required}"
: "${SURREAL_PASS:?required}"

duration=${MEMJEV_QUALIFICATION_DURATION:-30m}
case "$duration" in
  *m) duration_minutes=${duration%m} ;;
  *) printf '%s\n' 'MEMJEV_QUALIFICATION_DURATION must be expressed in minutes and be at least 30m' >&2; exit 2 ;;
esac
if (( duration_minutes < 30 )); then
  printf '%s\n' 'production qualification requires at least 30 minutes' >&2
  exit 2
fi

go run ./tests/qualification -minimum-events 10000000 -minimum-retrieval-documents 1000000 -minimum-tenants 2 -minimum-vector-documents 1000000 -require-vector

memory_samples=$(mktemp)
sampler_pid=
cleanup() {
  if [[ -n "$sampler_pid" ]]; then
    kill "$sampler_pid" >/dev/null 2>&1 || true
    wait "$sampler_pid" >/dev/null 2>&1 || true
  fi
  rm -f "$memory_samples"
}
trap cleanup EXIT

sample_memory() {
  while true; do
    docker stats --no-stream --format '{{.MemUsage}}' memjev-api-1 | awk '{print $1}' >> "$memory_samples"
    sleep 10
  done
}

sample_memory &
sampler_pid=$!
go run ./tests/load -endpoint "$MEMJEV_E2E_API_URL" -token "$MEMJEV_E2E_TOKEN" -other-token "$MEMJEV_E2E_OTHER_TOKEN" -rps 100 -duration "$duration" -concurrency 256 -require-vector
kill "$sampler_pid" >/dev/null 2>&1 || true
wait "$sampler_pid" >/dev/null 2>&1 || true
sampler_pid=

python3 - "$memory_samples" <<'PY'
import re
import sys

def parse(value):
    match = re.fullmatch(r"([0-9.]+)([KMG]iB|B)", value.strip())
    if not match:
        raise SystemExit(f"unrecognized docker memory sample: {value!r}")
    scale = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[match.group(2)]
    return float(match.group(1)) * scale

values = [parse(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if len(values) < 30:
    raise SystemExit("insufficient server RSS samples")
window = max(6, len(values) // 10)
start = sum(values[:window]) / window
end = sum(values[-window:]) / window
limit = start * 1.25 + 128 * 1024**2
print({"rss_start_bytes": int(start), "rss_end_bytes": int(end), "rss_limit_bytes": int(limit), "samples": len(values)})
if end > limit:
    raise SystemExit("server RSS did not remain bounded")
PY

printf '%s\n' 'retrieval production qualification gate: PASS'
