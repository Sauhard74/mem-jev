#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
docker run --rm --entrypoint /bin/promtool --volume "$repo_dir/ops/alerts:/rules:ro" \
  prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 \
  check rules /rules/evidence-pipeline.yaml
