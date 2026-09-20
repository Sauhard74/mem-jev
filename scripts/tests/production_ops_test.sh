#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
for script in load-production-env.sh production-preflight.sh verify-rollback-images.sh deploy-production.sh rollback-production.sh; do bash -n "$repo_dir/scripts/$script"; done
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
printf '{}' > "$tmp/credentials.json"; chmod 600 "$tmp/credentials.json"
printf 'fixture-key' > "$tmp/jev-api-key"; chmod 600 "$tmp/jev-api-key"
digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
query_key=MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=
kms_arn=arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555
base_env=(MEMJEV_DOMAIN=memory.example.com MEMJEV_OTLP_ENDPOINT=https://otel.example.com MEMJEV_BACKUP_AGE_RECIPIENT=age1fixture MEMJEV_CREDENTIALS_FILE_HOST="$tmp/credentials.json" MEMJEV_JEV_API_KEY_HOST_FILE="$tmp/jev-api-key" MEMJEV_JEV_MODEL=jev-1.13.0 MEMJEV_CADDY_IMAGE="registry/caddy@$digest" MEMJEV_SURREAL_AUTH_SCOPE=database MEMJEV_TEMPORAL_TLS_SERVER_NAME=temporal.example.com MEMJEV_TEMPORAL_API_KEY=fixture MEMJEV_RETRIEVAL_QUERY_KEY_ID=key-v1 MEMJEV_RETRIEVAL_QUERY_KEY_BASE64="$query_key" MEMJEV_PREVIOUS_SCHEMA_MAX_VERSION=14 MEMJEV_ARCHIVE_SSE=aws:kms MEMJEV_ARCHIVE_KMS_KEY_ID="$kms_arn")
if env "${base_env[@]}" MEMJEV_API_IMAGE=registry/api:latest MEMJEV_WORKER_IMAGE=registry/worker:latest MEMJEV_PREVIOUS_API_IMAGE="registry/api@$digest" MEMJEV_PREVIOUS_WORKER_IMAGE="registry/worker@$digest" "$repo_dir/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo "preflight accepted mutable images" >&2; exit 1
fi
env "${base_env[@]}" MEMJEV_API_IMAGE="registry/api@$digest" MEMJEV_WORKER_IMAGE="registry/worker@$digest" MEMJEV_PREVIOUS_API_IMAGE="registry/api@$digest" MEMJEV_PREVIOUS_WORKER_IMAGE="registry/worker@$digest" "$repo_dir/scripts/production-preflight.sh"
chmod 644 "$tmp/credentials.json"
if env "${base_env[@]}" MEMJEV_API_IMAGE="registry/api@$digest" MEMJEV_WORKER_IMAGE="registry/worker@$digest" MEMJEV_PREVIOUS_API_IMAGE="registry/api@$digest" MEMJEV_PREVIOUS_WORKER_IMAGE="registry/worker@$digest" "$repo_dir/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo "preflight accepted loose secret permissions" >&2; exit 1
fi
if env "${base_env[@]}" MEMJEV_JEV_MODEL=jev-latest MEMJEV_API_IMAGE="registry/api@$digest" MEMJEV_WORKER_IMAGE="registry/worker@$digest" MEMJEV_PREVIOUS_API_IMAGE="registry/api@$digest" MEMJEV_PREVIOUS_WORKER_IMAGE="registry/worker@$digest" "$repo_dir/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo "preflight accepted an unpinned Jev model" >&2; exit 1
fi
grep -q 'MEMJEV_PREVIOUS_API_IMAGE" "$MEMJEV_PREVIOUS_WORKER_IMAGE"' "$repo_dir/scripts/deploy-production.sh" || { echo "deploy does not preserve rollback images" >&2; exit 1; }
grep -q '/memjev-worker, healthcheck' "$repo_dir/compose.production.yaml" || { echo "worker health check is absent" >&2; exit 1; }
grep -q 'MEMJEV_TEMPORAL_TLS_SERVER_NAME' "$repo_dir/compose.production.yaml" || { echo "worker TLS settings are absent" >&2; exit 1; }
grep -q 'org.memjev.schema.max-version' "$repo_dir/Dockerfile" || { echo "images do not declare schema compatibility" >&2; exit 1; }
grep -q 'verify-rollback-images.sh' "$repo_dir/scripts/deploy-production.sh" || { echo "deploy does not inspect rollback image compatibility" >&2; exit 1; }
echo "production operator tests: PASS"
