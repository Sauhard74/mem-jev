#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"

mkdir -p .runtime
chmod 700 .runtime
token=${MEMJEV_E2E_TOKEN:-$(openssl rand -hex 24)}
token_hash=$(printf '%s' "$token" | shasum -a 256 | awk '{print $1}')
surreal_pass=${SURREAL_PASS:-$(openssl rand -hex 24)}
minio_pass=${MINIO_ROOT_PASSWORD:-$(openssl rand -hex 24)}
minio_kms=${MINIO_KMS_SECRET_KEY:-memjev-local:$(openssl rand -base64 32 | tr -d '\n')}

credential_path="$repo_dir/.runtime/credentials.json"
printf '{"credentials":[{"credential_sha256":"%s","tenant_id":"tenant_e2e","region":"local","scopes":["ingest:write"],"consent":"learn_and_recall"}]}' "$token_hash" > "$credential_path"
chmod 600 "$credential_path"

cat > .env.local <<EOF
SURREAL_USER=root
SURREAL_PASS=$surreal_pass
SURREAL_PORT=18000
MINIO_ROOT_USER=memjevlocal
MINIO_ROOT_PASSWORD=$minio_pass
MINIO_KMS_SECRET_KEY=$minio_kms
MINIO_PORT=19000
MEMJEV_ARCHIVE_BUCKET=memjev-canonical
MEMJEV_API_PORT=18080
MEMJEV_CREDENTIALS_FILE_HOST=$credential_path
MEMJEV_E2E_TOKEN=$token
MEMJEV_E2E_API_URL=http://127.0.0.1:18080
MEMJEV_E2E_SURREAL_URL=ws://127.0.0.1:18000
MEMJEV_E2E_S3_URL=http://127.0.0.1:19000
MEMJEV_E2E_S3_BUCKET=memjev-canonical
EOF
chmod 600 .env.local

set -a
# shellcheck disable=SC1091
source .env.local
set +a

docker compose --env-file .env.local down --volumes --remove-orphans >/dev/null 2>&1 || true
docker compose --env-file .env.local up --detach --build --wait
go test -race -tags=integration ./internal/store/... -count=1
go test -race -tags=e2e ./tests/e2e -run TestDurableIdempotentIngest -count=1 -v
printf '%s\n' 'integration gate: PASS'
