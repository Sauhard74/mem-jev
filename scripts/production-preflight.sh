#!/usr/bin/env bash
set -euo pipefail

required=(MEMJEV_DOMAIN MEMJEV_OTLP_ENDPOINT MEMJEV_BACKUP_AGE_RECIPIENT MEMJEV_CREDENTIALS_FILE_HOST MEMJEV_JEV_API_KEY_HOST_FILE MEMJEV_JEV_MODEL MEMJEV_SURREAL_AUTH_SCOPE MEMJEV_TEMPORAL_TLS_SERVER_NAME MEMJEV_TEMPORAL_API_KEY MEMJEV_RETRIEVAL_QUERY_KEY_ID MEMJEV_RETRIEVAL_QUERY_KEY_BASE64 MEMJEV_API_IMAGE MEMJEV_WORKER_IMAGE MEMJEV_CADDY_IMAGE MEMJEV_PREVIOUS_API_IMAGE MEMJEV_PREVIOUS_WORKER_IMAGE MEMJEV_PREVIOUS_SCHEMA_MAX_VERSION)
for name in "${required[@]}"; do [[ -n "${!name:-}" ]] || { printf '%s is required\n' "$name" >&2; exit 2; }; done
digest_pattern='@sha256:[0-9a-f]{64}$'
for name in MEMJEV_API_IMAGE MEMJEV_WORKER_IMAGE MEMJEV_CADDY_IMAGE MEMJEV_PREVIOUS_API_IMAGE MEMJEV_PREVIOUS_WORKER_IMAGE; do
  [[ "${!name}" =~ $digest_pattern ]] || { printf '%s must use an immutable digest\n' "$name" >&2; exit 2; }
done
[[ "$MEMJEV_DOMAIN" =~ ^[A-Za-z0-9.-]+$ ]] || { echo "MEMJEV_DOMAIN is invalid" >&2; exit 2; }
[[ "$MEMJEV_OTLP_ENDPOINT" == https://* ]] || { echo "MEMJEV_OTLP_ENDPOINT must use HTTPS" >&2; exit 2; }
[[ "$MEMJEV_SURREAL_AUTH_SCOPE" == database || "$MEMJEV_SURREAL_AUTH_SCOPE" == namespace ]] || { echo "production SurrealDB auth must not be root scoped" >&2; exit 2; }
[[ "$MEMJEV_PREVIOUS_SCHEMA_MAX_VERSION" =~ ^[0-9]+$ && "$MEMJEV_PREVIOUS_SCHEMA_MAX_VERSION" -ge 14 ]] || { echo "rollback image must support schema version 14" >&2; exit 2; }
[[ "$MEMJEV_JEV_MODEL" =~ ^jev-[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "MEMJEV_JEV_MODEL must be pinned" >&2; exit 2; }
for secret_file in "$MEMJEV_CREDENTIALS_FILE_HOST" "$MEMJEV_JEV_API_KEY_HOST_FILE"; do
  [[ -f "$secret_file" ]] || { echo "required secret file is missing" >&2; exit 2; }
  mode=$(stat -f '%Lp' "$secret_file" 2>/dev/null || stat -c '%a' "$secret_file")
  [[ "$mode" == 600 || "$mode" == 400 ]] || { echo "secret file permissions must be 0600 or 0400" >&2; exit 2; }
done
echo "production preflight: PASS"
