#!/usr/bin/env bash
set -euo pipefail

required=(MEMJEV_DOMAIN MEMJEV_OTLP_ENDPOINT MEMJEV_BACKUP_AGE_RECIPIENT MEMJEV_CREDENTIALS_FILE_HOST MEMJEV_API_IMAGE MEMJEV_WORKER_IMAGE MEMJEV_CADDY_IMAGE MEMJEV_PREVIOUS_API_IMAGE MEMJEV_PREVIOUS_WORKER_IMAGE)
for name in "${required[@]}"; do [[ -n "${!name:-}" ]] || { printf '%s is required\n' "$name" >&2; exit 2; }; done
digest_pattern='@sha256:[0-9a-f]{64}$'
for name in MEMJEV_API_IMAGE MEMJEV_WORKER_IMAGE MEMJEV_CADDY_IMAGE MEMJEV_PREVIOUS_API_IMAGE MEMJEV_PREVIOUS_WORKER_IMAGE; do
  [[ "${!name}" =~ $digest_pattern ]] || { printf '%s must use an immutable digest\n' "$name" >&2; exit 2; }
done
[[ "$MEMJEV_DOMAIN" =~ ^[A-Za-z0-9.-]+$ ]] || { echo "MEMJEV_DOMAIN is invalid" >&2; exit 2; }
[[ "$MEMJEV_OTLP_ENDPOINT" == https://* ]] || { echo "MEMJEV_OTLP_ENDPOINT must use HTTPS" >&2; exit 2; }
[[ -f "$MEMJEV_CREDENTIALS_FILE_HOST" ]] || { echo "credential file is missing" >&2; exit 2; }
mode=$(stat -f '%Lp' "$MEMJEV_CREDENTIALS_FILE_HOST" 2>/dev/null || stat -c '%a' "$MEMJEV_CREDENTIALS_FILE_HOST")
[[ "$mode" == 600 || "$mode" == 400 ]] || { echo "credential file permissions must be 0600 or 0400" >&2; exit 2; }
if [[ "${MEMJEV_JEV_ENABLED:-false}" == true ]]; then
  [[ -f "${MEMJEV_JEV_API_KEY_HOST_FILE:-}" ]] || { echo "Jev key file is missing" >&2; exit 2; }
fi
echo "production preflight: PASS"
