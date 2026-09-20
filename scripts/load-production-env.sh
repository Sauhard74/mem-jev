#!/usr/bin/env bash
set -euo pipefail

load_production_env() {
  local file=$1 line name value
  [[ -f "$file" ]] || { echo "production environment file is missing" >&2; return 2; }
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" =~ ^([A-Z][A-Z0-9_]*)=(.*)$ ]] || { echo "invalid production environment entry" >&2; return 2; }
    name=${BASH_REMATCH[1]}; value=${BASH_REMATCH[2]}
    if [[ "$value" =~ ^\"(.*)\"$ || "$value" =~ ^\'(.*)\'$ ]]; then value=${BASH_REMATCH[1]}; fi
    [[ "$value" != *'$('* && "$value" != *'`'* && "$value" != *'${'* ]] || { echo "production environment expansion is forbidden" >&2; return 2; }
    printf -v "$name" '%s' "$value"
    export "$name"
  done < "$file"
}
