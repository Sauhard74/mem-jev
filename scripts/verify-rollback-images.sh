#!/usr/bin/env bash
set -euo pipefail

required_schema=14
for image in "${MEMJEV_PREVIOUS_API_IMAGE:?}" "${MEMJEV_PREVIOUS_WORKER_IMAGE:?}"; do
  docker pull "$image" >/dev/null
  supported=$(docker image inspect --format '{{ index .Config.Labels "org.memjev.schema.max-version" }}' "$image")
  [[ "$supported" =~ ^[0-9]+$ && "$supported" -ge "$required_schema" ]] || { echo "rollback image is not compatible with schema version $required_schema" >&2; exit 2; }
done
