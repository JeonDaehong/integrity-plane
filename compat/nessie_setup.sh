#!/usr/bin/env bash
# Starts Nessie (compat/nessie/docker-compose.yml) and prints its Iceberg REST catalog prefix for
# the default warehouse on the last line of stdout.
set -euo pipefail

docker compose -f compat/nessie/docker-compose.yml up -d >&2
for i in $(seq 1 90); do
  curl -sf http://127.0.0.1:19120/iceberg/v1/config >/dev/null && break
  sleep 2
done
curl -sS --fail-with-body "http://127.0.0.1:19120/iceberg/v1/config?warehouse=warehouse" | tee /dev/stderr \
  | python -c 'import sys, json; c = json.load(sys.stdin); print(c.get("overrides", {}).get("prefix") or c["defaults"]["prefix"])'
