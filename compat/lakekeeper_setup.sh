#!/usr/bin/env bash
# Starts Lakekeeper (compat/lakekeeper/docker-compose.yml), bootstraps it and creates the S3
# warehouse "compat". Prints the warehouse's catalog prefix (its id) on the last line of stdout.
set -euo pipefail

docker compose -f compat/lakekeeper/docker-compose.yml up -d >&2
for i in $(seq 1 90); do
  curl -sf http://127.0.0.1:8185/health >/dev/null && break
  sleep 2
done
curl -sS -X POST http://127.0.0.1:8185/management/v1/bootstrap \
  -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}' >&2 || true
echo >&2
curl -sS --fail-with-body -X POST http://127.0.0.1:8185/management/v1/warehouse \
  -H 'Content-Type: application/json' -d '{
  "warehouse-name": "compat",
  "storage-profile": {"type": "s3", "bucket": "warehouse", "key-prefix": "lakekeeper",
    "endpoint": "http://s3:8333", "region": "us-east-1", "path-style-access": true,
    "flavor": "s3-compat", "sts-enabled": false},
  "storage-credential": {"type": "s3", "credential-type": "access-key",
    "aws-access-key-id": "admin", "aws-secret-access-key": "password"}
}' >&2
echo >&2
curl -sS --fail-with-body "http://127.0.0.1:8185/catalog/v1/config?warehouse=compat" | tee /dev/stderr \
  | python -c 'import sys, json; c = json.load(sys.stdin); print(c["overrides"].get("prefix") or c["defaults"]["prefix"])'
