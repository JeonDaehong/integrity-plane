#!/usr/bin/env bash
# Runs compat/soak.py against Lakekeeper (atomic multi-table commits, full table history) with S3
# storage (SeaweedFS). Needs the host name s3 to resolve to 127.0.0.1 (http://s3:8333).
# (Nessie commits atomically too, but serves only the current snapshot: catching up cannot work.)
# Extra arguments go to soak.py, e.g. --duration 3600.
set -euo pipefail

prefix=$(bash compat/lakekeeper_setup.sh 2>/dev/null | tail -1)
s3='"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO", "s3.endpoint": "http://127.0.0.1:9000", "s3.access-key-id": "admin", "s3.secret-access-key": "password", "s3.region": "us-east-1", "s3.path-style-access": "true"'
exec "${PYTHON:-python}" compat/soak.py \
  --upstream http://127.0.0.1:8185/catalog \
  --prefix "$prefix" \
  --props "{\"warehouse\": \"compat\", $s3}" \
  --storage $'[storage]\nendpoint = "http://127.0.0.1:9000"\nregion = "us-east-1"\naccess_key_id = "admin"\nsecret_access_key = "password"\nallow_http = true' \
  "$@"
