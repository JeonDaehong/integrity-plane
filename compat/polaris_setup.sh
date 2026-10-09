#!/usr/bin/env bash
# Starts Apache Polaris with a filesystem catalog "compat" at /tmp/polaris and grants the root
# principal full access to it (compatibility tests; credentials are test values).
set -euo pipefail

mkdir -p /tmp/polaris && chmod 777 /tmp/polaris
docker run -d --name polaris --user root -p 8183:8181 -p 8184:8182 \
  -v /tmp/polaris:/tmp/polaris \
  -e POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t \
  -e polaris.realm-context.realms=POLARIS \
  -e quarkus.otel.sdk.disabled=true \
  -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
  -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE"]' \
  -e polaris.readiness.ignore-severe-issues=true \
  apache/polaris:1.7.0

for i in $(seq 1 90); do
  curl -sf http://127.0.0.1:8184/q/health/ready >/dev/null && break
  sleep 2
done

api=http://127.0.0.1:8183/api
token=$(curl -sf "$api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | python -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
call() {
  curl -sf -X "$1" "$api/management/v1/$2" -H "Authorization: Bearer $token" \
    -H 'Content-Type: application/json' -d "$3"
  echo
}
call POST catalogs '{"catalog": {"name": "compat", "type": "INTERNAL", "readOnly": false,
  "properties": {"default-base-location": "file:///tmp/polaris"},
  "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///tmp/polaris"]}}}'
call PUT catalogs/compat/catalog-roles/catalog_admin/grants \
  '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
call PUT principal-roles/service_admin/catalog-roles/compat '{"catalogRole": {"name": "catalog_admin"}}'
echo "polaris ready"
