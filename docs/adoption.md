# Putting the Plane in front of a catalog

A step-by-step guide for a first deployment. The Plane is pre-alpha: try it on a copy of your
tables or on new tables first, and read [`limitations.md`](limitations.md).

## 1. Decide which tables need it

The Plane pays off for tables that several jobs or engines write, that reference each other, or
whose bad commits are expensive; it costs latency on large commits (README, "When to use it").
Tables without constraints pass through the Plane unchanged, so it can sit in front of a whole
catalog while only a few tables are enforced.

## 2. Run it

Container image (linux/amd64 and linux/arm64):

```sh
docker run -d --name integrity-plane -p 8181:8181 \
  -v /srv/integrity/integrity.toml:/etc/integrity/integrity.toml:ro \
  -v integrity-control:/var/lib/integrity \
  ghcr.io/jeondaehong/integrity-plane:<version>
```

Or a prebuilt binary from the GitHub release: `integrity-server integrity.toml`.

Start from [`deploy/integrity.example.toml`](../deploy/integrity.example.toml). The settings that
matter on day one:

| Setting | Why |
|---|---|
| `[upstream] catalog_uri` | the catalog the Plane forwards to |
| `[upstream.auth]` | the Plane's own credentials for reading metadata, onboarding and `verify`; writers' commits are forwarded with the writers' credentials |
| `[storage]` | how the Plane reads manifests and data files (S3 endpoint, region, credentials) |
| `[control_store] path` | a local SSD; this is the Plane's state (indexes, transaction log, registry) |
| `[server] admin_token` or `[[server.api_tokens]]` | protects the integrity API (constraints, rebuild, audit) |
| `[signing]` | optional; certificates signed with a key file, Vault Transit or an HSM (RFC 0005, RFC 0009) |

The control store must survive restarts (a volume or a host directory), and only one Plane may use
it ([`recovery.md`](recovery.md#single-node-and-power-loss)). Back it up only while the Plane is
stopped, and rebuild every domain after restoring a copy.

## 3. Point the writers at the Plane

Writers use the Plane as their Iceberg REST catalog; nothing else changes (same warehouse, same
object storage, same credentials).

Spark:

```properties
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=rest
spark.sql.catalog.lake.uri=http://integrity-plane:8181
# plus whatever the catalog needed before: warehouse, credential, token, io-impl, s3.endpoint, ...
```

PyIceberg:

```python
catalog = load_catalog("lake", type="rest", uri="http://integrity-plane:8181", **previous_settings)
```

iceberg-rust: `RestCatalogConfig` with the Plane's URI. Verified clients and catalogs are in
[`compatibility.md`](compatibility.md); other REST clients should work if they speak the Iceberg
REST protocol, but are untested.

Readers may keep using the upstream catalog directly; only writers must go through the Plane.

## 4. Close the way around it

The Plane forwards each writer's own credentials, so the catalog cannot tell a writer coming
through the Plane from one that is not. **The primary control is the network**: writers must not
reach the upstream catalog's endpoint, only the Plane may.

Kubernetes, with the catalog in namespace `catalog` and the Plane's pods labelled
`app: integrity-plane`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: catalog-only-from-the-plane
  namespace: catalog
spec:
  podSelector:
    matchLabels:
      app: iceberg-catalog
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {}
          podSelector:
            matchLabels:
              app: integrity-plane
      ports:
        - port: 8181
```

On AWS, the same with security groups: the catalog's group allows its port only from the Plane's
group. Readers that should use the catalog directly get their own rule, ideally with credentials
that cannot commit.

What is left if a writer still gets around the Plane: the next commit through it fails with
`BYPASS_DETECTED` and the domain stops taking commits until rebuilt (RFC 0008, RFC 0011), and
`integrity verify` names the snapshot. Watch `integrity_bypass_detected_total`.

## 5. Register constraints

```sh
integrity --url http://integrity-plane:8181 --token "$TOKEN" \
  constraints add --table db.customer --name pk_customer --type primary_key --columns customer_id
integrity ... constraints add --table db.orders --name fk_orders_customer --type foreign_key \
  --columns customer_id --ref-table db.customer --ref-constraint pk_customer
```

Registering scans the table's existing data (onboarding, about 700 000 keys/s on the benchmark
machine) and refuses the constraint, with a report, if the data already violates it. Commits to
the domain wait while the scan runs, so register large tables at a quiet time. An initial bulk
load is cheaper before the constraints exist than through them (`benchmarks.md`).

## 6. Operate it

- **Metrics** (`/metrics`, Prometheus): alert on `integrity_bypass_detected_total`,
  `integrity_signer_failures_total`, `integrity_domain_state{state="degraded"}`, and watch
  `integrity_validation_seconds` and `integrity_domain_queue_wait_seconds`.
- **Verify** each enforced table on a schedule: `integrity verify db.customer` (exit code 1 on a
  broken chain).
- **When the Plane is down**, writers cannot commit to enforced tables (fail closed). Restart it on
  the same control store; to move hosts, stop it and copy the whole control store. A copy taken
  earlier also works: the Plane catches up from the table history (RFC 0011).
- **Degraded domains** (`INDEX_DEGRADED`, `BYPASS_DETECTED`): find the cause in the audit log
  (`integrity audit`), then `integrity rebuild <constraint id>`.
- **Upgrades**: read the CHANGELOG. 0.1.0 needs one rebuild per domain after upgrading from 0.0.2
  (RFC 0008).

## 7. Errors writers will see

| Code | HTTP | Meaning | What the writer does |
|---|---|---|---|
| INT-003…INT-007 | 400 | a constraint is violated; details at `/v1/integrity/transactions/{id}` | fix the data |
| INT-009, INT-011, INT-016, INT-018 | 409 | stale base or a transient Plane state | engines retry automatically |
| INT-017 | 429 | too many commits waiting | retried with backoff |
| INT-010, INT-015 | 423 | the domain needs an operator | wait for a rebuild |

The full table is in [`compatibility.md`](compatibility.md).
