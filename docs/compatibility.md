# Compatibility

> **Status:** partial. Normative source: spec §15 (capability matrix) and §22 (error mapping, as
> amended by RFC 0003).

## Commit capability matrix (spec §15)

| Change | 0.1 behavior | Implemented |
|---|---|---|
| `append` (data files only) | Supported | Phase 6 (diff + extraction) |
| `overwrite`, copy-on-write | Supported; net delta = added − removed rows | Phase 6 |
| `replace` (compaction, manifest rewrite) | Supported only if projected rows are unchanged | Phase 6 |
| `delete` dropping whole files | Supported | Phase 6 |
| Several new snapshots on `main` in one commit | Supported; each validated in order | Phase 6 (classification, diff) |
| Equality deletes on exactly a PK/UNIQUE key | Supported when no other constraint needs the deleted rows (ADR 0009) | Phase 6 |
| Equality deletes on other fields, with explicit sequence numbers, or mixed with data-file removal | Rejected | Phase 6 |
| Position deletes (merge-on-read `DELETE` / `UPDATE` / `MERGE`) | Supported; rows removed = live rows the deletes newly hide (ADR 0017) | Yes |
| Compaction of a merge-on-read table (applying, rewriting or dropping position delete files) | Supported if live rows are unchanged | Yes (ADR 0017) |
| Deletion vectors (v3, Puffin) | Supported, as position deletes (ADR 0018) | Yes |
| Position and equality deletes in one table (Flink upsert repeating a key in one checkpoint) | Supported when the commit changes the deleted positions of its own new data files only | Yes (ADR 0028) |
| Position deletes changing rows of existing data files in a table with equality deletes; added data files with explicit sequence numbers there | Rejected | Rejected (ADR 0028) |
| A position delete file from an earlier snapshot naming a data file the commit adds; a file whose data sequence number changes | Rejected | Rejected (ADR 0028) |
| Removing equality delete files; removing data files while equality delete files exist | Rejected | Rejected (ADR 0008) |
| Schema change touching a constrained field | Rejected unless `int→long`, decimal precision widening (and `float→double` for NOT NULL columns) | Phase 6 |
| Schema/property changes not touching constrained fields | Pass-through | Phase 6 |
| `remove-snapshots`, properties, sort order, partition spec | Pass-through | Phase 6 |
| Commits to refs other than `main` | Pass-through, uncertified | Phase 6 |
| Moving `main` to an existing snapshot | Rejected | Phase 6 |
| Multi-table commit endpoint (from clients) | Rejected | Phase 7 (gateway) |
| Multi-table commits to the upstream catalog (`[ha] fence`, RFC 0012) | Used | see "Catalogs for several Planes" below |
| Unknown update action or requirement type | Rejected, naming it | Phase 6 |
| Non-Parquet data files | Rejected | Phase 6 |

Requirements in the request are checked against the metadata loaded under the domain queue; a
failed requirement is `STALE_BASE_SNAPSHOT`.

## Parquet data files (Phase 5)

What key extraction accepts, per table column type (ADR 0006):

| Table type | Accepted Parquet / Arrow representation |
|---|---|
| `boolean` | BOOLEAN |
| `int`, `long` | INT32, INT64 (`int` data under a `long` column is fine) |
| `decimal(p, s)` | INT32, INT64 or FIXED_LEN_BYTE_ARRAY decimal with the **same scale** `s` |
| `date` | DATE (INT32) |
| `timestamp`, `timestamp_ns` | TIMESTAMP(µs or ns, not UTC-adjusted) |
| `timestamptz`, `timestamptz_ns` | TIMESTAMP(µs or ns, UTC-adjusted) |
| `string` | BYTE_ARRAY (UTF-8) |
| `binary`, `fixed(L)` | BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY |
| `uuid` | FIXED_LEN_BYTE_ARRAY(16) |
| non-key types (NOT NULL only) | anything; only NULL-ness is read |

Compression: uncompressed, zstd, snappy, lz4, gzip. Rejected: files without field IDs, duplicate
field IDs, key fields nested in structs, any other type combination. Columns missing from a file read
as NULL; tables using Iceberg v3 `initial-default` on constrained columns are rejected at
classification.

Verified against files written by pyarrow 25, PyIceberg 0.12 and arrow-rs 60. Java (parquet-mr)
writers are verified in Phase 7.

## HTTP status mapping (spec §22, RFC 0003, RFC 0007)

| Situation | Codes | HTTP | Java (Spark) | PyIceberg | iceberg-rust |
|---|---|---|---|---|---|
| Constraint violated, unsupported, budget | INT-001, 003–008, 012, 014 | 400 | `BadRequestException`, fails fast | `BadRequestError` | error, fails fast |
| Stale base | INT-009 | 409 | `CommitFailedException`, bounded retry | `CommitFailedException` | retried |
| Transient Plane state | INT-011, INT-016, INT-018 | 409 | as above | as above | as above |
| Operator action needed | INT-010, INT-015 | 423 | `RESTException`, fails fast | `RESTError` | error |
| Request limit reached | INT-017 | 429 + `Retry-After` | retried with backoff, then `RESTException` | `RESTError` | not verified |
| Upstream response after forwarding | — | unchanged | — | — | — |

The message of a rejected commit reads `INT-005 FOREIGN_KEY_VIOLATION: commit rejected: INT-005 on
fk_orders_customer (11 keys); details: GET /v1/integrity/transactions/17`; the structured error with
sample keys is served there (ADR 0016). The gateway never answers a commit it did not forward with 5xx. `/v1/config` is forwarded without `uri`
overrides or idempotency-key support.

## Verified clients (Phase 7, `compat/`, CI workflow "Compatibility")

| Client | Version | Scenario |
|---|---|---|
| PyIceberg | 0.12.0 | append, FK violation (INT-005), referenced parent delete via COW delete (INT-006), PK duplicate (INT-003), NOT NULL (INT-007), child-then-parent delete |
| Spark (Iceberg Java REST client) | Spark 3.5.6, Iceberg 1.10.0 | same, with SQL `INSERT` / `DELETE` (copy-on-write); composite PRIMARY KEY and FOREIGN KEY on string columns (INT-003, INT-007, INT-005, MATCH SIMPLE NULL exemption, INT-006) |
| iceberg-rust | 0.10.1 | fast append, INT-005, INT-003, INT-007 |
| Spark + `integrity` CLI (Appendix A demo, `compat/demo_test.py`) | as above | constraints registered via CLI, INT-005, INT-006, compaction (`rewrite_data_files`) certified, restart after kill -9, a write straight to the upstream catalog pinpointed by `verify`, INT-010 until `rebuild` |

### Apache Polaris (CI job `polaris`)

Polaris 1.7.0 with a filesystem catalog (`compat/polaris_setup.sh`): clients authenticate with
OAuth2 client credentials through the gateway (the token endpoint is proxied), the catalog prefix
comes from `/v1/config?warehouse=…`, and the Plane uses its own client credentials
(`[upstream.auth]`) to read tables for validation, onboarding and `verify`. PyIceberg and Spark run
the same scenarios as above, then `integrity verify` succeeds on their tables.

### Lakekeeper and Nessie (CI jobs `lakekeeper`, `nessie`)

Lakekeeper 0.13.6 (Postgres, S3 warehouse; the catalog prefix is the warehouse id) and Nessie
0.108.8 (Iceberg REST endpoint, in-memory version store, S3 warehouse; prefix `main|warehouse`): Spark
runs the full scenario through the gateway, merge-on-read included, and format v3 on Lakekeeper
(Nessie 0.108 creates v2 tables when v3 is requested). Nessie serves only
the current snapshot of a table, without its parent, so `verify` cannot recompute certificates there
and reports them `UNVERIFIABLE` (never a break); commit-time validation and bypass detection are
unaffected.

For every client each violation reaches the gateway exactly once (no retry storm), the statement
fails with the integrity code in its message, and every snapshot on `main` carries a certificate.
Upstream: Iceberg REST fixture 1.10.1 with a filesystem warehouse. The CI job `compose-s3` also runs
the Appendix A demo against `deploy/docker-compose.yml`, where the catalog, the Plane and the
clients use S3 (SeaweedFS's S3 gateway; MinIO no longer publishes container images).

### Trino and Flink (checked by hand, not in CI)

Run once on 2026-10-21 against `deploy/docker-compose.yml` (S3, REST fixture 1.10.1, the Plane at
`da2c2f8`); not part of CI yet (#9).

| Client | Version | Result |
|---|---|---|
| Trino (Iceberg connector, REST catalog, native S3) | 470 | All 20 scenarios as expected: `INSERT`, merge-on-read `UPDATE` / `DELETE` / `MERGE`, `optimize`, `expire_snapshots`; INT-003 (within a statement, against the table, `UPDATE` of a key, `MERGE`, `INSERT … SELECT`), INT-007, INT-005, INT-006; one commit attempt per violation; `verify` OK |
| Flink SQL (batch, `iceberg-flink-runtime-1.20`) | Flink 1.20.3, Iceberg 1.10.0 | `INSERT`, `INSERT OVERWRITE`, upsert tables (equality deletes on the key) accepted; INT-003, INT-007, INT-005 rejected after one attempt. **An upsert batch that writes the same key twice is refused** (INT-012: Flink writes position and equality deletes in one commit, see the capability matrix), and `verify` reports snapshots with equality deletes `UNVERIFIABLE` |

Both engines report a rejection as `BadRequestException: Malformed request: INT-… ` (their wording
for HTTP 400); the integrity code and message follow. Trino commits table statistics in a second
commit after each write, which the Plane forwards.

Flink upsert / CDC jobs that update a key more than once between checkpoints therefore cannot write
through the Plane yet.

## File locations (RFC 0013)

Every file a commit names (manifest list, manifests, data, delete and deletion-vector files) must
lie under the table's `location`, a `write.data.path`, `write.metadata.path`,
`write.object-storage.path` or `write.folder-storage.path` property, or a
`[storage] allowed_locations` entry; otherwise the commit is refused with INT-012 (400) and nothing
is read. `s3a://` counts as `s3://`; `file:///p`, `file:/p` and `/p` are the same; paths with `.`
or `..` segments are refused.

## Catalogs for several Planes (RFC 0012)

Several Planes in front of the same tables (`[ha] fence = "multi-table"`) need an upstream catalog
that applies multi-table commits atomically **under concurrency** (a requirement on a table without
updates is checked when the transaction commits, not before) and serves each table's snapshot
history (catching up, RFC 0011). `compat/multitable_probe.py` checks the first, sequentially and
with 30 rounds of two concurrent transactions that assert each other's table; `compat/soak.py`
runs Planes with chaos against a catalog for an hour. Results of 2026-10-17:

| Catalog | Sequential | Concurrent transactions | Table history | Several Planes |
|---|---|---|---|---|
| Lakekeeper 0.13.6 | ✅ | ✅ never both committed | ✅ | **supported** (soak: `compat/soak_lakekeeper.sh`) |
| Nessie 0.108.8 | ✅ | ✅ | ❌ current snapshot only | no: a Plane cannot catch up |
| Apache Polaris 1.7.0 | ✅ | ❌ both committed in 29 of 30 rounds | ✅ | no |
| Iceberg REST fixture 1.10.1 | ✅ | ❌ both committed in 30 of 30 rounds; acknowledged changes lost | ✅ | no |

Reported upstream: [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552),
[apache/polaris#5807](https://github.com/apache/polaris/issues/5807).

With the other catalogs, run one Plane per domain (the default `fence = "none"`). The Plane
answers 409 when a catalog acknowledges a transaction whose snapshot is not on `main`.
