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
| Position deletes / deletion vectors | Rejected (0.2) | Rejected |
| Removing delete files; removing data files while delete files exist | Rejected in 0.1 | Rejected (ADR 0008) |
| Schema change touching a constrained field | Rejected unless `int→long`, decimal precision widening (and `float→double` for NOT NULL columns) | Phase 6 |
| Schema/property changes not touching constrained fields | Pass-through | Phase 6 |
| `remove-snapshots`, properties, sort order, partition spec | Pass-through | Phase 6 |
| Commits to refs other than `main` | Pass-through, uncertified | Phase 6 |
| Moving `main` to an existing snapshot | Rejected | Phase 6 |
| Multi-table commit endpoint | Rejected | Phase 7 (gateway) |
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

## HTTP status mapping (spec §22, RFC 0003)

| Situation | Codes | HTTP | Java (Spark) | PyIceberg | iceberg-rust |
|---|---|---|---|---|---|
| Constraint violated, unsupported, budget | INT-001, 003–008, 012, 014 | 400 | `BadRequestException`, fails fast | `BadRequestError` | error, fails fast |
| Stale base | INT-009 | 409 | `CommitFailedException`, bounded retry | `CommitFailedException` | retried |
| Transient Plane state | INT-011, INT-016 | 409 | as above | as above | as above |
| Operator action needed | INT-010, INT-015 | 423 | `RESTException`, fails fast | `RESTError` | error |
| Upstream response after forwarding | — | unchanged | — | — | — |

The gateway never answers a commit it did not forward with 5xx. `/v1/config` is forwarded without `uri`
overrides or idempotency-key support.

## Verified clients (Phase 7, `compat/`, CI workflow "Compatibility")

| Client | Version | Scenario |
|---|---|---|
| PyIceberg | 0.12.0 | append, FK violation (INT-005), referenced parent delete via COW delete (INT-006), PK duplicate (INT-003), NOT NULL (INT-007), child-then-parent delete |
| Spark (Iceberg Java REST client) | Spark 3.5.6, Iceberg 1.10.0 | same, with SQL `INSERT` / `DELETE` (copy-on-write) |
| iceberg-rust | 0.10.1 | fast append, INT-005, INT-003, INT-007 |
| Spark + `integrity` CLI (Appendix A demo, `compat/demo_test.py`) | as above | constraints registered via CLI, INT-005, INT-006, compaction (`rewrite_data_files`) certified, restart after kill -9, a write straight to the upstream catalog pinpointed by `verify`, INT-010 until `rebuild` |

For every client each violation reaches the gateway exactly once (no retry storm), the statement
fails with the integrity code in its message, and every snapshot on `main` carries a certificate.
Upstream: Iceberg REST fixture 1.10.1 with a filesystem warehouse. The CI job `compose-s3` also runs
the Appendix A demo against `deploy/docker-compose.yml`, where the catalog, the Plane and the
clients use S3 (SeaweedFS's S3 gateway; MinIO no longer publishes container images).
