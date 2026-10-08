# Compatibility

> **Status:** partial. Normative source: spec §15 (capability matrix) and §22 (error mapping). The
> HTTP mapping arrives with Phase 7; nothing here is reachable through a gateway yet.

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
