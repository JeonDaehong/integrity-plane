# Compatibility

> **Status:** partial. Normative source: spec §15 (capability matrix) and §22 (error mapping). The
> commit capability matrix and HTTP mapping arrive with Phases 6–7.

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
as NULL; tables using Iceberg v3 `initial-default` on constrained columns are not supported.

Verified against files written by pyarrow 25 and arrow-rs 60. Java (parquet-mr) writers are
verified in Phase 7.
