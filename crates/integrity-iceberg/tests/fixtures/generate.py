"""Generates the Parquet fixtures for integrity-iceberg's key extraction tests.

Written with pyarrow (the Parquet C++ implementation, as used by PyIceberg) rather than the
Rust writer under test, so that the tests read files produced by an independent implementation.
Columns carry Iceberg field IDs (`PARQUET:field_id`) exactly like Iceberg data files.

Usage:  python generate.py          (requires pyarrow; output is deterministic)
Expected contents are spelled out in ../extract.rs; keep both in sync.
"""

import datetime as dt
import decimal
import pathlib
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

OUT = pathlib.Path(__file__).parent


def field(name, typ, fid, nullable=True):
    return pa.field(name, typ, nullable=nullable, metadata={b"PARQUET:field_id": str(fid).encode()})


def write(name, schema, columns, **kwargs):
    table = pa.Table.from_arrays(columns, schema=schema)
    pq.write_table(
        table,
        OUT / name,
        row_group_size=2,  # several row groups per file
        compression="zstd",  # Iceberg's default codec
        **kwargs,
    )


U1 = uuid.UUID("00000000-0000-0000-0000-000000000001").bytes
U2 = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff").bytes
EPOCH = dt.datetime(1970, 1, 1)


def basic():
    """All key families, NULLs in every nullable column, edge values, 5 rows."""
    schema = pa.schema([
        field("id", pa.int64(), 1, nullable=False),
        field("customer_id", pa.int32(), 2),  # table type widened to long
        field("region", pa.string(), 3),
        field("amount", pa.decimal128(10, 2), 4),
        field("day", pa.date32(), 5),
        field("ts", pa.timestamp("us"), 6),
        field("ts_tz", pa.timestamp("us", tz="UTC"), 7),
        field("uid", pa.binary(16), 8),
        field("payload", pa.binary(), 9),
        field("flag", pa.bool_(), 10),
        field("score", pa.float64(), 11),
        field("ts_ns", pa.timestamp("ns"), 12),
    ])
    cols = [
        pa.array([1, -1, 0, 2**63 - 1, -(2**63)], pa.int64()),
        pa.array([7, None, -2147483648, 2147483647, 7], pa.int32()),
        pa.array(["eu", None, "", "a\x00b", "한글"], pa.string()),
        pa.array([decimal.Decimal("1.00"), decimal.Decimal("-0.01"), None,
                  decimal.Decimal("99999999.99"), decimal.Decimal("0.00")], pa.decimal128(10, 2)),
        pa.array([dt.date(1970, 1, 1), dt.date(1969, 12, 31), None, dt.date(2026, 10, 9),
                  dt.date(1, 1, 1)], pa.date32()),
        pa.array([EPOCH + dt.timedelta(microseconds=1), None, EPOCH - dt.timedelta(microseconds=1),
                  dt.datetime(2026, 10, 9, 12, 0, 0), EPOCH], pa.timestamp("us")),
        pa.array([dt.datetime(2026, 10, 9, tzinfo=dt.timezone.utc), None, None,
                  dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
                  dt.datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=dt.timezone.utc)],
                 pa.timestamp("us", tz="UTC")),
        pa.array([U1, U2, None, U1, U2], pa.binary(16)),
        pa.array([b"", b"\x00", None, b"\xff\x00\xff", b"abc"], pa.binary()),
        pa.array([True, False, None, True, False], pa.bool_()),
        pa.array([1.5, None, float("nan"), -0.0, 2.0], pa.float64()),
        pa.array([1000, 1, None, -1, 0], pa.timestamp("ns")),
    ]
    write("keys_basic.parquet", schema, cols)


def decimals():
    """Decimals in every Parquet physical encoding Iceberg uses: INT32, INT64, FIXED_LEN_BYTE_ARRAY."""
    schema = pa.schema([
        field("d9", pa.decimal128(9, 2), 1),
        field("d18", pa.decimal128(18, 4), 2),
        field("d38", pa.decimal128(38, 6), 3),
    ])
    cols = [
        pa.array([decimal.Decimal("1234567.89"), decimal.Decimal("-0.01"), None], pa.decimal128(9, 2)),
        pa.array([decimal.Decimal("99999999999999.9999"), None, decimal.Decimal("-1.0000")],
                 pa.decimal128(18, 4)),
        pa.array([None, decimal.Decimal("-99999999999999999999999999999999.999999"),
                  decimal.Decimal("0.000001")], pa.decimal128(38, 6)),
    ]
    write("keys_decimals.parquet", schema, cols, store_decimal_as_integer=True)


def no_field_ids():
    """A file without field IDs (e.g. from a migrated, non-Iceberg writer)."""
    table = pa.table({"id": pa.array([1, 2], pa.int64())})
    pq.write_table(table, OUT / "no_field_ids.parquet")


def nested():
    """A key-like field inside a struct (field 3 inside field 1)."""
    inner = pa.struct([field("k", pa.int64(), 3)])
    schema = pa.schema([field("s", inner, 1), field("id", pa.int64(), 2)])
    cols = [
        pa.array([{"k": 1}, {"k": 2}], inner),
        pa.array([10, 20], pa.int64()),
    ]
    write("nested.parquet", schema, cols)


if __name__ == "__main__":
    basic()
    decimals()
    no_field_ids()
    nested()
