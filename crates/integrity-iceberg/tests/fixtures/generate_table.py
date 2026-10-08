"""Generates a real Iceberg table (metadata, manifest lists, manifests, Parquet data) with PyIceberg.

The table `orders(id long required, customer_id int, region string, amount decimal(10,2))` goes
through one commit per operation the capability matrix (spec §15) distinguishes. Every metadata
version is kept, so tests can replay each commit as "parent metadata + new snapshot".

Usage:  python generate_table.py      (requires pyiceberg[sql-sqlite] and pyarrow)
Output: ./table/warehouse/...  and  ./table/commits.json  (the commit sequence, see below).
The table is built under a neutral absolute root (`/tmp/oip-fixture`, i.e. the current drive's
root on Windows) so that no machine-specific path ends up in the files, then copied here. Tests map
every location after `/warehouse/` onto ./table/warehouse/.
"""

import decimal
import json
import pathlib
import shutil
import tempfile

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DecimalType, IntegerType, LongType, NestedField, StringType

OUT = pathlib.Path(__file__).parent / "table"
BUILD = "/tmp/oip-fixture"

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "customer_id", IntegerType(), required=False),
    NestedField(3, "region", StringType(), required=False),
    NestedField(4, "amount", DecimalType(10, 2), required=False),
)

ARROW = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("customer_id", pa.int32()),
    pa.field("region", pa.string()),
    pa.field("amount", pa.decimal128(10, 2)),
])


def rows(*tuples):
    cols = list(zip(*tuples))
    return pa.Table.from_arrays(
        [pa.array(c, f.type) for c, f in zip(cols, ARROW)], schema=ARROW
    )


def d(x):
    return decimal.Decimal(x)


def main():
    build = pathlib.Path(BUILD)
    for d_ in (OUT, build):
        if d_.exists():
            shutil.rmtree(d_)
    (build / "warehouse").mkdir(parents=True)
    catalog_dir = tempfile.mkdtemp()
    catalog = SqlCatalog(
        "fixtures",
        uri=f"sqlite:///{pathlib.Path(catalog_dir) / 'catalog.db'}",
        warehouse=f"{BUILD}/warehouse",
    )
    catalog.create_namespace("db")
    table = catalog.create_table("db.orders", schema=SCHEMA)

    commits = []

    def record(name, branch="main"):
        table.refresh()
        meta = pathlib.Path(table.metadata_location.replace("file:///", "").replace("file:", ""))
        commits.append({"name": name, "branch": branch, "metadata": meta.name})

    record("create")
    table.append(rows((1, 7, "eu", d("1.00")), (2, 8, "us", d("2.00")), (3, 7, None, d("3.00"))))
    record("append_a")
    table.append(rows((4, 9, "eu", d("4.00")), (5, 8, "us", d("5.00"))))
    record("append_b")
    table.delete("id == 5")  # part of file B: copy-on-write rewrite
    record("delete_rows_cow")
    table.delete("id <= 3")  # all of file A: whole-file delete
    record("delete_whole_file")
    table.overwrite(rows((10, 1, "eu", d("10.00")), (11, None, "us", d("11.00"))))
    record("overwrite_all")
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "ingest").commit()
    table.append(rows((12, 2, "eu", d("12.00"))), branch="ingest")
    record("append_to_branch", branch="ingest")

    catalog.engine.dispose()
    shutil.rmtree(catalog_dir, ignore_errors=True)
    OUT.mkdir(parents=True)
    shutil.copytree(build / "warehouse", OUT / "warehouse")
    shutil.rmtree(build)
    (OUT / "commits.json").write_text(json.dumps(commits, indent=2) + "\n")


if __name__ == "__main__":
    main()
