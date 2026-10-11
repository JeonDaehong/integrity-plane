"""What the Plane costs a real engine: the same Spark jobs against the upstream catalog directly and
through the gateway, on the same storage.

Two Spark catalogs share one upstream REST catalog: `direct` talks to it, `plane` goes through the
gateway. Each scenario runs on both (alternating which goes first) on tables of the same shape:
customer(id PK) and events(id PK, cust FK -> customer, payload). Prints one line per scenario:

    OIP-SPARK <scenario> | direct <s> | plane <s> | overhead <%> | validation <s>

Usage: python spark_bench.py [--events N] [--customers N] [--rounds N] [--run SUFFIX]
Environment: GATEWAY (default http://127.0.0.1:8181), UPSTREAM (default http://127.0.0.1:8182),
CATALOG_PROPS (JSON, extra properties for both catalogs, e.g. S3 settings), ICEBERG_VERSION.
"""

import argparse
import json
import os
import statistics
import sys
import time
import urllib.request

from pyspark.sql import SparkSession

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8181")
UPSTREAM = os.environ.get("UPSTREAM", "http://127.0.0.1:8182")
ICEBERG = os.environ.get("ICEBERG_VERSION", "1.10.0")
EXTRA = json.loads(os.environ.get("CATALOG_PROPS", "{}"))
# Validating a large commit takes longer than the REST client's default socket timeout.
TIMEOUTS = {"rest.client.socket-timeout-ms": "3600000", "rest.client.connection-timeout-ms": "60000"}


def say(line):
    print(line, flush=True)


def metric(name):
    with urllib.request.urlopen(f"{GATEWAY}/metrics") as r:
        for line in r.read().decode().splitlines():
            if line.startswith(name + " "):
                return float(line.split()[-1])
    return 0.0


def register(body):
    req = urllib.request.Request(
        f"{GATEWAY}/v1/integrity/constraints",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        assert r.status in (200, 201), r.status


def session():
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}",
    ]
    b = (
        SparkSession.builder.appName("integrity-spark-bench")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "32")
    )
    for name, uri in [("direct", UPSTREAM), ("plane", GATEWAY)]:
        b = b.config(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
        b = b.config(f"spark.sql.catalog.{name}.type", "rest")
        b = b.config(f"spark.sql.catalog.{name}.uri", uri)
        for k, v in {**TIMEOUTS, **EXTRA}.items():
            b = b.config(f"spark.sql.catalog.{name}.{k}", v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


class Bench:
    def __init__(self, spark, events, customers, run):
        self.spark = spark
        self.events = events
        self.customers = customers
        # Each catalog's own namespace; the Plane enforces constraints on `p` only.
        self.ns = {"direct": f"direct.d{run}", "plane": f"plane.p{run}"}
        self.p = f"p{run}"

    def sql(self, side, text):
        """`text` with {ns} (catalog.namespace), {cat} and {db} filled in for `side`."""
        cat, db = self.ns[side].split(".")
        return self.spark.sql(text.format(ns=self.ns[side], cat=cat, db=db)).collect()

    def timed(self, side, statements):
        before = metric("integrity_validation_seconds_sum")
        t = time.perf_counter()
        for s in statements:
            self.sql(side, s)
        took = time.perf_counter() - t
        validation = metric("integrity_validation_seconds_sum") - before
        return took, validation

    def compare(self, name, statements, first="direct", rounds=1, per_round=None):
        """Runs `statements` (or `per_round(i)`) on both sides, `rounds` times, alternating."""
        times = {"direct": [], "plane": []}
        validation = []
        order = [first, "plane" if first == "direct" else "direct"]
        for i in range(rounds):
            for side in order if i % 2 == 0 else order[::-1]:
                stmts = per_round(i) if per_round else statements
                try:
                    took, v = self.timed(side, stmts)
                except Exception as e:  # noqa: BLE001 - reported, the run continues
                    say(f"OIP-SPARK {name} | {side} FAILED: {str(e)[:400]}")
                    return
                times[side].append(took)
                if side == "plane":
                    validation.append(v)
        d, p = statistics.median(times["direct"]), statistics.median(times["plane"])
        say(
            f"OIP-SPARK {name} | direct {d:.2f} s | plane {p:.2f} s | "
            f"overhead {100 * (p - d) / d:+.0f}% | validation {statistics.median(validation):.2f} s"
            + (f" | median of {rounds}" if rounds > 1 else "")
        )

    def setup(self):
        for side in ("direct", "plane"):
            self.sql(side, "CREATE NAMESPACE IF NOT EXISTS {ns}")
            self.sql(side, "CREATE TABLE {ns}.customer (id BIGINT, name STRING) USING iceberg")
            self.sql(
                side,
                "CREATE TABLE {ns}.events (id BIGINT, cust BIGINT, payload STRING) USING iceberg "
                "TBLPROPERTIES ('format-version'='2')",
            )
        register({"table": f"{self.p}.customer", "name": "pk_customer", "type": "PRIMARY_KEY", "columns": ["id"]})
        register({"table": f"{self.p}.events", "name": "pk_events", "type": "PRIMARY_KEY", "columns": ["id"]})
        register({
            "table": f"{self.p}.events", "name": "fk_events_customer", "type": "FOREIGN_KEY", "columns": ["cust"],
            "references": {"table": f"{self.p}.customer", "constraint": "pk_customer"},
        })

    def rows(self, start, end, tag):
        return (
            f"SELECT id, id % {self.customers} AS cust, "
            f"concat('{tag}-', sha2(cast(id AS STRING), 256)) AS payload FROM range({start}, {end})"
        )

    def run(self, rounds):
        c, n = self.customers, self.events
        self.compare("customer load", [
            f"INSERT INTO {{ns}}.customer SELECT id, concat('c', id) FROM range(0, {c})"
        ])
        self.compare(f"events initial load ({n} rows, PK + FK)", [
            f"INSERT INTO {{ns}}.events {self.rows(0, n, 'v1')}"
        ], first="plane")

        # Round i appends the same ids on both sides.
        step = 1_000_000
        self.compare(
            f"append {step} rows (PK + FK)", None, rounds=rounds,
            per_round=lambda i: [
                f"INSERT INTO {{ns}}.events {self.rows(n + i * step, n + (i + 1) * step, 'v1')}"
            ],
        )
        top = n + rounds * step

        # Copy-on-write MERGE: 500 K updates of the newest rows, 500 K inserts.
        self.compare("MERGE copy-on-write (500 K updates + 500 K inserts)", [
            f"MERGE INTO {{ns}}.events t USING ({self.rows(top - 500_000, top + 500_000, 'v2')}) s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET payload = s.payload "
            "WHEN NOT MATCHED THEN INSERT *"
        ], first="plane")
        top += 500_000
        self.compare("DELETE copy-on-write (100 K contiguous rows)", [
            f"DELETE FROM {{ns}}.events WHERE id >= {top - 600_000} AND id < {top - 500_000}"
        ])

        for side in ("direct", "plane"):
            self.sql(side, "ALTER TABLE {ns}.events SET TBLPROPERTIES ("
                     "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
                     "'write.merge.mode'='merge-on-read')")
        self.compare("DELETE merge-on-read (1 % of all rows)", [
            "DELETE FROM {ns}.events WHERE id % 100 = 7"
        ], first="plane")
        self.compare("UPDATE merge-on-read (100 K rows)", [
            f"UPDATE {{ns}}.events SET payload = 'v3' WHERE id >= 1000000 AND id < 1100000"
        ])
        self.compare("MERGE merge-on-read (100 K updates + 100 K inserts)", [
            f"MERGE INTO {{ns}}.events t USING ({self.rows(top - 100_000, top + 100_000, 'v4')}) s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET payload = s.payload "
            "WHEN NOT MATCHED THEN INSERT *"
        ], first="plane")
        self.compare(
            "DELETE merge-on-read (1 row)", None, rounds=rounds,
            per_round=lambda i: [f"DELETE FROM {{ns}}.events WHERE id = {12345 + 1000 * i}"],
        )
        self.compare("compaction (rewrite_data_files, all files)", [
            "CALL {cat}.system.rewrite_data_files(table => '{db}.events', "
            "options => map('rewrite-all', 'true'))"
        ])

        for side in ("direct", "plane"):
            got = self.sql(side, "SELECT count(*) AS n, count(DISTINCT id) AS k FROM {ns}.events")[0]
            say(f"OIP-SPARK check {side}: {got['n']} rows, {got['k']} distinct ids")



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=30_000_000)
    ap.add_argument("--customers", type=int, default=1_000_000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--run", default="", help="suffix of the namespaces, to run more than once")
    a = ap.parse_args()
    spark = session()
    b = Bench(spark, a.events, a.customers, a.run)
    b.setup()
    b.run(a.rounds)
    say("OIP-SPARK done")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
