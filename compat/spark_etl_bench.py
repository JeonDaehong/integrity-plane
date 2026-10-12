"""What the Plane costs a whole ETL job, not only its writes: the same nightly Spark job against the
upstream catalog directly and through the gateway, on the same storage.

The job reads raw feeds (customer changes and order events, unconstrained tables), deduplicates the
events, joins them to the customer dimension and a product table, upserts the dimension and the
fact table with MERGE and rebuilds a per-customer summary. Constraints (Plane side only):
customer(id PK, code UNIQUE), orders(id PK, cust FK -> customer). Every step is timed on both sides;
the job's total is what the Plane's overhead is reported against. Prints

    OIP-ETL <mode> round <i> <side> | <step> <s> ... | total <s> | validation <s>
    OIP-ETL <mode> summary | direct <s> | plane <s> | overhead <%> | validation <s>

Usage: python spark_etl_bench.py [--orders N] [--customers N] [--events N] [--rounds N] [--run X]
Environment as for spark_bench.py (GATEWAY, UPSTREAM, CATALOG_PROPS, ICEBERG_VERSION).
"""

import argparse
import statistics
import sys
import time

from spark_bench import metric, register, say, session


class Etl:
    def __init__(self, spark, orders, customers, events, run):
        self.spark = spark
        self.orders = orders
        self.customers = customers
        self.events = events
        self.run = run
        self.ns = {"direct": f"direct.d{run}", "plane": f"plane.p{run}"}
        self.src = {"direct": f"direct.s{run}", "plane": f"plane.s{run}"}

    def sql(self, side, text):
        cat, db = self.ns[side].split(".")
        return self.spark.sql(text.format(ns=self.ns[side], src=self.src[side], cat=cat, db=db)).collect()

    def setup(self):
        say("OIP-ETL progress setup")
        self.sql("direct", "CREATE NAMESPACE IF NOT EXISTS {src}")
        self.sql("direct", "CREATE TABLE {src}.products (id BIGINT, price DECIMAL(10,2)) USING iceberg")
        self.sql("direct", "INSERT INTO {src}.products SELECT id, CAST(1 + pmod(hash(id), 9900) / 100 AS DECIMAL(10,2)) "
                 "FROM range(0, 100000)")
        for side in ("direct", "plane"):
            self.sql(side, "CREATE NAMESPACE IF NOT EXISTS {ns}")
            self.sql(side, "CREATE TABLE {ns}.customer (id BIGINT, code STRING, name STRING, tier STRING) "
                     "USING iceberg TBLPROPERTIES ('format-version'='2')")
            self.sql(side, "CREATE TABLE {ns}.orders (id BIGINT, cust BIGINT, product BIGINT, "
                     "amount DECIMAL(18,2), status STRING, updated_at BIGINT, note STRING) "
                     "USING iceberg TBLPROPERTIES ('format-version'='2')")
            self.sql(side, "CREATE TABLE {ns}.customer_summary (cust BIGINT, orders BIGINT, "
                     "revenue DECIMAL(28,2), last_update BIGINT) USING iceberg")
        p = self.ns["plane"].split(".")[1]
        register({"table": f"{p}.customer", "name": "pk_customer", "type": "PRIMARY_KEY", "columns": ["id"]})
        register({"table": f"{p}.customer", "name": "uq_customer_code", "type": "UNIQUE", "columns": ["code"]})
        register({"table": f"{p}.orders", "name": "pk_orders", "type": "PRIMARY_KEY", "columns": ["id"]})
        register({
            "table": f"{p}.orders", "name": "fk_orders_customer", "type": "FOREIGN_KEY", "columns": ["cust"],
            "references": {"table": f"{p}.customer", "constraint": "pk_customer"},
        })
        # The starting state, untimed: the same rows on both sides.
        for side in ("direct", "plane"):
            t = time.perf_counter()
            self.sql(side, "INSERT INTO {ns}.customer SELECT id, concat('C', id), concat('name-', id), "
                     f"element_at(array('A','B','C'), 1 + CAST(pmod(id, 3) AS INT)) FROM range(0, {self.customers})")
            self.sql(side, f"INSERT INTO {{ns}}.orders SELECT id, pmod(hash(id), {self.customers}), "
                     "pmod(hash(id, 7), 100000), CAST(pmod(hash(id, 3), 100000) / 100 AS DECIMAL(18,2)), 'DONE', id, "
                     f"sha2(cast(id AS STRING), 256) FROM range(0, {self.orders})")
            say(f"OIP-ETL progress initial load {side} {time.perf_counter() - t:.1f} s")

    def feeds(self, r, top, custs):
        """Round r's raw feeds: customer changes and order events, written once, read by both sides."""
        half = self.events // 4  # distinct orders: half existing (the newest), half new
        upd, new_c = self.customers * 2 // 25, self.customers // 50  # 8 % changed, 2 % new
        self.sql("direct", f"CREATE TABLE {{src}}.raw_customers_{r} USING iceberg AS "
                 f"SELECT id * 12 + {r} AS id, concat('C', id * 12 + {r}) AS code, concat('name-', id, '-r{r}') AS name, "
                 f"'B' AS tier, current_timestamp() AS changed_at FROM range(0, {upd}) "
                 f"UNION ALL SELECT id, concat('C', id), concat('name-', id), 'A', current_timestamp() "
                 f"FROM range({custs}, {custs + new_c})")
        # Two events per order; codes past the last customer (about 1 %) do not resolve and are dropped.
        known = custs + new_c
        self.sql("direct", f"CREATE TABLE {{src}}.raw_orders_{r} USING iceberg AS "
                 f"SELECT * FROM (SELECT i AS event_id, {top - half} + pmod(i, {2 * half}) AS order_id, "
                 f"concat('C', pmod(hash(i, {r}), {int(known * 1.01)})) AS customer_code, "
                 "pmod(hash(i, 7), 100000) AS product_id, 1 + pmod(hash(i, 3), 5) AS qty, "
                 "element_at(array('NEW','PAID','SHIPPED','DONE'), 1 + CAST(pmod(i, 4) AS INT)) AS status, "
                 f"{r} * 100000000 + i AS event_ts, "
                 "concat(sha2(cast(i AS STRING), 256), sha2(cast(i + 1 AS STRING), 256)) AS raw_payload "
                 f"FROM (SELECT id AS i FROM range(0, {self.events})))")
        return top + half, known

    def job(self, side, r):
        """The nightly job; returns the time of each step and the gateway's validation time."""
        steps = [
            ("customer merge", f"MERGE INTO {{ns}}.customer t USING {{src}}.raw_customers_{r} s ON t.id = s.id "
             "WHEN MATCHED THEN UPDATE SET name = s.name, tier = s.tier "
             "WHEN NOT MATCHED THEN INSERT (id, code, name, tier) VALUES (s.id, s.code, s.name, s.tier)"),
            ("orders merge", "MERGE INTO {ns}.orders t USING ("
             "SELECT l.order_id AS id, c.id AS cust, l.product_id AS product, "
             "CAST(l.qty * p.price AS DECIMAL(18,2)) AS amount, l.status, l.event_ts AS updated_at, "
             "substr(l.raw_payload, 1, 64) AS note FROM ("
             "SELECT * FROM (SELECT e.*, row_number() OVER (PARTITION BY order_id ORDER BY event_ts DESC) AS rn "
             f"FROM {{src}}.raw_orders_{r} e) WHERE rn = 1) l "
             "JOIN {ns}.customer c ON c.code = l.customer_code "
             "JOIN {src}.products p ON p.id = l.product_id) s ON t.id = s.id "
             "WHEN MATCHED THEN UPDATE SET cust = s.cust, product = s.product, amount = s.amount, "
             "status = s.status, updated_at = s.updated_at, note = s.note "
             "WHEN NOT MATCHED THEN INSERT *"),
            ("summary rebuild", "INSERT OVERWRITE {ns}.customer_summary SELECT o.cust, count(*), sum(o.amount), "
             "max(o.updated_at) FROM {ns}.orders o JOIN {ns}.customer c ON c.id = o.cust GROUP BY o.cust"),
        ]
        before = metric("integrity_validation_seconds_sum") if side == "plane" else 0.0
        took = []
        for name, text in steps:
            t = time.perf_counter()
            self.sql(side, text)
            took.append((name, time.perf_counter() - t))
        validation = metric("integrity_validation_seconds_sum") - before if side == "plane" else 0.0
        return took, validation

    def mode(self, mode, rounds, r0, top, custs):
        totals = {"direct": [], "plane": []}
        steps = {"direct": {}, "plane": {}}
        validation = []
        for i in range(rounds):
            r = r0 + i
            say(f"OIP-ETL progress {mode} round {i} feeds")
            nxt = self.feeds(r, top, custs)
            for side in ("direct", "plane") if i % 2 == 0 else ("plane", "direct"):
                try:
                    took, v = self.job(side, r)
                except Exception as e:  # noqa: BLE001 - reported, the run stops here
                    say(f"OIP-ETL {mode} round {i} {side} FAILED: {str(e)[:400]}")
                    return None
                total = sum(s for _, s in took)
                totals[side].append(total)
                for name, s in took:
                    steps[side].setdefault(name, []).append(s)
                if side == "plane":
                    validation.append(v)
                say(f"OIP-ETL {mode} round {i} {side} | "
                    + " | ".join(f"{n} {s:.2f} s" for n, s in took)
                    + f" | total {total:.2f} s" + (f" | validation {v:.2f} s" if side == "plane" else ""))
            top, custs = nxt
        for name in steps["direct"]:
            d, p = statistics.median(steps["direct"][name]), statistics.median(steps["plane"][name])
            say(f"OIP-ETL {mode} step {name} | direct {d:.2f} s | plane {p:.2f} s | +{p - d:.2f} s")
        d, p = statistics.median(totals["direct"]), statistics.median(totals["plane"])
        say(f"OIP-ETL {mode} summary | direct {d:.2f} s | plane {p:.2f} s | overhead {100 * (p - d) / d:+.0f}% "
            f"| validation {statistics.median(validation):.2f} s | median of {rounds}")
        return top, custs

    def check(self):
        for side in ("direct", "plane"):
            o = self.sql(side, "SELECT count(*) AS n, count(DISTINCT id) AS k, sum(amount) AS s FROM {ns}.orders")[0]
            c = self.sql(side, "SELECT count(*) AS n, count(DISTINCT code) AS k FROM {ns}.customer")[0]
            say(f"OIP-ETL check {side}: orders {o['n']} rows {o['k']} ids sum {o['s']}; "
                f"customers {c['n']} rows {c['k']} codes")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=30_000_000)
    ap.add_argument("--customers", type=int, default=1_000_000)
    ap.add_argument("--events", type=int, default=6_000_000, help="raw order events per night")
    ap.add_argument("--rounds", type=int, default=2, help="nights per write mode")
    ap.add_argument("--run", default="", help="suffix of the namespaces, to run more than once")
    a = ap.parse_args()
    spark = session()
    etl = Etl(spark, a.orders, a.customers, a.events, a.run)
    try:
        etl.setup()
        state = etl.mode("copy-on-write", a.rounds, 0, a.orders, a.customers)
        if state:
            for side in ("direct", "plane"):
                for t in ("customer", "orders"):
                    etl.sql(side, f"ALTER TABLE {{ns}}.{t} SET TBLPROPERTIES ('write.delete.mode'='merge-on-read', "
                            "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')")
            etl.mode("merge-on-read", a.rounds, a.rounds, *state)
        etl.check()
        say("OIP-ETL done")
    except Exception as e:  # noqa: BLE001 - reported on the console
        say(f"OIP-ETL FAILED: {str(e)[:600]}")
        return 1
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
