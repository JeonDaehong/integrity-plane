"""Spark (Iceberg Java REST client) against the integrity gateway (Phase 7 compatibility).

Same scenario as pyiceberg_test.py with SQL. Violations must fail the statement with the integrity
code after exactly one commit attempt; DELETE runs copy-on-write.
"""

import json
import os
import sys
import urllib.request

from pyspark.sql import SparkSession

GATEWAY = "http://127.0.0.1:8181"
NS = "spark"
ICEBERG = os.environ.get("ICEBERG_VERSION", "1.10.0")
# Extra catalog properties for upstreams that need them (e.g. Polaris: credential, warehouse).
EXTRA = json.loads(os.environ.get("CATALOG_PROPS", "{}"))


def commits(table):
    with urllib.request.urlopen(f"{GATEWAY}/v1/integrity/status") as r:
        return json.load(r)["commit_requests"].get(f"{NS}.{table}", 0)


def expect_rejection(spark, code, table, sql):
    before = commits(table)
    try:
        spark.sql(sql).collect()
    except Exception as e:  # noqa: BLE001 - asserting on the engine's error
        message = str(e)
        assert code in message, f"expected {code}, got: {message[:2000]}"
        attempts = commits(table) - before
        assert attempts == 1, f"{code}: {attempts} commit attempts reached the gateway"
        assert "CommitStateUnknown" not in message, message[:2000]
        print(f"ok   {code} rejected, {attempts} attempt")
        return
    raise AssertionError(f"{code}: statement succeeded: {sql}")


def main():
    builder = (
        SparkSession.builder.appName("integrity-compat")
        .config("spark.jars.packages", ",".join(
            [f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG}"]
            + ([f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}"] if "io-impl" in EXTRA else [])
        ))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.gw", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gw.type", "rest")
        .config("spark.sql.catalog.gw.uri", GATEWAY)
    )
    for key, value in EXTRA.items():
        builder = builder.config(f"spark.sql.catalog.gw.{key}", value)
    spark = (
        builder
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gw.{NS}")
    spark.sql(f"CREATE TABLE gw.{NS}.customer (customer_id BIGINT, name STRING) USING iceberg")
    spark.sql(f"CREATE TABLE gw.{NS}.orders (order_id BIGINT, customer_id BIGINT) USING iceberg")

    spark.sql(f"INSERT INTO gw.{NS}.customer VALUES (1, 'alice'), (2, 'bob')")
    summary = spark.sql(f"SELECT summary FROM gw.{NS}.customer.snapshots").collect()[-1]["summary"]
    assert summary.get("integrity.cert-version") == "1", summary
    print("ok   customer insert certified", summary["integrity.cert"][:16])

    spark.sql(f"INSERT INTO gw.{NS}.orders VALUES (10, 1)")
    print("ok   order for an existing customer")

    expect_rejection(spark, "INT-005", "orders", f"INSERT INTO gw.{NS}.orders VALUES (11, 999)")
    expect_rejection(spark, "INT-006", "customer", f"DELETE FROM gw.{NS}.customer WHERE customer_id = 1")
    expect_rejection(spark, "INT-003", "customer", f"INSERT INTO gw.{NS}.customer VALUES (2, 'dup')")
    expect_rejection(spark, "INT-007", "customer", f"INSERT INTO gw.{NS}.customer VALUES (3, NULL)")

    spark.sql(f"DELETE FROM gw.{NS}.orders WHERE order_id = 10")
    spark.sql(f"DELETE FROM gw.{NS}.customer WHERE customer_id = 1")
    left = [r["customer_id"] for r in spark.sql(f"SELECT customer_id FROM gw.{NS}.customer").collect()]
    assert left == [2], left
    print("ok   child then parent delete; customer ids", left)

    certified = spark.sql(
        f"SELECT count(*) AS n, count(summary['integrity.cert']) AS c FROM gw.{NS}.customer.snapshots"
    ).collect()[0]
    assert certified["n"] == certified["c"], certified
    print("ok   all customer snapshots certified")

    # Composite keys: PRIMARY KEY (country, code), FOREIGN KEY (country, code) MATCH SIMPLE.
    spark.sql(f"CREATE TABLE gw.{NS}.region (country STRING, code STRING, name STRING) USING iceberg")
    spark.sql(f"CREATE TABLE gw.{NS}.store (id BIGINT, country STRING, code STRING) USING iceberg")
    spark.sql(f"INSERT INTO gw.{NS}.region VALUES ('KR', 'SEL', 'Seoul'), ('KR', 'PUS', 'Busan'), ('US', 'SEL', 'Selma')")
    print("ok   composite keys sharing a column value")
    expect_rejection(spark, "INT-003", "region", f"INSERT INTO gw.{NS}.region VALUES ('KR', 'SEL', 'again')")
    expect_rejection(spark, "INT-007", "region", f"INSERT INTO gw.{NS}.region VALUES ('KR', NULL, 'no code')")
    spark.sql(f"INSERT INTO gw.{NS}.store VALUES (1, 'KR', 'SEL'), (2, 'US', 'SEL')")
    # Both parts exist, but not as one parent key.
    expect_rejection(spark, "INT-005", "store", f"INSERT INTO gw.{NS}.store VALUES (3, 'US', 'PUS')")
    # MATCH SIMPLE: a NULL part exempts the row.
    spark.sql(f"INSERT INTO gw.{NS}.store VALUES (4, NULL, 'XXX')")
    expect_rejection(spark, "INT-006", "region", f"DELETE FROM gw.{NS}.region WHERE country = 'KR' AND code = 'SEL'")
    spark.sql(f"DELETE FROM gw.{NS}.region WHERE country = 'KR' AND code = 'PUS'")
    print("ok   composite PK/FK enforced; unreferenced parent deleted")

    # Merge-on-read: DELETE / UPDATE / MERGE write position delete files (ADR 0017).
    mor = ("TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', "
           "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')")
    spark.sql(f"CREATE TABLE gw.{NS}.mcust (customer_id BIGINT, name STRING) USING iceberg {mor}")
    spark.sql(f"CREATE TABLE gw.{NS}.morders (order_id BIGINT, customer_id BIGINT) USING iceberg {mor}")
    # One data file, so that deleting one of its rows needs a position delete.
    spark.sql(
        f"INSERT INTO gw.{NS}.mcust SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    spark.sql(f"INSERT INTO gw.{NS}.morders VALUES (10, 1)")
    expect_rejection(spark, "INT-006", "mcust", f"DELETE FROM gw.{NS}.mcust WHERE customer_id = 1")
    spark.sql(f"DELETE FROM gw.{NS}.mcust WHERE customer_id = 3")
    deletes = spark.sql(f"SELECT count(*) AS n FROM gw.{NS}.mcust.delete_files").collect()[0]["n"]
    assert deletes > 0, "the DELETE was expected to write position deletes"
    print("ok   merge-on-read DELETE of an unreferenced row;", deletes, "delete file(s)")
    spark.sql(f"UPDATE gw.{NS}.mcust SET name = 'bb' WHERE customer_id = 2")
    expect_rejection(spark, "INT-005", "morders", f"UPDATE gw.{NS}.morders SET customer_id = 999 WHERE order_id = 10")
    spark.sql(
        f"MERGE INTO gw.{NS}.mcust t USING (SELECT 4 AS id, 'd' AS name) s ON t.customer_id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = s.name "
        "WHEN NOT MATCHED THEN INSERT (customer_id, name) VALUES (s.id, s.name)"
    )
    expect_rejection(spark, "INT-003", "mcust", f"INSERT INTO gw.{NS}.mcust VALUES (2, 'dup')")
    spark.sql(f"INSERT INTO gw.{NS}.mcust VALUES (3, 'again')")
    print("ok   merge-on-read UPDATE / MERGE enforced; a deleted key can be inserted again")
    spark.sql(
        f"CALL gw.system.rewrite_data_files(table => '{NS}.mcust', "
        "options => map('min-input-files', '1', 'delete-file-threshold', '1'))"
    ).collect()
    head = spark.sql(f"SELECT operation, summary FROM gw.{NS}.mcust.snapshots ORDER BY committed_at").collect()[-1]
    assert head["operation"] == "replace", head
    ids = sorted(r["customer_id"] for r in spark.sql(f"SELECT customer_id FROM gw.{NS}.mcust").collect())
    assert ids == [1, 2, 3, 4], ids
    certified = spark.sql(
        f"SELECT count(*) AS n, count(summary['integrity.cert']) AS c FROM gw.{NS}.mcust.snapshots"
    ).collect()[0]
    assert certified["n"] == certified["c"], certified
    print("ok   compaction applying deletes certified; every snapshot certified")
    spark.stop()


if __name__ == "__main__":
    main()
    sys.exit(0)
