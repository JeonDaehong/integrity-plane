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
    spark = (
        SparkSession.builder.appName("integrity-compat")
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.gw", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gw.type", "rest")
        .config("spark.sql.catalog.gw.uri", GATEWAY)
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
    spark.stop()


if __name__ == "__main__":
    main()
    sys.exit(0)
