"""Appendix A demo, end to end: Spark and the `integrity` CLI against the gateway.

Run by the compatibility workflow after the gateway is up (repository root as working directory).
Constraints are registered through the CLI, not the configuration file. A second Spark catalog
points straight at the upstream catalog to play a writer that bypasses the Plane.

Step 8 restarts the gateway with kill -9 between commits; kills in the middle of a commit, at every
fault point, are covered by crates/integrity-server/tests/crash.rs.

With DEMO_S3=1 it runs against deploy/docker-compose.yml instead (MinIO): clients write through
S3FileIO, and INTEGRITY_RESTART is the shell command that kill -9s and restarts the Plane.
"""

import json
import os
import subprocess
import sys
import time
import urllib.request

from pyspark.sql import SparkSession

GATEWAY = "http://127.0.0.1:8181"
UPSTREAM = "http://127.0.0.1:8182"
NS = "demo"
ICEBERG = os.environ.get("ICEBERG_VERSION", "1.10.0")
CLI = os.environ.get("INTEGRITY_CLI", "./target/release/integrity")
SERVER = os.environ.get("INTEGRITY_SERVER", "./target/release/integrity-server")
S3 = os.environ.get("DEMO_S3") == "1"
RESTART = os.environ.get("INTEGRITY_RESTART")


def cli(*args, expect=0):
    r = subprocess.run([CLI, *args], capture_output=True, text=True)
    assert r.returncode == expect, f"integrity {' '.join(args)} exited {r.returncode}\n{r.stdout}\n{r.stderr}"
    return r.stdout


def status():
    with urllib.request.urlopen(f"{GATEWAY}/v1/integrity/status") as r:
        return json.load(r)


def commits(table):
    return status()["commit_requests"].get(f"{NS}.{table}", 0)


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
        print(f"ok   {code} rejected after {attempts} attempt")
        return
    raise AssertionError(f"{code}: statement succeeded: {sql}")


def head_summary(spark, catalog, table):
    rows = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {catalog}.{NS}.{table}.snapshots ORDER BY committed_at"
    ).collect()
    return rows[-1]


def catalog(builder, name, uri):
    builder = (
        builder.config(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{name}.type", "rest")
        .config(f"spark.sql.catalog.{name}.uri", uri)
        .config(f"spark.sql.catalog.{name}.cache-enabled", "false")
    )
    if S3:
        builder = (
            builder.config(f"spark.sql.catalog.{name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{name}.s3.endpoint", "http://127.0.0.1:9000")
            .config(f"spark.sql.catalog.{name}.s3.path-style-access", "true")
            .config(f"spark.sql.catalog.{name}.client.region", "us-east-1")
        )
    return builder


def restart_gateway():
    if RESTART:
        subprocess.run(RESTART, shell=True, check=True)
    else:
        subprocess.run(["pkill", "-9", "-f", SERVER.lstrip("./")], check=False)
        time.sleep(1)
        log = open("gateway-restarted.log", "w")  # noqa: SIM115 - kept open for the child
        subprocess.Popen([SERVER, "compat/integrity.toml"], stdout=log, stderr=log, start_new_session=True)
    for _ in range(60):
        try:
            return status()
        except OSError:
            time.sleep(0.5)
    raise AssertionError("gateway did not come back")


def main():
    packages = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG}"
    if S3:
        packages += f",org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}"
    builder = SparkSession.builder.appName("integrity-demo").config(
        "spark.jars.packages", packages
    ).config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    builder = catalog(builder, "gw", GATEWAY)
    builder = catalog(builder, "up", UPSTREAM)
    spark = builder.config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Tables via Spark, constraints via the CLI.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gw.{NS}")
    spark.sql(f"CREATE TABLE gw.{NS}.customer (customer_id BIGINT, name STRING) USING iceberg")
    spark.sql(f"CREATE TABLE gw.{NS}.orders (order_id BIGINT, customer_id BIGINT) USING iceberg")
    pk_customer = json.loads(cli("constraints", "add", "--table", f"{NS}.customer", "--name", "pk_customer",
                                 "--type", "primary_key", "--columns", "customer_id"))["id"]
    pk_orders = json.loads(cli("constraints", "add", "--table", f"{NS}.orders", "--name", "pk_orders",
                               "--type", "primary_key", "--columns", "order_id"))["id"]
    cli("constraints", "add", "--table", f"{NS}.orders", "--name", "fk_orders_customer",
        "--type", "foreign_key", "--columns", "customer_id",
        "--ref-table", f"{NS}.customer", "--ref-constraint", "pk_customer")
    listed = json.loads(cli("constraints", "list", "--table", f"{NS}.orders"))["constraints"]
    assert [c["name"] for c in listed] == ["pk_orders", "fk_orders_customer"], listed
    print("ok   constraints registered via CLI:", pk_customer, pk_orders)

    # 3. Customer 1: certificate #1.
    spark.sql(f"INSERT INTO gw.{NS}.customer VALUES (1, 'alice')")
    summary = head_summary(spark, "gw", "customer")["summary"]
    assert summary.get("integrity.cert-version") == "1", summary
    print("ok   customer 1 certified", summary["integrity.cert"][:16])

    # 4. Orders for customer 1 (two commits: two data files for step 7).
    spark.sql(f"INSERT INTO gw.{NS}.orders VALUES (10, 1)")
    spark.sql(f"INSERT INTO gw.{NS}.orders VALUES (11, 1)")
    print("ok   orders for customer 1")

    # 5. Order for a customer that does not exist.
    expect_rejection(spark, "INT-005", "orders", f"INSERT INTO gw.{NS}.orders VALUES (12, 999)")
    # 6. Deleting a referenced customer.
    expect_rejection(spark, "INT-006", "customer", f"DELETE FROM gw.{NS}.customer WHERE customer_id = 1")

    # 7. Compaction keeps every key: accepted and certified.
    spark.sql(
        f"CALL gw.system.rewrite_data_files(table => '{NS}.orders', options => map('min-input-files', '2'))"
    ).collect()
    head = head_summary(spark, "gw", "orders")
    assert head["operation"] == "replace", head
    assert head["summary"].get("integrity.cert-version") == "1", head
    print("ok   compaction accepted and certified")

    # 8. kill -9 and restart: nothing left to recover, enforcement continues.
    st = restart_gateway()
    assert st["unresolved_transactions"] == 0, st
    expect_rejection(spark, "INT-003", "orders", f"INSERT INTO gw.{NS}.orders VALUES (10, 1)")
    print("ok   restarted after kill -9; indexes intact")
    print(cli("verify", f"{NS}.orders"), end="")

    # 9. A writer that goes straight to the upstream catalog.
    spark.sql(f"INSERT INTO up.{NS}.orders VALUES (13, 1)")
    bypass = head_summary(spark, "up", "orders")["snapshot_id"]
    report = cli("verify", f"{NS}.orders", expect=1)
    print(report, end="")
    assert f"BROKEN at snapshot {bypass}" in report, report
    print("ok   verify pinpoints the bypassing snapshot", bypass)
    expect_rejection(spark, "INT-010", "orders", f"INSERT INTO gw.{NS}.orders VALUES (14, 1)")
    domain = json.loads(cli("domain", f"{NS}.orders"))
    assert domain["state"] == "Degraded", domain

    # The operator inspects the data and re-certifies the domain.
    cli("rebuild", str(pk_orders))
    spark.sql(f"INSERT INTO gw.{NS}.orders VALUES (14, 1)")
    print(cli("verify", f"{NS}.orders"), end="")
    audit = json.loads(cli("audit", "--table", f"{NS}.orders"))["events"]
    kinds = {e["kind"] for e in audit}
    for kind in ["CONSTRAINT_REGISTERED", "COMMIT_ACCEPTED", "COMMIT_REJECTED", "BYPASS_DETECTED", "INDEX_REBUILT"]:
        assert kind in kinds, (kind, kinds)
    print("ok   rebuilt, enforcing again; audit trail complete")
    spark.stop()


if __name__ == "__main__":
    main()
    sys.exit(0)
