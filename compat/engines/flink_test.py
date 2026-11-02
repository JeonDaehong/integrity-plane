"""Flink SQL (batch, iceberg-flink-runtime) through the Plane (#9).

Run from the repository root after
  compat/engines/fetch_flink_jars.sh
  docker compose -f deploy/docker-compose.yml -f compat/engines/docker-compose.yml up -d --build
Each statement runs in its own SQL client session (init: compat/engines/flink/init.sql). Results are
read back through Trino, which shares the catalog.
"""

from common import expect_ok, expect_rejection, integrity, key_constraints, sh, wait_until

NS = "flink"


def flink_sql(script):
    code, out = sh("docker", "exec", "-i", "flink-jobmanager", "sh", "-c", "cat > /tmp/stmt.sql",
                   stdin=script)
    assert code == 0, out
    return sh("docker", "exec", "flink-jobmanager", "/opt/flink/bin/sql-client.sh",
              "-i", "/opt/flink/init.sql", "-f", "/tmp/stmt.sql")


def run(sql):
    return flink_sql(f"USE gw.{NS};\n{sql};\n")


def trino(sql):
    code, out = sh("docker", "exec", "trino", "trino", "--catalog", "gw", "--schema", NS,
                   "--output-format", "TSV", "--execute", sql)
    assert code == 0, f"{sql}: {out}"
    return out.split()


def main():
    wait_until("Flink", lambda: flink_sql("SHOW CATALOGS;\n")[0] == 0)
    wait_until("Trino", lambda: sh("docker", "exec", "trino", "trino", "--execute", "SELECT 1")[0] == 0)
    code, out = flink_sql(f"CREATE DATABASE IF NOT EXISTS gw.{NS};\n")
    assert code == 0 and "[ERROR]" not in out, out

    expect_ok(run, "CREATE TABLE customer (customer_id BIGINT, name STRING) "
                   "WITH ('format-version' = '2')")
    expect_ok(run, "CREATE TABLE orders (order_id BIGINT, customer_id BIGINT) "
                   "WITH ('format-version' = '2')")
    key_constraints(NS)

    expect_ok(run, "INSERT INTO customer VALUES (1, 'alice'), (2, 'bob')")
    expect_ok(run, "INSERT INTO orders VALUES (10, 1)")
    print("ok   inserts", flush=True)
    rejected = lambda code, table, sql: expect_rejection(run, code, f"{NS}.{table}", sql)  # noqa: E731
    rejected("INT-003", "customer", "INSERT INTO customer VALUES (2, 'again')")
    rejected("INT-003", "customer", "INSERT INTO customer VALUES (5, 'e'), (5, 'f')")
    rejected("INT-007", "customer", "INSERT INTO customer VALUES (3, CAST(NULL AS STRING))")
    rejected("INT-005", "orders", "INSERT INTO orders VALUES (11, 999)")

    # INSERT OVERWRITE of an unpartitioned table replaces all of its rows.
    expect_ok(run, "CREATE TABLE ovr (id BIGINT, name STRING) WITH ('format-version' = '2')")
    integrity("constraints", "add", "--table", f"{NS}.ovr", "--name", "pk_ovr",
              "--type", "primary_key", "--columns", "id")
    expect_ok(run, "INSERT INTO ovr VALUES (1, 'a'), (2, 'b')")
    expect_ok(run, "INSERT OVERWRITE ovr VALUES (2, 'b2'), (3, 'c')")
    rejected("INT-003", "ovr", "INSERT OVERWRITE ovr VALUES (4, 'd'), (4, 'e')")
    assert trino("SELECT id FROM ovr ORDER BY id") == ["2", "3"]
    print("ok   INSERT OVERWRITE", flush=True)

    # Upsert table: equality deletes on the key (ADR 0009); a batch that writes one key twice also
    # writes position deletes in the same commit (ADR 0028).
    expect_ok(run, "CREATE TABLE ucust (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                   "WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')")
    integrity("constraints", "add", "--table", f"{NS}.ucust", "--name", "pk_ucust",
              "--type", "primary_key", "--columns", "id")
    expect_ok(run, "INSERT INTO ucust VALUES (1, 'a'), (2, 'b')")
    expect_ok(run, "INSERT INTO ucust VALUES (1, 'a2')")
    expect_ok(run, "INSERT INTO ucust VALUES (3, 'c'), (3, 'c2')")
    rows = trino("SELECT id, name FROM ucust ORDER BY id")
    assert rows[0::2] == ["1", "2", "3"], rows
    assert rows[1] == "a2", rows
    eq = trino("SELECT count(*) FROM \"ucust$files\" WHERE content = 2")
    assert int(eq[0]) > 0, "expected equality delete files"
    print("ok   upserts, including one key twice in a batch;", eq[0], "equality delete file(s)",
          flush=True)

    integrity("verify", f"{NS}.customer")
    integrity("verify", f"{NS}.orders")
    print("ok   verify", flush=True)


if __name__ == "__main__":
    main()
