"""Trino (Iceberg connector, REST catalog, native S3) through the Plane (#9).

Run from the repository root after
  docker compose -f deploy/docker-compose.yml -f compat/engines/docker-compose.yml up -d --build
Trino writes merge-on-read: DELETE, UPDATE and MERGE commit position delete files.
"""

from common import expect_ok, expect_rejection, integrity, key_constraints, sh, wait_until

NS = "trino"


def run(sql):
    return sh("docker", "exec", "trino", "trino", "--catalog", "gw", "--schema", NS,
              "--output-format", "TSV", "--execute", sql)


def query(sql):
    return expect_ok(run, sql).split()


def main():
    wait_until("Trino", lambda: sh("docker", "exec", "trino", "trino", "--execute", "SELECT 1")[0] == 0)
    expect_ok(run, f"CREATE SCHEMA IF NOT EXISTS gw.{NS}")
    expect_ok(run, "CREATE TABLE customer (customer_id BIGINT, name VARCHAR)")
    expect_ok(run, "CREATE TABLE orders (order_id BIGINT, customer_id BIGINT)")
    key_constraints(NS)

    expect_ok(run, "INSERT INTO customer VALUES (1, 'alice'), (2, 'bob'), (4, 'dora')")
    snapshots = expect_ok(run, "SELECT snapshot_id, parent_id, operation, "
                               "element_at(summary, 'integrity.cert-version') "
                               "FROM \"customer$snapshots\" ORDER BY committed_at")
    print(snapshots, flush=True)
    version = query("SELECT element_at(summary, 'integrity.cert-version') "
                    "FROM \"customer$snapshots\" ORDER BY committed_at DESC LIMIT 1")
    assert version == ["1"], version
    print("ok   insert certified", flush=True)
    expect_ok(run, "INSERT INTO orders VALUES (10, 1)")

    rejected = lambda code, table, sql: expect_rejection(run, code, f"{NS}.{table}", sql)  # noqa: E731
    rejected("INT-003", "customer", "INSERT INTO customer VALUES (2, 'again')")
    rejected("INT-003", "customer", "INSERT INTO customer VALUES (5, 'e'), (5, 'f')")
    rejected("INT-003", "customer", "INSERT INTO customer SELECT customer_id, name FROM customer")
    rejected("INT-003", "customer", "UPDATE customer SET customer_id = 2 WHERE customer_id = 4")
    rejected("INT-003", "customer",
             "MERGE INTO customer c USING (VALUES (6, 'f'), (6, 'g')) s (id, name) "
             "ON c.customer_id = s.id WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)")
    rejected("INT-007", "customer", "INSERT INTO customer VALUES (3, NULL)")
    rejected("INT-005", "orders", "INSERT INTO orders VALUES (11, 999)")
    rejected("INT-006", "customer", "DELETE FROM customer WHERE customer_id = 1")

    expect_ok(run, "UPDATE customer SET name = 'Bob' WHERE customer_id = 2")
    expect_ok(run, "MERGE INTO customer c USING (VALUES (4, 'Dora'), (7, 'gus')) s (id, name) "
                   "ON c.customer_id = s.id WHEN MATCHED THEN UPDATE SET name = s.name "
                   "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)")
    expect_ok(run, "DELETE FROM orders WHERE order_id = 10")
    expect_ok(run, "DELETE FROM customer WHERE customer_id = 1")
    deletes = query("SELECT count(*) FROM \"customer$files\" WHERE content = 1")
    assert int(deletes[0]) > 0, "expected position delete files"
    print("ok   merge-on-read UPDATE, MERGE, DELETE;", deletes[0], "delete file(s)", flush=True)

    expect_ok(run, "ALTER TABLE customer EXECUTE optimize")
    print("ok   optimize", flush=True)
    rows = query("SELECT customer_id, name FROM customer ORDER BY customer_id")
    assert rows == ["2", "Bob", "4", "Dora", "7", "gus"], rows
    uncertified = query("SELECT count(*) FROM \"customer$snapshots\" "
                        "WHERE element_at(summary, 'integrity.cert') IS NULL")
    assert uncertified == ["0"], uncertified
    print("ok   rows as expected, every snapshot certified", flush=True)

    integrity("verify", f"{NS}.customer")
    integrity("verify", f"{NS}.orders")
    print("ok   verify", flush=True)
    expect_ok(run, "ALTER TABLE customer EXECUTE expire_snapshots(retention_threshold => '0s')")
    expect_ok(run, "INSERT INTO customer VALUES (8, 'hal')")
    print("ok   expire_snapshots, then a commit", flush=True)


if __name__ == "__main__":
    main()
