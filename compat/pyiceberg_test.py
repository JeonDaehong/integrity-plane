"""PyIceberg against the integrity gateway (spec Appendix A, Phase 7 compatibility).

Valid commits succeed and carry certificates; violations fail with HTTP 400 and the integrity code,
after exactly one commit attempt (no retry storm).
"""

import json
import sys
import urllib.request

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

GATEWAY = "http://127.0.0.1:8181"
NS = "pyiceberg"

CUSTOMER = Schema(
    NestedField(1, "customer_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
)
ORDERS = Schema(
    NestedField(1, "order_id", LongType(), required=False),
    NestedField(2, "customer_id", LongType(), required=False),
)


def commits(table):
    with urllib.request.urlopen(f"{GATEWAY}/v1/integrity/status") as r:
        return json.load(r)["commit_requests"].get(f"{NS}.{table}", 0)


def rows(schema, *tuples):
    arrow = schema.as_arrow()
    cols = list(zip(*tuples))
    return pa.Table.from_arrays([pa.array(c, f.type) for c, f in zip(cols, arrow)], schema=arrow)


def expect_rejection(code, table, action):
    before = commits(table)
    try:
        action()
    except Exception as e:  # noqa: BLE001 - asserting on the client's exception
        message = str(e)
        assert code in message, f"expected {code}, got {type(e).__name__}: {message}"
        attempts = commits(table) - before
        assert attempts == 1, f"{code}: {attempts} commit attempts reached the gateway"
        print(f"ok   {code} rejected ({type(e).__name__}), {attempts} attempt")
        return
    raise AssertionError(f"{code}: commit was accepted")


def main():
    catalog = load_catalog("gw", type="rest", uri=GATEWAY)
    catalog.create_namespace_if_not_exists(NS)
    customer = catalog.create_table(f"{NS}.customer", CUSTOMER)
    orders = catalog.create_table(f"{NS}.orders", ORDERS)

    customer.append(rows(CUSTOMER, (1, "alice"), (2, "bob")))
    summary = customer.refresh().current_snapshot().summary
    assert summary["integrity.cert-version"] == "1", summary
    assert len(summary["integrity.cert"]) == 64, summary
    print("ok   customer insert certified", summary["integrity.cert"][:16])

    orders.append(rows(ORDERS, (10, 1)))
    print("ok   order for an existing customer")

    expect_rejection("INT-005", "orders", lambda: orders.append(rows(ORDERS, (11, 999))))
    expect_rejection("INT-006", "customer", lambda: customer.delete("customer_id == 1"))
    expect_rejection("INT-003", "customer", lambda: customer.append(rows(CUSTOMER, (2, "dup"))))
    expect_rejection("INT-007", "customer", lambda: customer.append(rows(CUSTOMER, (3, None))))

    # Children first, then the parent (spec §15 ordering consequence).
    orders.refresh().delete("order_id == 10")
    customer.refresh().delete("customer_id == 1")
    left = sorted(r["customer_id"] for r in customer.refresh().scan().to_arrow().to_pylist())
    assert left == [2], left
    print("ok   child then parent delete; customer ids", left)

    # Every snapshot on main is certified.
    for snap in customer.refresh().snapshots():
        assert "integrity.cert" in snap.summary, snap
    print("ok   all customer snapshots certified")


if __name__ == "__main__":
    main()
    sys.exit(0)
