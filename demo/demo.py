"""The README demo: PyIceberg writing through the Plane (deploy/docker-compose.yml).

Run after `docker compose -f deploy/docker-compose.yml up -d`; recorded by demo/record.sh. The
argument is the integrity CLI command, e.g. "docker exec integrity-plane integrity" (the image ships
the CLI). Exits non-zero if a commit is not decided as shown.
"""

import json
import shlex
import subprocess
import sys
import time
import urllib.request

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

PLANE = "http://127.0.0.1:8181"
S3 = {"s3.endpoint": "http://127.0.0.1:9000", "s3.access-key-id": "admin",
      "s3.secret-access-key": "password", "s3.region": "us-east-1"}
CLI = shlex.split(sys.argv[1] if len(sys.argv) > 1 else "integrity")
BOLD, GREEN, RED, DIM, RESET = "\033[1m", "\033[32m", "\033[31m", "\033[2m", "\033[0m"


def say(text):
    print(f"\n{BOLD}# {text}{RESET}", flush=True)
    time.sleep(1.2)


def run(*args):
    print(f"{DIM}$ integrity {' '.join(args)}{RESET}", flush=True)
    out = subprocess.run([*CLI, "--url", PLANE, *args], capture_output=True, text=True)
    text = (out.stdout or out.stderr).strip()
    try:
        registered = json.loads(text)
        text = f"registered {registered['name']} (constraint {registered['id']})"
    except (ValueError, KeyError, TypeError):
        pass
    print(text, flush=True)
    time.sleep(1.0)


def commit(what, action, accept):
    print(f"{DIM}>>> {what}{RESET}", flush=True)
    try:
        action()
        print(f"{GREEN}accepted{RESET}", flush=True)
        if not accept:
            unexpected.append(what)
    except Exception as e:  # noqa: BLE001 - shown as the writer sees it
        # "INT-003 DUPLICATE_PRIMARY_KEY: commit rejected: INT-003 on pk_customer (1 key); details…"
        message = str(e).split(": ", 1)[-1]
        code = message.split(":", 1)[0]
        where = message.split(" on ", 1)[-1].split(";", 1)[0]
        print(f"{RED}refused  {code}  on {where}{RESET}", flush=True)
        if accept:
            unexpected.append(what)
    time.sleep(1.5)


def rows(schema, *tuples):
    arrow = schema.as_arrow()
    cols = list(zip(*tuples))
    return pa.Table.from_arrays([pa.array(c, f.type) for c, f in zip(cols, arrow)], schema=arrow)


CUSTOMER = Schema(NestedField(1, "id", LongType(), required=False),
                  NestedField(2, "name", StringType(), required=False))
ORDERS = Schema(NestedField(1, "id", LongType(), required=False),
                NestedField(2, "customer_id", LongType(), required=False))

unexpected = []

# The Plane answers once its control store is open; the stack may still be starting.
for _ in range(60):
    try:
        urllib.request.urlopen(f"{PLANE}/v1/integrity/status", timeout=2)
        break
    except OSError:
        time.sleep(2)

catalog = load_catalog("lake", type="rest", uri=PLANE, **S3)
catalog.create_namespace_if_not_exists("shop")
customer = catalog.create_table("shop.customer", CUSTOMER)
orders = catalog.create_table("shop.orders", ORDERS)

say("Iceberg has no enforced keys. Register them with the Plane:")
run("constraints", "add", "--table", "shop.customer", "--name", "pk_customer",
    "--type", "primary_key", "--columns", "id")
run("constraints", "add", "--table", "shop.orders", "--name", "fk_orders_customer",
    "--type", "foreign_key", "--columns", "customer_id",
    "--ref-table", "shop.customer", "--ref-constraint", "pk_customer")

say("Writers use the Plane as their REST catalog. Valid commits go through:")
commit("customer.append([(1, 'alice'), (2, 'bob')])",
       lambda: customer.append(rows(CUSTOMER, (1, "alice"), (2, "bob"))), accept=True)
commit("orders.append([(10, customer 1)])",
       lambda: orders.append(rows(ORDERS, (10, 1))), accept=True)

say("Commits that would break a key never reach the catalog:")
commit("customer.append([(2, 'bob again')])",
       lambda: customer.refresh().append(rows(CUSTOMER, (2, "bob again"))), accept=False)
commit("orders.append([(11, customer 99)])",
       lambda: orders.refresh().append(rows(ORDERS, (11, 99))), accept=False)
commit("customer.delete('id == 1')   # still referenced by order 10",
       lambda: customer.refresh().delete("id == 1"), accept=False)

say("Every accepted snapshot carries a certificate anyone can recompute:")
run("verify", "shop.customer")

if unexpected:
    sys.exit(f"not decided as shown: {unexpected}")
