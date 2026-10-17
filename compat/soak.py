"""Soak and chaos test: several fenced Planes in front of one catalog (RFC 0011, RFC 0012).

Starts N Planes (each with its own control store, `[ha] fence = "multi-table"`) in front of the
Iceberg REST fixture (compat/docker-compose.yml, port 8182), then for DURATION seconds:

- W writer threads commit through a random Plane with PyIceberg: customer and order appends,
  deletes, with deliberate duplicates, dangling references and deletes of referenced customers;
- a chaos thread kills a random Plane with SIGKILL every few minutes and restarts it.

At the end every Plane is up again, takes one more commit, and the test checks the tables'
actual data (read straight from the catalog): customer and order ids unique, every order's
customer present, no customer without a name; every Plane's `verify` passes and no domain is
degraded. Prints counts per outcome; exits 1 on any violation or unexpected error.

The upstream catalog must apply multi-table commits atomically (compat/multitable_probe.py):
Nessie or Lakekeeper, not the REST fixture or Polaris 1.7.

Usage:
  python compat/soak.py [--planes 3] [--writers 6] [--duration 3600] [--chaos 180]
      [--upstream URI] [--prefix PREFIX] [--props JSON] [--storage TOML]
"""

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from collections import Counter

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

UPSTREAM = "http://127.0.0.1:8182"
PREFIX = ""
PROPS = {}
STORAGE = ""
NS = "soak"
SERVER = os.environ.get("INTEGRITY_SERVER", "target/release/integrity-server")
CLI = os.environ.get("INTEGRITY_CLI", "target/release/integrity")
CUSTOMER = Schema(
    NestedField(1, "customer_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
)
ORDERS = Schema(
    NestedField(1, "order_id", LongType(), required=False),
    NestedField(2, "customer_id", LongType(), required=False),
)
CONSTRAINTS = """
[[constraint]]
id = 1
table = "soak.customer"
name = "pk_customer"
type = "primary_key"
columns = [1]

[[constraint]]
id = 2
table = "soak.customer"
name = "nn_customer_name"
type = "not_null"
columns = [2]

[[constraint]]
id = 3
table = "soak.orders"
name = "pk_orders"
type = "primary_key"
columns = [1]

[[constraint]]
id = 4
table = "soak.orders"
name = "fk_orders_customer"
type = "foreign_key"
columns = [2]
references = { table = "soak.customer", constraint = 1 }
"""

stats = Counter()
errors = []
lock = threading.Lock()
stop = threading.Event()


def count(key, detail=None):
    with lock:
        stats[key] += 1
        if detail and len(errors) < 50:
            errors.append(detail)


class Plane:
    def __init__(self, n, workdir):
        self.n = n
        self.port = 8190 + n
        self.uri = f"http://127.0.0.1:{self.port}"
        self.dir = os.path.join(workdir, f"plane-{n}")
        os.makedirs(self.dir, exist_ok=True)
        self.config = os.path.join(self.dir, "integrity.toml")
        with open(self.config, "w") as f:
            f.write(f"""[server]
bind = "127.0.0.1:{self.port}"
open_api = true
[upstream]
catalog_uri = "{UPSTREAM}"
{f'prefix = "{PREFIX}"' if PREFIX else ""}
{STORAGE}
[control_store]
path = "{self.dir}/control"
[ha]
fence = "multi-table"
{CONSTRAINTS}""")
        self.proc = None
        self.up = threading.Event()

    def start(self):
        log = open(os.path.join(self.dir, "plane.log"), "a")
        self.proc = subprocess.Popen([SERVER, self.config], stdout=log, stderr=log)
        for _ in range(200):
            try:
                urllib.request.urlopen(f"{self.uri}/v1/integrity/status", timeout=2)
                self.up.set()
                return
            except Exception:  # noqa: BLE001 - starting
                time.sleep(0.1)
        raise RuntimeError(f"plane {self.n} did not start")

    def kill(self):
        self.up.clear()
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def status(self):
        with urllib.request.urlopen(f"{self.uri}/v1/integrity/status", timeout=10) as r:
            return json.load(r)


def rows(schema, tuples):
    arrow = schema.as_arrow()
    cols = list(zip(*tuples))
    return pa.Table.from_arrays([pa.array(c, f.type) for c, f in zip(cols, arrow)], schema=arrow)


def outcome(e):
    message = str(e)
    for code in ("INT-003", "INT-005", "INT-006", "INT-007"):
        if code in message:
            return f"rejected {code}"
    return None


def writer(planes, seed, keys):
    rnd = random.Random(seed)
    catalogs = {}
    while not stop.is_set():
        live = [p for p in planes if p.up.is_set()]
        if not live:
            time.sleep(0.2)
            continue
        plane = rnd.choice(live)
        op = rnd.random()
        try:
            catalog = catalogs.get(plane.n) or load_catalog(
                f"p{plane.n}", type="rest", uri=plane.uri, **PROPS
            )
            catalogs[plane.n] = catalog
            for attempt in range(8):
                try:
                    if op < 0.4:
                        t = catalog.load_table(f"{NS}.customer")
                        ids = rnd.sample(range(keys), rnd.randint(1, 3))
                        name = None if rnd.random() < 0.03 else "c"
                        t.append(rows(CUSTOMER, [(i, name) for i in ids]))
                        kind = "customer append"
                    elif op < 0.75:
                        t = catalog.load_table(f"{NS}.orders")
                        oid = rnd.randrange(keys * 4)
                        t.append(rows(ORDERS, [(oid, rnd.randrange(keys))]))
                        kind = "order append"
                    elif op < 0.9:
                        t = catalog.load_table(f"{NS}.orders")
                        t.delete(f"order_id == {rnd.randrange(keys * 4)}")
                        kind = "order delete"
                    else:
                        t = catalog.load_table(f"{NS}.customer")
                        t.delete(f"customer_id == {rnd.randrange(keys)}")
                        kind = "customer delete"
                    count(f"accepted {kind}")
                    break
                except CommitFailedException as e:
                    if outcome(e):
                        raise
                    count("retried 409")
                    time.sleep(rnd.random() * 0.2 * (attempt + 1))
            else:
                count("gave up after 409s")
        except Exception as e:  # noqa: BLE001 - classify every client error
            kind = outcome(e)
            if kind:
                count(kind)
            elif not plane.up.is_set() or "Connection" in str(e) or "connect" in str(e).lower():
                count("plane down")
                catalogs.pop(plane.n, None)
            elif type(e).__name__ == "ValidationException":
                # PyIceberg found a concurrent change itself before committing.
                count("client-side conflict")
            elif isinstance(e, PermissionError):
                # The client's own reads and writes of data files (object storage refused them):
                # nothing reached a Plane.
                count("storage refused the client", f"storage: {str(e)[:200]}")
            else:
                count("unexpected", f"plane {plane.n}: {type(e).__name__}: {str(e)[:300]}")


def chaos(planes, every, seed):
    rnd = random.Random(seed)
    while not stop.wait(rnd.uniform(every * 0.5, every * 1.5)):
        victim = rnd.choice(planes)
        if not victim.up.is_set():
            continue
        victim.kill()
        count("chaos kill")
        if stop.wait(rnd.uniform(5, 30)):
            victim.start()
            return
        victim.start()


def main():
    global UPSTREAM, PREFIX, PROPS, STORAGE
    ap = argparse.ArgumentParser()
    ap.add_argument("--planes", type=int, default=3)
    ap.add_argument("--writers", type=int, default=6)
    ap.add_argument("--duration", type=int, default=3600)
    ap.add_argument("--chaos", type=int, default=180)
    ap.add_argument("--keys", type=int, default=2000)
    ap.add_argument("--workdir", default="/data/soak")
    ap.add_argument("--upstream", default=UPSTREAM)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--props", default="{}", help="catalog properties for clients (JSON)")
    ap.add_argument("--storage", default="", help="a [storage] section for the Planes")
    args = ap.parse_args()
    UPSTREAM, PREFIX, PROPS, STORAGE = args.upstream, args.prefix, json.loads(args.props), args.storage

    shutil.rmtree(args.workdir, ignore_errors=True)
    upstream = load_catalog("up", type="rest", uri=UPSTREAM, **PROPS)
    upstream.create_namespace_if_not_exists(NS)
    for t in ("orders", "customer"):
        try:
            upstream.drop_table(f"{NS}.{t}")
        except Exception:  # noqa: BLE001 - absent
            pass
    upstream.create_table(f"{NS}.customer", CUSTOMER)
    upstream.create_table(f"{NS}.orders", ORDERS)

    planes = [Plane(n, args.workdir) for n in range(args.planes)]
    for p in planes:
        p.start()
    threads = [threading.Thread(target=writer, args=(planes, s, args.keys)) for s in range(args.writers)]
    threads.append(threading.Thread(target=chaos, args=(planes, args.chaos, 99)))
    for t in threads:
        t.start()
    started = time.time()
    while time.time() - started < args.duration:
        time.sleep(60)
        with lock:
            print(f"[{int(time.time() - started)}s] {dict(stats)}", flush=True)
    stop.set()
    for t in threads:
        t.join()
    for p in planes:
        if not p.up.is_set():
            p.start()

    failures = []
    # Each Plane takes one more commit: it catches up with everything the others did.
    for p in planes:
        c = load_catalog(f"final{p.n}", type="rest", uri=p.uri, **PROPS)
        try:
            c.load_table(f"{NS}.customer").append(rows(CUSTOMER, [(10_000_000 + p.n, "final")]))
        except Exception as e:  # noqa: BLE001
            failures.append(f"plane {p.n} refused its final commit: {e}")
        degraded = p.status().get("degraded")
        if degraded:
            failures.append(f"plane {p.n} has degraded domains: {degraded}")
        for t in ("customer", "orders"):
            r = subprocess.run([CLI, "--url", p.uri, "verify", f"{NS}.{t}"], capture_output=True, text=True)
            if r.returncode != 0:
                failures.append(f"plane {p.n}: verify {NS}.{t} failed: {r.stdout[-500:]}")

    customers = upstream.load_table(f"{NS}.customer").scan().to_arrow().to_pylist()
    orders = upstream.load_table(f"{NS}.orders").scan().to_arrow().to_pylist()
    cids = [r["customer_id"] for r in customers]
    oids = [r["order_id"] for r in orders]
    if len(cids) != len(set(cids)):
        failures.append(f"duplicate customer ids: {len(cids) - len(set(cids))}")
    if len(oids) != len(set(oids)):
        failures.append(f"duplicate order ids: {len(oids) - len(set(oids))}")
    dangling = [r for r in orders if r["customer_id"] not in set(cids)]
    if dangling:
        failures.append(f"orders without their customer: {dangling[:5]}")
    if any(r["name"] is None for r in customers):
        failures.append("customers without a name")
    if stats["unexpected"]:
        failures.append(f"{stats['unexpected']} unexpected client errors")

    for p in planes:
        p.kill()
    print("FINAL", json.dumps(dict(stats), sort_keys=True))
    print(f"FINAL rows: {len(cids)} customers, {len(oids)} orders")
    for e in errors[:20]:
        print("ERROR", e)
    for f in failures:
        print("FAIL", f)
    print("SOAK", "FAILED" if failures else "PASSED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
