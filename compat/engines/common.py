"""Shared helpers for the Trino and Flink compatibility tests (compat/engines/).

The engines run in containers next to deploy/docker-compose.yml (compat/engines/docker-compose.yml)
and reach the Plane as http://plane:8181; these scripts run on the host, send SQL through
`docker exec` and read the Plane's status on 127.0.0.1:8181.
"""

import json
import subprocess
import time
import urllib.request

PLANE = "http://127.0.0.1:8181"


def sh(*args, stdin=None):
    """Runs a command; returns (exit code, stdout + stderr)."""
    out = subprocess.run(args, input=stdin, capture_output=True, text=True)
    return out.returncode, (out.stdout or "") + (out.stderr or "")


def trino_rows(ns, sql):
    """A Trino query's result as a flat list of values (stdout only: the CLI warns on stderr)."""
    out = subprocess.run(["docker", "exec", "trino", "trino", "--catalog", "gw", "--schema", ns,
                          "--output-format", "TSV", "--execute", sql],
                         capture_output=True, text=True)
    assert out.returncode == 0, f"{sql}: {out.stdout}{out.stderr}"
    return out.stdout.split()


def wait_until(what, check, seconds=180):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if check():
            return
        time.sleep(3)
    raise AssertionError(f"timed out waiting for {what}")


def commits(table):
    with urllib.request.urlopen(f"{PLANE}/v1/integrity/status") as r:
        return json.load(r)["commit_requests"].get(table, 0)


def integrity(*args):
    """The integrity CLI shipped in the Plane's image."""
    code, out = sh("docker", "exec", "integrity-plane", "integrity", "--url", PLANE, *args)
    assert code == 0, f"integrity {' '.join(args)}: {out}"
    return out


def key_constraints(ns):
    """customer(customer_id, name) / orders(order_id, customer_id), as in the other client tests."""
    integrity("constraints", "add", "--table", f"{ns}.customer", "--name", "pk_customer",
              "--type", "primary_key", "--columns", "customer_id")
    integrity("constraints", "add", "--table", f"{ns}.customer", "--name", "nn_customer_name",
              "--type", "not_null", "--columns", "name")
    integrity("constraints", "add", "--table", f"{ns}.orders", "--name", "pk_orders",
              "--type", "primary_key", "--columns", "order_id")
    integrity("constraints", "add", "--table", f"{ns}.orders", "--name", "fk_orders_customer",
              "--type", "foreign_key", "--columns", "customer_id",
              "--ref-table", f"{ns}.customer", "--ref-constraint", "pk_customer")


def expect_rejection(run, code, table, sql):
    """`run(sql)` returns (exit code, output). The statement must fail with `code` after exactly
    one commit attempt reached the Plane."""
    before = commits(table)
    status, out = run(sql)
    assert status != 0 or "[ERROR]" in out, f"{code}: statement succeeded: {sql}\n{out[-2000:]}"
    assert code in out, f"expected {code} for {sql}, got:\n{out[-3000:]}"
    attempts = commits(table) - before
    assert attempts == 1, f"{code}: {attempts} commit attempts reached the Plane for {sql}"
    print(f"ok   {code} rejected after one attempt: {sql}", flush=True)


def expect_ok(run, sql):
    status, out = run(sql)
    assert status == 0 and "[ERROR]" not in out, f"failed: {sql}\n{out[-3000:]}"
    return out
