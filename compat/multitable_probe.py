"""Probes an Iceberg REST catalog's multi-table commit endpoint (RFC 0012).

Usage: python compat/multitable_probe.py URI [PREFIX] [TOKEN]

Creates namespace `mtprobe` with tables `a` and `b`, then checks that `/transactions/commit`
1. exists and applies a change to `a` with a requirement-only entry for `b` (no updates),
2. is atomic: a failed requirement on `b` leaves `a` unchanged,
3. checks `assert-ref-snapshot-id` on a table that has no update.
Prints one line per check and exits 1 if any fails.
"""

import json
import sys
import urllib.error
import urllib.request

uri = sys.argv[1].rstrip("/")
prefix = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else ""
token = sys.argv[3] if len(sys.argv) > 3 else ""
base = f"{uri}/v1/{prefix}/" if prefix else f"{uri}/v1/"


def call(method, path, body=None):
    req = urllib.request.Request(base + path, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data, timeout=30) as r:
            text = r.read().decode()
            return r.status, json.loads(text) if text else None
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except ValueError:
            return e.code, text


schema = {"type": "struct", "schema-id": 0,
          "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}]}
call("POST", "namespaces", {"namespace": ["mtprobe"]})
for t in ("a", "b"):
    call("DELETE", f"namespaces/mtprobe/tables/{t}")
    # Nessie requires stage-create.
    s, r = call("POST", "namespaces/mtprobe/tables",
                {"name": t, "schema": schema, "stage-create": False})
    assert s == 200, (s, r)


def uuid(t):
    return call("GET", f"namespaces/mtprobe/tables/{t}")[1]["metadata"]["table-uuid"]


def prop(t):
    return call("GET", f"namespaces/mtprobe/tables/{t}")[1]["metadata"].get("properties", {}).get("probe")


ok = True


def check(name, cond, detail):
    global ok
    ok &= bool(cond)
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f": {detail}"))


ident = lambda t: {"namespace": ["mtprobe"], "name": t}
change_a = lambda v: {"identifier": ident("a"),
                      "requirements": [{"type": "assert-table-uuid", "uuid": uuid("a")}],
                      "updates": [{"action": "set-properties", "updates": {"probe": v}}]}

# 1. A change to a, with a requirement-only entry for b.
s, r = call("POST", "transactions/commit", {"table-changes": [
    change_a("1"),
    {"identifier": ident("b"), "requirements": [
        {"type": "assert-table-uuid", "uuid": uuid("b")},
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}],
     "updates": []},
]})
check("endpoint applies a change with a requirement-only entry", s in (200, 204) and prop("a") == "1", (s, r))

# 2. Atomic: b's requirement fails, a must stay unchanged.
s, r = call("POST", "transactions/commit", {"table-changes": [
    change_a("2"),
    {"identifier": ident("b"), "requirements": [
        {"type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000000"}], "updates": []},
]})
check("a failed requirement on another table rejects the whole commit (409)",
      s == 409 and prop("a") == "1", (s, r, prop("a")))

# 3. assert-ref-snapshot-id on a requirement-only table: main of b is null, assert a snapshot id.
s, r = call("POST", "transactions/commit", {"table-changes": [
    change_a("3"),
    {"identifier": ident("b"), "requirements": [
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 12345}], "updates": []},
]})
check("assert-ref-snapshot-id is checked on a table without updates",
      s == 409 and prop("a") == "1", (s, r, prop("a")))

# 4. Isolation under concurrency: two transactions at once, each changing one table and asserting
# the other is unchanged. At most one of them may commit (otherwise the assertion on the table
# without updates was checked too early). Schema changes move `current-schema-id`.
import threading  # noqa: E402


def schema_change(t, other, other_schema_id, n):
    fields = [{"id": 1, "name": "id", "required": True, "type": "long"},
              {"id": 100 + n, "name": f"c{n}", "required": False, "type": "long"}]
    meta = call("GET", f"namespaces/mtprobe/tables/{t}")[1]["metadata"]
    return {"table-changes": [
        {"identifier": ident(t),
         "requirements": [{"type": "assert-current-schema-id",
                           "current-schema-id": meta["current-schema-id"]}],
         "updates": [{"action": "add-schema", "schema": {"type": "struct", "fields": fields},
                      "last-column-id": max(meta["last-column-id"], 100 + n)},
                     {"action": "set-current-schema", "schema-id": -1}]},
        {"identifier": ident(other),
         "requirements": [{"type": "assert-current-schema-id", "current-schema-id": other_schema_id}],
         "updates": []},
    ]}


both = 0
lost = 0
for n in range(1, 31):
    ma = call("GET", "namespaces/mtprobe/tables/a")[1]["metadata"]
    mb = call("GET", "namespaces/mtprobe/tables/b")[1]["metadata"]
    bodies = [schema_change("a", "b", mb["current-schema-id"], 2 * n),
              schema_change("b", "a", ma["current-schema-id"], 2 * n + 1)]
    results = [None, None]
    barrier = threading.Barrier(2)

    def run(i):
        barrier.wait()
        results[i] = call("POST", "transactions/commit", bodies[i])[0]

    threads = [threading.Thread(target=run, args=(i,)) for i in (0, 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if all(s in (200, 204) for s in results):
        both += 1
    # A transaction answered as committed must have changed its table.
    for i, (t, before) in enumerate((("a", ma), ("b", mb))):
        after = call("GET", f"namespaces/mtprobe/tables/{t}")[1]["metadata"]
        if results[i] in (200, 204) and after["current-schema-id"] == before["current-schema-id"]:
            lost += 1
check("concurrent transactions asserting each other's table: never both commit",
      both == 0, f"both committed in {both} of 30 rounds")
check("a transaction answered as committed was applied", lost == 0,
      f"{lost} acknowledged transactions left their table unchanged")

sys.exit(0 if ok else 1)
