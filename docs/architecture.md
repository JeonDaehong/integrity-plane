# Architecture

> Normative source: spec Part IV (§10–§13). This page records what exists so far.

## Crates and layering

The ten crates of spec §12 exist. Dependency direction is strictly downward
(`server → iceberg/txn/validator → index → core → types`) and is enforced for `integrity-types` /
`integrity-core` by `ci/check-layering.sh`. See [ADR 0001](adr/0001-workspace-layout-and-ci-gates.md).

| Crate | Status |
|---|---|
| `integrity-types` | Ids, constraint set version, error codes (Phase 1) |
| `integrity-core` | Constraint model, §7 NULL classification, key encoding ([RFC 0001](rfc/0001-key-encoding-v1.md)), key deltas (Phase 1) |
| `integrity-index` | `KeyIndex` trait, in-memory `MemoryIndex` (Phase 2), persistent `PersistentIndex` on redb (Phase 4) |
| `integrity-reference` | Relational oracle and shared proptest strategies (Phase 2) |
| `integrity-validator` | Validation planner and PK/UNIQUE/NOT NULL/FK validation (Phase 3) |
| `integrity-iceberg` | Parquet key extraction (Phase 5); metadata model, requirement checks, §15 classification, manifest diff (Phase 6) |
| `integrity-txn` | Durable transaction log, state machine, fault points (Phase 8) |
| `integrity-server` | REST gateway (Phase 7), transaction log and recovery (Phase 8) |
| others | Empty skeletons |

## Indexes (§13)

One index instance per constraint: a `Unique` index per PK/UNIQUE (key → `last_snapshot`), a
`Reference` index per FK (parent key → `child_count`). Keys and counts only, never row locations.

Changes are two-step. `stage` resolves a net delta against the current contents into exact writes
with no visible effect; `apply(staged, epoch)` makes them visible atomically, only while the index is
still at the staged base epoch, and is idempotent for the last `(staged, epoch)` pair. Rules and
rationale: [ADR 0003](adr/0003-key-index-staging-and-epochs.md).

`MemoryIndex` is a `BTreeMap` behind an `RwLock` and is not durable. `PersistentIndex` stores many
indexes in one redb file; each `apply` is one two-phase-commit write transaction covering entries and
metadata, the file is fully checksum-verified on open, and any storage-engine panic is converted into
a fail-closed `Corrupt` error ([ADR 0005](adr/0005-persistent-index-backend.md)). Both backends share
the staging and apply-decision code and pass the same conformance suite.

## Reference oracle (spec §28)

`integrity-reference` keeps every table in full and, for each commit, evaluates every enforced
constraint over the entire post-commit state by direct value comparison. It does **not** use key
encoding, key deltas, `classify` or indexes, so it shares no verdict logic with the engine it checks;
it reuses only the constraint model and registration checks.

- Verdict: `Accepted`, or `Rejected` with the full set of violated `(constraint, code)` pairs. A
  rejected commit changes nothing.
- A child row without a parent is `INT-005 FOREIGN_KEY_VIOLATION` when the commit wrote the child
  table and `INT-006 REFERENCED_ROW_DELETE` when it wrote the parent table. Because commits touch
  one table (spec §15) and the pre-commit state was valid, that attribution is exact.
- Registration runs the same evaluation over existing rows and fails with the violations
  (`ONBOARDING_VIOLATIONS`, spec §20).

Agreement between the oracle and `integrity-core` on every §7 row is property-tested in
`crates/integrity-reference/tests/oracle_vs_core.rs`.

## Validator (spec §8, §13.3)

`Validator::validate(CommitRows, indexes)` decides a single-table commit from the rows it adds and
removes, projected onto `Validator::projection(table)`. It classifies tuples (§7), counts added-key
multiplicities before probing (§8), then issues one batched lookup per index over distinct keys:
PK/UNIQUE keys whose count changes, FK child keys whose count rises, and parent keys that disappear.
It returns every violated `(constraint, code)` or the index deltas to stage; anything it cannot prove
is an error and the commit is rejected. Rules and rationale:
[ADR 0004](adr/0004-validator-verdicts-and-probes.md).

The Phase 3 exit test, `crates/integrity-validator/tests/differential.rs`, runs random operation
sequences through the validator (with in-memory indexes) and the oracle: verdicts must be identical
at every step, and the live indexes must equal indexes rebuilt from the oracle's final rows.

## Key extraction (spec §14 step 5)

`integrity_iceberg::extract_rows` reads only the requested top-level columns of a Parquet data file,
matched by Iceberg field ID, and returns them as a `RowBatch` typed by the table schema. Files without
field IDs, duplicate IDs, nested key fields and incompatible physical types are rejected; a field the
file does not contain reads as NULL. Rules and rationale:
[ADR 0006](adr/0006-parquet-key-extraction.md).

## Commit inspection (spec §14 steps 3–5, §15)

`integrity-iceberg` turns a REST `CommitTableRequest` into work for the validator: requirements are
checked against current metadata, updates are classified (pass-through, or `main` advancing through
a chain of new snapshots, or rejected; [ADR 0007](adr/0007-iceberg-commit-inspection.md)), and each
new snapshot is diffed against its parent by the live files of the manifests that differ, without
trusting client-written status or counts ([ADR 0008](adr/0008-manifest-diff.md)). The rows of added
and removed Parquet files then become `CommitRows`. All file reads go through `FileIo` with an
optional byte budget.

## Certificates (spec §18)

`integrity_core::certificate` implements [RFC 0002](rfc/0002-certificate-format-v1.md): a BLAKE3 digest
chaining each certified `main` snapshot to its parent's certificate, over the table UUID, the snapshot
ids, the digest of the constraints governing the table, and the digest of the snapshot's net key
changes per constraint (recomputable from its data files). `integrity_iceberg::inject_certificate`
writes it into the new snapshot's summary in the forwarded request; `snapshot_certificate` reads it
back. A chain starts at the table's anchor, recorded by the registry when the table was first bound
empty or when its domain was onboarded or rebuilt ([ADR 0011](adr/0011-registry-anchors-and-rebuild.md)).

## Gateway (spec §14, §22)

`integrity-server` is a proxy REST catalog ([ADR 0010](adr/0010-proxy-gateway.md)). A commit to a
table that has constraints, or is referenced by one, is handled in its domain's queue: every table of
its FK-connected domain is loaded and checked against its anchor (a table with data and no anchor
must be onboarded; a `main` head that is neither the anchor nor certified means a writer bypassed the
Plane, which refuses with `BYPASS_DETECTED` and degrades the domain), requirements are checked, the commit is classified, each new `main` snapshot is
validated against an overlay of the persistent indexes, certificates are injected, and the request is
forwarded. Every step is recorded in the transaction log (RFC 0004, `docs/recovery.md`): index changes are
applied only after the upstream commit succeeds, a crash at any point is resolved on restart, and a
repeated `Idempotency-Key` gets the recorded answer. Decisions use the status mapping of
[RFC 0003](rfc/0003-http-status-mapping.md). Everything else, including table creation and loads, is
forwarded unchanged; `/v1/config` is stripped of `uri` overrides and idempotency support.

## Registry, onboarding and rebuild (spec §19, §20, §23)

Constraints live in `registry.redb` in the control store, with per-table constraint set versions, the
constraint set of every version, anchors, degraded domains and the audit log
([ADR 0011](adr/0011-registry-anchors-and-rebuild.md)). Constraints in the configuration file are imported
once, when the registry is created. `POST /v1/integrity/constraints` resolves column names to field ids,
scans the whole domain at its current snapshots (FK parents first) and either installs the resulting
index contents and anchors or refuses with `ONBOARDING_VIOLATIONS` and a report. `DELETE` drops a
constraint (not a key an FK still references); `POST /v1/integrity/indexes/{id}/rebuild` rescans the
domain of a constraint and re-anchors it, clearing a degraded state if the data is valid. These
operations take a lock that every commit holds shared, so they never interleave with commits.

## Verification and observability (spec §18, §23, §25)

`GET /v1/integrity/verify?table=` walks `main` back from its head to the chain start (an uncertified
current or retired anchor, or the first snapshot) and recomputes every certificate from the data:
the constraint set of the version named in the snapshot summary (from the registry's version
history), the key delta of the snapshot's file changes (`Validator::net_key_deltas`, checked against
validation in the differential tests), and the parent's certificate. Each snapshot is `OK`,
`ANCHOR`, `MISSING` (written without the Plane), `MISMATCH` (forged or tampered), `MALFORMED` or
`UNVERIFIABLE` (equality deletes; expired history). A broken chain degrades the domain like a
bypass detected at commit time.

Also served: `GET /v1/integrity/transactions/{id}` (the log's records and decision),
`GET /v1/integrity/domains/{table}` (`Healthy`, `Degraded` with reasons, or `RecoveryRequired`),
`GET /v1/integrity/audit?table=&since=`, and Prometheus text at `/metrics` (commit verdicts,
validation and queue wait time, recovery outcomes, bypasses, domain states). Metrics carry no table
names or key values.

## Integrity domains and concurrency (spec §11)

A domain is the FK-connected component of a table among the configured constraints. Each domain
has one commit queue (an async lock): validation, publication upstream and index application of a
commit happen while holding it, so the "child insert vs. parent delete" race cannot occur. Commits in
different domains run in parallel. Recovery before a commit only resolves that domain's unfinished
transactions; another domain may be publishing its own at the same moment.

`crates/integrity-server/tests/concurrency.rs` runs, over twelve seeds, concurrent order inserters on a
hot parent key, a deleter and re-inserter of that parent, and child cleanup, with engine-like clients
that retry on 409. The upstream's global commit order is replayed and PK and FK are checked after
every commit; the indexes must equal the final data. A second test holds one domain's upstream commit
and checks that another domain commits meanwhile while the held domain stays serial.
