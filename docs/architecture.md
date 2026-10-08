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
| `integrity-iceberg` | Parquet key-column extraction by field ID (Phase 5); commit inspection is Phase 6 |
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
