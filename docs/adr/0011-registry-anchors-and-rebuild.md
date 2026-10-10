# 0011. Constraint registry, chain anchors, onboarding and rebuild

- Status: Accepted
- Date: 2026-10-09

## Context

Phase 10 replaces constraints from the configuration file with a registry managed through the
integrity API (spec §23), runs an onboarding scan when a constraint is registered on a non-empty
table (§20), rebuilds a domain's indexes on demand (§19), and detects writers that bypassed the
Plane (§18). Until now a commit whose parent had no certificate was treated as a chain root, so a
direct upstream write was silently absorbed.

## Decision

**Registry.** `registry.redb` in the control store holds one checksummed document (constraints,
per-table `ConstraintSetVersion`, the constraint sets of every version, chain anchors, persistent
degraded reasons) and an append-only audit table. The document is rewritten in one two-phase-commit
transaction per change. Constraints in the configuration file are imported only when the registry is
first created; afterwards the registry is the source of truth.

**Administrative exclusion.** Registration, drop and rebuild take an exclusive lock that every
commit to a constrained-or-not table holds shared from routing to response. Constraint changes
therefore never interleave with commits, and domain membership cannot change under a commit. Cost:
commits pause while an onboarding scan runs. Per-domain administrative locking is deferred.

**Anchors.** An anchor records, per table, its UUID and its `main` snapshot at the moment the Plane
started (or restarted, after a rebuild) enforcing its constraints. It is written:

- when a table is bound while empty (no snapshot);
- by onboarding and rebuild, for every table of the domain, at the snapshot that was scanned.

A table with data and no anchor is not enforced (`INDEX_DEGRADED`, onboarding required).
A table whose UUID differs from its anchor was replaced and is refused the same way.

**Bypass detection.** Before validating a commit, the head of `main` of every table of the domain
must be the anchor snapshot or carry a certificate. Otherwise the commit fails with
`BYPASS_DETECTED` (423), the domain is marked degraded in the registry (it survives restarts), and
the event is audited. Checking every member, not only the written table, matters: a child commit is
validated against the parent's index, which a bypassed parent write has made stale.

**Chain roots.** The certificate of a commit whose parent is the anchor and carries no certificate
uses 32 zero bytes as `cert_{n-1}` (RFC 0002 chain root). A certified parent always contributes its
certificate, including across constraint set versions.

**Onboarding and rebuild** share one scan:

1. recover the domain's unfinished transactions;
2. load every table of the domain and pin its `main`;
3. order tables so that FK parents come before children (a cycle between distinct tables is refused;
   a self-reference is allowed);
4. read every live data file's constrained columns and validate each table as one insert into
   empty indexes, so a parent's keys are present when its children are checked (with bounded
   memory, see the amendment below);
5. on violations: registration fails with `ONBOARDING_VIOLATIONS` (400) and a report naming each
   violated constraint; a failed rebuild leaves the domain degraded;
6. otherwise install every index's new contents at one new epoch in one transaction, then update
   the registry (constraint, versions, anchors, degraded cleared) in one transaction.

Tables with equality delete files are refused by the scan in 0.1; position deletes are applied
(ADR 0017). A crash before the install leaves the live indexes and the registry unchanged; a crash
between the install and the registry update leaves indexes whose contents already equal what the
scan derived from the pinned snapshots (no commit can run meanwhile). Repeating the operation is
safe either way.

**Rebuild re-certification.** The audit event `INDEX_REBUILT` records each table's previous anchor
and new anchor, linking the old chain to the new one.

## Consequences

- Direct upstream writes are refused at the next commit to the domain instead of being certified.
  An operator must inspect (`verify`) and rebuild to resume.
- Violation reports name constraints and codes; sample keys and file locations are not reported in
  0.1.
- The integrity API is protected by an optional bearer token (`[server] admin_token`); deployments
  must keep it private either way (`docs/threat-model.md`).

## Alternatives considered

- **Parent-only check.** Checking only the written table's parent misses a bypassed write to an FK
  parent table, which makes child validation unsound.
- **Trusting index epochs as the onboarding marker** (Phase 7–9 behaviour). Epochs say nothing about
  which snapshot the index reflects; anchors do.
- **Per-domain admin locks.** Needed when onboarding large tables must not stall unrelated domains;
  deferred because domain membership changes during registration make lock ordering subtle.

## Amendment: bounded-memory scan (2026-10-10)

The first scan validated each table against in-memory indexes and handed every index's full
contents to `replace_all`. It needed about 250–340 bytes of memory per key (15 GB for 60 M keys),
so one node could not onboard much more than 100 M keys. The scan now keeps memory bounded whatever
the table size:

- **Rows** are read a row group at a time (`for_each_live_batch`, `for_each_batch_from`): the key
  column chunks of one row group, plus the deleted positions of the table's position delete files
  and deletion vectors.
- **Validation** is split (`Validator::table_scan`, `TableScan`): the index-free part (NULL rules,
  spec §7) runs on each batch and emits every key; the keys of each PK/UNIQUE/FK constraint go to an
  external sorter (`KeySorter`) that keeps at most `limits.scan_memory` bytes (default 512 MiB,
  shared by the table's constraints) in memory and spills sorted, collapsed `(key, count)` runs to
  the index store's scratch directory, merging at most 64 runs at a time.
- **Checks on sorted keys:** a PK/UNIQUE key that occurs more than once is a duplicate; an FK key is
  looked up in the parent's new contents, in batches; the others become index entries
  (`last_snapshot` = the pinned snapshot, `child_count` = occurrences). Each offending key is
  reported once, in key order, so violation reports (counts and samples) are exactly those of
  validating the table as one commit.
- **Index contents** are written in key order into build tables beside the live tables, in
  transactions of 100 000 entries (`PersistentStore::build`). `PersistentStore::install` then
  renames every build table over its live table and moves all indexes to one new epoch in a single
  durable transaction (spec §19: "build index-v{n+1} beside index-v{n}, atomic pointer swap"),
  replacing `replace_all`, which was atomic only per index. Readers see the old indexes until then.
  Violations or errors discard the builds; a build left by a crash is discarded by the next build
  of the same index, and sort runs left by a crash are deleted when the store is opened.

The previous scan is kept as `onboard::scan_in_memory`, the reference: a property test
(`tests/onboard_scan.rs`) runs both on random domains written as real Parquet and Avro files, with
several row groups per file and budgets small enough to spill many runs, and requires identical
reports or identical installed indexes. `TableScan` is checked the same way against
`validate_with_details` (`integrity-validator/tests/table_scan.rs`).

Disk: the scratch directory needs about the key bytes plus 12 bytes per distinct key per
constraint while a table is scanned; build tables need about the size of the new index until the
install frees the old one. Sort runs hold key values (spec §24) and are deleted after each table.
