# 0005. Persistent index backend: redb

- Status: Accepted (performance confirmation pending, see below)
- Date: 2026-10-08
- Resolves: spec Appendix B.5 for correctness requirements; the benchmark comparison is deferred
  to Phase 11.

## Context

Spec §13.4 requires an embedded persistent backend with atomic batch writes, ordered iteration,
crash safety and checksums, chosen among `fjall`, `redb` and RocksDB, and never exposed in public
APIs. `CONTRIBUTING.md` rule 4 requires checking license, maintenance, MSRV and native dependencies.

Candidates as published on crates.io on 2026-10-08:

| Crate | Version | License | MSRV | Native code | Structure |
|---|---|---|---|---|---|
| `redb` | 4.3.0 (2026-09-15) | MIT OR Apache-2.0 | 1.90 (2.6.x: 1.85) | none | copy-on-write B-tree, one file |
| `fjall` | 3.1.12 (2026-10-03) | MIT OR Apache-2.0 | 1.90 | none | LSM tree |
| `rocksdb` | 0.25.0 (2026-08-16) | Apache-2.0 | 1.88 | C++ RocksDB via `librocksdb-sys` | LSM tree |

From redb's design document: every write is checksummed (a Merkle tree of XXH3-128); a commit is
one `fsync` (1PC+C) or two (2PC); after a crash, a commit slot that fails verification is rolled
back to the previous one. The document recommends 2PC when stored data may be attacker-influenced,
because XXH3 is not collision-resistant.

## Decision

Use **redb**, behind `PersistentStore` / `PersistentIndex` in `integrity-index`; no redb type is
public.

- **Why redb.** Pure Rust (no C++ toolchain, which matters for reproducible builds and for the
  Windows GNU toolchain used in development), checksummed pages with documented crash recovery,
  atomic multi-table write transactions, ordered iteration, a single file. Index workloads are
  read-heavy point lookups with one writer per domain, which suits a B-tree.
- **MSRV.** redb 4.x needs Rust 1.90, so the workspace MSRV is raised from 1.85 to 1.90 (allowed by
  ADR 0001). The 2.6.x line still supports 1.85 and is still patched, but redb 3.0 changed the file
  format; starting on 2.x would mean a format migration later.
- **Why not fjall.** Equally pure Rust and, after the MSRV raise, equally buildable. Its LSM design
  optimizes write throughput, which our workload is not bound by, at the cost of compaction and
  read amplification on point lookups. Not excluded: Phase 11 benchmarks compare the two.
- **Why not RocksDB.** Native C++ dependency (build complexity, supply chain, longer CI) for no
  correctness benefit at our scale.
- **Layout.** One store file holds many indexes: an entries table per constraint
  (`EncodedKey` bytes → 9-byte value) and one metadata table (kind, epoch, last applied
  `(epoch, BLAKE3 digest of the staged delta)`). An `apply` is a single write transaction updating
  entries and metadata together, so the replay identity of ADR 0003 is durable with the data.
- **Durability.** `Durability::Immediate` with **two-phase commit** on every write transaction,
  because keys are user data.
- **Open-time verification.** `PersistentStore::open` runs redb's full integrity check. If it fails,
  or redb reports that it had to repair the file, the store is `Corrupt`: a repaired file may have
  rolled back applied commits, so the domain must be rebuilt (spec §19) rather than trusted.
- **Read-time validation.** Keys read back are validated by the RFC 0001 decoder and values by a
  strict decoder; anything malformed is `Corrupt`.
- **Panics.** Testing found that redb 4.3.0 panics (`unreachable!()` in `Btree::get_helper`, reached
  from `Database::create`) on a deliberately corrupted file instead of returning an error. Every call
  into redb therefore runs under `catch_unwind`; a panic yields `Corrupt` and poisons the store so
  later calls fail closed. The workspace pins `panic = "unwind"` in both profiles.
- **New dependencies:** `redb` (above) and `blake3` 1.8 (CC0-1.0 OR Apache-2.0 OR Apache-2.0 WITH
  LLVM-exception; built with the `pure` feature, so no C/assembly) for staged-delta digests. BLAKE3
  is also the certificate hash of spec §18.

## Consequences

- The same conformance suite and the validator's differential suite run on both backends.
- Crash tests kill a writer process mid-`apply` repeatedly and require the reopened store to equal
  the state after a whole number of applies, never fewer than acknowledged. A deliberately
  non-atomic `apply` (entries and metadata in separate transactions) fails that test in its first
  round. A process kill keeps the OS page cache, so power-loss behavior rests on redb's documented
  fsync protocol and is not tested here.
- Opening a large store is O(size) because of the full integrity check. If that becomes a problem,
  quick-repair mode is the alternative to evaluate, by ADR.
- Phase 11 benchmarks must compare redb against fjall on the spec §29 workloads; if redb is the
  bottleneck, switching is an internal change behind `KeyIndex`.

## Alternatives considered

See the table and the reasons above. Storing indexes in the transaction log itself was rejected:
indexes are derived state with a different lifecycle (rebuild, swap).

## Amendment: atomic multi-index application

`PersistentStore::apply_all` applies the staged deltas of several indexes of one store in a single
two-phase-commit transaction, each with the same idempotence rules as `apply`. The gateway applies
the indexes of a commit this way: one fsync per commit instead of one per index, and no state in
which some indexes of a commit are updated and others not. Looking up an existing index uses a read
transaction; a write transaction is opened only to create one.

## Amendment (Phase 11): store identity

The store file holds a 32-byte identity (`oip/index-store/v1`), created once when the file is
created. The registry records the identity its anchors were built with; a gateway that opens a
different one (the file was deleted and recreated, or swapped) marks every enforced table degraded
until its domain is rebuilt. Without this, an empty recreated store would accept any duplicate.
