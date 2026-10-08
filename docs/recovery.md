# Recovery

> **Status:** partial. Normative source: spec §16 and §19. The transaction log, state machine and
> recovery procedure are Phase 8; only index-level crash behavior exists so far.

## Index store (Phase 4)

- An `apply` is atomic: entries and the index metadata (epoch, last applied digest) are written in
  one durable two-phase-commit transaction. After a crash the store holds the state after a whole
  number of applies. Tested by killing a writer process mid-`apply`
  (`crates/integrity-index/tests/crash.rs`).
- Replaying the last `(staged delta, epoch)` after a restart is a no-op success; any other delta at an
  already applied epoch is `EpochConflict` (ADR 0003). This is what §16 recovery will rely on.
- On open the store's checksums are fully verified. A failed check, a repair, or a panic inside the
  storage engine yields `Corrupt`; the domain must then be rebuilt (§19), never trusted.
- Not tested: power loss (a process kill keeps the OS page cache).
