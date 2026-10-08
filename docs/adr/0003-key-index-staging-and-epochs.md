# 0003. KeyIndex staging, epochs and replay

- Status: Accepted
- Date: 2026-10-08

## Context

Spec §13.2 fixes the `KeyIndex` shape (`get_many`, `stage`, `apply`, `epoch`) and requires `apply`
to be atomic and idempotent per `(txn_id, epoch)` so that recovery (§16) can replay it. It leaves
open what `stage` checks, when `apply` refuses, and how idempotence is detected. These rules must
hold for the in-memory backend (Phase 2) and the persistent backend (Phase 4) alike.

## Decision

- **One index instance per constraint.** A PK/UNIQUE constraint has a `Unique` index
  (key → `last_snapshot`); an FK has a `Reference` index (parent key → `child_count`). The FK id in
  the spec's `(parent key, fk id)` key is implicit in the instance.
- **`stage` resolves, never mutates.** It takes an `IndexDelta { snapshot, changes: NetDelta }`
  and returns the exact value to write per key, plus the epoch it was resolved against. It fails if
  the delta is inconsistent with the index: a unique key added while present, removed while absent,
  or changed by anything other than ±1; a reference count going negative or overflowing. The
  validator must already have rejected such commits, so these errors mean corruption or a bug, and
  callers fail closed.
- **`apply(staged, epoch)`**:
  - succeeds and moves the index to `epoch` only if `epoch` is greater than the current epoch **and**
    the index is still at the staged base epoch (otherwise `StaleStage`: the values were computed
    against contents that no longer exist);
  - if `epoch` is not newer, succeeds as a no-op only when `(epoch, staged)` equals the most recently
    applied pair; anything else is `EpochConflict`.
  Epochs need not be consecutive, so one domain-wide epoch counter can drive many indexes.
- **Replay identity is the staged content**, not just the epoch. Accepting any delta whose epoch is
  already applied would silently drop a different delta, so the backend compares the full staged
  writes (in memory) or a digest of them (persistent backend, Phase 4).
- **Only the last apply is replayable.** Recovery has at most one unresolved transaction per domain
  (spec §16), so older replays are conflicts.
- **Errors never contain key values** (keys may be PII).

## Consequences

- A crash between `stage` and `apply` loses nothing: staging has no effect.
- Phase 4's persistent backend must persist `(epoch, staged digest)` atomically with the writes.
- Unique `last_snapshot` is updated only when a key is newly added; a copy-on-write rewrite nets to
  zero and leaves it unchanged.

## Alternatives considered

- *Epoch must be exactly current + 1*: simpler to reason about, but couples every index to every
  commit in its domain.
- *Idempotence by epoch alone*: rejected as fail-open (see above).
