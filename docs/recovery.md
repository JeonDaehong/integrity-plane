# Recovery

> Normative source: spec §16 and §19, [RFC 0004](rfc/0004-transaction-log-v1.md). Rebuild (§19) is
> Phase 10.

## Transaction log

Every commit to a constrained table that moves `main` is a transaction in `txn.redb` (control store):

```text
PREPARED → VALIDATED → COMMITTING → COMMITTED
   │           │            └──────→ ABORTED
   │           └───────────────────→ ABORTED
   ├──→ REJECTED   (validation failed)
   └──→ ABORTED
```

- `VALIDATED` is written, durably, together with the staged index deltas (the exact values to
  write) before anything is forwarded.
- `COMMITTING` is written immediately before the request goes upstream. A transaction in
  `VALIDATED` was therefore never forwarded.
- After the upstream answer: success ⇒ indexes applied at the transaction's epoch, then
  `COMMITTED`; a 4xx ⇒ `ABORTED`; a 5xx or no answer ⇒ resolved as below.
- Terminal records store the response, returned again for a repeated `Idempotency-Key`.

## Resolution (on start and before every commit)

| Last record | Action |
|---|---|
| `PREPARED` | `ABORTED` (interrupted during validation) |
| `VALIDATED` | `ABORTED` (never forwarded) |
| `COMMITTING` | reload the table: final snapshot present ⇒ re-apply the staged deltas (idempotent) and `COMMITTED`; absent ⇒ `ABORTED`; upstream unreachable ⇒ commits answer `RECOVERY_REQUIRED` (409) until it answers |

Re-applying is safe because an index accepts the same `(staged delta, epoch)` twice (ADR 0003): a
crash between two index applies leaves one index updated and one not, and recovery completes the
second without touching the first.

## Verified by fault injection

`crates/integrity-server/tests/crash.rs` (feature `fault-injection`) runs the gateway binary, makes
it abort at each fault point, restarts it, commits again, and checks that the persistent indexes
hold exactly the keys of the snapshots the upstream table contains and that no transaction is left
unresolved:

| Fault point | Upstream outcome | Result |
|---|---|---|
| `AfterPreparedLog`, `AfterValidatedLog`, `BeforeUpstream` | not reached | aborted, indexes unchanged |
| `AfterUpstreamBeforeLog`, `DuringIndexApply`, `BeforeCommittedLog` | committed | indexes completed by recovery |
| `AfterUpstreamUnknown` | committed, answered 500 | indexes completed by recovery |
| `AfterUpstreamUnknown` | refused, answered 500 | aborted, indexes unchanged |

Moving the `COMMITTING` record after the upstream call, or skipping the re-apply, makes this test
fail: upstream holds the snapshot while the indexes miss its keys.

## Index store

- An index `apply` is atomic (entries and metadata in one two-phase-commit transaction) and
  replayable (ADR 0003, ADR 0005).
- On open, the index store is fully checksum-verified; failure, a repair, or a storage-engine panic
  yields `Corrupt`, and the domain must be rebuilt (§19), never trusted.
- Not tested: power loss (a process kill keeps the OS page cache).
