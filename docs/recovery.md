# Recovery

> Normative source: spec §16 and §19, [RFC 0004](rfc/0004-transaction-log-v1.md),
> [ADR 0011](adr/0011-registry-anchors-and-rebuild.md) for rebuild.

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
- `VALIDATED` and `COMMITTING` of a commit about to be forwarded are written in one durable write
  (the same two records).
- After the upstream answer: success ⇒ indexes applied at the transaction's epoch, all indexes in
  one atomic transaction, then `COMMITTED`; a 4xx ⇒ `ABORTED`; a 5xx or no answer ⇒ resolved as below.
- Terminal records store the response, returned again for a repeated `Idempotency-Key`.

## Resolution (on start and before every commit)

| Last record | Action |
|---|---|
| `PREPARED` | `ABORTED` (interrupted during validation) |
| `VALIDATED` | `ABORTED` (never forwarded) |
| `COMMITTING` | reload the table: final snapshot present ⇒ re-apply the staged deltas (idempotent) and `COMMITTED`; absent ⇒ `ABORTED`; upstream unreachable ⇒ commits answer `RECOVERY_REQUIRED` (409) until it answers |

Re-applying is safe because an index accepts the same `(staged delta, epoch)` twice (ADR 0003). The
indexes of one commit are applied in one atomic transaction, so a crash leaves all of them updated
or none.

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

## Degraded domains and rebuild

A domain refuses commits with `INDEX_DEGRADED` (423) when an index apply failed in this process
(until recovery completes it) or, persistently in the registry, after `BYPASS_DETECTED` or a rebuild
that found violations. `POST /v1/integrity/indexes/{id}/rebuild`:

1. resolves the unfinished transactions of every domain;
2. loads every table of the domain of constraint `id` and pins its `main`;
3. validates all data, FK parents first, against in-memory indexes;
4. on violations, leaves the domain degraded and returns `ONBOARDING_VIOLATIONS` with a report;
5. otherwise replaces each index's contents at a new epoch (`replace_all`, one atomic transaction
   per index), then records the new anchors and clears the degraded state in one registry
   transaction, and audits `INDEX_REBUILT` with the previous and new anchors.

A crash between steps 5's index replacements leaves indexes whose contents already match the pinned
data (no commit runs during a rebuild) and the registry unchanged; running the rebuild again is safe.
Onboarding (registering a constraint) runs the same scan. A kill between two index replacements
(fault point `DuringRebuildSwap`) is covered by `tests/crash.rs`.

## Lost control-store files

| File lost (deleted and recreated empty) | Detected by | Effect |
|---|---|---|
| `indexes.redb` | store identity differs from the one the registry recorded | every enforced table degraded until its domain is rebuilt |
| `txn.redb` | the audit log names transaction ids the new log never assigned | as above (unapplied commits may be missing from the indexes) |
| `registry.redb` | not detectable from the other files | constraints from the configuration file are re-imported; tables with data must be onboarded again |

"Delete index ⇒ rebuild ⇒ identical verdicts" (spec §27) is `a_lost_index_store_is_refused_until_rebuilt_with_identical_verdicts`
in `tests/admin.rs`.
- Not tested: power loss (a process kill keeps the OS page cache).
