# 0025. Bounded memory after the merge: deciding, staging, logging and applying a commit in chunks

- Status: Accepted
- Date: 2026-10-14
- Implements: RFC 0006 (chunked staged deltas)
- Amends: ADR 0019 (what happens after the merge), ADR 0003 (apply from a stream)

## Context

ADR 0019 keeps the memory of reading and sorting a commit bounded. After the two sorted sides of a
constraint are merged, the gateway still holds every changed key several times: the merged list
(`merge_sides` → `Vec<Counted>`), the net delta (`CommitScan::counts` → `NetDelta`), the probe keys
and results (`Validator::decide`), the overlay of the commit's steps with every value it read and
wrote (`Overlay`), the final staged deltas (`Overlay::into_staged`), their encoding for the log and
the staged deltas handed to `apply_all`. An append of `n` new keys needs about 120 bytes of memory
per key for staging alone (RFC 0006); with the copies above, an initial load of a few hundred million
rows does not fit one node.

redb is not the problem: one write transaction of 20 M inserts needs no memory beyond its cache
(RFC 0006, Motivation). Every structure above is produced and consumed in key order.

## Decision

A commit with **one step** (one new snapshot) and **no equality deletes** is decided, staged, logged
and applied as streams of chunks; nothing holds more than one chunk of keys per constraint at a time
beyond the sorters' budget (`limits.scan_memory`). These are the appends, initial loads, deletes,
rewrites and compactions engines write.

1. **Merged keys** go to a **run** per constraint instead of a list: `(key, added ‖ removed)`
   records of the keys that matter (`CommitScan::counts_matter`), in key order, held in memory up to
   the constraint's share of `limits.scan_memory` and appended to one file in the index store's
   scratch directory beyond it (`integrity_index::Run`). Each merge writes its own run on its own
   thread, as it did its list.
2. **The merge counts** on the way what the decision needs before reading the run: each
   constraint's number of changed keys, whether any key repeats among the added rows and whether
   a PK/UNIQUE key falls by more than one (`MergedKeysWriter`).
3. **`CommitScan::decide_streamed`** records the duplicates among the added rows first (spec §8,
   before any probe), in a pass of its own over the constraints whose merge saw any. Then one pass
   per constraint, in `Validator::decide`'s order, reads the run at most `chunk` (100 000) keys at a
   time: it probes the batch against the indexes directly (no overlay: a one-step commit has nothing
   earlier to see), records violations, and feeds the key delta digest (RFC 0002,
   `KeyDeltaDigest`; the validator keeps constraints in id order, as the digest takes them). The
   referencing constraints follow, so the same violations are recorded and the same inconsistency
   is reported first as deciding at once.
4. **Violation details** keep their samples and a count; the merged keys are distinct, so each is
   counted once (`add_distinct_key`), and a key already recorded as an intra-commit duplicate is not
   recorded again when the index also has it, as the set of keys did before.
5. **Staging** happens in the same pass while nothing is violated: each batch is resolved against
   its current values (the probe's for a unique index, the foreign key's own index otherwise) by the
   resolution every backend uses (`integrity_index::stage_from`), into a staged run with its
   `StagedHeader`. Once anything is violated, staging stops; the epoch check of
   `ValidatedDeltas::stage` runs before an acceptance is returned.
6. **The log** (RFC 0006) gets each staged run in chunks: every chunk but the last of each index in
   a durable write of its own before `VALIDATED`, the last ones with `VALIDATED` and `COMMITTING`.
7. **Apply** writes the staged runs into one index transaction (`PersistentStore::apply_streamed`),
   checking the digest before committing; recovery reads the logged chunks back and does the same.
   The ADR 0003 idempotence check compares the logged digest.

Commits with several steps, and commits with equality deletes, keep the current path: their memory
still grows with the keys they change. Engines write them for multi-snapshot REST transactions and
for equality-delete upserts (refused together with removed data files, ADR 0009), not for bulk
loads; `docs/limitations.md` says so.

The streamed decision is tested against deciding at once on every commit of the validator's
differential suite (in-memory and persistent indexes) and of `commit_scan.rs` (real Parquet files),
at chunk sizes 1 to 3 and budgets that spill every run or none: same decision, same violations
(count and samples) or error, same key delta digest, same staged headers and writes. The gateway
tests and the crash suite run it at the default chunk size.

## Consequences

- An append's memory no longer grows with its keys; scratch disk grows with them instead (merged
  keys and staged writes: about the key bytes twice, plus about 35 bytes per key), and the log holds
  the staged writes until the commit is resolved (RFC 0006).
- Small commits keep one chunk per index: no file is written below the budget, and the log still
  takes one durable write.
- Two decision paths exist for keyed constraints: `Validator::decide` (several steps, equality
  deletes, and the reference) and the streamed one. The differential tests keep them equal.

## Alternatives considered

- **Spilling the overlay** so that multi-step commits are bounded too: lookups into earlier steps'
  writes become merge-joins of sorted runs, and the final staged deltas a k-way merge of steps. It
  doubles the work for commits no bulk writer produces; it can follow if one does.
- **Spill-backed `NetDelta` and `StagedDelta`** behind the existing types: the overlay looks values
  up by binary search, which a file cannot serve; every consumer would still assume random access.
- **Refusing commits above a key count** (fail closed, `UNSUPPORTED_COMMIT_OPERATION`): bounded, but
  forces writers to split initial loads, which engines cannot do on request.
