# 0021. Sorted key deltas

- Status: Accepted
- Date: 2026-10-11
- Extends: ADR 0003, ADR 0019

## Context

With Spark on Amazon S3, inserting 1 M rows into a table with a PRIMARY KEY and a FOREIGN KEY took
11.4 s through the Plane against 0.74 s directly (`docs/benchmarks.md`). Index probes were a small
part of it. A profile showed the time in per-key work around the probes: every changed key was
inserted into a `BTreeMap` or cloned about six times (the commit's key multisets, `net()` computed
three times, the overlay's cache of base values and its pending writes, a map of base values built
per lookup, the staged writes), each lookup was a binary search, and every index value written
allocated.

The keys already arrive in key order: the external sorter (ADR 0019) yields them sorted, and every
later step walks them in that order.

## Decision

- `NetDelta` holds its changes as one slice in strictly increasing key order, shared on clone.
  `NetDelta::from_sorted` checks the order (anything else is `DeltaError::NotSorted`), `plus` merges
  two deltas. The validator's plan keeps one `NetDelta` per constraint, computed once; intra-commit
  duplicates are still recorded from the added multiplicities before netting (spec §8).
- `CommitScan::counts` appends the net change of each reported key; `decide` builds the deltas with
  `from_sorted`, so a caller that reports keys out of order is refused, not silently mis-validated.
  `CommitScan::counts_matter` says which keys need reporting at all: those added more than once or
  whose count changes. The gateway reports only those, so the merge output of a rewrite stays small.
- The gateway merges the two sorted sides of each constraint on its own thread and reports the
  results in constraint order.
- `StagedDelta` writes and the overlay's pending writes and cached base values are vectors in key
  order. Lookups start where the previous one ended (one comparison when keys come in order) and
  fall back to binary search; new entries are appended or merged in one pass.
- Index values are encoded without a heap allocation.

The transaction log record (RFC 0004) and the staged delta digest are unchanged: both were already
written in key order.

## Consequences

- The same commit does several times less work per changed key; see `docs/benchmarks.md`.
- Memory per changed key is lower (no tree nodes), but a commit's net change is still held in
  memory: an append of N rows holds about N keys per keyed constraint. Spilling large net deltas
  is possible future work.
- Writing the index (one B-tree insert per changed key in one transaction) is now the largest
  single cost of a large append.

## Alternatives considered

- **Keeping the maps and only removing redundant `net()` calls.** Removes part of the cost; the
  lookups and clones in the overlay remain.
- **Streaming the net delta to disk** instead of holding it: needed for appends larger than
  memory, but a larger change across the validator, the staged delta and the transaction log.
- **Answering the client before the index is written.** Would hide the index write from the
  commit's latency, but changes when a commit counts as done (ADR 0015, RFC 0004); not done here.
