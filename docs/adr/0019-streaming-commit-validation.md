# 0019. Streaming commit validation

- Status: Accepted
- Date: 2026-10-10
- Extends: ADR 0004, ADR 0011 (amendment: bounded-memory scan)

## Context

Commit validation read every row a commit adds and removes into memory (`commit_rows`), built a
multiset of keys per constraint for each side, and compared the rows of both sides of a `replace`
in a hash map. A copy-on-write rewrite or a compaction therefore needed memory proportional to the
rows it rewrites: about 5 GB for a compaction of 10 M rows, so rewriting 100 M rows at once could
not be validated on one node. Most of those rows cancel out: a compaction changes no key, and
deleting one row of a 1 M-row file changes one key.

## Decision

Commits without equality deletes are validated by streaming (`pipeline::Streamed`):

- **Rows** of both sides are read a row group at a time (`for_each_commit_batch`), in the order
  `commit_rows` would put them on each side.
- **Validation** is split (`Validator::commit_scan`, `CommitScan`): the index-free part (NULL rules
  on added rows, NULL-violating removed rows as inconsistencies) runs on each batch and emits every
  key with its side. Validation errors are kept per (constraint, side) and reported in the order
  validating all rows at once would meet them.
- **Keys** of each PK/UNIQUE/FK constraint go to one external sorter per side (`KeySorter`, within
  `limits.scan_memory` shared by all sorters of the commit, spilling to the index store's scratch
  directory). Merging the two sorted sides gives each distinct key once with its count on each
  side: an added count above one is an intra-commit duplicate (spec §8, before any probe), and only
  the net change of the key enters the plan. The probes and the decision are those of
  `validate_with_details`, unchanged.
- **`replace`** compares both sides' rows by sorting an injective encoding of each projected row
  (`encode_row`) per side and comparing the two sorted streams (`check_sides`).

Commits with equality deletes keep the previous path (`commit_rows`): which keys they remove
depends on index state, and they are refused together with removed data files anyway.

Equivalence is tested at three levels: `for_each_commit_batch` against `commit_rows` (random
snapshots with position deletes and deletion vectors on both sides, `integrity-iceberg/tests`),
`CommitScan` against `validate_with_details` on every commit of the validator's differential
suite (any batch size, either side first), and the whole streamed step against reading it whole on
real files with tiny memory budgets (`integrity-server/tests/commit_scan.rs`).

## Consequences

- Validation memory no longer grows with the rows a commit rewrites; disk for sort runs does (about
  the key bytes of both sides, plus the projected rows of a `replace`).
- Small commits pay for a few sorter allocations instead of hash maps; no spill happens below the
  budget.
- Intra-commit duplicate keys are still kept in memory for the violation report (one entry per
  duplicate key, as before).
- `verify` still recomputes certificates with `net_key_deltas` over rows read whole.

## Alternatives considered

- **Hashing rows or keys to compare sides.** A collision would accept a changed row or miss a key
  change; exact comparison by sorting is required (fail closed).
- **Spilling only above a size threshold** with the old path below it: two code paths for the same
  decision. The sorter is already in-memory below its budget.
- **Validating per data file pair** (old file against its rewrite): engines do not pair files;
  compaction merges many into one.
