# 0014. Indexes store keys and counts, not row locations

- Status: Accepted
- Date: 2026-10-09

## Context

A database index maps keys to row locations. In Iceberg, row locations (data file path and
position) change whenever files are rewritten: compaction, copy-on-write updates and deletes,
manifest-level maintenance. Writers rewrite files routinely and independently of key changes.

## Decision

- A unique index (PRIMARY KEY, UNIQUE) maps an encoded key to its presence (the last snapshot that
  wrote it). Its multiplicity is at most one by invariant.
- A reference index (FOREIGN KEY) maps a parent key to the number of child rows that reference it.
- Validation works on net key deltas: rows removed and added by a commit are read from the changed
  data files, and only keys whose count changes touch an index (spec §8, §13).

## Consequences

- Compaction and other rewrites that keep the key multiset unchanged produce empty deltas and no
  index writes (`replace` is validated by checking exactly that).
- Deleting a child row requires reading its FK value from the file it is removed from, which the
  manifest diff provides (ADR 0008).
- The Plane cannot answer "where is this key" queries; it was never meant to.

## Alternatives considered

- **Row locators.** Every compaction would rewrite large parts of every index, and locators would
  be stale whenever a writer rewrote files outside the view of the Plane.
- **Iceberg v3 row lineage (`_row_id`).** May speed up delete handling later; not required, and not
  available on v2 tables.
