# 0028. Position and equality deletes in one table (Flink upsert)

- Status: Proposed
- Date: 2026-10-22
- Amends: ADR 0009 (equality deletes), ADR 0017 (position deletes)

## Context

ADR 0009 supports Flink upsert commits: data files plus equality delete files on exactly one
PK/UNIQUE key. ADR 0017 supports position deletes. A table that has both kinds of live delete
files is refused (`UNSUPPORTED_COMMIT_OPERATION`), because ADR 0017 computes a data file's live
rows from position deletes alone; with equality deletes in the table, a row whose position is
deleted may already be gone, and would be removed twice.

Flink writes both kinds as soon as a key changes twice between two checkpoints. Its upsert writer
(`BaseEqualityDeltaWriter`) deletes a key it has not written in this checkpoint with an equality
delete, and a key it has written in this checkpoint with a position delete on the row it wrote:

```text
checkpoint: upsert (4, 'd'), upsert (4, 'd2')
  data file F (new):          pos 0 = (4, 'd'), pos 1 = (4, 'd2')
  equality delete (new):      key 4          -- rows of earlier snapshots
  position delete (new):      (F, 0)         -- the row written a moment ago
```

Found by hand with Flink 1.20.3 (`docs/compatibility.md`): such a commit is refused, so a CDC job
that updates a key twice between checkpoints cannot write through the Plane, and neither can any
later commit to that table once its first position delete file is live.

Iceberg's sequence number rules make the two kinds independent in this shape. An equality delete
with data sequence number `s` applies to data files with data sequence number `< s`; a position
delete applies to the data file at its path with data sequence number `<= s`. A data file added
by a commit, with an inherited sequence number, is therefore never touched by an equality delete
of the same or an earlier commit, and an equality delete only reaches rows of older snapshots.

## Decision

In a table with live equality delete files (or a commit adding some), position deletes are
supported when, in addition to ADR 0009's conditions:

1. **Only added data files change.** Every data file whose deleted positions differ between the
   two snapshots is a data file the commit adds. A kept data file whose positions change, or a
   removed data file, is refused (removed data files already are, ADR 0009).
2. **Positions on an added data file come from the same commit.** A position delete file that is
   live in the parent snapshot and names an added data file is refused. Iceberg would not apply it
   (its sequence number is lower than the new file's), while ADR 0017's live rows would.
3. **Added data files inherit their sequence number.** An added data file with an explicit data
   sequence number is refused: with an older number, equality deletes of this or earlier commits
   would hide some of its rows, which the Plane counts as added.

Rule 2 holds for every table, also without equality deletes: it closes the same mismatch for
position deletes alone. Rule 3 holds for every commit to a table with live equality delete files,
including today's ADR 0009 commits without position deletes. Today such a commit is accepted and
its hidden rows counted as added: the index then holds keys that are not in the table, so a later
foreign key can be accepted against a parent row nobody can read. Normal writers never set explicit
sequence numbers on new files in such tables (compaction does, and is refused there).

**Semantics.** The commit's rows are ADR 0017's, restricted to added files, and ADR 0009's:

- rows added = live rows of added data files, positions taken from the commit's own position
  delete files;
- keys removed = ADR 0009's probe of the equality delete tuples in `K`'s index;
- nothing else changes: earlier delete files touch only kept files, whose rows do not change.

This is not a change to constraint semantics, key encoding, certificates, the transaction log or
status mapping (no RFC): it derives the same `CommitRows` the validator already checks.

**Oracle.** The reference oracle applies a commit in Iceberg order: equality deletes to existing
rows (NULL-safe), then the commit's rows minus its own deleted positions. The differential suite
gains Flink-shaped commits: a key upserted twice in one commit, an existing key upserted twice, a
key inserted and deleted in one commit, and two writers upserting the same new key in one commit
(a real duplicate, which must be refused).

**Tests** (`changes.rs` and the validator suite): each refusal above; the accepted shape with
positions on one and on several added files; and the Flink compatibility scenario of
`docs/compatibility.md`, which must then pass.

## Consequences

- Flink upsert and CDC jobs work through the Plane while their only key constraint is the upsert
  key (ADR 0009's other limits stay: no other PK/UNIQUE on the table, no FK declared on it).
- Commits to such tables read every live position delete file (ADR 0017), and their number grows
  with every checkpoint that repeats a key. Compacting tables with equality delete files is still
  refused (ADR 0009), so this growth is unbounded until it is supported. That is the next limit for
  Flink tables and needs its own decision (applying equality deletes during a rewrite means reading
  the deleted keys against the data, which the Plane avoids today).
- `verify` still reports snapshots with equality deletes `UNVERIFIABLE`.
- Rule 2 may refuse commits that are accepted today, only when an old position delete file names
  the path of a new data file; no known writer reuses data file paths.
- Rule 3 refuses commits accepted today that set explicit sequence numbers on new data files in a
  table with equality deletes; no known writer does.

## Alternatives considered

- **Compute live rows with both kinds of deletes for every file.** Exact in general, but equality
  deletes of older snapshots would have to be read and matched against the rows of every data file
  whose live rows change, which is the scan ADR 0009 avoids. The restricted shape covers what Flink
  writes without it.
- **Ask Flink users to avoid repeated keys per checkpoint.** Not under their control for CDC.
- **Accept and mark such snapshots unverified.** Fail closed forbids accepting a commit whose rows
  the Plane cannot derive exactly.
