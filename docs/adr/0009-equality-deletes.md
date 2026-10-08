# 0009. Equality deletes on a PK/UNIQUE key (Flink upsert)

- Status: Accepted
- Date: 2026-10-09

## Context

Spec §15 supports "equality deletes whose fields == exactly a PK/UNIQUE key (Flink upsert)" using
"delete-file contents only". An Iceberg equality delete file with field ids `E` and data sequence
number `s` removes every row of data files with sequence number `< s` whose values on `E` equal a
row of the delete file (NULL equals NULL). Flink upsert commits a data file and an equality delete
file in the same snapshot, so new rows (same sequence number) are not deleted.

## Decision

- **Input.** `CommitRows` carries optional `equality_deletes { fields, keys }`: the equality field
  ids and the delete tuples read from the snapshot's added equality delete files (projected on
  those fields).
- **Supported shape** (otherwise `UNSUPPORTED_COMMIT_OPERATION`):
  - every added equality delete file in the commit has the same field set `E`;
  - `E` equals, as a set, the columns of exactly one enforced PK or UNIQUE constraint `K` of the
    table; delete tuples are reordered into `K`'s key order;
  - no other enforced constraint needs values of the deleted rows: no other PK/UNIQUE on the table,
    no FK declared on the table, and no FK referencing a key of the table other than `K`
    (NOT NULL is unaffected by deletes);
  - the commit removes no data files;
  - added equality delete entries inherit their sequence number (no explicit `sequence_number`), so
    they apply exactly to rows that existed before the commit.
- **Semantics in the validator.** Each delete tuple is classified with `K`'s NULL rules; tuples that
  yield an index key are probed in `K`'s index and those present become `K`'s removed keys (once
  each, however often they appear). Tuples that cannot be indexed (NULLs under PK or NULLS DISTINCT)
  match no indexed row and change nothing in `K`. The usual rules then apply: added keys counted
  before probes, net delta, duplicates, and the `REFERENCED_ROW_DELETE` check for FKs referencing
  `K` on keys whose count falls.
- **Oracle.** The reference oracle applies equality deletes literally: before adding the commit's
  rows it removes every existing row whose `E` values equal (NULL-safe) a delete tuple. The
  differential suite covers upserts and pure deletes on a table with a PK, a NOT NULL column and an
  FK referencing the PK.
- **Later commits.** A table with equality delete files still cannot have data files removed
  (copy-on-write, compaction) through the Plane in 0.1 (ADR 0008); appends and further upserts work.

## Consequences

- Flink upsert tables whose only key constraint is the upsert key are supported end to end, without
  reading existing data.
- Compaction of such tables needs merge-on-read support (0.2).
- The validator gains one probe per distinct delete key in `K`'s index.

## Alternatives considered

- *Read existing rows matching the deletes* (a scan): rejected by the spec for 0.1.
- *Treat every delete tuple as a removal without probing*: would underflow the index for deletes
  of keys that do not exist (Flink emits deletes for new keys too).
