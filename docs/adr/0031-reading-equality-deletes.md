# 0031. Reading equality deletes: compaction of Flink upsert tables

- Status: Accepted
- Date: 2026-11-02
- Amends: ADR 0008 (removing equality delete files), ADR 0009 (removing data files), ADR 0028
  (rule 1 and rule 3), ADR 0011 (onboarding and rebuild of tables with equality deletes)

## Context

ADR 0009 accepts Flink upsert commits without reading existing data: an added equality delete
removes, from the key's index, the keys it names that the index holds. The Plane never needs to
know which rows an equality delete hides, so it never reads one after its commit.

Everything else on such a table is refused, because it needs exactly that:

- **Compaction** (Spark `rewrite_data_files`, Trino `optimize`) removes data files whose live rows
  are their rows minus every equality delete that applies to them, and adds files holding those
  rows. Refused (ADR 0009): the Plane cannot compute the removed live rows.
- **Removing equality delete files** (`remove-dangling-deletes`, rewrites that drop applied
  deletes). Refused (ADR 0008).
- **Copy-on-write `DELETE`/`UPDATE`/`MERGE` from Spark** on a Flink table: removes data files.
- **Onboarding, rebuild and `verify`** (ADR 0011, RFC 0002): need every live row of the table.

So a Flink upsert table cannot be maintained through the Plane: its data and delete files only
grow, and every commit reads all live position delete files (ADR 0028), whose number grows with
each checkpoint that repeats a key. This is the largest limit for CDC tables (roadmap 0.2).

## Decision

### Live rows with equality deletes

The Plane computes a data file's live rows exactly as Iceberg readers do (spec "Scan planning",
ADR 0030):

- rows of the file, minus positions named by position deletes and deletion vectors that apply to
  it (ADR 0017, 0018, 0030), minus
- rows whose values on the delete's field set `E` equal (NULL-safe) a row of a live equality delete
  file with data sequence number **greater than** the data file's.

Partitioned equality delete files stay refused (ADR 0030), so every equality delete is global and
its sequence number alone decides where it applies. Every live equality delete file still has the
field set `E` of the table's one upsert key `K` (ADR 0009); a different field set is refused.

To apply them the Plane reads the `E` columns of the live equality delete files that can apply to
any data file it reads (sequence number greater than the smallest one among those data files) into
a map from delete tuple to the **largest** sequence number deleting it. A row of a data file with
sequence number `s` is hidden when its tuple maps to a number greater than `s`. The map is held in
memory within `limits.scan_memory`; a commit whose deletes do not fit is refused with
`VALIDATION_BUDGET_EXCEEDED` (fail closed; spilling the map to disk is a later change).

Delete tuples and data rows are compared as encoded keys: both are read with the key extraction
of data files (ADR 0006) against the table's current schema, so a delete file written before a
type was widened (`int` → `long`) matches the rows it named, and a column of another key family
is a type mismatch that refuses the commit. NULL equals NULL here, as in ADR 0009 and in Iceberg
readers' delete sets, but unlike key comparison under the constraints: a delete tuple holding a
NULL hides the rows with a NULL in the same place, whatever the constraint's NULL rules.

### Which commits use it

A commit to a table with live equality delete files is validated with exact live rows when it adds
**no** equality delete files and any of:

1. it removes data files (compaction, copy-on-write);
2. it adds data files with an explicit data sequence number (a compaction committed with the
   starting snapshot's sequence number, so that equality deletes committed after the rewrite
   started still apply to the rewritten rows);
3. it changes the deleted positions of a kept data file;
4. it removes equality delete files.

The changed rows are then ADR 0017's, computed with both kinds of deletes: live rows before of
removed data files and of kept files whose live rows shrink, live rows after of added data files
and of kept files whose live rows grow. They go to the validator unchanged, so `replace` must
leave the multiset of projected rows unchanged (ADR 0008) and every other commit is checked like
any commit, with every constraint of the table.

Removing an equality delete file (case 4) is accepted only when it **applies to no data file live
in the new snapshot** (every such file has a data sequence number at least the delete's). Removing
a delete that still hides rows would bring them back; that is a real change of rows that this ADR
does not compute, and stays refused.

A commit that adds equality delete files keeps ADR 0009 and ADR 0028 unchanged: no reading,
removing data files refused. Flink never removes files in the commit that adds its deletes.

ADR 0028 rule 3 (no explicit sequence numbers on added data files) and rule 1 (only added data files
change positions) no longer apply to the commits above, whose rows are exact. Rules 2 and 4 stay.
An explicit sequence number may still not exceed the snapshot's own (the largest sequence number
of the manifests the snapshot lists), in any commit: a later equality delete applies only to files
older than it, so a file numbered above its snapshot would keep rows that the index, following
ADR 0009, no longer holds.

### Onboarding, rebuild and verify

They use the same live rows, so tables with equality delete files can be onboarded and rebuilt
(a snapshot diffed against no parent is read with every live equality delete applied), and
`verify` recomputes the snapshots of such tables that add no equality deletes (compactions and the
other commits above) instead of reporting them `UNVERIFIABLE`. Their memory bound is the same map:
a table whose live equality deletes do not fit in `limits.scan_memory` is refused for onboarding
and rebuild.

Snapshots that add equality deletes (every Flink checkpoint) stay `UNVERIFIABLE`: their key delta
names the keys the index held when they committed, which only a scan of the parent snapshot's
live rows could recompute. That is a separate change.

This is not a change to constraint semantics, key encoding, certificates, the transaction log or
status mapping (no RFC): it derives the same `CommitRows` the validator already checks, for commits
that were refused.

## Consequences

- Flink upsert tables can be compacted through the Plane (Spark `rewrite_data_files`, Trino
  `optimize`), dangling equality deletes removed, and their tables onboarded, rebuilt and verified.
- These commits read every live equality delete file newer than the oldest data file they touch.
  For a table compacted regularly that is the deletes since the last compaction.
- Commits that add equality deletes (every Flink checkpoint) cost what they cost today.
- A table whose live equality deletes outgrow `limits.scan_memory` cannot be compacted through the
  Plane until the map spills to disk; the refusal names the limit.

## Test plan

- **Oracle first.** The reference oracle already applies equality deletes in Iceberg order
  (ADR 0028). Add explicit sequence numbers to its model: a data file added with sequence number
  `s` is hidden by deletes with a greater number, including deletes of earlier commits.
- **Differential suite** (validator over the persistent and in-memory index): Flink-shaped
  sequences (upserts, a key twice per checkpoint, deletes of missing keys) followed by
  - a compaction that applies all deletes, with inherited and with starting sequence numbers;
  - a compaction started before a later upsert committed, so that the later delete must hide a
    rewritten row;
  - a compaction that loses or duplicates a row (refused, `replace` not preserving rows);
  - removing dangling equality deletes (accepted) and removing a delete that still applies
    (refused);
  - a copy-on-write delete of a key that a delete already hid (no double removal).
- **Unit tests** (`changes.rs`): the delete map (largest sequence number wins, NULL-safe tuples,
  field order), the budget refusal, refusal of a commit that adds equality deletes and removes data
  files.
- **Compatibility** (`engines` CI job): Flink upserts, then Trino `optimize` through the Plane;
  `verify` OK on the Flink table. Spark `rewrite_data_files` on a Flink table in the `clients` job
  if Spark can write to it there.

## Alternatives considered

- **Keep refusing; document "compact without the Plane".** A writer that goes around the Plane is
  what the Plane detects as a bypass (RFC 0008); every compaction would degrade the domain.
- **Accept compaction when the removed and added raw row counts match.** Unsound: the removed files
  contain hidden rows, so equal counts say nothing about which keys readers see.
- **Store, per key, the sequence number of its live row in the index.** Would let the Plane decide
  which equality deletes hide which rows without reading data, but indexes store keys and counts only
  (ADR 0014); it changes the index format and still needs the data files' keys.
- **Spill the delete map to disk now.** Same result with bounded memory; deferred until a real
  table needs it, behind the same budget refusal.
