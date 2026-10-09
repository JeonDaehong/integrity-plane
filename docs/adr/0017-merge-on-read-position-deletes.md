# 0017. Merge-on-read position deletes

- Status: Accepted
- Date: 2026-10-09
- Amends: ADR 0008 (manifest diff), spec §15 (moves position deletes from 0.2 into the current
  release)

## Context

Spark (and other engines) write `DELETE`, `UPDATE` and `MERGE` on merge-on-read tables as position
delete files: Parquet files of `(file_path, pos)` that hide individual rows of existing data files.
Until now any commit that added or removed position delete files was refused, which made
merge-on-read tables unusable through the Plane. Validation needs the rows a commit removes and
adds; with position deletes those are not whole files.

## Decision

The manifest diff computes, for both snapshots, the **live rows** of the data files involved: a data
file's rows minus the positions named by the snapshot's live position delete files. Then:

- rows removed = live rows (before) of removed data files + rows of kept data files whose position
  becomes deleted;
- rows added = live rows (after) of added data files + rows of kept data files whose deletion is
  undone (a delete file removed without its data file).

Consequences of computing it this way:

- A position deleted twice (already deleted in the parent, or listed twice) is removed once.
- Compaction of a merge-on-read table (`rewrite_data_files` applying deletes, rewriting or dropping
  delete files) is a `replace` exactly when the live rows are unchanged, as for any compaction.
- Deletes naming a file that is in neither snapshot change nothing.
- Onboarding, rebuild and `verify` use the same computation, so they see the same rows.

To compute it, the diff reads every manifest of both snapshots when either has delete manifests,
and every live position delete file; data files are read only when their live rows change.

Still refused (`UNSUPPORTED_COMMIT_OPERATION`): deletion vectors (format v3, Puffin), position and
equality deletes in the same table, positions beyond the end of a file, delete files whose row
count disagrees with their manifest, and removing equality delete files.

This is not a change to constraint semantics, key encoding, certificates, the transaction log or
status mapping: it derives the same `CommitRows` the validator already checks.

## Consequences

- Commits on tables with position deletes read more: all manifests of both snapshots and all live
  position delete files, within the inline validation budget. Tables with many small delete files
  benefit from regular compaction.
- Verified with Spark 3.5 / Iceberg 1.10 (`DELETE`, `UPDATE`, `MERGE` on merge-on-read tables,
  compaction applying deletes) against the reference catalog, Polaris and Lakekeeper.

## Alternatives considered

- **Only diffing changed delete files** (as for data files). Misses rows already deleted by
  unchanged delete files, so a re-deleted row would be removed twice and the validator would see an
  inconsistent index.
- **Using v3 row lineage.** Not available on v2 tables, which is what Spark writes by default.
- **Supporting deletion vectors now.** Needs a Puffin reader and v3 tables in the compatibility
  tests; planned separately.
