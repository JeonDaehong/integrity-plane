# 0023. Decode only the changed rows of kept data files

- Status: Accepted
- Date: 2026-10-12
- Amends: ADR 0017 (how the rows of kept data files are read), ADR 0019

## Context

A merge-on-read delete leaves its data files in place and adds position delete files. For each kept
data file whose deleted positions change, validation needs the key columns of exactly the newly
deleted (and restored) rows. Until now it decoded every row of each row group holding such a
position, converted all of them to key values and kept the few it needed. A delete spread over a
table (Spark `DELETE ... WHERE id % 100 = 7`) touches every row group, so it converted every key of
the table: 16.4 s through the Plane against 3.6 s directly for 1 % of 33 M rows on S3
(`docs/benchmarks.md`).

## Decision

- Kept data files are read with a Parquet row selection: the reader still fetches the key column
  chunks of the row groups holding a changed position (and no others), but decodes and converts
  only the changed rows; it skips the rest inside the pages.
- Each row group read must yield exactly the selected rows, with their positions; fewer or more is
  an error (the commit is refused). The file's row count is still checked against the manifest, and
  every deleted position, before and after, against the file's row count.
- Data files a commit adds or removes are read whole, as before: all of their live rows are needed.

## Consequences

- What is read is unchanged (the key column chunks of the touched row groups); the work after
  reading follows the number of changed rows instead of the size of the row groups.
- A row group is no longer fully decoded when only some of its rows change, so a row group whose
  pages hold fewer values than its metadata declares is only detected when a selected row is
  missing. This was already the case for row groups without a changed position.
- Covered by the streaming-against-oracle property test of position deletes (`tests/manifests.rs`)
  and a test of selected rows against whole-file extraction across row group and page boundaries
  (`tests/extract.rs`).
