# 0024. Read, feed, sort and merge both sides of a commit at once

- Status: Accepted
- Date: 2026-10-13
- Amends: ADR 0019 (how a streamed validation uses threads)

## Context

A streamed validation (ADR 0019) read the rows of a commit on one thread and, on a second one,
checked the NULL rules and encoded the keys of both sides, pushing them into one external sorter
per side and constraint; the sorter sorted and spilled full buffers on that same thread. Each
constraint's two sorted sides were then merged on one thread, which also read the spilled runs of
both sides. For a compaction of 10 M rows (`integrity-bench --scale`, local), the feeding thread
was busy 2.5 s of 2.9 s while the reader waited, and the merge took 2.3 s; the commit took about
5 s to validate.

Nothing ties the two sides together before the merge: removed rows only yield keys (or prove the
indexes inconsistent), and the NULL rules and their violations concern the added rows alone.

## Decision

- The rows of each side are read on a thread of their own (`for_each_side_batch`, the same rows in
  the same order as `for_each_commit_batch` gives for that side; the commit's position deletes are
  read once, before). A side with no data file to read and no kept file starts no thread.
- The added rows are fed to the validator's scan on the validating thread, as before. The removed
  rows are fed on another thread to a `RemovedScan` split off the scan, whose errors are handed
  back before deciding, as if fed to the scan (`CommitScan::removed`, `CommitScan::absorb`).
- Each sorter starts on the thread that feeds it. Once it has taken 256 KiB of keys (or its memory
  budget, when smaller) it moves to a thread of its own, which is sent the keys in chunks, so that
  sorting and spilling run beside the encoding. Small commits start no sorter thread.
- When any sorter moved to a thread, each constraint's two sides are merged on a thread of their
  own, each side read ahead on another; the rows of a `replace` are compared the same way, at the
  same time. Otherwise everything is merged on the validating thread.
- Results are used in the same order as before (the rows of a `replace`, then the keys by
  constraint); every thread is joined before anything is reported, and a sorter that failed is
  reported first, since it explains why feeding stopped. Decisions and details are unchanged.

## Consequences

- Locally, a compaction of 10 M rows validates in 3.2–4.0 s instead of 5.0–5.7 s, a copy-on-write
  delete of one row in a 1 M-row file in 0.19–0.34 s instead of 0.36–0.51 s; appends (one side)
  and single-row commits are unchanged (`docs/benchmarks.md`).
- Memory: as before, plus up to four chunks queued per sorter thread (1 MiB) and two batches of
  4 096 records read ahead per merged side.
- A large commit uses up to three threads besides the validating one (two readers, the removed
  side's feeder), five per constraint (two sorters, a merge, two read-ahead) and six for the rows
  of a `replace` (an encoder, two sorters, a comparison, two read-ahead); all are scoped to the
  validation and joined before it returns.
- Covered by the streaming-against-whole differential test (`integrity-server/tests/commit_scan.rs`,
  now with budgets small enough to move every sorter to a thread and large enough to move none) and
  the per-side reads against the oracle (`integrity-iceberg/tests/manifests.rs`).
