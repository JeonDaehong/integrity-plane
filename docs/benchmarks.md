# Benchmarks

> Status: first measurements (Phase 11), spec §29. Non-normative; numbers from one machine.
> Reproduce with `cargo run --release -p integrity-bench -- --parent-rows N`.

## Method

`crates/integrity-bench` runs the real gateway in-process, in front of an in-process fake Iceberg
REST catalog, and drives it over HTTP like an engine: each commit reads the table head, writes a
Parquet data file, an Avro manifest and a manifest list, and posts the commit. The Plane reads those
files back through its `FileIo` (local filesystem), validates against the persistent (redb) indexes,
records the transaction log, and applies index changes with durable two-phase commits. Validation
time, bytes read, keys validated and queue wait come from the gateway's own `/metrics`; end-to-end
latency is measured by the client and includes HTTP, metadata loads, logging, the upstream commit
and index application.

Dataset: `customer(id PK)` with N parent rows written before the Plane knew the table (1 M rows per
file), onboarded by registering the PRIMARY KEY; `orders(id PK, ref FK → customer)`.

| Scenario | What is measured |
|---|---|
| Onboarding | `POST /v1/integrity/constraints` for the parent PK: scan every parent file, validate, install the index |
| Child append | 20 sequential commits of 10 000 rows, `ref` uniformly random over the parent keys (warm index) |
| Compaction | one `replace` commit rewriting all child files into one with the same rows |
| One writer | 100 sequential single-row appends: the per-commit cost without contention |
| Hot parent key | 16 concurrent writers, 20 single-row appends each to one new table, all referencing parent key 0, retrying on 409 |
| Baseline | the same 16 writers sent straight to the catalog (no Plane, no constraints), new table |

Not measured: object storage over a network (bytes read are reported so the cost can be estimated;
in 0.1 the Plane fetches whole data files, not only key column chunks), a 1 B-row parent (spec §29
target), multi-node anything.

## Hardware

| | |
|---|---|
| CPU | AMD Ryzen 5 5600X, 6 cores / 12 threads |
| Memory | 32 GB |
| Disk (data and control store) | NVMe SSD (system temp directory) |
| OS | Windows 11 Pro, Rust 1.99, release build |

## Results: 1 M parent rows

| Scenario | Time | Throughput | Notes |
|---|---|---|---|
| Onboarding scan of 1000000 parent rows (PRIMARY KEY) | 3.44 s | 290749 keys/s | parent data 7.9 MiB |
| Child append of 10000 rows (PK + FK into 1000000 parent keys), 20 commits | validation p50 63.9 ms / max 94.2 ms; end-to-end p50 117.2 ms / p99 137.0 ms | 156433 keys/s | read per commit p50 0.2 MiB |
| Compaction of 200000 child rows (20 files → 1, `replace`) | validation 1.06 s; end-to-end 1.09 s | 377192 keys/s | read 8.6 MiB |
| Overhead: table load (what readers do before reading data files) | direct p50 0.1 ms; through the Plane p50 0.3 ms | +0.1 ms | data files are read from storage directly |
| Overhead: single-row commit, client side included | straight to the catalog p50 5.2 ms; unconstrained table through the Plane p50 5.4 ms | +0.2 ms | constrained: see the next row |
| Wide append: 10000 rows × 1024 B payload, PK on id, 5 commits | client writing the file and committing: straight to the catalog p50 71.5 ms; through the Plane p50 125.9 ms | +54.4 ms | Plane read per commit 0.2 MiB of a 9.9 MiB data file (1.6 %) |
| One writer: 100 sequential single-row FK appends | end-to-end p50 14.9 ms / p99 22.7 ms; server validation p50 ≤ 2.5 ms | 64 commits/s | |
| Hot parent key: 16 writers × 20 single-row FK appends to one new table | queue wait p50 ≤ 50.0 ms / p99 ≤ 100.0 ms; end-to-end incl. retries p50 189.7 ms / p99 5.95 s | 17 commits/s | 4221 attempts for 320 commits (409 retries) |
| Baseline: the same writers straight to the catalog (no Plane) | end-to-end incl. retries p50 186.1 ms / p99 1.88 s | 41 commits/s | 2593 attempts for 320 commits |
| Control store after the run | indexes 128.5 MiB / txn log 64.3 MiB / registry 0.5 MiB | | |

## Results: 10 M parent rows

| Scenario | Time | Throughput | Notes |
|---|---|---|---|
| Onboarding scan of 10000000 parent rows (PRIMARY KEY) | 37.18 s | 268950 keys/s | parent data 78.9 MiB |
| Child append of 10000 rows (PK + FK into 10000000 parent keys), 20 commits | validation p50 65.2 ms / max 140.3 ms; end-to-end p50 117.4 ms / p99 186.4 ms | 153400 keys/s | read per commit p50 0.2 MiB |
| Compaction of 200000 child rows (20 files → 1, `replace`) | validation 1.08 s; end-to-end 1.10 s | 372079 keys/s | read 7.3 MiB |
| One writer: 100 sequential single-row FK appends | end-to-end p50 12.5 ms / p99 14.9 ms; server validation p50 ≤ 2.5 ms | 79 commits/s | |
| Hot parent key: 16 writers × 20 single-row FK appends to one new table | queue wait p50 ≤ 50.0 ms / p99 ≤ 50.0 ms; end-to-end incl. retries p50 57.3 ms / p99 8.67 s | 24 commits/s | 3832 attempts for 320 commits (409 retries) |
| Baseline: the same writers straight to the catalog (no Plane) | end-to-end incl. retries p50 133.7 ms / p99 1.14 s | 68 commits/s | 2219 attempts for 320 commits |
| Control store after the run | indexes 1028.0 MiB / txn log 64.3 MiB / registry 0.5 MiB | | |

## Large tables on AWS (`--scale`)

One EC2 `m6id.4xlarge` in ap-northeast-2 (16 vCPU Xeon Platinum 8375C, 64 GB, data on the local
NVMe instance store), Amazon Linux 2023, Rust 1.99, 2026-10-10. Each run is
`integrity-bench --scale N --file-rows 1000000 --dir <nvme>`: N rows loaded in 1 M-row files
straight to the catalog, a PRIMARY KEY onboarded, then through the Plane a copy-on-write delete of
one row, compaction of 10 files and 50 merge-on-read deletes. Memory is the process's resident set
sampled every second; "during onboarding" is the highest sample during the registration request.
"Before" is commit 0f0d373 (it has no onboarding markers, so its figure is the peak of the whole
run, which for that version is onboarding).

### Onboarding before and after the bounded-memory scan (ADR 0011 amendment)

| Rows | Onboarding before | Onboarding after | Memory before | Memory during onboarding, after |
|---|---|---|---|---|
| 30 M | 94.95 s | 41.44 s | 11.1 GB (whole run) | 1.5 GB |
| 60 M | 193.02 s | 85.96 s | 21.6 GB (whole run) | 1.6 GB |
| 100 M | 328.68 s | 145.27 s | 35.5 GB (whole run) | 1.5 GB |
| 1 000 M | not run (about 360 GB needed) | 1598.00 s | | 1.5 GB |

Before, memory grew with the number of keys (about 360 bytes per key here); one billion keys would
have needed about 360 GB. Now onboarding holds `limits.scan_memory` (512 MiB) plus the index store's
page cache (at most 1 GiB) at any size, at about 625 000–725 000 keys/s, 2.2 times the previous rate.
The copy-on-write, compaction and merge-on-read rows of both versions match (the commit path did not
change); the peak memory of the new version's runs, about 5 GB, is the 10 M-row compaction.

### 1 000 M rows, after (peak memory of the whole run 5.0 GB)

| Scenario | Time | Data | Notes |
|---|---|---|---|
| Load 1000000000 rows in 1000 files of 1000000 rows (no Plane) | 19.28 s | | |
| Onboarding scan of 1000000000 keys | 1598.00 s | 625782 keys/s | |
| Copy-on-write delete of 1 row (rewrites a 1000000-row file) | end-to-end 1.91 s; validation 1.89 s | read 15.9 MiB | status 200 |
| Compaction of 10 files (10000000 rows) into one | end-to-end 26.66 s; validation 26.64 s | read 158.4 MiB | status 200 |
| Merge-on-read delete of 1 row, 50 times (delete files accumulate) | first: end-to-end 524.4 ms / validation 505.5 ms; last: 145.3 ms / 122.6 ms | read first 8.8 MiB, last 8.8 MiB | |

### 100 M rows, after (peak memory of the whole run 5.0 GB)

| Scenario | Time | Data | Notes |
|---|---|---|---|
| Load 100000000 rows in 100 files of 1000000 rows (no Plane) | 1.56 s | | |
| Onboarding scan of 100000000 keys | 145.27 s | 688356 keys/s | |
| Copy-on-write delete of 1 row (rewrites a 1000000-row file) | end-to-end 1.90 s; validation 1.90 s | read 15.9 MiB | status 200 |
| Compaction of 10 files (10000000 rows) into one | end-to-end 26.66 s; validation 26.65 s | read 158.4 MiB | status 200 |
| Merge-on-read delete of 1 row, 50 times (delete files accumulate) | first: end-to-end 481.4 ms / validation 477.5 ms; last: 87.3 ms / 82.1 ms | read first 8.0 MiB, last 8.1 MiB | |

### 100 M rows, before (peak memory of the whole run 35.5 GB)

| Scenario | Time | Data | Notes |
|---|---|---|---|
| Load 100000000 rows in 100 files of 1000000 rows (no Plane) | 1.64 s | | |
| Onboarding scan of 100000000 keys | 328.68 s | 304249 keys/s | |
| Copy-on-write delete of 1 row (rewrites a 1000000-row file) | end-to-end 3.41 s; validation 3.40 s | read 15.9 MiB | status 200 |
| Compaction of 10 files (10000000 rows) into one | end-to-end 25.37 s; validation 25.36 s | read 158.4 MiB | status 200 |
| Merge-on-read delete of 1 row, 50 times (delete files accumulate) | first: end-to-end 632.4 ms / validation 628.3 ms; last: 86.4 ms / 81.0 ms | read first 8.0 MiB, last 8.1 MiB | |

## Large tables (local Windows machine, `--scale`)

`integrity-bench --scale N --file-rows 1000000 --settle 120` loads N rows in 1 M-row files straight
to the catalog, onboards a PRIMARY KEY, then measures through the Plane: a copy-on-write delete of
one row (its 1 M-row file rewritten), compaction of 10 files, and 50 merge-on-read deletes of one row
each. `--settle 120` waits two minutes after the load and after onboarding: on the benchmark machine
(Windows, real-time antivirus on) measurements taken right after gigabytes of new files were
written ran up to 2–3 times slower and varied widely. Memory is the benchmark process's working set
(the Plane runs in it), sampled every second by an external script; "during onboarding" is the
highest sample between the start and end of the registration request.

### Onboarding before and after the bounded-memory scan (ADR 0011 amendment)

Same machine, same day, same options; "before" is commit 0f0d373.

| Rows | Onboarding before | Onboarding after | Memory during onboarding, before | after |
|---|---|---|---|---|
| 30 M | 141.25 s | 49.13 s | 10.3 GB | 965 MB |
| 60 M | 365.79 s | 174.78 s | 14.1 GB | 1.2 GB |
| 100 M | not run (about 25 GB needed) | 170.35 s | | 1.3 GB |

Before, the scan held every key of the domain in memory (about 250–340 bytes per key), so 100 M
keys would have needed about 25–34 GB. Now the keys are sorted externally within
`limits.scan_memory` (512 MiB by default); what remains is that budget plus the index store's
page cache (redb, 1 GiB at most), so memory stays near 1.3 GB whatever the table size. The scan is
also faster: sorting runs of keys and appending them in order beats inserting each key into an
in-memory B-tree and then copying it into the store.

### 100 M rows, after (peak memory of the whole run 5.4 GB)

| Scenario | Time | Data | Notes |
|---|---|---|---|
| Load 100000000 rows in 100 files of 1000000 rows (no Plane) | 4.45 s | | |
| Onboarding scan of 100000000 keys | 170.35 s | 587023 keys/s | |
| Copy-on-write delete of 1 row (rewrites a 1000000-row file) | end-to-end 2.58 s; validation 2.55 s | read 15.9 MiB | status 200 |
| Compaction of 10 files (10000000 rows) into one | end-to-end 38.08 s; validation 38.07 s | read 158.4 MiB | status 200 |
| Merge-on-read delete of 1 row, 50 times (delete files accumulate) | first: end-to-end 245.3 ms / validation 235.6 ms; last: 140.9 ms / 130.2 ms | read first 8.0 MiB, last 8.1 MiB | |

### 60 M rows, after (peak memory of the whole run 5.4 GB)

| Scenario | Time | Data | Notes |
|---|---|---|---|
| Load 60000000 rows in 60 files of 1000000 rows (no Plane) | 9.01 s | | |
| Onboarding scan of 60000000 keys | 174.78 s | 343285 keys/s | |
| Copy-on-write delete of 1 row (rewrites a 1000000-row file) | end-to-end 2.59 s; validation 2.58 s | read 15.9 MiB | status 200 |
| Compaction of 10 files (10000000 rows) into one | end-to-end 38.84 s; validation 38.83 s | read 158.4 MiB | status 200 |
| Merge-on-read delete of 1 row, 50 times (delete files accumulate) | first: end-to-end 161.1 ms / validation 150.6 ms; last: 1.26 s / 1.24 s | read first 8.0 MiB, last 79.1 MiB | |

### 60 M rows, before (peak memory of the whole run 14.1 GB)

| Scenario | Time | Data | Notes |
|---|---|---|---|
| Load 60000000 rows in 60 files of 1000000 rows (no Plane) | 3.09 s | | |
| Onboarding scan of 60000000 keys | 365.79 s | 164027 keys/s | |
| Copy-on-write delete of 1 row (rewrites a 1000000-row file) | end-to-end 2.71 s; validation 2.68 s | read 15.9 MiB | status 200 |
| Compaction of 10 files (10000000 rows) into one | end-to-end 51.08 s; validation 51.07 s | read 158.4 MiB | status 200 |
| Merge-on-read delete of 1 row, 50 times (delete files accumulate) | first: end-to-end 197.2 ms / validation 185.9 ms; last: 1.70 s / 1.69 s | read first 8.0 MiB, last 79.1 MiB | |

What this shows:

- **Onboarding and rebuild** now need about 1.3 GB whatever the table size, and scratch disk for
  the sorted runs and the new index; their time grows with the number of keys (about 170 s for
  100 M keys here).
- **The commit path is unchanged** by the new scan; differences between the runs above are within
  run-to-run noise (copy-on-write 2.6–2.7 s, compaction 38–80 s across runs on this machine).
- **Copy-on-write work follows the rewritten file, not the change:** deleting one row of a 1 M-row
  file validates 2 M keys (before and after), about 2.6 s.
- **Compaction** holds both sides of the rewrite in memory while validating: the peak memory of
  the whole run (about 5 GB) is the 10 M-row compaction, not onboarding. Streaming it is the next
  step.
- **Merge-on-read** deletes stay at 120–250 ms while the deleted rows sit in 1 M-row files; a delete
  in a large file reads that file's whole key column (79 MB for a 10 M-row file, 1.2–1.7 s).

## What the Plane adds to Iceberg

| Path | Extra cost | Why |
|---|---|---|
| Reading tables | about 0.1 ms per table load; nothing on data | engines read data files from storage directly; the Plane only relays the metadata request |
| Commits to tables without constraints | about 0.2–0.6 ms | forwarded unchanged |
| Commits to constrained tables | about 5–10 ms, plus about 5 µs per changed key | validation, transaction log, index update |
| Storage reads for validation | the key column chunks of the files a commit adds or removes (1.6 % of a 10 MB file with 1 KB rows) | footer and projected column chunks are read with ranged requests |

## Reading the numbers

- **Append validation** (spec target: < 1 s for a 10 K-row child append against a warm 1 B-row
  parent): about 60–65 ms at 1 M and 10 M parent keys. Probes are B-tree point lookups, so cost grows
  with the logarithm of the index size plus cache misses; 1 B keys was not run (the index alone
  would be about 100 GB at the measured ~100 bytes per key).
- **Per-commit cost:** one writer commits about 80 times per second (12–13 ms end to end, of which
  a few ms are validation). A commit makes four durable writes: PREPARED, then VALIDATED and
  COMMITTING together, one atomic index application for all its indexes, COMMITTED; plus the audit
  event.
- **Contention:** 16 writers racing on one table lose the optimistic-concurrency race about 11 times
  per successful commit. Through the Plane they reach about 25 commits/s, against about 70 commits/s
  sent straight to the catalog. Stale commits are answered 409 before they enter the domain queue,
  and inside the queue the written table's requirements are checked first, but each winning commit
  still validates and logs inside the queue (spec §11), which lengthens every round of the race.
- **Run-to-run variance** on this desktop machine is large (about ±30 % between consecutive runs);
  compare runs made back to back.
- **Storage:** the persistent index takes about 100 bytes per key (1 GiB for 10 M keys). The
  transaction log keeps the staged index delta of every commit and is never truncated in 0.1, so it
  grows with write volume.

## History

| Change | One writer | 16 writers, one table |
|---|---|---|
| Phase 11 | 54–59 commits/s | 14 commits/s (p99 about 20 s) |
| No write transaction to look up an index; one atomic apply for all indexes of a commit; VALIDATED and COMMITTING in one write; stale commits answered before the queue | 76–82 commits/s | 23–25 commits/s (p99 about 6–9 s) |
| Only key column chunks read from data files (ranged reads); index values read during validation reused when staging | (unchanged) | (unchanged); wide 10 MB file: 100 % → 1.6 % read |

Measured with alternating runs of both versions on the machine above (200 K parent rows).
