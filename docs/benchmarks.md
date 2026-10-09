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
