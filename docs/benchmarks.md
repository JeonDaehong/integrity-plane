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
| Hot parent key | 16 concurrent writers, 20 single-row appends each, all referencing parent key 0, retrying on 409 |

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
| Onboarding scan of 1000000 parent rows (PRIMARY KEY) | 4.67 s | 213978 keys/s | parent data 7.9 MiB |
| Child append of 10000 rows (PK + FK into 1000000 parent keys), 20 commits | validation p50 67.4 ms / max 95.6 ms; end-to-end p50 130.2 ms / p99 147.9 ms | 148414 keys/s | read per commit p50 0.2 MiB |
| Compaction of 200000 child rows (20 files → 1, `replace`) | validation 1.26 s; end-to-end 1.30 s | 317747 keys/s | read 7.3 MiB |
| Hot parent key: 16 writers × 20 single-row FK appends to one table | queue wait p50 ≤ 100.0 ms / p99 ≤ 250.0 ms; end-to-end incl. retries p50 85.5 ms / p99 21.63 s | 12 commits/s | 3136 attempts for 320 commits (409 retries) |
| Control store after the run | indexes 64.3 MiB / txn log 64.3 MiB / registry 0.5 MiB | | |

## Results: 10 M parent rows

| Scenario | Time | Throughput | Notes |
|---|---|---|---|
| Onboarding scan of 10000000 parent rows (PRIMARY KEY) | 45.70 s | 218816 keys/s | parent data 78.9 MiB |
| Child append of 10000 rows (PK + FK into 10000000 parent keys), 20 commits | validation p50 81.0 ms / max 111.9 ms; end-to-end p50 145.5 ms / p99 199.8 ms | 123519 keys/s | read per commit p50 0.2 MiB |
| Compaction of 200000 child rows (20 files → 1, `replace`) | validation 1.81 s; end-to-end 1.85 s | 221488 keys/s | read 7.3 MiB |
| Hot parent key: 16 writers × 20 single-row FK appends to one table | queue wait p50 ≤ 250.0 ms / p99 ≤ 250.0 ms; end-to-end incl. retries p50 128.0 ms / p99 25.73 s | 9 commits/s | 3428 attempts for 320 commits (409 retries) |
| Control store after the run | indexes 1028.0 MiB / txn log 64.3 MiB / registry 0.5 MiB | | |

## Reading the numbers

- **Append validation** (spec target: < 1 s for a 10 K-row child append against a warm 1 B-row
  parent): 67 ms at 1 M and 81 ms at 10 M parent keys. Probes are B-tree point lookups, so cost grows
  with the logarithm of the index size plus cache misses; 1 B keys was not run (the index alone
  would be about 100 GB at the measured ~100 bytes per key).
- **End-to-end commit latency** is roughly twice the validation time: every commit makes about
  seven durable writes (transaction log records, one index apply per changed index, the audit
  event), each a redb two-phase commit with its own fsyncs.
- **Hot key:** one domain's commits are serial by design (spec §11), so throughput is bounded by
  that per-commit cost (9–12 commits/s here). Queue wait stays at or below 250 ms (histogram
  bucket bound); the long end-to-end tail comes from writers losing the optimistic-concurrency race
  on the same table and retrying after 409, as they would against any Iceberg catalog.
- **Storage:** the persistent index takes about 100 bytes per key (1 GiB for 10 M keys). The
  transaction log keeps every commit's staged index delta and is never truncated in 0.1, so it
  grows with write volume (64 MiB after ~360 commits here).
