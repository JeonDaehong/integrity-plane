# Changelog

## Unreleased

- The transaction log deletes a commit's staged index deltas with its terminal record and prunes
  finished transactions after `[txn_log] retention` (default `24h`, at least `30m`; RFC 0006):
  it no longer grows with write volume, and keeps only recent transactions in memory. A request
  repeating a pruned idempotency key is validated as a new request.
- A commit's staged index changes are logged in chunks of `[txn_log] chunk_writes` (default
  100 000) and applied by streaming them from the log into one index transaction (RFC 0006, ADR
  0025): logging and applying no longer hold a commit's changes in memory. A log written by 0.0.2
  still recovers; 0.0.2 cannot recover a commit logged in chunks.
- A commit of one snapshot without equality deletes is decided after the merge in chunks too
  (ADR 0025): its merged keys, probes, staged writes and key delta digest go through runs that
  spill to scratch beyond `limits.scan_memory`, so an append's memory no longer grows with the
  keys it adds. Commits of several snapshots keep the previous path.
- Request limits (RFC 0007, new code INT-017 `RATE_LIMITED`, HTTP 429 with `Retry-After`):
  at most `[limits] max_queued_commits` (default 32) commits wait for one domain; an optional
  `commits_per_client` rate per client (peer address, or `client_header` behind a proxy); ten
  failed integrity API authentications in a minute lock the client out for that minute; one
  `verify` at a time. Refused requests are never forwarded. Metric
  `integrity_rate_limited_total{reason}`.
- `verify` streams each snapshot's rows and sorts their keys within `limits.scan_memory`, spilling
  to scratch, instead of reading them whole: verifying a table no longer needs memory for the rows
  of its largest snapshot.

## 0.0.2 — 2026-10-14

Second pre-release. **Pre-alpha, not production-ready**: see
[`docs/limitations.md`](docs/limitations.md) before trying it. No change to constraint semantics,
key encoding, certificate format, transaction log records or status mapping (upgrading a 0.0.1
control store in place is not tested). This release is about memory and speed on large tables.

With Spark 3.5 on Amazon S3 (one m6id.4xlarge, `docs/benchmarks.md`), against commit 9318c84 (the
first Spark measurement, part-way through this release): an initial load of 30 M rows through the Plane takes 38 s instead of 164 s (6 s directly),
compacting 33 M rows 59 s instead of 111 s (33 s directly), and a whole nightly ETL job 30–31 s
against 22–24 s directly.

- Onboarding and rebuild run with bounded memory (ADR 0011 amendment): rows are read a row group at
  a time, keys are sorted externally within `limits.scan_memory` (default 512 MiB), and the new
  indexes are built beside the live ones and installed in one transaction. On one AWS
  m6id.4xlarge, 100 M keys onboard in 145 s and 1.5 GB (before: 329 s and 36 GB), and 1 000 M keys
  in 27 minutes and 1.5 GB (`docs/benchmarks.md`).
- Commit validation streams rows and sorts keys externally (ADR 0019): copy-on-write rewrites and
  compactions no longer need memory proportional to the rows they rewrite. Compacting 100 M rows
  needs 1.5 GB instead of 40.7 GB, in 128 s instead of 304 s (`docs/benchmarks.md`).
- Merge-on-read deletes read only the row groups holding changed positions: one delete in a
  10 M-row file takes 88 ms instead of up to 7 s.
- An accepted commit is answered as soon as upstream accepts it; its index write and `COMMITTED`
  record follow while it still holds its domain queue (ADR 0022). The audit and transaction APIs
  wait for answered commits to finish.
- Rewrites (copy-on-write, compaction) validate several times faster: keys sorted by prefix, no
  per-row allocations, and reading, row comparison and key checks on separate threads.
- Key deltas, staged index writes and the validation overlay are kept in key order instead of
  per-key maps (ADR 0021): commits that change many keys validate several times faster.
- Each manifest and delete file of a commit is read once, up to 16 at a time (ADR 0020): on
  Amazon S3 a merge-on-read delete in a 100-file table takes 0.5–0.65 s instead of 4.7–8.5 s.
- Merge-on-read deletes decode only the changed rows of the data files they keep (ADR 0023): a
  delete spread over every row group still reads those key column chunks but no longer converts
  every key in them. On S3 a Spark delete of 1 % of 33 M rows takes 13.0 s through the Plane
  instead of 16.4 s (3.6 s directly).
- The two sides of a commit (rows added and rows removed) are read, sorted and merged on threads
  of their own (ADR 0024): with Spark on S3, compacting 33 M rows validates in 32 s instead of
  50 s.
- Key extraction reads only the footer and the key column chunks of each Parquet file, with ranged
  reads, instead of whole data files; deletion vector blobs are read by offset and size (ADR 0006
  amendment).
- Commits still being finished after their answer are no longer reported as awaiting recovery.
- `compat/spark_bench.py` and `compat/spark_etl_bench.py` time Spark statements and a nightly ETL
  job directly against the catalog and through the Plane; the README says when the Plane fits.
- `integrity-bench --settle` waits after large file writes before measuring; `--s3` writes the
  generated files to an S3 prefix and the Plane reads them from there.

## 0.0.1 — 2026-10-09

First public pre-release. **Pre-alpha, not production-ready**: see
[`docs/limitations.md`](docs/limitations.md) before trying it.

### What it does

The Open Integrity Plane is a proxy Iceberg REST catalog that enforces PRIMARY KEY, UNIQUE,
NOT NULL and FOREIGN KEY constraints at commit time, for any engine that writes through a REST
catalog, and chains a verifiable certificate through every snapshot it accepts.

- **Constraints:** PRIMARY KEY, UNIQUE (NULLS DISTINCT / NOT DISTINCT), NOT NULL, FOREIGN KEY
  (MATCH SIMPLE / FULL, ON DELETE RESTRICT), composite keys, on top-level columns.
- **Commits:** append, copy-on-write and merge-on-read `DELETE` / `UPDATE` / `MERGE` (position
  deletes and format v3 deletion vectors), compaction with unchanged rows, equality deletes on a
  key. Anything that cannot be proven is refused (`UNSUPPORTED_COMMIT_OPERATION`).
- **Certificates:** a BLAKE3 chain in snapshot summaries (RFC 0002), optionally signed with
  Ed25519 (RFC 0005); `integrity verify` recomputes them from the data files and names the first
  snapshot written around the Plane.
- **Safety:** a durable transaction log with crash recovery at every fault point (RFC 0004),
  idempotent retries, per-domain commit queues, bypass detection, detection of lost control-store
  files, rebuild and onboarding of existing data.
- **Operations:** integrity API and `integrity` CLI (constraints, verify, rebuild, disable, audit
  with actors, transactions, domains, keys), structured violation reports with redactable sample
  keys, Prometheus metrics, Docker image and Compose stack.

### Verified with

- Clients: Spark 3.5 with Iceberg 1.10, PyIceberg 0.12, iceberg-rust 0.10.
- Catalogs: Iceberg REST reference catalog 1.10, Apache Polaris 1.7, Lakekeeper 0.13, Nessie 0.108.
- Storage: local filesystem and S3 (SeaweedFS's S3 gateway).

### Measurements

About 60–65 ms to validate a 10 000-row child append against 1–10 M parent keys; about 80 commits/s
for one writer on one domain. Method and details: [`docs/benchmarks.md`](docs/benchmarks.md).

### Known limitations

Single node; signing keys are files (no HSM/KMS yet); no CHECK constraints or multi-table commits;
on Nessie `verify` cannot recompute certificates. Full list:
[`docs/limitations.md`](docs/limitations.md).
