# Changelog

## Unreleased

- Onboarding and rebuild run with bounded memory (ADR 0011 amendment): rows are read a row group at
  a time, keys are sorted externally within `limits.scan_memory` (default 512 MiB), and the new
  indexes are built beside the live ones and installed in one transaction. On one AWS
  m6id.4xlarge, 100 M keys onboard in 145 s and 1.5 GB (before: 329 s and 36 GB), and 1 000 M keys
  in 27 minutes and 1.5 GB (`docs/benchmarks.md`).
- Commit validation streams rows and sorts keys externally (ADR 0019): copy-on-write rewrites and
  compactions no longer need memory proportional to the rows they rewrite.
- `integrity-bench --settle` waits after large file writes before measuring.

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
