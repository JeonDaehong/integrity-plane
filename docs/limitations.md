# Known limitations (0.1)

The Open Integrity Plane is pre-alpha. This page lists what it does not do, or does only partly.
Everything here is either out of scope for 0.1 (spec §27) or a gap found while building it.

## Out of scope for 0.1 (by design)

- CHECK constraints, branch-gated publication, picking up constraints from engine DDL: planned
  for 0.2. Certificates can be signed (RFC 0005). Merge-on-read position deletes and v3 deletion
  vectors are supported (ADR 0017, ADR 0018).
- Multi-table atomic commits (`/transactions/commit` is refused), high availability: 0.3.
- Other table formats than Iceberg (ADR 0012); `ON DELETE CASCADE`; per-key locking (ADR 0015).

## Commits

- Anything outside the capability matrix in [`compatibility.md`](compatibility.md) is refused with
  `UNSUPPORTED_COMMIT_OPERATION`, including position and equality deletes in the same table, removing equality delete files, rollbacks of `main` to an existing snapshot,
  non-Parquet data files and key columns nested in structs.
- On tables with position deletes, each commit reads all manifests of both snapshots and every live
  position delete file.
- Commits to branches other than `main` pass through uncertified.
- Validation reads the footer and the key column chunks of every data file a commit adds or
  removes (ranged reads), so its cost follows the key columns, not the file size; manifests are
  read whole. About 5 µs per changed key on the benchmark machine.
- Commit validation streams rows and sorts keys within `limits.scan_memory` (ADR 0019), and a
  commit of one snapshot is then decided, staged, logged and applied in chunks (ADR 0025), so large
  appends and rewrites need scratch disk instead of memory. A commit of several snapshots still
  holds the keys it changes in memory after sorting them, and one with equality deletes is read
  whole. `verify` reads the rows of each snapshot it recomputes whole.
- One domain commits about 80 times per second with one writer on the benchmark machine, and about
  25 times per second when 16 writers race on one table (about a third of the same race without
  the Plane; `benchmarks.md`).

## Constraints, onboarding and rebuild

- Constraints are on top-level columns only; NOT NULL on one column.
- Registering, dropping and rebuilding pause commits in all domains while they run.
- Tables with equality delete files cannot be onboarded or rebuilt; compact them first. Position
  deletes are applied; their deleted positions are held in memory during the scan (a few bytes
  each).
- Onboarding and rebuild run on one thread and read one table at a time; they need scratch disk
  space in the control store for sorted key runs and the new indexes (ADR 0011).
- Foreign keys that form a cycle between different tables are refused (a self-reference is fine).
- Violation reports give counts and up to ten sample keys per constraint, but not the files the
  offending rows are in.
- Dropping a constraint leaves its index data in the store.

## Certificates and verification

- Certificates are signed only with `[signing]` configured; the key is a file in the control
  store. Next step: signer backends that keep the key in an HSM or KMS (PKCS#11 and Vault Transit
  first, both with Ed25519; then cloud KMS). AWS KMS and Azure Key Vault may need a second signature
  algorithm (ECDSA P-256), which is a certificate format change (new RFC).
- A forged certificate copied into a bypassing snapshot passes the commit-time check; only `verify`
  detects it.
- `verify` cannot recompute snapshots with equality deletes (reported `UNVERIFIABLE`) or chains whose
  snapshots have expired, and only verifies back to the latest anchor.

## Operations

- Single node. The control store is local files; there is no replication.
- The transaction log keeps finished transactions for `[txn_log] retention` (default 24 h), then
  deletes them; idempotency keys are honoured for that long. The audit log and the per-transaction
  error reports in `registry.redb` are never pruned.
- The persistent index takes about 100 bytes per key.
- Losing `registry.redb` cannot be detected from the other files; constraints from the
  configuration file are re-imported and tables with data must be onboarded again.
- The integrity API has one optional shared bearer token and no rate limiting; audit actors are
  self-declared (`X-Integrity-Actor` or `User-Agent`), not authenticated.
- A disabled domain (spec §19) accepts anything, uncertified, until it is rebuilt.
- Verified clients: Spark 3.5 with Iceberg 1.10, PyIceberg 0.12, iceberg-rust 0.10, against the
  Iceberg REST fixture catalog; Spark and PyIceberg also against Apache Polaris 1.7 (OAuth2,
  catalog prefix), Lakekeeper 0.13 and Nessie 0.108. On Nessie, `verify` cannot recompute
  certificates (the catalog serves only the current snapshot). The client tests use S3 through
  SeaweedFS (MinIO no longer publishes container images); AWS S3 was used only by the benchmarks.
- Validating a commit costs time per row it adds, removes or rewrites: with Spark on S3,
  inserting 1 M rows takes 4.6–5.1 s through the Plane against 0.7 s directly, an initial load of
  30 M rows 38 s against 6 s, compacting 33 M rows 59 s against 33 s (`docs/benchmarks.md`). A
  whole nightly ETL job (reads, joins, two MERGEs, an aggregate) took 30–31 s against 22–24 s.
  Sorting and probing every key, and for a rewrite comparing every row, dominate.
- Commits to a table with position delete files read every manifest of both snapshots and every
  live position delete file (each once, 16 at a time; ADR 0020), so their cost grows with the
  number of manifests and live delete files. On S3 a merge-on-read delete of one row in a 100-file
  table takes about 0.5 s, 0.65 s with 50 accumulated delete files (`docs/benchmarks.md`).
- Not tested: power loss (only process kills), Linux and Windows only.
