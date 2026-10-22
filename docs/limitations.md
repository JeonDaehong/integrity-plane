# Known limitations (0.1)

The Open Integrity Plane is pre-alpha. This page lists what it does not do, or does only partly.
Everything here is either out of scope for 0.1 (spec §27) or a gap found while building it.

## Out of scope for 0.1 (by design)

- CHECK constraints, branch-gated publication, picking up constraints from engine DDL: planned
  for 0.2. Certificates can be signed (RFC 0005). Merge-on-read position deletes and v3 deletion
  vectors are supported (ADR 0017, ADR 0018).
- Multi-table atomic commits from clients (`/transactions/commit` is refused): 0.3.
- Other table formats than Iceberg (ADR 0012); `ON DELETE CASCADE`; per-key locking (ADR 0015).

## Commits

- Anything outside the capability matrix in [`compatibility.md`](compatibility.md) is refused with
  `UNSUPPORTED_COMMIT_OPERATION`, including removing equality delete files, rollbacks of `main` to an existing snapshot,
  non-Parquet data files and key columns nested in structs.
- Flink upsert tables (ADR 0009, ADR 0028): the upsert key must be the table's only PK/UNIQUE
  constraint and no FK may be declared on the table. They cannot be compacted through the Plane
  (data files cannot be removed while equality delete files exist), and every commit reads all
  live position delete files, whose number grows with each checkpoint that repeats a key.
- On tables with position deletes, each commit reads all manifests of both snapshots and every live
  position delete file.
- Files outside a table's location and `write.*.path` properties (imported with `add_files`,
  migrated) are not read unless listed in `[storage] allowed_locations` (RFC 0013): such commits
  are refused with `UNSUPPORTED_COMMIT_OPERATION`.
- Commits to branches other than `main` pass through uncertified.
- Validation reads the footer and the key column chunks of every data file a commit adds or
  removes (ranged reads), so its cost follows the key columns, not the file size; manifests are
  read whole. About 5 µs per changed key on the benchmark machine.
- Commit validation streams rows and sorts keys within `limits.scan_memory` (ADR 0019), and a
  commit of one snapshot is then decided, staged, logged and applied in chunks (ADR 0025), so large
  appends and rewrites need scratch disk instead of memory. A commit of several snapshots still
  holds the keys it changes in memory after sorting them, and one with equality deletes is read
  whole. `verify` streams each snapshot's rows the same way (one snapshot at a time, each within
  `limits.scan_memory`, beside any commits validating at the same time).
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

- Certificates are signed only with `[signing]` configured. The key is a file in the control
  store, a Vault Transit key or a key in a PKCS#11 token (RFC 0009); PKCS#11 is tested with SoftHSM2
  only, not with hardware HSMs. Cloud KMS is not supported: AWS KMS and Azure Key Vault would need a
  second signature algorithm (ECDSA P-256), a certificate format change (new RFC); Google Cloud KMS
  (Ed25519) is not implemented yet.
- The signing backend is called once per certificate, on the commit path; its latency adds to
  every commit, and while it cannot sign, commits are refused (INT-018).
- A snapshot written around the Plane is refused at the next commit (RFC 0008, RFC 0011), except
  one whose certificate was computed correctly for valid data: catching up accepts it, as a commit
  through the Plane would have been. With `[signing] require = true` only snapshots signed with
  this Plane's keys are followed.
- `verify` cannot recompute snapshots with equality deletes (reported `UNVERIFIABLE`) or chains whose
  snapshots have expired, and only verifies back to the latest anchor.

## Operations

- Each Plane is one process with its own control store (local files). For availability, run
  several behind a load balancer with `[ha] fence = "multi-table"` (RFC 0012). The upstream catalog
  must apply multi-table commits atomically, including under concurrency, and serve the table
  history: of the catalogs tested only **Lakekeeper** does (`compatibility.md`). Polaris 1.7 and the
  REST fixture let concurrent transactions skip each other's assertions; Nessie serves only the
  current snapshot, so a Plane cannot catch up.
  Without the fence, run one Plane per domain: while it is down, writers cannot commit to enforced
  tables (fail closed). A restored control store, or another Plane, catches up from the table
  history (RFC 0011); FK-linked tables that both moved need fenced commits or a rebuild
  ([`recovery.md`](recovery.md#single-node-and-power-loss)).
- Planes behind one load balancer each keep their own registry: constraints must be registered on
  each, in the same order (the constraint set version is part of the certificate). Commits across
  one domain are serialized by the catalog: concurrent commits to a domain through different Planes
  retry on 409.
- Catching up validates every missed snapshot again, so it costs what those commits cost, inside
  the next commit's latency and read budget.
- Index stores written before RFC 0008 record no heads: every enforced domain refuses commits
  (`INDEX_DEGRADED`) until it is rebuilt once.
- The transaction log keeps finished transactions for `[txn_log] retention` (default 24 h), then
  deletes them; idempotency keys are honoured for that long. Each keeps the response given, for an
  accepted commit the table's whole metadata (compressed, RFC 0010), so the log grows with commits
  per retention period times the size of the table's metadata; expire snapshots to keep it small. The audit log and the per-transaction
  error reports in `registry.redb` are never pruned.
- The persistent index takes about 100 bytes per key.
- Losing `registry.redb` cannot be detected from the other files; constraints from the
  configuration file are re-imported and tables with data must be onboarded again.
- Integrity API tokens are configured statically (one per operator, ADR 0026); changing them needs
  a restart. Ten failed authentications in a minute lock a client out for the rest of that minute
  (RFC 0007). Audit actors of commits are
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
- Not tested: power loss (only process kills; what the Plane relies on is in
  [`recovery.md`](recovery.md#single-node-and-power-loss)). Linux and Windows only.
