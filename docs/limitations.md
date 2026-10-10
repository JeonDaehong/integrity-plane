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
- The transaction log keeps every staged index delta and is never truncated; it grows with write
  volume.
- The persistent index takes about 100 bytes per key.
- Losing `registry.redb` cannot be detected from the other files; constraints from the
  configuration file are re-imported and tables with data must be onboarded again.
- The integrity API has one optional shared bearer token and no rate limiting; audit actors are
  self-declared (`X-Integrity-Actor` or `User-Agent`), not authenticated.
- A disabled domain (spec §19) accepts anything, uncertified, until it is rebuilt.
- Verified clients: Spark 3.5 with Iceberg 1.10, PyIceberg 0.12, iceberg-rust 0.10, against the
  Iceberg REST fixture catalog; Spark and PyIceberg also against Apache Polaris 1.7 (OAuth2,
  catalog prefix), Lakekeeper 0.13 and Nessie 0.108. On Nessie, `verify` cannot recompute
  certificates (the catalog serves only the current snapshot). S3 is tested
  against SeaweedFS (MinIO no longer publishes container images), not against AWS S3.
- Not tested: power loss (only process kills), Linux and Windows only.
