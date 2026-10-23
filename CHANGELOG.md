# Changelog

## Unreleased

**Added:**
- Flink upsert jobs that change a key twice between checkpoints: tables with both position and
  equality delete files are accepted when a commit changes the deleted positions of its own new
  data files only (ADR 0028).

**Fixed (integrity):** three commit shapes whose rows Iceberg reads differently from the Plane
are refused (ADR 0028). Ordinary writers do not produce them; a crafted commit could have made the
index count rows that readers cannot see, or hide rows that readers see:
- a new data file with an explicit sequence number in a table with equality delete files;
- a position delete file from an earlier snapshot naming a data file the commit adds;
- a file live in both snapshots whose data sequence number changes.

**Fixed (integrity):** delete files are applied as Iceberg readers apply them (ADR 0030): position
deletes and deletion vectors only within their partition spec and tuple, at or above the data
file's sequence number, only to a `referenced_data_file` when set, and a deletion vector replaces
the position delete files of its data file. Partitioned equality delete files are refused: the
Plane treated them as global, so after partition evolution a Flink upsert could leave a duplicate
that readers see. Flink upsert tables must be unpartitioned for now.

**Fixed:** registering a constraint on a column with a v3 `initial-default` is refused: older
files read as the default to readers but as NULL to key extraction.

**Security:**
- Commits are matched to constraints by table UUID, not by the name in the path (ADR 0029).
  Lakekeeper resolves names ignoring case, so a commit to `DB/ORDERS` reached `db.orders` without
  validation. Commits to tables without constraints now cost one table load and carry an added
  `assert-table-uuid` requirement.
- Request paths with `.` or `..` segments (also as `%2e`) or a `\` are refused (400,
  `UNSUPPORTED_COMMIT_OPERATION`). The gateway routed on the raw path but the forwarded URL resolved
  them, so a commit to a constrained table could reach the catalog without validation (#5).

## 0.1.0 — 2026-10-17

Third pre-release. **Pre-alpha, not production-ready**: see
[`docs/limitations.md`](docs/limitations.md) and [`docs/security-review.md`](docs/security-review.md).
This release is about running the Plane next to others and recovering on its own: index heads and
catching up from the table history, several Planes behind a load balancer (on Lakekeeper), signing
keys in Vault or an HSM, a smaller transaction log, fixes from an internal security review, and
published container images and binaries.

**Upgrading from 0.0.2:**
- Add an API token (`[server] admin_token` or `[[server.api_tokens]]`), or `[server] open_api =
  true` where an open API is intended: the Plane does not start without one, nor with the
  example's placeholder `change-me` (ADR 0027).
- Rebuild every domain once (`POST /v1/integrity/indexes/{id}/rebuild`): 0.0.2 index stores record
  no heads (RFC 0008) and answer `INDEX_DEGRADED` until rebuilt.
- Tables with files outside their location (imported with `add_files`) need
  `[storage] allowed_locations` (RFC 0013).
- The container image runs as uid 10001: `chown -R 10001:10001` a control-store volume created by
  an earlier image.
- A 0.0.2 binary cannot read a 0.1.0 transaction log (compressed terminal records, RFC 0010).

Measured on AWS (`docs/benchmarks.md`): compacting 100 M rows 45 s (70 s in 0.0.2's version C),
a merge-on-read delete of one row 9 ms (74 ms), a 20 M-row append in 23 s of validation and
2.4 GB; one hour of three Planes under chaos (17 kills) left no duplicate or dangling key.


- Hostile Parquet files can no longer abort the Plane by declaring huge sizes: the footer and every
  page header of a column chunk are checked before the decoder allocates what they declare
  (collections longer than their bytes, a schema element with more children than the schema, pages
  over 256 MiB or past their chunk, dictionaries with more values than their bytes hold). Two such
  files found by fuzzing made the Parquet decoder allocate 5 and 6 GiB; they are now refused as
  `UNSUPPORTED_COMMIT_OPERATION`.

- The Plane reads only a table's own files (RFC 0013): every manifest list, manifest, data and
  delete file of a commit must lie under the table's `location` or `write.*.path` properties, or
  `[storage] allowed_locations`; otherwise the commit is refused (INT-012) before anything is
  read. Before, a writer could make the Plane read any file its credentials reach (another
  table's data file, a local file) and learn key values from the violation report. Onboarding,
  rebuild, catching up and `verify` apply the same rule.

- The integrity API is closed by default (ADR 0027): the Plane refuses to start without
  `[server] admin_token` or `[[server.api_tokens]]`, unless `[server] open_api = true` (local tests
  only). **Upgrading:** add a token, or `open_api = true` where an open API is intended.

- Several Planes in front of the same tables (RFC 0012): with `[ha] fence = "multi-table"`, a
  commit is forwarded as one multi-table transaction that also asserts `main` of every other
  table of its integrity domain, so the catalog refuses it (409) if another Plane, or a writer,
  changed the domain after it was validated. Certified snapshots record the domain state they
  were validated against (`integrity.domain-state`), and catching up replays FK-linked tables in
  that order. The client gets the table as loaded after the commit; an acknowledged transaction
  whose snapshot is not on `main` is answered 409. Usable upstream: Lakekeeper (atomic under
  concurrency, full history; `compat/multitable_probe.py`, `compat/soak.py`). Not usable: Polaris 1.7
  and the Iceberg REST fixture (concurrent transactions skip each other's assertions, the fixture
  can lose changes), Nessie (serves only the current snapshot). CI runs every client with the fence
  for functional coverage. A Plane that first sees a table after other Planes filled it follows the
  table's whole certified history instead of asking for onboarding.

- Catching up from the table history (RFC 0011): when `main` of a domain table moved past the
  snapshot its indexes reflect, the Plane validates the certified snapshots since then again from
  their data files, checks that they reproduce their certificates, and applies them before the
  next commit, instead of refusing until a rebuild. A control store restored from an older copy, or
  a second Plane, now continues on its own; uncertified, forged, rolled-back snapshots and
  FK-linked tables that both moved are still refused. `[catch_up] enabled`, `max_snapshots`;
  `INDEX_CAUGHT_UP` audit events; `integrity_catch_up_snapshots_total`. With
  `[signing] require = true`, only snapshots signed with this Plane's keys are followed.

- Releases publish a container image to `ghcr.io/jeondaehong/integrity-plane` (linux/amd64,
  linux/arm64) and binaries for Linux, macOS and Windows. The image now runs as an unprivileged
  user (uid 10001): a control-store volume created by an earlier image needs
  `chown -R 10001:10001` once. `integrity-server` and `integrity` answer `--version`.
- `docs/adoption.md`: a first deployment step by step, including keeping writers from reaching
  the upstream catalog directly.
- Terminal records in the transaction log are compressed with zstd (RFC 0010). They keep the
  response given, for an accepted commit the table's whole metadata, for `[txn_log] retention`;
  in a benchmark of 3 200 commits to one table the log shrank from 4 096 MiB to 523 MiB, with
  the same commit rates. Logs written before are
  read unchanged; a version without RFC 0010 cannot read the new records (downgrade note in the
  RFC).

- Signing keys outside the Plane (RFC 0009): `[signing] backend = "vault-transit"` signs with a
  HashiCorp Vault Transit `ed25519` key (pinned to one key version per run), `backend = "pkcs11"`
  with an Ed25519 key in a PKCS#11 token. Signatures and verification are unchanged (RFC 0005).
  A commit whose certificate cannot be signed is refused before forwarding with the new code
  INT-018 `SIGNER_UNAVAILABLE` (409); `integrity_signer_failures_total` counts them. New
  dependency: `cryptoki` (Apache-2.0). On Unix, a newly created key file's directory is flushed
  too.

- Index heads (RFC 0008): the index store records the `main` snapshot each table's indexes
  reflect, atomically with every index change, and a commit is refused with `BYPASS_DETECTED` when
  `main` of any table of its domain is not that snapshot. A certified snapshot no longer passes
  on its certificate alone, which catches a second Plane in front of the same tables, a control
  store restored from an older copy, and `main` moved back around the Plane. **Upgrading:** index
  stores from 0.0.2 record no heads; every enforced domain answers `INDEX_DEGRADED` until it is
  rebuilt once (`POST /v1/integrity/indexes/{id}/rebuild`).

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
- Named integrity API tokens (ADR 0026): `[[server.api_tokens]]` entries with a `name` and a
  `token` or its `token_sha256`; `admin_token` is the token named `admin`. Audit events of the API
  name the token's holder as `actor` with `authenticated: true`, and keep a differing
  `X-Integrity-Actor`/`User-Agent` as `declared_actor`. Commit events stay declared
  (`authenticated: false`).
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
