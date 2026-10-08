# Open Integrity Plane — Design Specification

> **Status:** Draft v1 (2026-10-07).
> **License:** Apache-2.0 · **Language:** Rust (edition 2024) · **First format:** Apache Iceberg (v2, v3 where noted)

Normative keywords **MUST / MUST NOT / SHOULD / MAY** follow RFC 2119.

---

## Part I — Thesis

### 1. The gap

Open table formats gave object storage ACID snapshots, but not relational integrity. Today:

- Iceberg has no PK/FK concept in its metadata beyond `identifier-field-ids`.
- Engines that accept `PRIMARY KEY` / `FOREIGN KEY` on lakehouse tables treat them as **informational** (e.g. Unity Catalog FKs, Snowflake-managed Iceberg identifier fields are not enforced as UNIQUE/NOT NULL).
- Primary-key table formats (Hudi, Paimon) **merge** duplicates instead of **rejecting** them, and only within one table.
- Referential integrity is pushed to post-hoc tests (dbt tests, Great Expectations), which detect bad data *after* it is visible to readers.

Nobody enforces **cross-table referential integrity at the commit boundary, independent of the writing engine**. That is this project.

### 2. Core thesis

> **A managed lakehouse table never publishes a snapshot that violates its declared integrity constraints — and every published snapshot carries a verifiable proof that it was checked.**

```text
Today:     write → commit → (maybe) detect later
Plane:     write → validate → commit + certificate   OR   reject
```

### 3. What is genuinely new

The project MUST be honest about prior art (§5) and MUST concentrate effort on these three ideas, which together do not exist in any open project we know of:

1. **Engine-independent, commit-time constraint enforcement for open table formats.**
   PK / UNIQUE / NOT NULL / FK are enforced at the catalog commit boundary, so Spark, Flink, Trino, PyIceberg and iceberg-rust writers all get the same verdict without modification.

2. **Cross-table referential integrity with persistent bidirectional key indexes.**
   Child inserts probe the parent key index; parent deletes probe a reverse-reference index. Validation cost is proportional to the *changed* key set, never to table size.

3. **Integrity certificates (verifiable snapshot lineage).**
   Every accepted snapshot receives a certificate — a hash-chained digest of *(constraint set, parent certificate, key delta digest, verdict)* — written into the snapshot summary and the Plane's log. Anyone holding the table metadata can run `integrity verify` and learn:
   - whether the current snapshot was validated;
   - under which constraint set version;
   - **where the chain breaks** if a writer bypassed the Plane.

   This turns "you can bypass the gateway" from an unprovable caveat into a **detectable, attributable event**, without the Plane having to scan anything. Readers and downstream engines MAY refuse to read uncertified snapshots.

Everything else in this document (indexes, recovery, audit) exists to make those three promises true.

### 4. What this project is NOT

Not a data-quality framework, dbt test runner, profiler, table format, CDC engine, query engine, catalog UI, or general schema registry. Those are integration points at most.

### 5. Prior art and positioning (verified 2026-10-07)

| Project | What it does | Relation to us |
|---|---|---|
| Iceberg REST catalog spec | Server-side commits with `requirements` (optimistic concurrency); multi-table commit endpoint | Our integration boundary |
| Lakekeeper (Rust REST catalog) | Iceberg REST catalog on iceberg-rust; extensible via traits incl. `ContractValidation` | **Closest neighbor.** Potential host for an embedded mode (§17.3). MUST evaluate before Phase 7 and record an ADR. |
| Apache Polaris, Gravitino, Nessie | Catalogs / governance | Upstream catalogs we proxy |
| Delta Lake | Enforces NOT NULL / CHECK per table; catalog-managed commits | No cross-table FK; format-specific |
| Hudi / Paimon | Record-key indexes, upsert merge | Merge ≠ reject; single-table |
| Write-Audit-Publish (Iceberg branches) | Manual pattern: write to branch, test, fast-forward | We automate it with real constraint semantics (§17.2) |
| Great Expectations / dbt tests | Post-hoc checks | Detect, don't prevent |

References:
- https://iceberg.apache.org/docs/nightly/rest-protocol/
- https://github.com/lakekeeper/lakekeeper
- https://docs.delta.io/delta-catalog-managed-tables/
- https://github.com/apache/datafusion/issues/16309

The positioning statement is: **"database-grade integrity for lakehouse tables, with proof."** We do not compete on dashboards or on being a catalog.

---

## Part II — Principles

1. **Fail closed.** If a constraint cannot be proven for a commit, the commit is rejected with an explicit code. No silent advisory fallback, ever.
2. **Correct before fast.** A global serial commit queue that is correct beats fine-grained locking that is not.
3. **Indexes are derived state.** Table metadata + data files are authoritative; indexes are always rebuildable from them.
4. **Deterministic verdicts.** Same table state + same constraint set ⇒ same verdict and same certificate digest.
5. **Incremental.** Work is proportional to the changed key set and the files touched by the commit.
6. **Format isolation.** `integrity-core` knows nothing about Iceberg, HTTP or object storage.
7. **Explicit capability.** Every Iceberg operation is either supported (with documented semantics) or rejected with `UNSUPPORTED_COMMIT_OPERATION`. There is no third state.
8. **Honest guarantees.** Enforcement is guaranteed only on the managed commit path; certificates make violations of that assumption visible.
9. **Public design.** Semantic changes go through public RFCs (`docs/rfc/`).

---

## Part III — Semantics

### 6. Constraint model

```rust
pub struct Constraint {
    pub id: ConstraintId,
    pub table: TableId,
    pub name: String,
    pub kind: ConstraintKind,
    pub mode: EnforcementMode,      // Enforced | Disabled (explicit only; no advisory default)
    pub version: ConstraintSetVersion,
}

pub enum ConstraintKind {
    PrimaryKey(KeySpec),
    Unique(UniqueSpec),
    ForeignKey(ForeignKeySpec),
    NotNull(ColumnRef),
    Check(CheckSpec),               // 0.2+, behind feature flag
}

pub struct KeySpec { pub columns: Vec<ColumnRef> }          // ordered; composite = one tuple

pub struct UniqueSpec { pub key: KeySpec, pub nulls: NullsMode }

pub enum NullsMode { Distinct /* default, SQL standard */, NotDistinct }

pub struct ForeignKeySpec {
    pub child: KeySpec,
    pub parent_table: TableId,
    pub parent_constraint: ConstraintId,  // MUST reference a PK or UNIQUE constraint
    pub match_mode: MatchMode,            // Simple (default) | Full
    pub on_delete: ReferentialAction,     // Restrict only in 0.1
}
```

`ColumnRef` is an **Iceberg field ID**, never a column name, so renames do not break constraints.

The constraint set of a table has a monotonically increasing `ConstraintSetVersion`. Every certificate records it.

### 7. NULL semantics (normative)

| Constraint | Rule |
|---|---|
| PRIMARY KEY | Every key column is implicitly NOT NULL. Any NULL ⇒ `NOT_NULL_VIOLATION`. |
| UNIQUE, `NULLS DISTINCT` (default) | A tuple containing any NULL never conflicts with another tuple. Any number of such rows is allowed. Not indexed. |
| UNIQUE, `NULLS NOT DISTINCT` | NULL compares equal to NULL; the tuple is indexed with a NULL marker. |
| FK, `MATCH SIMPLE` (default) | If any child FK column is NULL, no parent match is required. |
| FK, `MATCH FULL` | All-NULL ⇒ no match required. Partially NULL ⇒ `FOREIGN_KEY_VIOLATION`. |

Each row of this table MUST have dedicated unit and property tests.

### 8. Commit-level semantics

Constraints are checked against the **post-commit state**. Within one commit:

1. Compute the commit's **net key delta** per constraint: `added_keys` and `removed_keys` as multisets.
2. **Intra-commit duplicates are violations.** If `added_keys` contains a PK/UNIQUE tuple twice, reject before any index lookup.
3. A key both removed and re-added in the same commit (COW rewrite of a row) nets to "unchanged".
4. Then probe the indexes with the net delta.

> ⚠ Lookup deduplication (§13.3) applies to FK parent probes only. PK/UNIQUE probes MUST first count multiplicities, otherwise in-batch duplicates are masked.

`RESTRICT` and `NO ACTION` are equivalent in 0.1 because constraints are never deferred beyond a commit. `CASCADE` / `SET NULL` are out of scope until the protocol is proven; a cascade would require the Plane to *author* commits, which is a different product.

### 9. Key encoding

A typed, versioned, **order-preserving (memcomparable)** binary encoding.

```text
EncodedKey = format_version:u8 · for each column: type_family_tag · null_marker · value_bytes
```

Rules:

- **Type families**, not physical types: `int` and `long` share the *Integer* family, so a child `int` FK matches a parent `long` PK and `int → long` widening needs no re-encode.
- **Supported key families in 0.1:** Integer, Decimal (scale MUST match between FK sides; encoded as unscaled value), String (raw UTF-8 bytes, binary comparison, no collation), Binary/Fixed, UUID, Date, Timestamp (µs or ns, tz and non-tz are *different* families), Boolean.
- **Rejected as keys in 0.1:** float, double (NaN and −0.0 make equality ambiguous), nested types, variant, geospatial. Rejection happens at constraint registration with `INVALID_CONSTRAINT`.
- Strings and binaries use escape-terminated framing so that composite keys sort correctly and remain unambiguous.
- Property: for the supported domain, `encode(a) == encode(b) ⇔ a ≡ b` **and** `encode(a) < encode(b) ⇔ a < b`, where ≡ is the equality defined above. Both are property-tested and fuzzed.

---

## Part IV — Architecture

### 10. Topology

```text
   Spark   Flink   Trino   PyIceberg   iceberg-rust
     └───────┴───────┴──────────┴────────────┘
                        │  Iceberg REST (catalog.uri = Plane)
                        ▼
        ┌──────────────────────────────────────┐
        │            Integrity Plane           │
        │  REST gateway ─▶ Commit pipeline     │
        │  Constraint registry   Domain queues │
        │  Key indexes           Txn log       │
        │  Certificate chain     Audit log     │
        └──────────┬──────────────────┬────────┘
                   │ proxied commits  │ reads key columns only
                   ▼                  ▼
          Upstream REST catalog    Object storage
          (Polaris/Lakekeeper/…)   (Iceberg metadata + Parquet)
```

- Writers still write data files and manifests themselves. The Plane **owns only the publication decision.**
- The upstream catalog MUST NOT be reachable by writers directly (network policy / credential scoping). This is a deployment requirement, documented in `docs/threat-model.md`.

### 11. Integrity domains and concurrency (MVP)

An **integrity domain** is a connected component of the FK graph (a table with no FKs is its own domain).

- Each domain has **one serial commit queue**. Commits in different domains run in parallel.
- Within a domain, a commit's validation, decision, upstream publication and index finalization happen with the queue held. No two commits in a domain interleave, so the T1-delete-parent / T2-insert-child race is impossible by construction.
- Rationale: lakehouse commit rates are commits per second at most, but each commit can carry millions of keys. Per-key lock acquisition for millions of keys costs more than serialization saves. Fine-grained key-range locking is an **optimization**, allowed only after Phase 9 benchmarks show the queue is a bottleneck, and only via RFC.
- Registering an FK merges two domains; the merge drains both queues first.

### 12. Crates

```text
crates/
  integrity-types        ids, field refs, error codes (no deps)
  integrity-core         constraint model, key encoding, delta model, verdicts, certificates
  integrity-index        KeyIndex trait + in-memory and embedded persistent backends
  integrity-validator    PK / UNIQUE / NOT NULL / FK validators + validation planner
  integrity-txn          transaction state machine, durable log, recovery
  integrity-iceberg      Iceberg adapter: commit inspection, manifest diff, Parquet key extraction
  integrity-server       axum REST gateway + /v1/integrity API + metrics
  integrity-cli          `integrity` operator CLI (incl. `verify`)
  integrity-reference    in-memory relational oracle for differential tests
  integrity-bench        benchmarks
```

Dependency direction is strictly downward: `server → iceberg/txn/validator → index → core → types`. `integrity-core` MUST NOT depend on `iceberg`, `object_store`, `axum` or `tokio`.

### 13. Indexes

#### 13.1 What is stored

Indexes store **keys and counts, not row locations.** Row locators (file path + position) change on every compaction and copy-on-write rewrite; storing them would force full index rewrites on routine maintenance.

| Index | Key | Value |
|---|---|---|
| Unique index (per PK/UNIQUE) | `EncodedKey` | `{ last_snapshot: SnapshotId }` (presence; multiplicity ≤ 1 by invariant) |
| Reference index (per FK) | `(parent EncodedKey, fk ConstraintId)` | `{ child_count: u64 }` |

Child deletes decrement `child_count`. To know which parent key a deleted child row referenced, the adapter reads the FK columns of the deleted rows from the source data files (§15). If Iceberg v3 row lineage (`_row_id`) is available it MAY be used to speed this up, but it is not required.

#### 13.2 Trait

```rust
pub trait KeyIndex: Send + Sync {
    fn get_many(&self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>>;
    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta>;   // no visible effect
    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> Result<()>; // atomic, idempotent per epoch
    fn epoch(&self) -> Result<IndexEpoch>;
}
```

`apply` MUST be idempotent for a given `(txn_id, epoch)` so that recovery can replay it safely.

#### 13.3 Validation planner

Before probing, the planner groups the net delta by constraint, counts multiplicities (§8), and deduplicates FK parent probes (10 000 orders with 8 000 distinct customers ⇒ 8 000 probes). Probes are batched through `get_many`.

#### 13.4 Backend

Phase 2 uses an in-memory `BTreeMap` backend. Phase 4 selects an embedded persistent backend (candidates: `fjall`, `redb`, RocksDB) by benchmark and ADR. Requirements: atomic batch writes, ordered iteration, crash safety, checksums. The backend MUST NOT appear in public APIs.

---

## Part V — The commit protocol

### 14. Pipeline

```text
receive UpdateTable request
  │ 1. authenticate, resolve table, load constraint set (version V)
  │ 2. enqueue on the table's integrity domain; wait for turn
  │ 3. load current table metadata from upstream; check request requirements
  │    (stale base ⇒ 409 so the client refreshes and retries — standard Iceberg behavior)
  │ 4. classify operation against the capability matrix (§15); unsupported ⇒ reject
  │ 5. read manifests/files added & removed by the new snapshot; project key columns only
  │ 6. compute net key delta, intra-commit duplicates, NOT NULL
  │ 7. probe indexes (PK/UNIQUE, FK parents, reverse references)
  │ 8a. FAIL ⇒ record REJECTED, return 400 with integrity error body
  │ 8b. PASS ⇒ stage index delta; compute certificate; log VALIDATED (fsync)
  │ 9. inject certificate into snapshot summary; log COMMITTING; forward to upstream
  │ 10. upstream OK ⇒ apply index delta; log COMMITTED; return upstream response
  │     upstream definite failure ⇒ discard staged delta; log ABORTED; pass error through
  │     upstream unknown (timeout/5xx) ⇒ stay COMMITTING; resolve via §16 before releasing queue
  ▼
release domain queue
```

### 15. Iceberg capability matrix (0.1)

Classification uses the snapshot `operation` and the added/removed manifest entries, not client-supplied claims (summaries are hints only).

| Change | 0.1 behavior | Cost |
|---|---|---|
| `append` (data files only) | Supported | Read key cols of added files |
| `overwrite`, copy-on-write (files removed + added; Spark default for DELETE/UPDATE/MERGE) | Supported. Net delta = multiset(keys(added)) − multiset(keys(removed)) | Read key cols of added **and** removed files |
| `replace` (compaction, `rewrite_data_files`) | Supported. MUST verify the key multiset (after applying delete files) is unchanged; otherwise reject | Read key cols of both sides; no index writes |
| `delete` dropping whole files | Supported | Read key cols of removed files |
| Equality deletes whose fields == exactly a PK/UNIQUE key (Flink upsert) | Supported | Delete-file contents only |
| Equality deletes on any other fields | **Rejected** (would need a table scan) | — |
| Position deletes / v3 deletion vectors (merge-on-read) | **Rejected in 0.1**, planned 0.2 | — |
| Schema change touching a constrained field | Rejected unless it is `int→long` / decimal precision widening (same family, §9) | — |
| Schema/property changes not touching constrained fields | Pass-through | — |
| `remove-snapshots` (expire), set/remove properties, sort/partition spec | Pass-through | — |
| Commits to refs other than `main` | Pass-through, **uncertified** | — |
| Moving `main` to a non-child snapshot (rollback, cherry-pick, `set-current-snapshot`) | Rejected in 0.1; requires re-validation by diff (0.2) | — |
| Multi-table commit endpoint | Rejected in 0.1 (0.3) | — |

Any change not listed is rejected with `UNSUPPORTED_COMMIT_OPERATION` naming the change type. This table is normative and lives in `docs/compatibility.md`.

**Ordering consequence:** parent and child rows in different tables are separate commits in 0.1. Insert parents first, children second; delete children first, parents second.

### 16. Durability and recovery

Transaction states:

```text
PREPARED → VALIDATED → COMMITTING → COMMITTED
    └──────────┴────────────┴─────→ ABORTED
    └─→ REJECTED   (terminal; validation failed)
```

The transaction log is an append-only, checksummed, fsync'd log in the control store. A `VALIDATED` record contains: `txn_id`, `request_id`, table, base snapshot, proposed snapshot id, constraint set version, index epoch, staged delta reference, certificate.

Recovery on start (and when an upstream call ends in an unknown state):

1. For every transaction in `VALIDATED` or `COMMITTING`, load table metadata from upstream.
2. If the proposed snapshot id is in the table's snapshot log on `main` ⇒ apply staged delta (idempotent), mark `COMMITTED`.
3. Otherwise ⇒ discard staged delta, mark `ABORTED`.
4. If upstream is unreachable ⇒ the domain stays blocked and reports `RECOVERY_REQUIRED`. It does **not** guess.

Because the domain queue is held across the upstream call, there is never more than one unresolved transaction per domain, which keeps recovery trivially deterministic.

Duplicate `request_id`s return the recorded decision instead of re-executing.

### 17. Publication modes

#### 17.1 Inline gateway (0.1)
Synchronous validation inside the commit request, as in §14. Bounded by `max_inline_validation_bytes` (key-column bytes to read). Exceeding the budget ⇒ `VALIDATION_BUDGET_EXCEEDED` with a hint to use 17.2. Clients' HTTP timeouts MUST be documented in `docs/iceberg.md`.

#### 17.2 Branch-gated publication (0.2)
Writers commit freely to an ingest branch (e.g. `ingest`). The Plane validates each new branch snapshot asynchronously and fast-forwards `main` only when valid, issuing the certificate on `main`. This is Write-Audit-Publish with real constraint semantics and no request-time latency limit. Rejected snapshots stay on the branch with an attached violation report.

#### 17.3 Embedded (investigate)
Run the validation pipeline inside a Rust catalog (e.g. via Lakekeeper's extension traits) instead of as a proxy. Decision via ADR after Phase 7.

### 18. Integrity certificates

```text
cert_n = H( "oip-cert-v1"
          ‖ table_uuid ‖ snapshot_id_n ‖ parent_snapshot_id
          ‖ constraint_set_digest(V) ‖ key_delta_digest_n ‖ cert_{n-1} )
```

- `H` = BLAKE3. Digests are over canonical encodings, so independent verifiers reproduce them bit-for-bit.
- Written into the snapshot summary as `integrity.cert`, `integrity.cert-version`, `integrity.constraint-set-version`, and recorded in the transaction log. (Phase 6 MUST confirm the gateway can set summary fields of the client's `add-snapshot` update across target clients; if not, the log is the source and the summary is optional — record in ADR.)
- **Optional signing** (0.2): an Ed25519 signature over `cert_n`, with the public key published at `/v1/integrity/keys`.
- `integrity verify <table>` walks `main` from the current snapshot back and reports the first snapshot whose certificate is missing or does not chain. `key_delta_digest` can be recomputed from data files, so verification needs no trust in the Plane's indexes.
- A broken chain emits `BYPASS_DETECTED` and moves the domain to `DEGRADED` (§19) until an operator re-certifies via index rebuild.

### 19. Degraded state and rebuild

A domain enters `DEGRADED` on: certificate chain break, index checksum failure, unresolved recovery, or failed rebuild. In `DEGRADED`, commits to the domain are rejected with `INDEX_DEGRADED` (unless an operator explicitly sets the domain to `Disabled` mode, which is audited and makes subsequent snapshots uncertified).

Rebuild (`POST /v1/integrity/indexes/{id}/rebuild`):

1. Pin the current snapshot of every table in the domain.
2. Scan key columns only (using manifest stats for pruning, never as proof).
3. Build `index-v{n+1}` beside `index-v{n}`.
4. Validate all constraints globally; any violation ⇒ rebuild fails with a violation report (§20).
5. Atomic pointer swap; issue a re-certification record linking the old chain to the new one.

The rebuilt index MUST produce the same verdicts as the live index (differential test).

### 20. Onboarding existing tables

Registering an enforced constraint on a non-empty table runs a rebuild-style scan first.
- No violations ⇒ constraint becomes `Enforced`, first certificate issued.
- Violations ⇒ registration fails with a downloadable violation report (keys + file locations). Cleaning legacy data is the user's job; the Plane never enforces "only new violations" because that would make the post-commit state invalid by definition.

---

## Part VI — Interfaces

### 21. Format adapter boundary

```rust
pub trait TableFormatAdapter: Send + Sync {
    type Request;
    async fn inspect(&self, req: &Self::Request) -> Result<CommitCandidate>;   // classify + list touched files
    async fn extract(&self, c: &CommitCandidate, cols: &ProjectedKeys) -> Result<KeyDelta>;
    async fn publish(&self, c: ValidatedCommit) -> Result<PublishOutcome>;     // Committed | Failed | Unknown
    async fn is_published(&self, table: &TableId, snapshot: SnapshotId) -> Result<bool>;
    async fn scan_keys(&self, table: &TableId, at: SnapshotId, cols: &ProjectedKeys) -> Result<KeyStream>;
}
```

Iceberg is the only adapter in 0.x. Delta/Hudi/Paimon adapters are out of scope until 0.4 and MUST NOT leak concepts into `integrity-core`.

### 22. Iceberg REST gateway

Implements the subset needed for the demo: config, namespaces (list/create/load), tables (create/load/commit/drop), with all other endpoints proxied verbatim or returning 501.

**Error mapping (critical — clients branch on status code):**

| Situation | HTTP | Client effect |
|---|---|---|
| Stale base / failed requirement | 409 | Client refreshes and retries (desired) |
| Constraint violation, unsupported operation, budget exceeded | 400 | Client fails fast, no retry, cleans up (desired) |
| Transient Plane state: recovery required, storage read failure (INT-011, INT-016) | 409 | Client refreshes and retries a bounded number of times, then fails definitely (RFC 0003) |
| Operator action needed: domain degraded, chain broken (INT-010, INT-015) | 423 | Client fails fast (RFC 0003) |
| Never used for integrity verdicts | 500 / 502 / 503 / 504 | Java clients report `CommitStateUnknown`; never return these for a decision we actually made (RFC 0003) |

The 400 body is a standard Iceberg `ErrorModel` whose `message` begins with the integrity code and whose `stack` is empty; the full structured error is available at `/v1/integrity/transactions/{txn_id}`. Phase 7 compatibility tests MUST confirm this mapping against Java (Spark), PyIceberg and iceberg-rust clients.

**Orphan files:** rejected commits leave data/manifest files in object storage. The audit record lists them; cleanup is via standard `remove_orphan_files` (0.1) or an optional targeted cleanup job (0.2).

### 23. Integrity API (`/v1/integrity`)

```http
POST   /v1/integrity/constraints                 register (runs onboarding scan)
GET    /v1/integrity/constraints?table=…
DELETE /v1/integrity/constraints/{id}
GET    /v1/integrity/transactions/{txn_id}       decision + structured error
GET    /v1/integrity/audit?table=…&since=…
POST   /v1/integrity/indexes/{id}/rebuild
GET    /v1/integrity/domains/{id}                state: Healthy | Degraded | RecoveryRequired
GET    /v1/integrity/verify?table=…              certificate chain report
```

Example:

```json
POST /v1/integrity/constraints
{
  "table": "prod.orders",
  "name": "fk_orders_customer",
  "type": "FOREIGN_KEY",
  "columns": ["customer_id"],
  "references": { "table": "prod.customer", "constraint": "pk_customer" },
  "match": "SIMPLE"
}
```

In 0.1 constraints are registered **only** through this API (and CLI). Picking up engine DDL (`ALTER TABLE … ADD CONSTRAINT`) or Iceberg `identifier-field-ids` is 0.2+, after verifying how each engine transmits constraints to a REST catalog.

### 24. Error codes

Stable, machine-readable, never reused.

```text
INT-001 INVALID_CONSTRAINT            INT-009 STALE_BASE_SNAPSHOT
INT-002 CONSTRAINT_NOT_FOUND          INT-010 INDEX_DEGRADED
INT-003 DUPLICATE_PRIMARY_KEY         INT-011 RECOVERY_REQUIRED
INT-004 DUPLICATE_UNIQUE_KEY          INT-012 UNSUPPORTED_COMMIT_OPERATION
INT-005 FOREIGN_KEY_VIOLATION         INT-013 ONBOARDING_VIOLATIONS
INT-006 REFERENCED_ROW_DELETE         INT-014 VALIDATION_BUDGET_EXCEEDED
INT-007 NOT_NULL_VIOLATION            INT-015 BYPASS_DETECTED
INT-008 CHECK_VIOLATION               INT-016 STORAGE_READ_FAILED (RFC 0003)
```

```json
{
  "code": "INT-005",
  "constraint": "fk_orders_customer",
  "table": "prod.orders",
  "sample_keys": [{ "customer_id": 999999 }],
  "violation_count": 1,
  "transaction_id": "txn-01J…",
  "message": "Foreign key constraint violated: 1 child key has no parent in prod.customer"
}
```

Key values in errors are capped (`sample_keys` ≤ 10) and MAY be redacted by policy (`errors.redact_keys: true`) because keys can be PII.

### 25. Audit and observability

Audit events (append-only): `CONSTRAINT_REGISTERED`, `CONSTRAINT_DROPPED`, `COMMIT_ACCEPTED`, `COMMIT_REJECTED`, `COMMIT_ABORTED`, `TXN_RECOVERED`, `INDEX_REBUILT`, `DOMAIN_DEGRADED`, `BYPASS_DETECTED`. Each carries `txn_id`, actor, table, constraint ids, base/result snapshot, verdict, certificate.

Prometheus metrics: `integrity_commits_total{verdict}`, `integrity_validation_seconds`, `integrity_keys_validated_total`, `integrity_bytes_read_total`, `integrity_domain_queue_wait_seconds`, `integrity_index_probe_seconds`, `integrity_recovery_total`, `integrity_bypass_detected_total`, `integrity_domain_state`. Optional OpenTelemetry tracing. Never log key values by default.

### 26. Configuration

```toml
[server]
bind = "0.0.0.0:8181"

[upstream]
catalog_uri = "http://iceberg-rest:8181"

[control_store]
path = "/var/lib/integrity"          # txn log + indexes

[limits]
max_inline_validation_bytes = "2GiB"

[errors]
redact_keys = false
```

Env overrides: `INTEGRITY__SECTION__KEY`. Secure defaults; no advisory mode switch exists in config.

---

## Part VII — Scope, quality and roadmap

### 27. 0.1.0 scope (the "first vertical slice")

**In:** PK, UNIQUE, NOT NULL, FK (RESTRICT, MATCH SIMPLE/FULL); composite keys; inline gateway; capability matrix §15; persistent index; durable txn log + recovery; certificates + `integrity verify`; rebuild; onboarding scan; audit; metrics; Docker Compose demo with MinIO.

**Out (explicitly):** CHECK (0.2), merge-on-read deletes (0.2), branch-gated mode (0.2), signed certificates (0.2), DDL pickup (0.2), multi-table commits (0.3), fine-grained locking (RFC only), HA/multi-node (0.3+), other formats (0.4), CASCADE (unplanned), UI, CDC, query optimization.

**0.1.0 definition of done:**
1. `docker compose up` starts MinIO + upstream catalog + Plane.
2. Constraints registered via API/CLI.
3. Spark, PyIceberg and iceberg-rust writers: valid commits succeed, invalid commits fail with 400 + integrity code, no retry storm.
4. Compaction on a constrained table succeeds.
5. Kill -9 at every fault point (§28) ⇒ restart ⇒ no invalid committed state, no lost accepted commit.
6. Delete index ⇒ rebuild ⇒ identical verdicts.
7. Commit directly to upstream (bypass) ⇒ `integrity verify` pinpoints the snapshot.
8. Benchmark report published with hardware and dataset methodology.
9. Known limitations documented.

### 28. Testing strategy

- **Unit:** key encoding, NULL table (§7), multiset deltas, state machine, error mapping.
- **Property (proptest):** encoding equality/ordering; delta algebra (`apply(d) ∘ apply(−d) = id`).
- **Differential oracle:** `integrity-reference` holds full tables in memory and evaluates constraints naively. Random operation sequences run against the oracle and the engine; final state, verdict sequence and certificate chain MUST match. This is the primary correctness gate.
- **Fault injection:** test-only `FaultPoint` enum (`AfterValidatedLog`, `BeforeUpstream`, `AfterUpstreamBeforeLog`, `AfterUpstreamUnknown`, `DuringIndexApply`, `DuringRebuildSwap`, …), killing the process at each point.
- **Concurrency:** many writers × FK insert/parent delete × hot parent key; invariant checked after each run.
- **Compatibility:** real Spark, PyIceberg, iceberg-rust clients against MinIO in CI.
- **Fuzzing (`cargo fuzz`):** key decoder, txn log records, REST payloads, manifest parsing. The server MUST NOT panic on malformed input.

### 29. Performance targets (non-normative)

- 1 B-row parent, 10 K-row append child commit, warm index: < 1 s **validation**, and report key-column bytes read separately.
- Compaction validation throughput (keys/s) reported.
- Domain queue wait p99 under the hot-key workload reported.

Benchmarks MUST report object-store bytes read and end-to-end commit latency, not only in-memory probe time.

### 30. Roadmap

| Version | Adds |
|---|---|
| 0.1 | Vertical slice above |
| 0.2 | CHECK (DataFusion, row-local, deterministic functions only); MoR position deletes & DVs; branch-gated publication; signed certificates; rollback re-validation; DDL pickup |
| 0.3 | Multi-table atomic commits (REST `/transactions/commit`); HA with shared control store |
| 0.4 | Second format adapter (driven by community demand) |
| later | Incrementally maintainable assertions; planner hints from *enforced* uniqueness |

### 31. Open-source hygiene

Apache-2.0 `LICENSE` + `NOTICE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, public RFCs and ADRs, dependency license checks (`cargo deny`) in CI, SBOM, reproducible container builds. Required ADRs for 0.1: Iceberg first; proxy gateway vs embedded; indexes as derived state; counts instead of locators; serial domain queues; key encoding; certificate format; index backend; error/status mapping.

An Apache Incubator proposal is considered only after there are independent users and committers. The goal is a project that survives its original author, not a badge.

---

## Appendix A — Five-minute demo

1. `docker compose up` (MinIO, upstream catalog, Plane).
2. Create `customer(customer_id PK)` and `orders(order_id PK, customer_id FK → customer)` via Spark; register constraints via CLI.
3. Insert customer 1 → ✅ certificate #1.
4. Insert order → customer 1 → ✅
5. Insert order → customer 999 → ❌ `INT-005`, Spark job fails immediately (no retries).
6. Delete customer 1 → ❌ `INT-006`.
7. Run compaction on `orders` → ✅ (keys unchanged).
8. `kill -9` the Plane mid-commit, restart → recovery report, no inconsistent state.
9. Append to `orders` by pointing a writer straight at the upstream catalog → `integrity verify prod.orders` shows exactly which snapshot broke the chain.

Step 9 is the moment no other tool can reproduce.

## Appendix B — Open questions (resolve by ADR)

1. Can the gateway set `add-snapshot` summary fields for all target clients, or must certificates live only in the Plane log plus a table property?
2. Does Lakekeeper's `ContractValidation` trait see enough of the commit (added/removed files) to host the pipeline in embedded mode?
3. Exact Iceberg Java client behavior per status code on commit (verify 400 vs 422, retry config interaction).
4. Iceberg v3 row lineage: is `_row_id` reliably populated by all writers we target, and does it simplify reference-count maintenance?
5. Index backend choice (`fjall` vs `redb` vs RocksDB) under the benchmark suite.
