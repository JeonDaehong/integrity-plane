# 0007. Iceberg commit inspection: own metadata model and update-level classification

- Status: Accepted
- Date: 2026-10-09
- Resolves: spec Appendix B.1 at the protocol level (client confirmation in Phase 7)

## Context

Phase 6 must read Iceberg table metadata and REST `CommitTableRequest`s, classify each commit
against the spec §15 capability matrix, diff manifests, and put certificates into snapshot
summaries. The obvious dependency is iceberg-rust (`iceberg` crate).

As published on crates.io on 2026-10-09, `iceberg` 0.10.1 requires Rust 1.94 (ours: 1.90), pins
`arrow`/`parquet` 58 (ours: 60, so two Arrow builds), and brings tokio, reqwest, a cache and
encryption crates as non-optional dependencies.

## Decision

- **Own minimal model.** `integrity-iceberg` parses the subset of table metadata and of the REST
  commit request it needs with `serde` / `serde_json` (MIT OR Apache-2.0). Fields it does not use are
  ignored, never interpreted. Manifests are read with `apache-avro` (6b). iceberg-rust stays an
  option for the gateway's catalog client (Phase 7 ADR).
- **The request JSON is kept.** The gateway forwards the client's JSON, changing only the summary
  of the new `main` snapshot (certificate fields). Nothing is re-serialized from the typed model, so
  unknown-but-harmless fields survive.
- **Requirements** are checked against the metadata the Plane loaded under the domain queue (spec
  §14 step 3); a failed requirement is `STALE_BASE_SNAPSHOT` (409, client retries). Unknown
  requirement types and `assert-create` are unsupported.
- **Update-level classification** (`classify`):
  - `main` does not move ⇒ **pass-through** (metadata-only updates, snapshots and refs on other
    branches, expiry). Snapshots on other refs are uncertified.
  - `main` moves to snapshots added by the same request ⇒ **main change**: the new snapshots from
    the current `main` to the final target form a chain (several per commit are allowed, e.g.
    PyIceberg overwrite), each validated by manifest diff (ADR 0008). A chain that does not start at
    the current `main` is stale (409); a main move off that chain is unsupported.
  - `main` moves to an existing snapshot (rollback, cherry-pick, publishing a branch head) ⇒
    unsupported in 0.1. `main` as a tag, or removing `main` ⇒ unsupported.
  - Schema becoming current: every constrained field must stay top-level, keep its type or be
    promoted `int → long`, `float → double` or decimal precision widening with the same scale, and
    must not have an `initial-default`.
  - Known metadata-only actions pass through; `assign-uuid` and any unknown action are unsupported,
    naming the action.
  - The snapshot `operation` must be one of the four spec values; it is recorded as a hint only.
    Snapshots without a manifest list (old v1 metadata) are unsupported.
- **Appendix B.1 (certificates in snapshot summaries).** In the REST protocol the new snapshot,
  including its free-form `summary` map, is part of the `add-snapshot` update in the request body;
  the manifest list does not contain or hash the summary. The gateway can therefore add
  `integrity.*` fields before forwarding and the upstream catalog persists them in the new metadata
  file. Whether clients tolerate a committed snapshot whose summary differs from the one they sent
  is verified per client (Spark, PyIceberg, iceberg-rust) in Phase 7; if one does not, the Plane log
  becomes the certificate source of record as the spec allows.

## Consequences

- No MSRV raise and a single Arrow build.
- The model must track Iceberg spec evolution: unknown actions, requirement types and format
  versions above 3 are rejected, so new features fail closed until supported.
- Tests construct metadata and requests as JSON (one or more per §15 update-level row).

## Alternatives considered

- *iceberg-rust now*: see Context; revisit when its MSRV and Arrow version align.
- *Re-serializing the typed request*: risks dropping fields the Plane does not model.
