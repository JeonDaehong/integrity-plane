# 0008. Manifest diff without trusting client-written metadata

- Status: Accepted
- Date: 2026-10-09

## Context

To validate a new `main` snapshot the Plane needs the rows it adds and removes (spec §14 step 5).
Iceberg's usual way to find a snapshot's changes reads only manifests whose `added_snapshot_id` is
the snapshot and entries whose status is ADDED or DELETED with that snapshot id. All of these are
written by the client being validated, and spec §15 says classification must not rely on client
claims. A client could, for example, list a new data file as EXISTING in a new manifest.

## Decision

- **Diff live files of the manifests that differ.** Manifests are immutable files (an Iceberg
  invariant the catalog relies on too), so a manifest path present in both the parent's and the new
  manifest list is unchanged and is not read. For manifests only in the new list ("introduced") and
  only in the parent list ("dropped"), collect live entries (EXISTING or ADDED) by file path. Files
  live only in introduced manifests are **added**; files live only in dropped ones are **removed**.
  Entry status is used only for liveness, which is what readers use; declared snapshot ids, sequence
  numbers and summary counts are ignored.
- **Re-listing a file counts as adding it.** If an introduced manifest lists a data file that stays
  live in an unchanged manifest, readers will see its rows twice; the Plane treats the file as added,
  so the validator sees its keys again and duplicate PK/UNIQUE keys are rejected.
- **Strict listings.** A manifest listed twice in one list, a file live twice among the manifests
  read, or a file whose content type does not match its manifest's is rejected.
- **Row counts are checked.** Each data file's extracted row count must equal the manifest's
  `record_count`.
- **Declared operation is checked against the diff:** `append` may not remove files, `delete` may
  not add files, and `replace` must leave the multiset of projected rows unchanged (so no index
  changes). `overwrite` is unconstrained here; the validator decides.
- **Several new snapshots in one commit** (e.g. PyIceberg's overwrite = delete snapshot + append
  snapshot) form a chain from the current `main`; each step is diffed against its parent and must be
  valid on its own, because every step is reachable by time travel. Steps are validated in order
  against the index state including the previous steps (overlay, Phase 7).
- **Delete files are out of this step.** Adding position deletes / deletion vectors is unsupported
  (merge-on-read, 0.2). Adding equality deletes is handled in step 6d. Removing delete files, and
  removing data files from a table whose parent snapshot has delete manifests, are unsupported: the
  rows of such data files are not the table's rows.
- **Only Parquet data files.** Other formats are unsupported.
- **Budget.** All reads go through `FileIo`; `Budgeted` counts whole files and fails with
  `VALIDATION_BUDGET_EXCEEDED` once the configured limit is passed.

## Consequences

- Validation cost is proportional to the manifests and data files the commit touches, not to table
  size: unchanged manifests are never read.
- A storage layer that allows overwriting an existing manifest path in place would defeat the
  "same path, same content" assumption; the threat model requires immutable (or write-once) object
  paths for metadata, as Iceberg itself does.
- I/O failures map to `INT-016 STORAGE_READ_FAILED` (RFC 0003, HTTP 409: clients retry a bounded
  number of times).
- Tests replay a real PyIceberg table (append, copy-on-write delete, whole-file delete, two-snapshot
  overwrite, branch commit) and synthetic Avro manifests for compaction, rewrites, delete files and
  adversarial listings.

## Alternatives considered

- *Iceberg's snapshot-change scan (trust `added_snapshot_id` and entry status)*: cheaper by a small
  constant but trusts the client under validation.
- *Diff all live files of both snapshots*: reads every manifest of the table on every commit.
