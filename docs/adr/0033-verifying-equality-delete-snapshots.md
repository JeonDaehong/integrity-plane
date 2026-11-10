# 0033. Verifying snapshots that add equality deletes

- Status: Accepted
- Date: 2026-11-10
- Amends: ADR 0031 (verify scope), RFC 0002 (how a verifier recomputes the key delta; the format
  is unchanged)

## Context

`verify` recomputes every certificate of `main` from the data files (spec §18): the key delta of a
snapshot is computed from the rows it adds and removes, which the data files determine (RFC 0002:
"It can be recomputed from the snapshot's added and removed data files alone").

That is not true of a snapshot that adds equality deletes (every Flink upsert checkpoint). Its key
delta for the upsert key `K` is the commit's added keys minus the deleted keys **the index held at
commit time** (ADR 0009): a delete of a key that does not exist changes nothing. Which keys existed
is the parent snapshot's live rows, which the snapshot's own files do not show. So these snapshots
are reported `UNVERIFIABLE` (ADR 0031), and a Flink table's chain is almost entirely unverifiable:
a bypass or a tampered certificate on such a table goes unnoticed by `verify`.

Rescanning the parent snapshot for every checkpoint would be exact but costs one full table scan
per checkpoint (a Flink job commits every minute or so).

## Decision

### The replay index

When the chain being verified contains a snapshot that adds equality deletes, `verify` keeps a
**replay index**: the key counts of `K` (the one PK/UNIQUE constraint the table may have under
ADR 0009) as of the snapshot being verified, held in a scratch index store under the verifier's
scratch directory and deleted afterwards.

- It is built once, by scanning the live rows (ADR 0031: deletes applied) of the parent of the
  first such snapshot, as onboarding does (ADR 0011).
- It then moves with the chain: every later snapshot, in order, has its key delta for `K`
  recomputed from the data (exact rows, or ADR 0009's rule against the replay index for snapshots
  adding equality deletes), compared with the certificate, and **applied to the replay index
  whether or not the certificate matched**, so the replay follows the data, never the
  certificates.
- A snapshot adding equality deletes is recomputed by ADR 0009's rule: its delta for `K` is its
  added keys, minus each distinct delete tuple that yields an index key present in the replay index
  (once), under `K`'s NULL rules. Then the certificate is computed as for any snapshot.
- A new constraint set version (the governing constraints change) rebuilds the replay index by a
  scan at the first snapshot of that version that needs it.

Snapshots before the replay starts, and chains without equality deletes, are verified as today.

### Failures

- A snapshot the replay cannot read (budget, unreadable file) is `UNVERIFIABLE`, and so is every
  later snapshot adding equality deletes until the replay index is rebuilt at a later snapshot
  (the replay cannot follow a change it could not read).
- A replay index that does not fit the scratch directory makes those snapshots `UNVERIFIABLE`,
  naming the limit.
- A count that would fall below zero, or a duplicate key under `K`, means the data violates `K`:
  the snapshot is `MISMATCH` (a certificate claims a valid commit).

### Scope and cost

- One table scan per constraint set version present in the chain (in practice: one), plus the rows
  each snapshot changes, as today. Memory is bounded as in onboarding and commit validation
  (`limits.scan_memory`, spilling to scratch).
- Only `K`'s delta needs the replay: ADR 0009 refuses equality deletes on tables with other key
  constraints or a declared FK, so `K` is the only constraint in their key delta.

This is not a change to constraint semantics, key encoding, the certificate format, the
transaction log or status mapping (no RFC): it computes the same key delta digest RFC 0002 defines,
from the data and the chain instead of the snapshot's files alone. RFC 0002's sentence about
recomputing from added and removed files gains "or, for snapshots that add equality deletes, from
the keys live in the parent snapshot (ADR 0033)".

## Consequences

- Flink upsert tables verify end to end: a bypass, a forged or a stale certificate on a checkpoint
  is reported as for any other snapshot.
- `verify` of such a table costs one table scan plus the chain's changes, and scratch disk for the
  replay index (about the size of `K`'s index).
- Chains on catalogs that expire parents (Nessie) are unaffected: a snapshot whose parent is not
  served stays `UNVERIFIABLE`, and the replay starts after it.

## Test plan

- **Oracle**: the equality reader histories (ADR 0031, 0032) certified by the real validator over a
  persistent index, then verified: every snapshot `OK`; a certificate swapped between two
  checkpoints, a checkpoint certified as if a deleted key had not existed, and a checkpoint written
  around the Plane are each reported at the right snapshot and nowhere earlier.
- **Server** (`tests/`): a Flink-shaped table committed through the gateway, `verify` all `OK`; then
  a bypassing commit, reported as `MISSING` with later snapshots unchanged in status.
- **Compatibility** (`engines` job): `integrity verify` on the Flink upsert tables reports `ok`.

## Alternatives considered

- **Rescan the parent for every checkpoint.** Exact and simple, but one full scan per checkpoint.
- **Record the deleted keys that existed in the snapshot summary.** Cheap to verify, but it puts
  key values (possibly PII, threat model) into table metadata, grows every summary, and the
  verifier would have to trust what it is verifying.
- **Keep them `UNVERIFIABLE`.** The status quo: the certificate chain says nothing about a Flink
  table.
