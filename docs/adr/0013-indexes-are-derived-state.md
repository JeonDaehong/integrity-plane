# 0013. Indexes are derived state

- Status: Accepted
- Date: 2026-10-09

## Context

Validating a commit needs to know which keys exist in each constrained table, without scanning the
table. Something must therefore remember keys between commits. If that memory were the source of
truth, losing or corrupting it would lose the meaning of the constraints.

## Decision

- The table data is the source of truth. Every index is a cache of facts derivable from the data at
  the anchored snapshots: it can always be rebuilt by scanning the domain (spec §19, ADR 0011).
- Index state is never trusted when its provenance is in doubt: a checksum failure, a repaired file
  (ADR 0005), a recreated store file (store identity) or a lost transaction log degrades the domain
  until it is rebuilt.
- Index changes are applied only after the upstream commit succeeded, from deltas staged and logged
  beforehand (RFC 0004), so indexes never contain keys of commits that did not happen.
- Certificates are computed from data-file contents, not from index contents, so `verify` needs no
  index at all (RFC 0002).

## Consequences

- Rebuild and onboarding share one scan and one install path; their cost is proportional to the
  data of the domain.
- Backups of the control store are optional for correctness (they shorten recovery).
- The differential tests compare indexes with indexes rebuilt from the oracle state.

## Alternatives considered

- **Indexes as the system of record** (as in a database): would require replicating and backing
  them up with the same care as the data, and would turn any index bug into silent data corruption.
