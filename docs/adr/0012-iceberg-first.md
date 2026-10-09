# 0012. Apache Iceberg is the first and only format in 0.x

- Status: Accepted
- Date: 2026-10-09

## Context

The Plane needs a commit boundary it can stand in front of, a place in each snapshot to record a
certificate, and client libraries in several languages that already talk to that boundary. Delta
Lake, Hudi and Paimon each have a different commit protocol (log files, timelines, primary-key
merge semantics), and most of them commit by writing to storage directly rather than through a
service.

## Decision

- 0.x supports Apache Iceberg only (format v2, v3 where noted in `docs/compatibility.md`).
- The integration boundary is the Iceberg REST catalog protocol: every Iceberg client that supports
  REST catalogs can commit through the Plane without modification (ADR 0010).
- Iceberg concepts stay in `integrity-iceberg`; `integrity-core` and `integrity-types` are format
  independent, and the layering check enforces it (spec §21).
- A second format adapter is considered at 0.4, driven by demand.

## Consequences

- One protocol to make correct first: the capability matrix (spec §15) and its tests are
  Iceberg-specific.
- Certificates live in snapshot summary properties (ADR 0007, RFC 0002), which every Iceberg reader
  ignores, so certified tables stay readable by any engine.

## Alternatives considered

- **Delta Lake first.** Commits are optimistic writes of log files to object storage; there is no
  service to place in front of writers without a custom log store, and no per-commit summary map
  that every client preserves.
- **Several formats from the start.** Splits effort before any one path is proven correct, against
  the first priority of the project (correctness of committed state).
