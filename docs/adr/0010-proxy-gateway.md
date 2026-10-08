# 0010. Proxy gateway first; embedded mode later

- Status: Accepted
- Date: 2026-10-09
- Resolves: spec Appendix B.2

## Context

Spec §17.3 asks whether the validation pipeline can run inside a Rust catalog (Lakekeeper) instead
of a proxy, and Appendix B.2 whether Lakekeeper's extension trait sees enough of a commit. The
pipeline needs to (1) see the updates and current metadata, (2) read manifests and data files,
(3) add certificate fields to the new snapshot's summary, (4) learn the definite outcome of the
upstream commit before applying index deltas, all while holding the domain queue.

Lakekeeper's `ContractVerification` trait (main branch, 2026-10-09) offers
`check_table_updates(&self, table_updates: &[TableUpdate], current_metadata: &TableMetadata)
-> Result<ContractVerificationOutcome, ErrorModel>`, plus create/drop/rename checks.

## Decision

- **0.1 is a proxy gateway** (`integrity-server`): writers talk to the Plane as their REST catalog;
  the Plane forwards to any upstream REST catalog (Polaris, Lakekeeper, Nessie, the reference REST
  server).
- **B.2 answer:** the trait sees enough to *validate* — the updates include the new snapshot and its
  manifest list location, and the current metadata gives the parent — provided the extension has its
  own storage access to read manifests and data files. It cannot *certify*: the outcome is
  accept/reject and the updates cannot be amended, so certificate fields cannot be added to the
  summary. Nor does it receive the commit's outcome, so index deltas could not be applied with the
  guarantees of spec §14–16.
- **Embedded mode is revisited** when a catalog offers both an amendable pre-commit hook and a
  durable post-commit outcome; until then the proxy is the only mode.

## Consequences

- Deployments must route writers to the Plane and keep the upstream catalog unreachable to them
  (spec §10, `docs/threat-model.md`).
- The gateway owns HTTP status mapping (RFC 0003) and must stay compatible with Spark, PyIceberg and
  iceberg-rust clients (Phase 7 compatibility tests).

## Alternatives considered

- *Embedded validation without certificates*: possible with Lakekeeper today, but loses the
  certificate chain, the project's distinguishing feature, and the post-commit guarantees.
