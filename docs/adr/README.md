# Architecture Decision Records

One file per decision: `NNNN-short-title.md`, numbered sequentially, never renumbered.
Use [`template.md`](template.md). Superseded ADRs stay in place with their status updated.

| # | Title | Status |
|---|---|---|
| [0001](0001-workspace-layout-and-ci-gates.md) | Workspace layout, MSRV and CI gates | Accepted |
| [0002](0002-proptest-for-property-tests.md) | proptest for property tests | Accepted |
| [0003](0003-key-index-staging-and-epochs.md) | KeyIndex staging, epochs and replay | Accepted |
| [0004](0004-validator-verdicts-and-probes.md) | Validator: full violation sets, probe rules, errors vs verdicts | Accepted |
| [0005](0005-persistent-index-backend.md) | Persistent index backend: redb | Accepted |
| [0006](0006-parquet-key-extraction.md) | Parquet key extraction | Accepted |
| [0007](0007-iceberg-commit-inspection.md) | Iceberg commit inspection: own model, update-level classification | Accepted |
| [0008](0008-manifest-diff.md) | Manifest diff without trusting client-written metadata | Accepted |
| [0009](0009-equality-deletes.md) | Equality deletes on a PK/UNIQUE key (Flink upsert) | Accepted |
| [0010](0010-proxy-gateway.md) | Proxy gateway first; embedded mode later | Accepted |
| [0011](0011-registry-anchors-and-rebuild.md) | Constraint registry, chain anchors, onboarding and rebuild | Accepted |
| [0012](0012-iceberg-first.md) | Apache Iceberg is the first and only format in 0.x | Accepted |
| [0013](0013-indexes-are-derived-state.md) | Indexes are derived state | Accepted |
| [0014](0014-counts-instead-of-locators.md) | Indexes store keys and counts, not row locations | Accepted |
| [0015](0015-serial-domain-queues.md) | One serial commit queue per integrity domain | Accepted |
| [0016](0016-violation-reports-disabled-domains-actors.md) | Violation reports, disabled domains and audit actors | Accepted |

The decisions spec §31 requires for 0.1 are all recorded: Iceberg first (0012), proxy gateway (0010),
indexes as derived state (0013), counts instead of locators (0014), serial domain queues (0015) and
index backend (0005); key encoding, certificate format and error/status mapping are RFCs 0001–0003.
