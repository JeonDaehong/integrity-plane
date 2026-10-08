# Architecture Decision Records

One file per decision: `NNNN-short-title.md`, numbered sequentially, never renumbered.
Use [`template.md`](template.md). Superseded ADRs stay in place with their status updated.

| # | Title | Status |
|---|---|---|
| [0001](0001-workspace-layout-and-ci-gates.md) | Workspace layout, MSRV and CI gates | Accepted |
| [0002](0002-proptest-for-property-tests.md) | proptest for property tests | Accepted |
| [0003](0003-key-index-staging-and-epochs.md) | KeyIndex staging, epochs and replay | Accepted |
| [0004](0004-validator-verdicts-and-probes.md) | Validator: full violation sets, probe rules, errors vs verdicts | Accepted |

ADRs required for 0.1 by spec §31 that are still to be written: Iceberg first; proxy gateway vs
embedded; indexes as derived state; counts instead of locators; serial domain queues; key encoding (specified by RFC 0001);
certificate format; index backend; error/status mapping.
