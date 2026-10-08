# 0002. proptest for property tests

- Status: Accepted
- Date: 2026-10-08

## Context

Spec §28 requires property tests for key encoding (§9), NULL semantics (§7)
and delta algebra. Later phases need random operation sequences for differential tests against the
oracle, which benefit from shrinking to a minimal failing sequence.

## Decision

Use [`proptest`](https://crates.io/crates/proptest) `1.x` as a **dev-dependency only**, declared once in
`[workspace.dependencies]` and opted into per crate.

- License: MIT OR Apache-2.0.
- Maintenance: actively maintained under the `proptest-rs` organization; widely used.
- MSRV: builds on our MSRV (1.85), verified locally and by the CI MSRV job.
- Native dependencies: none. Its `getrandom` dependency needs `dlltool` on the `windows-gnu` target,
  i.e. a MinGW-w64 install (see README); MSVC and Linux are unaffected.
- Layering: dev-dependencies are excluded from `ci/check-layering.sh`, and proptest is not an I/O
  crate in any case.

Regression files (`*.proptest-regressions`) found by real failures are committed alongside the test.

## Consequences

Strategies are written per crate (`tests/common/mod.rs` in `integrity-core`); they become the input
generators for the Phase 3 differential tests.

## Alternatives considered

- *quickcheck*: weaker shrinking and less control over composite strategies.
- *Hand-rolled random tests*: no shrinking; failures would be hard to read.
