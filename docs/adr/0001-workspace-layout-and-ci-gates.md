# 0001. Workspace layout, MSRV and CI gates

- Status: Accepted
- Date: 2026-10-08

## Context

Phase 0 needs a Cargo workspace matching spec §12 and CI that enforces the hard rules in
`CONTRIBUTING.md` from the first commit, before there is any code to protect.

## Decision

- **Layout.** One virtual workspace, crates under `crates/`, exactly the ten crates of spec §12.
  Intra-workspace dependencies are declared now, following the strictly downward direction
  `server → iceberg/txn/validator → index → core → types`, so the layering exists before code does.
  `integrity-cli` and `integrity-bench` declare their dependencies when they get code.
- **Edition / MSRV.** Edition 2024, `rust-version = "1.85"` (the first release supporting edition
  2024). Raising the MSRV is allowed when a dependency needs it, and is noted in the PR.
  No `rust-toolchain.toml`: it would override the MSRV job's toolchain.
- **Lints.** Workspace-level: `unsafe_code = forbid`, `missing_docs`, `unused_must_use = deny`,
  and clippy `unwrap_used`, `dbg_macro`, `todo`, `unimplemented` (all warnings are errors in CI).
  `unwrap` is allowed in tests. Rationale: the server must not panic on malformed input (spec §28).
- **Unpublished.** All crates are `publish = false` until 0.1.0.
- **CI gates** (`.github/workflows/ci.yml`): fmt, clippy `-D warnings`, test on Linux and Windows,
  MSRV `cargo check`, `cargo deny check` (licenses, advisories, bans, sources), and
  `ci/check-layering.sh`, which fails if `integrity-types` or `integrity-core` transitively depend on
  Iceberg, object storage, HTTP, async runtime, Arrow/Parquet or storage-engine crates
  (`CONTRIBUTING.md` rule 2).
- **Licenses.** `deny.toml` allows only permissive, Apache-2.0-compatible licenses. Extending the list
  requires justification.

## Consequences

- Violating the layering rule fails CI rather than relying on review.
- The forbidden-crate list in `ci/check-layering.sh` is a denylist; new I/O crates must be added to it
  when they enter the workspace.
- CI uses `--locked`, so `Cargo.lock` is committed.

## Alternatives considered

- *Enforcing layering with cargo-deny `bans.wrappers`*: only constrains direct dependents, not
  transitive paths into `integrity-core`.
- *An `xtask` crate using `cargo metadata`*: more robust but adds a dependency and code in Phase 0;
  can replace the script later.
