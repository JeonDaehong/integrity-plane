# Contributing

Thanks for your interest. This project's first priority is the **correctness of committed table state**;
contributions are reviewed with that in mind.

## Before you write code

- Read the specification (`integrity_plane_design.md`).
- Changes to constraint semantics, key encoding, certificate format, transaction log format or
  HTTP status mapping require an RFC in `docs/rfc/` **before** code.
- Non-trivial design choices require an ADR in `docs/adr/`.

## Rules that reviews enforce

1. **Fail closed.** No code path may accept a commit it could not prove. Unsupported operations are
   rejected with `UNSUPPORTED_COMMIT_OPERATION`; there is no advisory or "warn and continue" mode.
2. `integrity-types` and `integrity-core` must not depend on Iceberg, object storage, HTTP, async
   runtimes or other I/O crates (checked by `ci/check-layering.sh`).
3. Tests before optimization. Non-trivial algorithms need an oracle or differential test first.
4. New dependencies: check license, maintenance status, MSRV and native dependencies, justify them
   in the PR, and make sure `cargo deny check` passes.
5. Do not copy code from projects with incompatible licenses.

## Checks

All of these must pass before a PR is merged:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo deny check
ci/check-layering.sh
```

## Licensing of contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this
project shall be licensed under the Apache License, Version 2.0, without any additional terms or
conditions (Apache-2.0 §5).
