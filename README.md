# Open Integrity Plane

Commit-time PRIMARY KEY / UNIQUE / NOT NULL / FOREIGN KEY enforcement for Apache Iceberg tables,
independent of the writing engine, with verifiable integrity certificates.

> **Status: pre-alpha, under construction. Nothing is implemented yet.**
> Do not use this for any data you care about. No feature is supported until it appears in the list below.

## Supported features

None yet.

## Design

- Specification: [`integrity_plane_design.md`](integrity_plane_design.md)
- Architecture decisions: [`docs/adr/`](docs/adr/)
- Semantic changes (RFCs): [`docs/rfc/`](docs/rfc/)

## Building

Requires Rust 1.90 or newer (edition 2024). On Windows with the `x86_64-pc-windows-gnu` toolchain, a MinGW-w64 install (e.g. WinLibs) must be on `PATH`; some test dependencies need its `dlltool`. The MSVC toolchain needs no extra setup.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo deny check
```

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
