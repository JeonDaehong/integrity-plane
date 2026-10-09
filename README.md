# Open Integrity Plane

Commit-time PRIMARY KEY / UNIQUE / NOT NULL / FOREIGN KEY enforcement for Apache Iceberg tables,
independent of the writing engine, with verifiable integrity certificates.

> **Status: pre-alpha (0.0.0), not production-ready.** Do not use this for data you care about.
> Only what is listed below is implemented, and only within the limits stated in the linked docs.

## Supported features

All of these are covered by tests in this repository; none has been run in production.

- **Proxy REST catalog.** Writers use the gateway (`integrity-server`) as their Iceberg REST catalog;
  it forwards to an upstream REST catalog. Verified clients: Spark 3.5 with Iceberg 1.10, PyIceberg
  0.12, iceberg-rust 0.10 ([`docs/compatibility.md`](docs/compatibility.md)).
- **Constraints** on top-level columns: PRIMARY KEY, UNIQUE (NULLS DISTINCT / NOT DISTINCT),
  NOT NULL, FOREIGN KEY (MATCH SIMPLE / FULL, ON DELETE RESTRICT), checked at commit time against
  the committed data, before the commit reaches the upstream catalog.
- **Commit shapes** from the capability matrix: append, copy-on-write overwrite and delete,
  compaction (`replace` with unchanged keys), equality deletes on a key. Anything the Plane cannot
  prove is refused (`UNSUPPORTED_COMMIT_OPERATION`), including multi-table commits and
  merge-on-read position deletes.
- **Certificates** chained through snapshot summaries, and `verify`, which recomputes them from the
  data files and names the first snapshot written around the Plane or tampered with.
- **Bypass detection** at commit time: a table whose `main` was moved without the Plane puts its
  domain in a degraded state until an operator rebuilds it.
- **Crash recovery** through a durable transaction log, idempotent retries (`Idempotency-Key`),
  and per-domain commit queues ([`docs/recovery.md`](docs/recovery.md)).
- **Integrity API and `integrity` CLI:** register constraints (with an onboarding scan of existing
  data), list, drop, rebuild, verify, disable a domain, audit log with actors, transaction and
  domain status; structured violation reports with sample keys (redactable); Prometheus metrics at
  `/metrics`.

What is not supported, or only partly: [`docs/limitations.md`](docs/limitations.md). Measurements:
[`docs/benchmarks.md`](docs/benchmarks.md). Threats and mitigations:
[`docs/threat-model.md`](docs/threat-model.md).

## Design

- Specification: [`integrity_plane_design.md`](integrity_plane_design.md)
- Architecture decisions: [`docs/adr/`](docs/adr/)
- Semantic changes (RFCs): [`docs/rfc/`](docs/rfc/)

## Trying it

```sh
cargo build --release -p integrity-server -p integrity-cli
./target/release/integrity-server deploy/integrity.example.toml   # edit upstream and storage first
./target/release/integrity constraints add --table db.customer --name pk_customer     --type primary_key --columns customer_id
./target/release/integrity verify db.customer
```

Or the whole stack (S3 storage, Iceberg REST catalog, Plane):
`docker compose -f deploy/docker-compose.yml up -d --build`.

`compat/demo_test.py` runs the full demo of the specification (Appendix A) with Spark in CI, against
both a local warehouse and the compose stack.

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
