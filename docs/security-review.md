# Security review

The Plane has had **no external security review**. This page records the internal review of
2026-10-17 and lists what an external reviewer should look at first. The threat model is in
[`threat-model.md`](threat-model.md).

## Scope

- `integrity-server`: the REST gateway (proxying, commit validation, the integrity API), the
  transaction log, the control store, signing backends.
- Parsers of untrusted input: commit requests (JSON), Avro manifests and manifest lists, Parquet
  data files, Puffin deletion vectors (`integrity-iceberg`).
- Not in scope: the upstream catalog, object storage, engines.

## Assets

1. **Integrity of published table state:** no snapshot violating the constraints reaches `main`
   through the Plane, and certificates are not issued for data that was not validated.
2. **Key values:** stored in the indexes, the transaction log (staged deltas) and violation
   reports; may be personal data.
3. **Credentials:** writers' catalog credentials (forwarded), the Plane's storage and upstream
   credentials, API tokens, the signing key.

## Internal review, 2026-10-17

| Area | Finding | Status |
|---|---|---|
| File locations named by writers | A writer could make the Plane read any file its credentials reach (other tables, local files) and read keys from violation reports | **Fixed:** RFC 0013, only the table's own locations |
| Integrity API without tokens | Open by default: anyone reaching the Plane could disable a domain (commits forwarded unchecked) | **Fixed:** ADR 0027, closed unless `open_api = true` |
| Plain HTTP | Writer credentials and API tokens cross the Plane's port unencrypted | Documented: terminate TLS in front (`threat-model.md`) |
| Second Plane, restored control store | Certified snapshots the indexes never saw were accepted | **Fixed:** RFC 0008 (heads), RFC 0011 (catch up by validating again) |
| Catalogs acknowledging lost transactions | The REST fixture can answer 204 for a transaction whose snapshot it dropped | **Fixed:** the Plane checks the snapshot is on `main` (RFC 0012) |
| Cross-table assertions in multi-table commits | Polaris 1.7 and the REST fixture do not isolate them; several Planes would be unsound there | Documented: several Planes only on Lakekeeper (`compatibility.md`) |
| Token comparison | Constant-time over every configured token | OK |
| Failed authentication | 10 per minute per client, then 429 (RFC 0007) | OK |
| Request bodies | 64 MiB limit; per-domain queue bound; optional per-client commit rate | OK |
| Signing keys | File (owner-only, flushed), Vault Transit, PKCS#11; backend signatures verified before use | OK (RFC 0009) |
| Terminal records | zstd frames bounded to 256 MiB when read back | OK (RFC 0010) |
| Storage engine panics | Caught; the index store is then treated as corrupt (fail closed) | OK (ADR 0005) |
| Parsers | Fuzzed in CI (`fuzz.yml`): commit requests, manifests, Parquet keys | Partial: no Puffin fuzz target |
| Hostile Parquet sizes | The Parquet decoder allocates declared sizes (dictionary value counts, schema child counts): two fuzzed files allocated 5 and 6 GiB and aborted the process | **Fixed:** footer and page headers checked first (`parquet_pages.rs`); 10 min of fuzzing after the fix found nothing |
| Audit actors of commits | Self-declared (`X-Integrity-Actor`, `User-Agent`) | Documented (ADR 0026) |

## For an external reviewer: where to look first

1. **Commit validation soundness** (`integrity-validator`, `pipeline.rs`): can any sequence of
   commits put a duplicate key, a dangling reference or a NULL key on `main`? Key encoding
   (RFC 0001) collisions, NULL semantics (spec §7), multi-step commits, merge-on-read deletes
   (ADR 0017, 0018), rewrites.
2. **Proxy behaviour** (`gateway.rs`): which requests are forwarded unchecked
   (`Classification::PassThrough`, other refs, metadata-only updates); can a commit move `main` of
   an enforced table without being classified as a main change?
3. **Bypass detection and catching up** (RFC 0008, 0011, 0012): can a writer around the Plane get
   a snapshot followed that a commit through the Plane would have refused?
4. **File access** (RFC 0013, `fileio.rs`, `io.rs`): normalization, prefix matching, symlinks on
   local warehouses.
5. **Parsers of untrusted files** (`integrity-iceberg`): memory and CPU bounds on hostile Avro,
   Parquet and Puffin input; the read budget.
6. **Integrity API** (`admin.rs`, `lib.rs`): authorization of every route, report contents with
   `redact_keys`.
7. **Recovery** (`integrity-txn`, RFC 0004, 0006): can a crash or a replayed idempotency key make
   the indexes disagree with `main` without detection?
8. **Supply chain:** `cargo deny` runs in CI; release images are built reproducibly
   (`release-hygiene.yml`).

Please report vulnerabilities as described in [`SECURITY.md`](../SECURITY.md).
