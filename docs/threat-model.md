# Threat model

> Normative sources: spec §10, §18, §24, §25. Scope: 0.1, a single Plane in front of one upstream
> REST catalog. The Plane is pre-alpha and has had no external security review.

## What the Plane protects

The guarantee is about **published table state**: a snapshot that reaches `main` of a constrained
table through the Plane satisfies the registered constraints, and carries a certificate that lets
anyone recompute that it was checked. The Plane does not protect confidentiality of table data,
and it does not replace the access control of the catalog or the object store.

## Trust boundaries

| Party | Trusted for | Not trusted for |
|---|---|---|
| Writers (engines, their users) | nothing | anything they send: commit requests, manifests, manifest lists, data files, `operation` and status fields (ADR 0008) |
| Upstream REST catalog | storing metadata and applying commits it accepts | — (a compromised catalog can publish anything; see below) |
| Object storage | returning the bytes that were written | — (rewritten files are detected by `verify`) |
| Operators of the integrity API | deciding which constraints exist and when to rebuild | — |
| Control store (`indexes.redb`, `txn.redb`, `registry.redb`) | durability of what the Plane wrote | — (checksums detect accidental damage, not forgery) |

## Threats and mitigations

### Writers bypassing the Plane

A writer that reaches the upstream catalog directly can commit anything.

- **Deployment requirement:** writers must not be able to reach the upstream catalog (network
  policy, credentials scoped to the Plane). This is the primary control.
- **Detection at commit time:** before validating, the Plane checks every table of the domain:
  `main` must be its anchor or carry a certificate. Otherwise the commit fails with
  `BYPASS_DETECTED` (423), the domain stays degraded until an operator rebuilds it, and the event is
  audited (ADR 0011). Checking every table matters: a bypassed write to an FK parent makes the
  validation of its children unsound.
- **Detection after the fact:** `integrity verify` recomputes every certificate from the data files
  back to the chain start and names the first snapshot that is missing a certificate or whose
  certificate does not match. It catches a writer that copies a valid-looking certificate into its
  own snapshot, which the commit-time check alone does not.

### Tampering with data files after publication

Iceberg data files are immutable by convention, not by enforcement. A party with write access to
the bucket can rewrite a file. `verify` recomputes key deltas from the current file contents, so a
rewrite that changes keys shows up as `MISMATCH`. Object-store versioning or write-once retention
policies are the preventive control.

### Malicious or malformed client input

- Commit requests, metadata, manifests and Parquet files are parsed defensively. Panics inside the
  third-party Avro and Parquet decoders are caught and become errors (`UNSUPPORTED_COMMIT_OPERATION`,
  400); anything the Plane cannot prove is refused (fail closed).
- Inputs are covered by malformed-input property tests in `cargo test` and by libFuzzer targets
  (`fuzz/`, CI workflow "Fuzz").
- Client-declared `operation`, manifest entry status and record counts are checked against the
  actual file contents, never trusted (ADR 0008).

### Denial of service

- Inline validation reads at most `limits.max_inline_validation_bytes` per commit
  (`VALIDATION_BUDGET_EXCEEDED`, 400); request bodies are limited to 64 MiB.
- Each integrity domain commits serially (ADR 0015): a writer that floods one domain slows that
  domain, not others. There is no rate limiting per client.
- Onboarding and rebuild scans are unbounded and pause all commits while they run; they require the
  integrity API, which must be restricted to operators.

### Upstream credentials

The Plane reads tables with its own credentials (`[upstream.auth]`) when configured, so validation
does not depend on what each writer may read; give that principal read access to the constrained
tables and nothing more. Writers' commits are forwarded with the writers' credentials, so the
catalog still decides who may write. Requests to the integrity API are never forwarded upstream:
the API token stays inside the Plane.

### The integrity API

Registering, dropping and rebuilding constraints change what is enforced; `verify` and `rebuild`
are expensive. Set `[server] admin_token` so `/v1/integrity/*` (except `status`) requires
`Authorization: Bearer <token>`, and keep the API off untrusted networks either way. Without a
token the API is open to anyone who can reach the gateway. The token is compared by digest; it is
not rate-limited. Audit events name an actor, but it is whatever the request declares
(`X-Integrity-Actor` or `User-Agent`): useful for attribution among cooperating operators, not
evidence. Disabling a domain (spec §19) switches enforcement off for it until a rebuild; it is a
privileged, audited action and must be restricted like the rest of the API.

### Control-store damage or loss

- Accidental corruption of an index store is detected on open (full checksum verification), and a
  storage-engine panic on a malformed file is contained (ADR 0005); the domain must be rebuilt.
- A deleted and recreated `indexes.redb` or `txn.redb` is detected (store identity; transaction ids
  in the audit log) and degrades every enforced table until rebuilt (`docs/recovery.md`).
- The checksums are not cryptographic: someone who can write the control-store directory can forge
  consistent contents and make the Plane accept invalid commits. The directory must be writable
  only by the Plane process. `verify` does not depend on the control store's indexes and will still
  report invalid chains.

### A compromised upstream catalog

The Plane reads table metadata from the upstream catalog and trusts it to apply exactly the commit
it forwarded. A compromised catalog can publish arbitrary snapshots; without certificates,
`verify` reports them as `MISSING`. Certificates are unsigned in 0.1 and everything they cover is
public, so an attacker who controls the catalog can also compute certificates that `verify`
accepts, including for data the Plane never validated. With `[signing]` enabled (RFC 0005) every
certificate is also signed with an Ed25519 key only the Plane holds: verifiers that pin the Plane's
public key (`integrity verify --trusted-key … --require-signatures`) reject such forgeries. Protect
the key file like the rest of the control store; without signing, a certificate proves consistency
of the chain, not who produced it.

## Key values and PII

Primary and foreign keys can be personal data (emails, national ids).

- Commit error responses, metrics and log messages name constraints, tables, snapshot ids and
  integrity codes, never key values. Metrics carry no table names.
- Structured violation reports (`/v1/integrity/transactions/{id}`, onboarding and rebuild reports)
  include up to ten sample keys per violated constraint, and they are kept in the audit log. Set
  `errors.redact_keys = true` to keep key values out of reports and storage entirely (ADR 0016).
- Key values are stored in the persistent indexes (encoded, not encrypted) and in the transaction
  log (staged index deltas). Protect the control-store directory like the tables themselves, and
  include it in data-deletion procedures: deleting a row from a table removes its key from the
  indexes, but older transaction log records keep it (the log is not truncated in 0.1).
- Onboarding and rebuild spill sorted key runs to `indexes.redb.scratch/` in the control store
  while a table is scanned; they are deleted after each table and when the Plane starts.
