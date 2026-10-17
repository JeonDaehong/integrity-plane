# RFC 0012: Fenced domain commits (several active Planes)

- Status: Accepted
- Date: 2026-10-17
- Affects: semantics (how commits are forwarded, catch-up order, RFC 0011), snapshot summary
  (adds `integrity.domain-state`)

## Summary

With `[ha] fence = "multi-table"`, the Plane forwards a commit to an enforced table as one
multi-table transaction (`POST …/transactions/commit`) that, besides the client's change, asserts
that `main` of the committed table and of **every other table of its domain** is still the
snapshot the commit was validated against. A commit validated against a domain that changed
meanwhile, through another Plane or not, is refused atomically by the catalog (409) and the client
retries. Every certified snapshot also records that domain state, so catching up (RFC 0011) can
replay commits of several tables in the order they happened.

Together, several Planes with their own control stores can take commits for the same tables at
once, behind a load balancer: high availability without leader election or a shared disk.

## Motivation

A Plane validates a commit against its indexes, which reflect the domain at its heads (RFC 0008),
and holds the domain's queue so nothing changes while it validates (ADR 0015). The queue is local:
another Plane, or a writer around the Plane, can change a table of the domain between the
validation and the commit, and a single-table commit only asserts its own table. With foreign keys
that is unsound: a child insert validated against a parent key and a parent delete validated
against no child reference can both commit.

An Iceberg REST catalog that applies a multi-table commit atomically, checking requirements on
tables without updates when the transaction commits, makes itself the arbiter of the order of a
domain's commits. Not every catalog does (`compatibility.md`, "Catalogs for several Planes",
measured 2026-10-17): Lakekeeper 0.13.6 does and serves the table history; Nessie 0.108.8 commits
atomically but serves only the current snapshot; Polaris 1.7.0 and the REST fixture 1.10.1 check
those requirements before committing, so concurrent transactions skip each other's assertions (and
the fixture can acknowledge a change it then loses). The fence is only sound with the first kind.

## Specification

### Forwarding

For a commit that moves `main` of an enforced table `T` of domain `D`, after validation and
certification:

- The Plane sends `POST {base}/transactions/commit` with `table-changes`:
  - `T`: the client's requirements and updates (with certificates), plus `assert-table-uuid` and
    `assert-ref-snapshot-id` for `main` equal to the snapshot the commit was validated on (replacing
    a client requirement on `main` of `T`, which the Plane has already checked);
  - every other table `U` of `D`: no updates, requirements `assert-table-uuid` and
    `assert-ref-snapshot-id` for `main` equal to `U`'s head the validation used.
- On success (any 2xx) the Plane loads `T`; if the commit's final snapshot is not `main` or an
  ancestor of it, the catalog lost the change and the Plane answers 409 (nothing is applied).
  Otherwise it answers the client `200` with that `LoadTableResult`. The metadata may already include later commits; it always includes this one.
  Recovery (RFC 0004) is unchanged: a `COMMITTING` transaction is decided by whether its final
  snapshot is in `T`.
- A 409 (a requirement failed) is passed to the client as before; engines refresh and retry, and
  the retry catches up first (RFC 0011). Other answers are handled as for single-table commits.
- Domains of one table are forwarded the same way (one entry); commits that do not move `main` of
  an enforced table are forwarded unchanged.

### Recorded order

Every certified snapshot gets the summary field

```text
integrity.domain-state = <uuid>@<snapshot id or "-">,…   other tables of the domain, sorted by uuid
```

the state the commit was validated against. It is not part of the certificate (RFC 0002): catching
up validates every snapshot again, in the order it reconstructs, so a forged order can only
produce a history every step of which is valid.

### Catching up in order

RFC 0011's restriction on tables linked by a foreign key that both moved is lifted when every
pending snapshot carries `integrity.domain-state`: starting from the heads, the Plane repeatedly
takes the pending table whose next snapshot's parent is its current head and whose recorded domain
state equals the current state of the other tables, validates it, applies it with its head in one
index transaction, and advances. If no table can advance before all are caught up, refuse as
RFC 0008. A crash in between leaves heads at snapshots that really were `main` one after another,
and the next commit continues from there.

Snapshots without the field (committed without the fence) keep RFC 0011's restriction.

### Configuration

```toml
[ha]
fence = "multi-table"   # default "none": single-table commits, one Plane per domain
```

All Planes in front of the same tables must have the same constraints, registered in the same
order (the constraint set version is part of the certificate, RFC 0002); otherwise their
certificates differ and catching up refuses the other Plane's commits. Each Plane onboards the
tables once. With `[signing] require = true` they share their signing keys (RFC 0009 backends).

The upstream catalog must support `/transactions/commit` with requirement-only table changes. The
Plane does not probe for it; a catalog that answers 404/405/501 makes every fenced commit fail
(fail closed, `UNSUPPORTED_COMMIT_OPERATION`).

## Compatibility and migration

- Off by default: nothing changes until `[ha] fence` is set.
- With the fence, clients get 409 more often under contention across a domain (not only on their
  table). Writers already retry 409.
- The response to a successful commit is the table as loaded right after it, not the catalog's
  answer to the commit itself.
- Snapshot summaries gain one field; readers ignore unknown fields.

## Test plan

- Fake catalog with atomic `/transactions/commit` and requirement checks.
- Two Planes alternately committing to an FK parent and child: every commit is accepted or
  refused exactly as one Plane would decide; the final indexes of both equal a single Plane's.
- A child insert and a parent delete validated at the same time on two Planes: one commits, the
  other gets 409 and, retried, `FOREIGN_KEY_VIOLATION` / `REFERENCED_ROW_DELETE`.
- Catch-up in recorded order across FK-linked tables; a broken or missing order is refused.
- Compatibility (CI `compat` job): the probe against the REST fixture; Spark and PyIceberg through a
  fenced Plane.

## Alternatives

- **Leader election** (a lease in object storage) with one active Plane: needs fencing anyway, as a
  paused leader can still forward; the catalog's requirements are that fence.
- **Asserting only the committed table** (single-table commits, today): unsound across FK-linked
  tables with more than one Plane.
- **Recording the order in the certificate**: a certificate format change; replaying with full
  validation makes it unnecessary.
