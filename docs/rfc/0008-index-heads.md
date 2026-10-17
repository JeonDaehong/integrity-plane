# RFC 0008: Index heads

- Status: Accepted
- Date: 2026-10-17
- Affects: semantics (bypass detection, spec §10, ADR 0011), status mapping (when INT-010/INT-015
  are returned; no new code)

## Summary

The index store records, for every enforced table, the `main` snapshot its indexes reflect: its
**head**. A head is written in the same atomic transaction as the index changes that make it true.
Before validating a commit, every table of the domain MUST have `main` equal to its recorded head;
otherwise the commit is refused with `BYPASS_DETECTED`. This replaces the rule "`main` is the
anchor or carries a certificate", which accepts certified snapshots the indexes never saw.

## Motivation

The commit-time bypass check (ADR 0011) accepts any `main` that carries a certificate. A
certificate says that *some* Plane validated the snapshot, not that *these* indexes contain its
keys. Two cases pass the check while the indexes miss keys, so a duplicate key or a dangling
reference can be accepted (`recovery.md`, "Single node and power loss"):

1. A second Plane with its own control store in front of the same tables. Each one's commits are
   certified; the other's indexes never saw them.
2. A control store restored from an older copy (backup, VM or disk snapshot) after commits went
   through. The restored files agree with each other, so store identities and the audit log's
   transaction ids raise nothing, but the indexes lack the later commits.

A bypassing writer that rolls `main` back to an older certified snapshot, or to the anchor, passes
the current check the same way.

## Specification

### Heads

- A head is `(table_uuid, snapshot)` for a table identifier, where `snapshot` is the `main`
  snapshot id or none for a table without one. Heads are stored in the index store
  (`indexes.redb`, table `oip/index-heads/v1`), keyed by table identifier.
- Every index write that changes what the indexes reflect MUST write the head in the same
  transaction:
  - applying an accepted commit, and recovery re-applying one: the committed table's head becomes
    `(uuid, final_snapshot)`, also when the commit changes no index entry;
  - installing builds (onboarding, rebuild, spec §19 step 5): every scanned table's head becomes
    `(uuid, pinned main)`;
  - anchoring an empty table on first sight (ADR 0011): its head becomes `(uuid, none)`, written
    before the registry's anchor.
- Re-applying a commit writes the same head again (idempotent, ADR 0003).

### Check before validation

For every table of the domain, after the anchor checks of ADR 0011 (no anchor, replaced table):

| Recorded head | `main` | Result |
|---|---|---|
| `(uuid, s)` with `uuid` the table's and `s` = `main` | — | continue |
| any other head | certified | `BYPASS_DETECTED` (INT-015, 423): "certified by another Plane or before a restored control store"; domain degraded, audited |
| any other head | not certified | `BYPASS_DETECTED` as today |
| none | — | `INDEX_DEGRADED` (INT-010, 423): the index store does not record what it reflects; rebuild the domain. Not persisted as degraded. |

The parent-certificate check of the committing table (chain continuity, RFC 0002) is unchanged.

### Spec changes

§10 and ADR 0011: the commit-time check compares `main` with the indexes' recorded head; the
anchor still roots the certificate chain and is what `verify` checks back to.

## Compatibility and migration

- Index stores written before this RFC have no heads. Every enforced domain answers `INDEX_DEGRADED`
  to commits until it is rebuilt once (`POST /v1/integrity/indexes/{id}/rebuild`); the rebuild
  records the heads. No head can be inferred soundly from an old store: the transaction log is
  pruned and the registry's anchor only names the last onboarding or rebuild. Release notes MUST
  say so.
- An older Plane opening a store with heads ignores the table (redb tables are independent);
  downgrading after commits leaves heads stale, which the newer Plane then reports as a bypass.
- Certificates, the transaction log and the key encoding are unchanged.

## Test plan

- Index store: a head is written atomically with an apply (also an apply without writes), with an
  install, and on its own; it survives reopening; a failed apply writes no head.
- Gateway (`tests/admin.rs`):
  - two Planes with separate control stores on one catalog: a commit through the second after one
    through the first is `BYPASS_DETECTED`, and the duplicate key is not accepted;
  - a control store restored from a copy taken before a commit: the next commit is
    `BYPASS_DETECTED`; after a rebuild, commits are enforced again;
  - a bypassing rollback of `main` to an earlier certified snapshot is `BYPASS_DETECTED`;
  - an index store without heads answers `INDEX_DEGRADED` until a rebuild.
- Crash tests (`tests/crash.rs`) keep passing: recovery writes the head with the re-apply.

## Alternatives

- **Heads in the registry.** The registry is a different file from the indexes; the head could
  not change atomically with them, and a crash between the two writes would need its own recovery.
- **Plane identity in certificates.** Would catch two Planes but not a restored control store, and
  changes the certificate format.
- **Comparing with the transaction log's last committed snapshot.** The log is pruned (RFC 0006)
  and can be restored together with the indexes.
