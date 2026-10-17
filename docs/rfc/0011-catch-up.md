# RFC 0011: Catching up from the table history

- Status: Accepted
- Date: 2026-10-17
- Affects: semantics (bypass detection, RFC 0008), status mapping (when INT-015 is returned)

## Summary

When `main` of a domain table has moved past the head its indexes reflect (RFC 0008), the Plane no
longer refuses at once. If every snapshot between the head and `main` is a certified link of the
table's chain, the Plane **catches up**: it validates each snapshot again from its data files, as a
commit, checks that this gives the snapshot's certificate, and applies the result to its indexes,
with the new heads, in one atomic transaction. Only what cannot be proved this way is still
`BYPASS_DETECTED`.

Catching up is what lets a Plane whose indexes are behind (a restored control store, a standby, a
second Plane, RFC 0012) take commits again without a full rebuild, and it is the base of high
availability.

## Motivation

RFC 0008 made a Plane refuse any `main` its indexes did not see, then required a rebuild: a full
scan of every table of the domain. That is correct but makes every lag an outage, and it is exactly
what a second Plane in front of the same tables, or a standby taking over, always sees.

The history needed to catch up is already public and durable: each certified snapshot names its
parent and carries a certificate that binds the table, the snapshot, the constraint set and the
key delta (RFC 0002), and `verify` already recomputes those from the data files (spec §18).

## Specification

### When

Before validating a commit, for each table
of the domain whose `main` differs from its recorded head:

1. `main` must descend from the head along parent links in the table metadata (a head of none
   counts as the root). Otherwise (a rollback, a replaced history, an expired head): refuse as
   RFC 0008.
2. Every snapshot after the head must carry a certificate. A snapshot whose changes the Plane
   cannot validate (outside the capability matrix, equality deletes) stops catch-up.

### Which tables may catch up together

The order in which snapshots of *different* tables were committed is not recorded anywhere. For
tables connected by a foreign key that order matters: two Planes (or a Plane and its restored copy)
can each accept one commit, a child insert and a parent delete, that are valid alone and violate
the key together. So:

- If the tables with pending snapshots are pairwise unconnected by foreign keys (in particular,
  if only one table moved), their order cannot change any outcome and they catch up.
- Otherwise, refuse as RFC 0008 (rebuild). RFC 0012 records the order of a domain's commits and
  lifts this restriction for chains that carry it.

### Applying

The pending snapshots are validated again against the indexes, table by table, oldest snapshot
first, exactly as commits are (the same pipeline, validator and limits). The certificate each
validation computes (from the same key delta, constraint set and parent certificate) must equal the
one in the snapshot; a different value, a violation, or a constraint set version other than the
table's current one is `BYPASS_DETECTED`. Nothing is trusted that is not recomputed.

Certificates are public functions of public data (RFC 0002): a writer around the Plane can compute
one for valid data, and catching up then accepts that snapshot, which is what a commit through the
Plane would have allowed. Where who committed matters, `[signing] require = true` makes catching up
follow only snapshots signed with a key the registry records as this Plane's (RFC 0005); others are
`BYPASS_DETECTED`. Planes that follow each other then share their signing keys.

Every staged change of every caught-up table is applied at one new epoch, with their heads, in one
atomic index transaction (RFC 0008 "Heads"). A crash before it leaves the old heads, and the next
commit catches up again.

### Bounds

- Catch-up reads data files like validation does, so it runs inside the commit's domain queue,
  within `limits.scan_memory`, and counts against the commit's read budget
  (`limits.max_inline_validation_bytes`); a catch-up larger than the budget answers
  `VALIDATION_BUDGET_EXCEEDED` (400) to that commit and leaves the domain untouched, and the
  operator rebuilds or raises the budget.
- At most `[catch_up] max_snapshots` (default 1 000) snapshots per table; more is refused as
  RFC 0008.
- A domain with an enforced table whose head is a snapshot `main` no longer descends from is
  refused as before, whatever else moved.
- `[catch_up] enabled = false` restores RFC 0008's behaviour.

### Audit and metrics

`INDEX_CAUGHT_UP` audit events name each table, the old and new head and the number of snapshots.
`integrity_catch_up_snapshots_total` counts the snapshots caught up.

## Compatibility and migration

- Domains that RFC 0008 would refuse with `BYPASS_DETECTED` now often continue; commits that
  carried an uncertified, forged, rolled-back or unverifiable snapshot are refused as before.
- An index store without heads (written before RFC 0008) still needs one rebuild: catch-up needs a
  head to start from.
- No format changes: certificates, signatures, the transaction log and the index store are
  unchanged (heads and epochs as in RFC 0008).

## Test plan

- Differential: a random sequence of commits through Plane A, a copy of A's control store taken at
  a random point, then B (on the copy) takes the next commit: B's indexes equal A's (entry for
  entry), and its verdict equals A's.
- A restored control store (RFC 0008 test) catches up instead of refusing.
- Refused as before: an uncertified snapshot, a forged certificate (key delta mismatch), a rollback,
  equality deletes, an unknown constraint set version, more than `max_snapshots`, FK-linked tables
  that both moved.
- A forged certificate whose data violates a constraint: `BYPASS_DETECTED` from the validation,
  nothing applied. A forged certificate on valid data: `BYPASS_DETECTED` from the certificate
  comparison (its key delta or chain does not match).
- Crash during catch-up (fault point before the index transaction): heads unchanged; the next
  commit catches up.

## Alternatives

- **Replicating the control store** (spec's 0.3 sketch, a shared disk): couples availability to a
  storage system the deployment may not have, and redb on network file systems is unsafe.
- **Revalidating FK-linked tables in some chosen order:** the real order is not recorded; a
  chosen order can accept a combination no single Plane ever validated.
- **Trusting certificates instead of validating** (signed ones at least): cheaper, but a signature
  says a Plane validated the snapshot against *its* view; with FK-linked tables two views can
  disagree (above). Validating again needs no trust and costs one more validation per commit.
