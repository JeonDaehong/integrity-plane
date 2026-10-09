# 0011. Constraint registry, chain anchors, onboarding and rebuild

- Status: Accepted
- Date: 2026-10-09

## Context

Phase 10 replaces constraints from the configuration file with a registry managed through the
integrity API (spec §23), runs an onboarding scan when a constraint is registered on a non-empty
table (§20), rebuilds a domain's indexes on demand (§19), and detects writers that bypassed the
Plane (§18). Until now a commit whose parent had no certificate was treated as a chain root, so a
direct upstream write was silently absorbed.

## Decision

**Registry.** `registry.redb` in the control store holds one checksummed document (constraints,
per-table `ConstraintSetVersion`, the constraint sets of every version, chain anchors, persistent
degraded reasons) and an append-only audit table. The document is rewritten in one two-phase-commit
transaction per change. Constraints in the configuration file are imported only when the registry is
first created; afterwards the registry is the source of truth.

**Administrative exclusion.** Registration, drop and rebuild take an exclusive lock that every
commit to a constrained-or-not table holds shared from routing to response. Constraint changes
therefore never interleave with commits, and domain membership cannot change under a commit. Cost:
commits pause while an onboarding scan runs. Per-domain administrative locking is deferred.

**Anchors.** An anchor records, per table, its UUID and its `main` snapshot at the moment the Plane
started (or restarted, after a rebuild) enforcing its constraints. It is written:

- when a table is bound while empty (no snapshot);
- by onboarding and rebuild, for every table of the domain, at the snapshot that was scanned.

A table with data and no anchor is not enforced (`INDEX_DEGRADED`, onboarding required).
A table whose UUID differs from its anchor was replaced and is refused the same way.

**Bypass detection.** Before validating a commit, the head of `main` of every table of the domain
must be the anchor snapshot or carry a certificate. Otherwise the commit fails with
`BYPASS_DETECTED` (423), the domain is marked degraded in the registry (it survives restarts), and
the event is audited. Checking every member, not only the written table, matters: a child commit is
validated against the parent's index, which a bypassed parent write has made stale.

**Chain roots.** The certificate of a commit whose parent is the anchor and carries no certificate
uses 32 zero bytes as `cert_{n-1}` (RFC 0002 chain root). A certified parent always contributes its
certificate, including across constraint set versions.

**Onboarding and rebuild** share one scan:

1. recover the domain's unfinished transactions;
2. load every table of the domain and pin its `main`;
3. order tables so that FK parents come before children (a cycle between distinct tables is refused;
   a self-reference is allowed);
4. read every live data file's constrained columns and validate each table as one insert against
   in-memory indexes, so a parent's keys are present when its children are checked;
5. on violations: registration fails with `ONBOARDING_VIOLATIONS` (400) and a report naming each
   violated constraint; a failed rebuild leaves the domain degraded;
6. otherwise replace each persistent index's contents at a new epoch (`replace_all`, atomic per
   index), then update the registry (constraint, versions, anchors, degraded cleared) in one
   transaction.

Tables with equality delete files are refused by the scan in 0.1; position deletes are applied
(ADR 0017). A crash between two
`replace_all` calls leaves indexes whose contents already equal what the scan derived from the pinned
snapshots (no commit can run meanwhile), and the registry unchanged; repeating the operation is safe.

**Rebuild re-certification.** The audit event `INDEX_REBUILT` records each table's previous anchor
and new anchor, linking the old chain to the new one.

## Consequences

- Direct upstream writes are refused at the next commit to the domain instead of being certified.
  An operator must inspect (`verify`) and rebuild to resume.
- Violation reports name constraints and codes; sample keys and file locations are not reported in
  0.1.
- The integrity API is protected by an optional bearer token (`[server] admin_token`); deployments
  must keep it private either way (`docs/threat-model.md`).

## Alternatives considered

- **Parent-only check.** Checking only the written table's parent misses a bypassed write to an FK
  parent table, which makes child validation unsound.
- **Trusting index epochs as the onboarding marker** (Phase 7–9 behaviour). Epochs say nothing about
  which snapshot the index reflects; anchors do.
- **Per-domain admin locks.** Needed when onboarding large tables must not stall unrelated domains;
  deferred because domain membership changes during registration make lock ordering subtle.
