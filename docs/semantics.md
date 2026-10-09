# Semantics

Mirrors spec Part III (§6–§9). The spec is normative; this page records what is implemented and
where the implementation pins down details the spec leaves open.

| Area | Spec | Implemented in | Status |
|---|---|---|---|
| Constraint model | §6 | `integrity_core::constraint` | Model and registration checks (Phase 1) |
| NULL semantics | §7 | `integrity_core::nulls::classify` | Implemented (Phase 1) |
| Commit-level semantics | §8 | `integrity_core::delta`, `integrity-validator` | Implemented for single-table commits (Phase 3) |
| Key encoding | §9 | `integrity_core::key` | Implemented per [RFC 0001](rfc/0001-key-encoding-v1.md) |

The gateway applies these semantics to every commit of a constrained table (`integrity-server`).

## Constraint model (§6)

PRIMARY KEY, UNIQUE (`NULLS DISTINCT` default, or `NULLS NOT DISTINCT`), FOREIGN KEY (`MATCH SIMPLE`
default, or `MATCH FULL`; `ON DELETE RESTRICT` only) and single-column NOT NULL. Columns are field
IDs, never names. Enforcement mode is `Enforced` or `Disabled`; there is no advisory mode. CHECK is
0.2 and not present in the model.

Registration rejects with `INT-001 INVALID_CONSTRAINT` when:

- the name is empty, a key has no columns, or a key repeats a column;
- a column does not exist;
- a key column's type is not a key family (float, double, time, nested, variant, geospatial,
  unknown);
- an FK's parent constraint does not exist, is on a different table than declared, is not a
  PRIMARY KEY or UNIQUE constraint, or is not `Enforced`;
- FK child and parent keys differ in arity or in any column's family (this includes decimal scale);
- **the FK references its own table.** Self-referencing FKs are rejected in 0.1 (fail closed): the
  validator assumes a commit never writes both sides of an FK (ADR 0004). Lifting this needs an RFC
  defining how parent and child rows added or removed by the same commit interact.

NOT NULL accepts any existing column, including types that cannot be keys.

## NULL semantics (§7)

`classify(role, schema, tuple)` returns `Key(encoded)`, `Exempt` or `Violation(code)`:

| Role | Any NULL | All NULL | No NULL |
|---|---|---|---|
| PRIMARY KEY | `Violation(INT-007)` | `Violation(INT-007)` | `Key` |
| UNIQUE NULLS DISTINCT | `Exempt` (not indexed, never conflicts) | `Exempt` | `Key` |
| UNIQUE NULLS NOT DISTINCT | `Key` (NULL marker; NULL = NULL) | `Key` | `Key` |
| FK MATCH SIMPLE | `Exempt` (no parent needed) | `Exempt` | `Key` (must match a parent) |
| FK MATCH FULL | `Violation(INT-005)` | `Exempt` | `Key` |

Values are type-checked against the key schema even when the tuple is exempt.

## Key deltas (§8)

`KeyDelta { added, removed }` holds multisets of encoded keys. Two separate facts come out of it:

- `added.duplicates()`: keys added more than once. For PK/UNIQUE these MUST be checked before any
  index probe;
- `net()`: per-key signed change `added − removed`, with zero changes dropped, so a copy-on-write
  rewrite of a row nets to "unchanged".

Netting first would hide duplicates: `added = {k, k}`, `removed = {k}` nets to `+1` while the
post-commit state holds `k` twice. `KeyMultiset::apply` applies a net delta atomically and refuses
(leaving the multiset unchanged) if any count would go negative or overflow.

## Key encoding (§9)

See [RFC 0001](rfc/0001-key-encoding-v1.md) for the byte layout. Decisions beyond the spec text:

- decimal scale is part of the family and is written into the key, so equal unscaled values at
  different scales never compare equal;
- `timestamp` and `timestamp_ns` share a family (normalized to nanoseconds); tz and non-tz do not;
- `time` is not a key type in 0.1;
- NULL sorts before all values;
- `EncodedKey`'s `Debug` output never shows key bytes, because keys can be PII.
