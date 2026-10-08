# RFC 0002: Integrity certificate format v1

- Status: Accepted
- Date: 2026-10-09
- Affects: certificate format

## Summary

Pins the exact bytes behind spec §18's formula
`cert_n = H("oip-cert-v1" ‖ table_uuid ‖ snapshot_id_n ‖ parent_snapshot_id ‖
constraint_set_digest(V) ‖ key_delta_digest_n ‖ cert_{n-1})`, so that any independent verifier
reproduces certificates bit for bit (Principle 4).

## Motivation

The spec names the inputs but not their encodings: how the UUID and ids are written, what a
constraint set digest covers, how key deltas are ordered, what the first certificate chains to.
Two implementations that differ on any of these cannot verify each other.

## Specification

All integers are big-endian. `H` is BLAKE3 with 32-byte output. `‖` is concatenation. `str(s)` is
`u32 length ‖ UTF-8 bytes`.

### Certificate

```text
cert_n = H( "oip-cert-v1"                       11 ASCII bytes, no terminator
          ‖ table_uuid                          16 bytes (the Iceberg table-uuid, parsed from its
                                                 canonical text form, case-insensitive)
          ‖ snapshot_id_n                       i64
          ‖ parent_flag ‖ parent_snapshot_id    u8 (0 = no parent, then i64 0; 1 = parent, then i64)
          ‖ constraint_set_digest               32 bytes
          ‖ key_delta_digest_n                  32 bytes
          ‖ cert_{n-1}                          32 bytes; 32 zero bytes for a chain root )
```

A **chain root** is the first certificate of a table: the first certified snapshot after the
constraint set was onboarded (spec §20), or the first snapshot of a table created empty with its
constraints. Every later certificate uses the certificate of its parent snapshot.

### Constraint set digest

Covers every **enforced** constraint that governs commits to the table: constraints declared on it,
and foreign keys on other tables that reference one of its keys (they decide `REFERENCED_ROW_DELETE`).
Disabled constraints are excluded.

```text
constraint_set_digest = H( "oip-constraints-v1" ‖ version:u64 ‖ count:u32 ‖ constraint* )
constraint            = id:u64 ‖ str(table_id) ‖ kind
kind = 0x01 ‖ key                                   PRIMARY KEY
     | 0x02 ‖ key ‖ nulls:u8                        UNIQUE (0 = DISTINCT, 1 = NOT DISTINCT)
     | 0x03 ‖ key ‖ str(parent_table_id)
            ‖ parent_constraint:u64 ‖ match:u8      FOREIGN KEY (match 0 = SIMPLE, 1 = FULL;
            ‖ on_delete:u8                           on_delete 0 = RESTRICT)
     | 0x04 ‖ field:i32                             NOT NULL
key  = n:u32 ‖ field:i32 × n                        in key order
```

Constraints are sorted by id. `version` is the table's `ConstraintSetVersion`. Names are excluded:
renaming a constraint does not change what is enforced. `table_id` is the Plane's table identifier
(the Iceberg table UUID in canonical lowercase text).

### Key delta digest

The net key change of the snapshot for every PK, UNIQUE and FK constraint **declared on the table**,
including constraints whose net change is empty:

```text
key_delta_digest = H( "oip-key-delta-v1" ‖ count:u32 ‖ entry* )
entry            = constraint_id:u64 ‖ n:u32 ‖ change × n
change           = len:u32 ‖ encoded_key ‖ delta:i64
```

Entries are sorted by constraint id; changes by encoded key bytes (RFC 0001 order); zero changes are
omitted. `delta` is the net count change (added − removed) of that key in that constraint. It can be
recomputed from the snapshot's added and removed data files alone.

### Snapshot summary fields

| Key | Value |
|---|---|
| `integrity.cert` | 64 lowercase hex characters |
| `integrity.cert-version` | `1` |
| `integrity.constraint-set-version` | the decimal `version` |

## Compatibility and migration

No prior format. A v2 would use a new domain string (`oip-cert-v2`) and `integrity.cert-version`;
verifiers dispatch on the version field.

## Test plan

Golden vectors for each digest and for a two-step chain, pinned in
`crates/integrity-core/tests/certificate.rs`; properties: determinism, and every input field
(including constraint order independence and key order independence) changing the output as
specified.

## Alternatives

- *Hash the table UUID as text*: textual variants (case) of the same UUID would give different
  certificates.
- *Digest the gross added/removed multisets instead of the net delta*: would make a compaction
  certificate depend on how files were rewritten rather than on what changed.
- *Include constraint names*: would make cosmetic renames break verification.
