# RFC 0001: Key encoding v1

- Status: Accepted
- Date: 2026-10-08
- Affects: key encoding

## Summary

Fixes the exact byte layout of `EncodedKey` format version 1, which spec §9 describes only in outline.
Every index entry, probe, key delta digest and (later) certificate depends on these bytes, so they are
pinned here before any of those exist.

## Motivation

Spec §9 requires a typed, versioned, order-preserving encoding over *type families*, but leaves open:
how decimal scale is represented, whether µs and ns timestamps share a family, the escape scheme for
variable-length values, and where NULL sorts. Two implementations that disagree on any of these would
produce different verdicts and different certificate digests (Principle 4).

## Specification

### Layout

```text
EncodedKey  = version:u8 · column+          (version = 0x01)
column      = tag:u8 · [scale:u8 if Decimal] · null_marker:u8 · [value if null_marker = 0x01]
null_marker = 0x00 (NULL) | 0x01 (present)
```

A key has at least one column. The schema of a key is the sequence of its column families, and is
recoverable from the bytes alone.

### Type families and tags

| Tag | Family | Logical types mapped to it | Value bytes (present) |
|---|---|---|---|
| `0x01` | Boolean | boolean | `0x00` = false, `0x01` = true |
| `0x02` | Integer | int, long | i64, big-endian, sign bit flipped (8 bytes) |
| `0x03` | Decimal(scale) | decimal(p, s) for any p ≤ 38 | unscaled value as i128, big-endian, sign bit flipped (16 bytes) |
| `0x04` | Date | date | days since 1970-01-01 as i32, big-endian, sign bit flipped (4 bytes) |
| `0x05` | Timestamp | timestamp, timestamp_ns | nanoseconds since epoch as i128, big-endian, sign bit flipped (16 bytes) |
| `0x06` | TimestampTz | timestamptz, timestamptz_ns | as Timestamp |
| `0x07` | String | string | UTF-8 bytes, escaped (below) |
| `0x08` | Binary | binary, fixed(L) | raw bytes, escaped (below) |
| `0x09` | Uuid | uuid | 16 raw bytes |

Decisions this table makes:

1. **Decimal scale is part of the family**, written as one byte after the tag. `decimal(10,2)` and
   `decimal(12,2)` share a family, so precision widening needs no re-encode. Different scales are
   different families and can never produce equal bytes. Without the scale byte, unscaled `100` at
   scale 2 (1.00) and at scale 0 (100) would encode identically; registration already rejects FKs with
   mismatched scales (spec §9), and the scale byte makes that a property of the encoding as well.
2. **µs and ns timestamps share a family.** Both are normalized to i128 nanoseconds, so a µs value `v`
   and an ns value `v·1000` are equal, and ordering is preserved across precisions. tz and non-tz stay
   separate families (spec §9).
3. **`time` is not a key family in 0.1.** It is absent from the spec §9 supported list, so it is
   rejected at registration with `INVALID_CONSTRAINT`, like float, double, nested, variant and
   geospatial types.
4. **Integer covers int and long only.** `int → long` widening needs no re-encode.

### Escaping of String and Binary

Each `0x00` byte of the value is written as `0x00 0xFF`; the value is terminated by `0x00 0x00`. All
other bytes are written unchanged. This makes each column prefix-free and preserves bytewise order,
so concatenated columns compare lexicographically as tuples.

### Ordering

Within one schema, `encode(a) < encode(b)` exactly when tuple `a` sorts before tuple `b`, comparing
columns left to right, where NULL sorts before every non-NULL value and non-NULL values compare by:
false < true; integers, decimal unscaled values, dates and timestamps numerically; strings, binaries
and UUIDs bytewise (no collation). Ordering across different schemas is defined by the bytes but has no
semantic meaning.

### Canonical form and decoding

Every byte string has at most one decoding, and a decoder MUST reject anything that is not the exact
output of the encoder: unknown version or tag, null marker other than `0x00`/`0x01`, boolean byte
other than `0x00`/`0x01`, `0x00` followed by anything but `0x00`/`0xFF` inside an escaped value,
invalid UTF-8 in a String, truncation, an empty column list. Index backends MUST validate keys read
from storage through this decoder before use.

### Properties (tested)

For tuples `a`, `b` of the supported domain:

- `decode(encode(s, a)) = (s, a)`;
- `encode(s, a) = encode(t, b)` ⇔ `s = t` and `a ≡ b` (NULL ≡ NULL here; whether NULLs *conflict* is
  decided by spec §7, not by the encoding);
- for a fixed schema, `encode(s, a) < encode(s, b)` ⇔ `a < b` in the order above;
- for any byte string `x`, decoding never panics, and if it succeeds, re-encoding yields `x`.

## Compatibility and migration

No prior format exists. A future v2 uses a different version byte; v1 keys stay decodable and an
index is rebuilt (spec §19) to migrate.

## Test plan

Unit tests for edge values (integer extremes, embedded `0x00`/`0xFF`, prefixes, decimal scales,
timestamp precisions) and proptests for each property above, in `crates/integrity-core/tests/`.
The decoder is a fuzz target in Phase 11.

## Alternatives

- *Decimal encoded without scale* (spec's literal "unscaled value"): rejected for the collision above.
- *Separate µs and ns timestamp families*: also safe, but makes a µs FK child unable to reference an ns
  parent for no semantic reason.
- *Length-prefixed strings*: not order-preserving for composite keys.
