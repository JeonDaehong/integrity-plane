# RFC 0004: Transaction log format v1

- Status: Accepted
- Date: 2026-10-09
- Affects: txn log format

## Summary

Defines the durable transaction log of spec §16: its storage, record layout and checksums, the state
machine, what a `VALIDATED` record must contain for recovery to finish a commit without
re-validating, and how recorded decisions answer repeated request ids.

## Motivation

Phase 7 keeps the outcome of an in-flight commit in memory. A crash between "upstream accepted the
commit" and "indexes updated" then leaves indexes behind the table, silently: the next commit would be
validated against stale indexes. Spec §16 requires a log that makes every such window recoverable,
deterministically, and Phase 8 requires surviving a kill at every fault point.

## Specification

### Storage

A redb database `txn.redb` in the control store directory, separate from the index store, written
with durable two-phase commits (as ADR 0005). Two tables:

- `oip/txn-log/v1`: `seq: u64 → record` — append-only; `seq` is strictly increasing. Records are
  never updated or deleted in 0.1.
- `oip/txn-staged/v1`: `txn_id: u64 → staged deltas` — written together with the `VALIDATED`
  record, in the same write transaction.

### Record

```text
record = version:u8 (=1) · kind:u8 · txn_id:u64 · len:u32 · payload · checksum:[u8; 32]
checksum = BLAKE3("oip-txn-record-v1" ‖ version ‖ kind ‖ txn_id ‖ len ‖ payload)
```

`kind`: 1 `PREPARED`, 2 `VALIDATED`, 3 `COMMITTING`, 4 `COMMITTED`, 5 `ABORTED`, 6 `REJECTED`.
`payload` is UTF-8 JSON (fields below). A record whose checksum, version or kind does not verify is
corruption: the log refuses to open (the domain is degraded, never guessed around).

### State machine

```text
PREPARED → VALIDATED → COMMITTING → COMMITTED
   │           │            └──────→ ABORTED
   │           └───────────────────→ ABORTED
   ├──→ REJECTED   (validation failed; terminal)
   └──→ ABORTED    (error before validation finished)
```

Any other transition is refused. A transaction's state is that of its last record. `txn_id`s are
allocated from the log, strictly increasing, never reused.

### Payloads

- `PREPARED`: `request_id` (the `Idempotency-Key`, or null), `table` (UUID), `identifier`,
  `load_path` (the REST path to reload the table).
- `VALIDATED`: `base_snapshot` (or null), `snapshots` (new snapshot ids, oldest first),
  `final_snapshot`, `constraint_set_version`, `epoch` (the index epoch the commit will be applied
  at), `certificates` (hex, one per snapshot). The staged deltas — one RFC 0004 encoded
  `StagedDelta` per index, resolved against the indexes as they were — are stored in
  `oip/txn-staged/v1` under the same `txn_id`.
- `COMMITTING`: empty object. Written immediately before the request is forwarded upstream.
- `COMMITTED`, `ABORTED`, `REJECTED`: `status` (HTTP status returned to the client) and `body`
  (the response body, JSON), so that a repeated `request_id` gets the same answer.

### Staged delta encoding

```text
staged = version:u8 (=1) · base_epoch:u64 · n:u64 · (key_len:u32 · encoded_key · value){n}
value  = 0x00 (delete) | 0x01 · last_snapshot:i64 (unique) | 0x02 · child_count:u64 (reference)
```

Keys are RFC 0001 keys in ascending order and must decode; anything else is corruption. The
BLAKE3 digest of ADR 0003 is computed over the same writes, so a decoded staged delta replays as the
identical apply.

### Recovery (spec §16)

On start and before each commit, every transaction whose last record is `VALIDATED` or
`COMMITTING` is resolved by reloading the table from upstream:

1. `final_snapshot` is in the table's snapshots ⇒ apply each index's staged delta at `epoch`
   (idempotent per ADR 0003), then append `COMMITTED`.
2. Otherwise ⇒ append `ABORTED` (the staged deltas are discarded).
3. Upstream unreachable ⇒ nothing is appended; commits to the domain answer `RECOVERY_REQUIRED`
   (409, RFC 0003) until a later attempt succeeds.

`PREPARED` without a later record (crash during validation) is appended `ABORTED`.

### Request ids

If a commit carries an `Idempotency-Key` that the log has a terminal record for, the gateway returns
the recorded `status` and `body` without re-executing. While a transaction with that key is
unresolved, recovery runs first. `/v1/config` advertises `"idempotency-key-lifetime": "PT30M"`;
recorded decisions are kept for the life of the log in 0.1 (no expiry).

## Compatibility and migration

First version. A v2 record uses `version = 2`; readers dispatch on it.

## Test plan

Unit tests: state machine (every legal and illegal transition), encode/decode round trips, a flipped
byte anywhere in a record or staged delta is detected, reopen preserves state, unresolved
transactions and recorded decisions survive restarts. Fault injection (Phase 8 exit): the gateway
process is killed at each fault point and restarted; afterwards the indexes equal the keys of the
snapshots the upstream table actually contains, and the next commit succeeds.

## Alternatives

- *A hand-rolled append-only file*: needs its own torn-write handling; redb already provides
  checksummed atomic commits.
- *Logging in the index store*: would make log and index writes atomic together, but couples two
  stores with different lifecycles (indexes are rebuilt and swapped, spec §19).
- *Re-validating after a crash*: the indexes the commit was validated against may have changed;
  replaying the stored staged deltas is deterministic.
