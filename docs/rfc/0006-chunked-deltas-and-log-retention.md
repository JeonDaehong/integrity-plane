# RFC 0006: Chunked staged deltas and transaction log retention

- Status: Accepted
- Date: 2026-10-14
- Affects: txn log format (RFC 0004)

## Summary

A commit's index changes are staged and logged in chunks of a bounded number of writes, and applied
by streaming those chunks into the one index transaction, so that the memory a commit needs no longer
grows with the keys it adds. The transaction log deletes a transaction's staged deltas when it
reaches a terminal state, and prunes terminal transactions after a retention period, so that it no
longer grows with write volume. Constraint semantics, key encoding, certificates, the index store and
status mapping are unchanged.

## Motivation

ADR 0019 bounded the memory of reading and sorting a commit's keys, but everything after the merge
of the two sorted sides still holds every changed key at once:

1. the net delta of each constraint (`CommitScan::counts` → `NetDelta`);
2. the keys handed to each index probe, and the probe results;
3. the staged writes (`StagedDelta`, one per index), resolved against the current index values;
4. their RFC 0004 encoding, written as one value of `oip/txn-staged/v1` with the `VALIDATED` record;
5. the staged deltas again, handed whole to the index's write transaction.

An append of `n` new keys therefore needs memory proportional to `n`: a few
gigabytes for the 30 M-row initial load of `docs/benchmarks.md`, and more than one node has for a
load of a few hundred million rows. Measured on the development machine (redb 4.3, one unique index,
integer keys): staging 20 M new keys peaks at 2.5 GB, about 120 bytes per key, all of it the
in-memory delta; applying them in one redb transaction adds nothing to that peak, because redb keeps
a write transaction's dirty pages within half its cache and writes the rest to the file before the
commit. Copy-on-write rewrites and compactions are not affected (their unchanged keys cancel out),
appends and initial loads are.

Separately, `oip/txn-log/v1` and `oip/txn-staged/v1` are never pruned (RFC 0004: "never updated or
deleted in 0.1"), and the log keeps every transaction in memory: disk and memory grow with every
commit ever made, and the staged deltas, which are only needed until a transaction is resolved, are
most of that disk.

## Specification

### Chunks

A **chunk** is at most `C` consecutive staged writes of one index, in key order. `C` is fixed by the
implementation (100 000, the transaction size of ADR 0011's build tables) and is not part of any
format: readers accept any chunk size from 1 to 2^32 − 1. Chunk numbers start at 0 and are
consecutive. A staged delta with `w` writes has `max(1, ⌈w / C⌉)` chunks; an empty one has one empty
chunk.

The **digest** of a chunked staged delta is the ADR 0003 digest of the whole delta
(`StagedDelta::digest`, over the base epoch, the total write count and every write in order),
unchanged. A chunked delta and the same delta in one piece have the same digest.

### Validation (non-normative)

The validator processes the merged keys of each constraint in key order, a bounded batch at a time:
it probes the batch, records violations, resolves the batch's staged writes against the values the
probe returned and hands complete chunks to the transaction log. Violation details keep a count and
at most `SAMPLE_LIMIT` samples per violation, never every offending key. Nothing of a commit is held
in memory beyond one batch per constraint, the sorters' budget (`limits.scan_memory`) and the
violation samples. The decision and every recorded violation are those of validating the whole
commit at once (see Test plan).

The key delta digest of RFC 0002 needs each constraint's change count before its changes. The
validator counts while it merges and writes the net changes to a spill file when they exceed the
budget, then hashes them in a second pass. RFC 0002 is unchanged.

### Transaction log v2: staged chunks

A new table `oip/txn-staged/v2`:

```text
key   = txn_id:u64 · constraint_id:u64 · chunk:u32          (20 bytes, big-endian)
value = version:u8 (=2) · base_epoch:u64 · n:u32 · (key_len:u32 · encoded_key · value){n}
        · checksum:[u8; 32]
checksum = BLAKE3("oip-staged-chunk-v2" ‖ txn_id ‖ constraint_id ‖ chunk ‖ every byte before it)
value (of a write) = as RFC 0004: 0x00 delete | 0x01 · last_snapshot:i64 | 0x02 · child_count:u64
```

Keys within a chunk, and from one chunk to the next, MUST be strictly increasing RFC 0001 keys;
anything else, or a checksum that does not verify, is corruption.

The `VALIDATED` payload gains a field `staged`: one entry per index the commit changes, in constraint
id order,

```json
{ "constraint": 7, "base_epoch": 41, "chunks": 3, "writes": 250000, "digest": "<64 hex>" }
```

and a transaction whose `VALIDATED` payload has `staged` keeps its staged deltas only in
`oip/txn-staged/v2`. Recovery reads every chunk of every entry, checks that the chunks are
consecutive, that the write count and the recomputed digest match the entry, and fails closed
(`TxnError::Corrupt`, the domain is degraded) otherwise.

**Write order.** Every chunk except the last chunk of each index MAY be written in durable writes of
its own before the `VALIDATED` record; the last chunk of every index MUST be written in the same
durable write as `VALIDATED` (and `COMMITTING`, as today). A commit whose deltas all fit in one chunk
per index therefore writes exactly one durable write, as before. Chunks of a transaction without a
`VALIDATED` record are garbage: they are deleted with its `ABORTED` or `REJECTED` record, and at open
for any transaction whose last record is `PREPARED` (which recovery aborts anyway).

**Reading v1.** A `VALIDATED` payload without `staged` is a v1 transaction: its staged deltas are
read from `oip/txn-staged/v1` exactly as in RFC 0004. New transactions are always written as v2.

### Streamed apply

Applying a commit (and recovery finishing one) reads each index's staged delta chunk by chunk, in
order, and writes it into the same single durable index transaction as today (`apply_all`): every
index of the commit still moves to the new epoch atomically, and the ADR 0003 idempotence check
(`last_applied = (epoch, digest)`) is unchanged. The digest is computed while the chunks are read and
compared with the `VALIDATED` entry before the transaction commits; a mismatch aborts the transaction
and fails closed. The index store format does not change.

### Transaction log retention

- A terminal record (`COMMITTED`, `ABORTED`, `REJECTED`) and the deletion of the transaction's
  staged deltas (v1 value or every v2 chunk) are one durable write.
- Terminal payloads gain `finished_at` (Unix milliseconds). A terminal record without it (written by
  0.0.2 or earlier) counts as finished when the log was first opened by a version that prunes; that
  time is recorded once as `opened_at` in `oip/txn-meta/v1`.
- A new setting `[txn_log] retention` (duration, default `24h`, minimum `30m`, the advertised
  `idempotency-key-lifetime`). At open and then at most once a minute after a terminal record, every
  record of each transaction that has been terminal for longer than `retention` is deleted, in one
  durable write per at most 10 000 transactions. Its request id is forgotten with it.
- A new table `oip/txn-meta/v1` holds `next_txn` and `next_seq` (u64), rewritten in every pruning
  write, so that transaction ids and record sequence numbers stay strictly increasing and are never
  reused when the newest transactions have been pruned. At open, each is the larger of the stored
  value and one past the largest in the log.
- In memory, the log keeps the state and request id of transactions that are unresolved or within
  `retention`; payloads (prepared, validated, recorded responses) are read from the log when needed.

A pruned transaction is unknown to `GET /v1/integrity/transactions/{id}` (404). A request repeated
with a pruned `Idempotency-Key` is executed as a new request: it is validated against the current
indexes, so a repeated append of the same keys is rejected (duplicate key), never applied twice. The
audit log (`registry.redb`) is not affected.

## Compatibility and migration

- A 0.0.2 `txn.redb` opens unchanged: v1 staged deltas of unresolved transactions are read and
  resolved as before; staged deltas of terminal v1 transactions are deleted by the first pruning
  write, and their records once `retention` has passed after the upgrade. A 0.0.2 binary cannot open
  a log written by this version if any v2 transaction is unresolved (it would find `VALIDATED`
  without a v1 staged value and report corruption, failing closed); downgrading requires resolving
  every transaction first.
- Index store, certificates, key encoding, constraint semantics and status mapping: unchanged.
  Certificates written before and after the change verify identically.

## Test plan

- **Chunked against whole (differential).** Every commit of the validator's differential suite and
  of `integrity-server/tests/commit_scan.rs` is validated with chunk sizes 1, 2, 3 and 100 000:
  identical decisions, violations, key delta digests, certificates, staged digests and final index
  contents.
- **Encoding.** v2 chunk round trips; a flipped byte anywhere in a chunk, a missing, repeated or
  reordered chunk, a write count or digest that does not match the `VALIDATED` entry: each is
  detected as corruption. v1 transactions written by the 0.0.2 code still resolve.
- **Fault injection.** A new fault point `BetweenStagedChunks` (some chunks written, no `VALIDATED`),
  and the existing `DuringIndexApply` with multi-chunk deltas, with the RFC 0004 invariant after
  restart: indexes equal the keys of the snapshots upstream actually contains, no accepted commit
  is lost, and the next commit succeeds.
- **Index.** Streamed apply against one-piece apply on random deltas and chunk sizes (proptest):
  identical contents, epoch and `last_applied`; a chunk sequence whose digest does not match changes
  nothing.
- **Retention.** Staged deltas disappear with the terminal record; transactions older than
  `retention` disappear at open and after later terminal records; transaction ids are not reused
  after the newest ones are pruned; a request id within retention returns the recorded answer, one
  beyond it is validated anew.
- **Memory.** `integrity-bench` appends of 10 M and 100 M new keys with peak resident memory
  reported; the target is memory independent of the number of keys (about `limits.scan_memory`
  plus a constant), measured on the benchmark machine before and after.

## Alternatives

- **One staged value per transaction, streamed into redb.** redb values are written whole; a
  multi-gigabyte value needs the same memory.
- **Staged deltas outside redb (a spill file per transaction, referenced from `VALIDATED`).** Two
  stores must then be made durable and consistent with each other; redb already gives checksummed
  atomic writes, and chunk rows in it are deleted with the terminal record.
- **Applying a chunked delta in one index transaction per chunk** (an earlier draft of this RFC,
  with a pending-apply marker in the index metadata). It changes the index store format and gives up
  moving every index of a commit to the new epoch atomically, for nothing: the measurement above
  shows one redb transaction does not need memory proportional to its writes.
- **Applying a large delta through a build table and a rename (as rebuild does).** Requires copying
  the whole index, not only the changed keys: an append of 1 M keys to a 1 000 M-key index would
  rewrite 1 000 M entries.
- **Count-based instead of time-based retention.** The idempotency lifetime clients are promised is a
  duration; a count bound would prune within it under heavy load.
