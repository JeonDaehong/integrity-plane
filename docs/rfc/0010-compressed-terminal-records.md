# RFC 0010: Compressed terminal records

- Status: Accepted
- Date: 2026-10-17
- Affects: txn log format (terminal record payload; RFC 0004, RFC 0006)

## Summary

A terminal record (`COMMITTED`, `ABORTED`, `REJECTED`) stores the response given to the client,
returned again for a repeated `Idempotency-Key` (RFC 0004). Its payload is now the same JSON,
compressed as one zstd frame. Records written before stay readable; what is recorded and replayed
does not change.

## Motivation

The response to an accepted commit is the catalog's `LoadTableResult`: the table's whole metadata,
every snapshot included. Finished transactions are kept for `[txn_log] retention` (default 24 h,
RFC 0006), so the log holds one copy of the metadata per commit of that period, and the copies grow
with the table's snapshot count. In the version E benchmark (`docs/benchmarks.md`), 3 200
single-row commits to one table left a 4 GiB transaction log.

Not recording responses of requests without an `Idempotency-Key` would not help much: the Plane
advertises idempotency keys in `/v1/config` (RFC 0003), so Java clients send one with every commit.
Replaying from `metadata-location` instead of a stored body would depend on a metadata file that
table maintenance may delete within the retention period.

## Specification

- A terminal record's payload MUST be a single zstd frame (RFC 8878) whose content is the RFC 0004
  terminal JSON (`status`, `body`, `finished_at`). Writers use compression level 3.
- A payload that begins with `{` is a terminal record written before this RFC, read as JSON. A
  payload that begins with the zstd magic number `28 B5 2F FD` is decompressed first. Anything else,
  or a frame that does not decompress to valid terminal JSON, is `Corrupt`.
- Decompression is bounded: a frame that would decompress to more than 256 MiB is `Corrupt` (the
  gateway accepts request bodies up to 64 MiB; upstream responses are not larger in practice).
- Other records (`PREPARED`, `VALIDATED`, `COMMITTING`) and staged chunks are unchanged.

## Compatibility and migration

- A log written by 0.0.2 or earlier is read unchanged; new terminal records are compressed.
- Downgrading: a version without this RFC reports a log containing compressed terminal records as
  corrupt. Operators who need to downgrade must wait for the retention period to prune them, or
  start with a new log (the indexes are then refused until rebuilt, `recovery.md`).

## Test plan

- Round trip: a terminal record with a large metadata body is recorded compressed, reads back
  identical, survives reopening, and replays for its idempotency key.
- A terminal record written as plain JSON (as before) is still read.
- A corrupt frame and an oversized frame are `Corrupt`.
- A body of repetitive table metadata (a few thousand snapshots) is stored in a fraction of its
  JSON size.

## Alternatives

- **Not storing the body when there is no idempotency key**: Java clients always send one.
- **Storing only `metadata-location`**: the file may be deleted by metadata cleanup before the
  retention period ends; a replay must not depend on it.
- **lz4**: faster, but compresses JSON about half as well; zstd is already compiled into the
  workspace (Parquet).
- **New dependency:** `zstd` 0.14 (MIT; binds the reference C library through `zstd-sys`), already
  in the dependency tree through `parquet`.
