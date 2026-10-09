# 0018. Iceberg v3 deletion vectors

- Status: Accepted
- Date: 2026-10-09
- Extends: ADR 0017

## Context

In format v3, merge-on-read deletes are deletion vectors: a Roaring bitmap of deleted row positions
for exactly one data file, stored as a `deletion-vector-v1` blob in a Puffin file. The manifest
entry names the data file (`referenced_data_file`) and the blob's location (`content_offset`,
`content_size_in_bytes`). A table keeps at most one vector per data file; a later delete replaces
it with a superset.

## Decision

- A deletion vector is one more source of deleted positions for ADR 0017's live-row computation:
  the positions of its bitmap, for its referenced data file. Replacing a vector (old entry removed,
  new one added) therefore removes exactly the newly marked rows.
- The blob is checked before use: length field, magic bytes `D1 D3 39 64`, CRC-32 of magic and
  bitmap, no trailing bytes, cardinality equal to the manifest's record count. Positions must name
  rows of the data file. Anything else is `UNSUPPORTED_COMMIT_OPERATION`.
- The bitmap (64-bit Roaring, portable serialization) is decoded with the `roaring` crate (0.11,
  MIT OR Apache-2.0, pure Rust, maintained by the RoaringBitmap project, MSRV 1.90). A test decodes
  bytes written by hand from the Roaring format specification, so compatibility with other
  implementations does not rest on the crate alone; Spark-written vectors are checked in the
  compatibility jobs. Decoder panics are contained.
- The Puffin footer is not read: the manifest entry locates the blob.

## Consequences

- Spark `DELETE` / `UPDATE` / `MERGE` on format v3 merge-on-read tables are validated and
  certified.
- Puffin files are read whole (as data files are); vectors are small compared to data files.

## Alternatives considered

- **Decoding Roaring bitmaps in-house.** Three container types and two serializations; a widely
  used implementation is less risky, and the hand-written fixture guards the format.
- **Reading the Puffin footer to locate blobs.** Redundant with the manifest entry, and more code
  to trust.
