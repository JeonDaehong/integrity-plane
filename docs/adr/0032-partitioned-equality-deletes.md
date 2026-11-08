# 0032. Partitioned equality deletes: Flink upsert tables with partitions

- Status: Accepted
- Date: 2026-11-08
- Amends: ADR 0030 (partitioned equality deletes), ADR 0009 (supported shape), ADR 0031 (live rows
  with partitioned deletes)

## Context

ADR 0030 refuses equality delete files with a non-empty partition tuple. An Iceberg equality
delete applies to a data file only when the data file's sequence number is lower and its partition
(spec id and tuple) equals the delete's, or the delete's spec is unpartitioned (spec "Scan
Planning"). ADR 0009 removes a deleted key from the index as if the delete reached every row, which
is right only when the key's live row lies in the delete's partition.

Flink writes partitioned upsert tables only when the partition's source columns are among the
equality fields (`PRIMARY KEY (...) NOT ENFORCED` with `PARTITIONED BY` on key columns); every
checkpoint writes, per partition, a data file and an equality delete file under the table's default
spec. So in a table written only that way the key determines the partition, and the delete written
for a key lands where the key's row is. That is a property of the writer, which the Plane must not
assume (fail closed): a file whose rows are not in the partition its manifest entry names, a delete
tuple written into another partition, or a data file left under an older spec after partition
evolution, each makes the Plane drop a key that readers still see.

ADR 0030 named what is missing: a proof that every key's live row lies in the partition of the
delete, by applying the partition transforms of key columns in the Plane.

## Decision

### Key-determined specs

A partition spec is **key-determined** for an upsert key `K` (ADR 0009) when the source field of
each of its partition fields is a column of `K` and its transform is one the Plane computes:
`identity`, `bucket[N]`, `truncate[W]`, `year`, `month`, `day`, `hour`, `void`. Any other transform,
or a source field outside `K`, makes the spec not key-determined. The unpartitioned spec is
key-determined. Partition specs are read from the table metadata (`partition-specs`); a manifest
naming a spec id the metadata lacks, or a spec added in the commit being validated, is refused.

The Plane computes transforms as the Iceberg specification defines them (Appendix A "Partition
Transforms" and Appendix B "32-bit Hash Requirements": Murmur3 x86 32-bit, seed 0, over the
specified byte representation of each type), for the key types the Plane encodes. Values are taken
from the key as read against the table's current schema (ADR 0006); for widened types the result
is the same as for the original type (`int` and `long` hash and truncate alike, per the spec), and
a widening whose transform result would differ is refused.

### The partition invariant

**Every row of every data file the Plane reads lies in the partition its manifest entry names**:
for a data file of a key-determined spec `S`, the tuple of `S`'s transforms over the row's key equals
the file's partition tuple. The Plane checks it on every row whose key it already reads:

- rows of data files a commit adds (all commits on a table with a key-determined partitioned spec);
- rows of every live data file during onboarding and rebuild.

A row that breaks it refuses the commit (`UNSUPPORTED_COMMIT_OPERATION`, naming the file), and
onboarding or rebuild of the table. Equality delete tuples are checked the same way against their
delete file's partition tuple.

Live data files written before this check existed were never checked. The table's anchor records
whether the invariant holds for all live data files (`partitions_checked`): set by onboarding and
rebuild when every live file passed, and by a commit to a table with no live data files; kept by
every validated commit (each one checks what it adds), including those catching up validates
again (RFC 0011, RFC 0012); absent in anchors written before this ADR.

### When partitioned equality deletes are accepted

A commit that adds equality delete files with a non-empty partition tuple (ADR 0009's index path)
is accepted when, in addition to ADR 0009 and ADR 0028:

1. the table's anchor has `partitions_checked`;
2. every added equality delete file and every data file live in the parent snapshot have the same
   partition spec id `S`, and `S` is key-determined for `K`;
3. every delete tuple lies in its delete file's partition (the invariant above).

Then the live row of a key `k` named by a delete in partition `P` lies in a data file of spec `S`
whose tuple is `S(k) = P`, with a lower sequence number: the delete reaches it, as ADR 0009
assumes. Otherwise the commit is refused; condition 2 fails after partition evolution until a
rewrite moves the old spec's files to the new one (accepted through ADR 0031, below), and condition
1 fails until the domain is rebuilt.

Unpartitioned (global) equality deletes keep ADR 0009 unchanged.

### Reading partitioned equality deletes

ADR 0031's live rows apply partitioned deletes as readers do, without needing the invariant: the
delete map is keyed by the delete's partition spec id and tuple as well as its tuple (global
deletes under no partition), and a row of a data file with sequence number `s` is hidden when its
tuple maps to a number greater than `s` under the global entry or under the data file's own spec id
and tuple. Partitioned equality deletes therefore no longer refuse compaction, copy-on-write
deletes, onboarding, rebuild or `verify` of such tables.

This is not a change to constraint semantics, key encoding, certificates, the transaction log or
status mapping (no RFC). The anchor field is an additive change to the registry document: anchors
without it read as `false`.

## Consequences

- Flink upsert tables partitioned by key columns (`identity`, `bucket`, `truncate`, time
  transforms of key columns) commit through the Plane, and can be compacted, onboarded and rebuilt.
- Every commit to a table with a key-determined partitioned spec computes the transforms for the
  rows it adds: one hash or truncation per row and partition field, over columns already read.
- Domains onboarded before this change need a rebuild before their first partitioned equality
  delete is accepted; the refusal says so.
- Partition evolution of an upsert table blocks Flink checkpoints until the files of the old spec
  are rewritten (or the table is rebuilt after such a rewrite done without the Plane).
- Tables partitioned on columns outside the upsert key stay refused: their deletes do not reach a
  key's rows in another partition, which only a scan could see.

## Test plan

- **Transforms first.** Unit tests against the Iceberg specification's hash and transform examples
  (Appendix B values for `int`, `long`, `decimal`, `date`, `time`, `timestamp`, `timestamptz`,
  `string`, `uuid`, `fixed`, `binary`; truncate and temporal examples of Appendix A, including
  negative values), plus a differential test against `iceberg-rust`'s transforms in the
  `compat/iceberg-rust` client.
- **Reader oracle.** Extend `equality_reader.rs`: random partitioned histories (identity and
  bucket specs on the key, a spec change mid-history, files whose partition tuple is wrong, delete
  tuples in the wrong partition); the naive reader applies deletes by spec id and tuple; every
  accepted commit's rows equal the oracle's; ADR 0009 commits whose deletes would not reach a live
  row are refused.
- **Unit tests** (`changes.rs`): conditions 1–3 each refuse; a spec added in the same commit is
  refused; a non-key-determined spec is refused.
- **Compatibility** (`engines` CI job): a Flink upsert table `PARTITIONED BY (bucket)` or by an
  identity key column; upserts, Trino `optimize`, rows unchanged; a duplicate key from another
  engine refused.

## Alternatives considered

- **Trust the writer's partition tuples.** No reading cost, but a mislabeled file or delete makes
  the index drop a key readers still see, and the next duplicate is accepted.
- **Read, for each delete, the rows it may reach** (ADR 0031's map on the checkpoint's parent). Exact
  for any partitioning, but it is a scan of the partitions touched per checkpoint, which ADR 0009
  avoids; it remains the way to support specs that are not key-determined.
- **Store each key's partition in the index.** Indexes hold keys and counts only (ADR 0014).
- **Identity transforms only.** Needs no hashing, but Flink upsert tables are usually bucketed on
  the key; bucket is the common case.
