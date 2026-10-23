# 0030. Delete files apply as Iceberg readers apply them

- Status: Accepted
- Date: 2026-10-23
- Amends: ADR 0009 (equality deletes), ADR 0017 (position deletes), ADR 0018 (deletion vectors)

## Context

The rows a commit adds and removes must be the rows readers see before and after it. ADR 0017
computes a data file's live rows from every live position delete file that names its path. Iceberg
readers apply a delete more narrowly (spec, "Scan Planning"):

- a position delete file or deletion vector applies to a data file only if both have the same
  partition spec and partition tuple, and the data file's data sequence number is at most the
  delete's;
- a position delete file with a `referenced_data_file` applies to that file only;
- where a deletion vector applies to a data file, position delete files are not applied to it
  (the vector must contain their deletes); a data file has at most one vector;
- an equality delete applies to data files of its partition only, unless its spec is
  unpartitioned (a global delete).

Where the Plane applied a delete that readers do not, it removed keys from its index that readers
still see, and the next duplicate of such a key was accepted. ADR 0009 treats every equality
delete as global, which is wrong for partitioned ones. That happens without a crafted writer: after
partition evolution, a Flink upsert's equality delete (written under the new spec) does not reach
rows in files of the old spec, so readers see the old and the new row while the Plane counts one.

## Decision

- Manifests are read with each file's partition tuple and each manifest list entry's partition
  spec id (and sequence number, ADR 0028).
- Position deletes and deletion vectors are applied by the rules above, per snapshot: a delete that
  readers would not apply to a file changes none of its rows. Two live deletion vectors for one
  data file are refused.
- An equality delete file with a non-empty partition tuple is refused
  (`UNSUPPORTED_COMMIT_OPERATION`): which rows it reaches depends on where each key's rows lie,
  which the Plane does not know without reading them. Equality deletes of an unpartitioned spec are
  global, as ADR 0009 assumes.
- Where the metadata omits a sequence number or spec id (v1 manifests, which have no deletes), that
  condition is not checked.

Partition tuples are compared as decoded from the manifests: files written under the same spec
encode the same tuple identically.

## Consequences

- Flink upsert tables must be unpartitioned (or written with unpartitioned equality deletes) to
  commit through the Plane. Supporting partitioned ones needs a proof that every key's rows lie in
  the partition of its delete (identity or bucket transforms of key columns, applied by the Plane);
  that is a separate decision.
- Commits that readers and the Plane used to see differently now agree: a crafted delete in the
  wrong partition, at a lower sequence number, or for another referenced file changes nothing, and
  a deletion vector that drops older position deletes brings their rows back, as readers see it.
- No change for writers that follow the spec (Spark, Trino, Flink on unpartitioned tables).

## Alternatives considered

- **Refuse every mismatch instead of mirroring it.** Also sound, but a valid table state (old
  position delete files kept next to a deletion vector that contains them) would be refused.
- **Read the rows an equality delete may reach.** Exact for partitioned deletes, but it is the scan
  ADR 0009 avoids.
