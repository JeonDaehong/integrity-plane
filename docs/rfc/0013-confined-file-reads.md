# RFC 0013: Reading only the table's files

- Status: Accepted
- Date: 2026-10-17
- Affects: semantics (which commits are supported), status mapping (INT-012 for files outside
  the table), configuration (`[storage] allowed_locations`)

## Summary

The Plane reads only files under the locations of the table they belong to: the table's
`location`, the paths its `write.data.path`, `write.metadata.path`, `write.object-storage.path` and
`write.folder-storage.path` properties name, and locations the operator allows in
`[storage] allowed_locations`. A manifest list, manifest, data, delete or deletion-vector file
anywhere else is not read: the commit is refused with `UNSUPPORTED_COMMIT_OPERATION` (400), and
onboarding, rebuild and `verify` report the table as not readable.

## Motivation

A writer controls every file location in its commit: the manifest list in `add-snapshot`, and
through it the manifests and the data files. The Plane reads them with its own credentials, which
can reach more than the writer's (other buckets, other tables, local files on its host). Without a
limit a writer can make the Plane read a file it could not read itself, for example another
table's Parquet file listed as a data file of its own commit, and learn its key values from the
violation report (duplicate keys come with sample values), or probe local paths through error
messages: the Plane acts as a confused deputy.

## Specification

- **Allowed prefixes** of a table: its metadata `location` and the four properties above, plus
  every `[storage] allowed_locations` entry. Each is normalized (below) and matched as a prefix
  ending at a `/`: `s3://b/t` allows `s3://b/t/x`, not `s3://b/t2/x`.
- **Normalization:** `s3a://` is read as `s3://`; `file:///p`, `file:/p` and `/p` are the same
  local path; `\` is read as `/`. A location with a `.` or `..` path segment, or an empty one
  after the scheme, is never allowed.
- **Commits:** every file the validation reads is checked before it is read. A file outside is
  `UNSUPPORTED_COMMIT_OPERATION`: "file … is outside the locations of table …". Nothing is read.
- **Onboarding, rebuild, catching up and `verify`** apply the same check to the table's own
  history: a table whose files lie elsewhere cannot be onboarded until its locations are allowed.
- A table without a `location` has no allowed prefix except `allowed_locations`.

## Compatibility and migration

- Tables whose files all lie under their location (the default layout of every engine) are
  unaffected.
- Tables with files elsewhere (imported with `add_files`, migrated, a changed `write.data.path`)
  need those locations in `[storage] allowed_locations`.

## Test plan

- Unit: normalization and prefix matching, including `..`, `s3a://`, `file:` forms, sibling
  prefixes.
- Gateway: a commit whose data file is another table's file is refused with INT-012 and the other
  table's key values appear nowhere in the answer or the audit log; a commit under the table's
  location is accepted; `allowed_locations` admits an extra location.

## Alternatives

- **Checking only the manifest list:** manifests and data files are listed inside it.
- **Reading with the writer's credentials:** the Plane does not hold them for object storage
  (writers sign their own S3 requests), and validation needs one consistent view.
