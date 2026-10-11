# 0020. Concurrent metadata reads

- Status: Accepted
- Date: 2026-10-11
- Extends: ADR 0008, ADR 0017

## Context

Once a table has position delete files, diffing a commit needs the complete picture of deleted
positions (ADR 0017): every manifest of both snapshots and every live delete file. These were read
one request at a time, and every manifest listed in both snapshots was read twice. On local disk
that costs little; on Amazon S3 each request waits tens of milliseconds, so a merge-on-read delete
of one row in a 100-file table took 4.7 s, and 8.5 s once 50 delete files had accumulated, while it
read only 8 MB (`docs/benchmarks.md`).

## Decision

- `diff_snapshots` reads both manifest lists, then every manifest it needs exactly once: all of
  both snapshots when either has delete manifests, otherwise only the dropped and introduced ones.
- The delete files of both sides are read once each (a file listed on both sides once).
- Independent reads run on up to `READ_CONCURRENCY` (16) scoped threads per step
  (`io::concurrently`). Results are used in listing order, so the outcome does not depend on
  which read finishes first; on failure the error of the first failing file in listing order is
  returned, and files after a failure are not started. `FileIo` is therefore `Sync`.
- Data files are still streamed one after another, in the order validation consumes them.

Nothing is cached between commits: every commit reads what it validates. A cache keyed by path
would rely on files never changing at a path, and a commit that was refused could leave behind a
manifest that is later rewritten at the same path before it is committed.

## Consequences

- A commit costs a few rounds of requests instead of one request per file: see
  `docs/benchmarks.md` for the S3 figures before and after.
- A commit with many delete files keeps up to 16 reads in flight against object storage, from
  short-lived threads of the inspecting blocking task.
- With a validation budget (spec §17.1), reads started concurrently may together pass the limit
  before one of them reports it; the commit is refused either way.
- Requests per commit still grow with the number of manifests and live delete files of the table;
  compacting delete files (which engines do) keeps that number down.

## Alternatives considered

- **Caching manifests and delete files across commits by path.** Fewer requests, but correct only
  if no file is ever rewritten at the same path; see above. Could be added later for files of
  committed snapshots only.
- **Async reads on the runtime.** Inspection is synchronous code on a blocking thread; scoped
  threads give the same overlap without changing every reader's signature.
- **Reading only the delete files that touch changed data files.** Which data files a delete file
  touches is only known after reading it (Parquet position deletes may name any file).
