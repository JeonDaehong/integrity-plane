# 0015. One serial commit queue per integrity domain

- Status: Accepted
- Date: 2026-10-09

## Context

A child insert and a parent delete committed concurrently can each be valid alone and invalid
together. Some form of mutual exclusion over the keys involved is needed between validation and
publication.

## Decision

- An integrity domain is a connected component of the foreign-key graph (spec §11). Each domain has
  one commit queue; validation, upstream publication and index application of a commit happen while
  holding it. Commits in different domains run in parallel.
- Registering, dropping and rebuilding constraints, which can merge or split domains, take a lock
  that every commit holds shared, so they never interleave with commits (ADR 0011).
- Finer-grained locking (key ranges) is allowed only after benchmarks show the queue is the
  bottleneck, and only via an RFC.

## Consequences

- The concurrent FK insert / parent delete race is impossible by construction; the concurrency tests
  check the invariant after every commit of many randomized runs.
- The commit rate of a domain is bounded by the per-commit cost (about 9–12 commits/s on the
  benchmark machine, `docs/benchmarks.md`), which matches lakehouse commit rates (commits per second
  at most, each carrying many keys).
- Constraint changes pause commits in every domain while they run.

## Alternatives considered

- **Per-key locks.** A commit can carry millions of keys; acquiring and ordering that many locks
  costs more than serialization saves at lakehouse commit rates.
- **Optimistic validation with a re-check at publication.** Needs index snapshots per commit and
  still serializes at the end; more complexity for the same throughput.
