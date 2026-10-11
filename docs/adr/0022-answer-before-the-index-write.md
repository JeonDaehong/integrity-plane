# 0022. Answer before the index write

- Status: Accepted
- Date: 2026-10-11
- Amends: ADR 0015 (what holding a domain queue covers), `docs/recovery.md`

## Context

After the upstream catalog accepts a commit, the gateway writes the commit's staged deltas to the
indexes (one atomic transaction for all of them), appends `COMMITTED` to the transaction log and only
then answers the client. For a commit that adds many keys the index write is the largest single
cost left: about 1.3 s for an append of 1 M rows with a PRIMARY KEY and a FOREIGN KEY
(`docs/benchmarks.md`). The client waits for it although the commit has already happened: the table's
`main` has moved, every reader sees the new snapshot, and nothing the gateway does afterwards can
undo it.

The gateway already answers success when the index write fails (the commit happened; recovery
re-applies the logged deltas and nothing else commits in the domain meanwhile), so answering before
the write changes when the answer is sent, not what it says.

## Decision

- Once upstream accepts a commit, the gateway answers the client with upstream's response and
  finishes the commit in a background task: index write (on a blocking thread), `COMMITTED` with the
  response, audit event.
- The task holds the domain's commit queue and the shared side of the constraint lock until it is
  done. The next commit in the domain waits for the queue; registering, dropping or rebuilding
  constraints and disabling a domain wait for the exclusive side of the lock. Everything that reads
  or replaces indexes therefore sees the finished state. (`verify` takes the shared side, but
  recomputes certificates from data files and does not read indexes.) Commits in other domains are
  unaffected.
- If the task fails (index write or log), the domain is marked for recovery as before: the log still
  holds `VALIDATED`/`COMMITTING` with the staged deltas, and recovery, which runs before every commit
  in the domain and at start, applies them idempotently and appends `COMMITTED` (RFC 0004). A crash
  of the process after the answer is the same case.
- Nothing changes in the log format (RFC 0004) or in what a repeated `Idempotency-Key` returns: a
  retry waits for the queue and then gets the recorded `COMMITTED` decision.

## Consequences

- A commit's latency no longer includes the index write and the final log record.
- For a short time after the answer, `GET /v1/integrity/transactions/{id}` can show `COMMITTING`, and
  `/metrics` and the audit log do not show the commit yet.
- The fault points after upstream (`AfterUpstreamBeforeLog`, `DuringIndexApply`,
  `BeforeCommittedLog`) now abort after the client was answered; the crash tests check the state
  after restart, which is unchanged.
- Process shutdown does not wait for running tasks; the transactions they leave are resolved by
  recovery at the next start.

## Alternatives considered

- **Keep answering after the write.** Simplest; the client pays for work that cannot change the
  outcome.
- **Release the domain queue before the index write.** The next commit would be validated against
  indexes that do not yet contain the previous commit: rejected outright (fail closed).
- **Make the index write cheaper only.** One B-tree insert per changed key is close to the storage
  engine's speed; it would still be on the client's path.
