# RFC 0003: HTTP status mapping v1

- Status: Accepted
- Date: 2026-10-09
- Affects: status mapping, error codes (adds INT-016)
- Resolves: spec Appendix B.3

## Summary

Fixes which HTTP status and error body the gateway returns for each integrity outcome of a table
commit, based on how the Iceberg clients actually handle commit responses, and changes spec §22 where
its 503 rows would make clients believe a commit's state is unknown.

## Motivation: what clients do (verified 2026-10-09)

Java (`ErrorHandlers.CommitErrorHandler`, `RESTTableOperations`, `ExponentialHttpRequestRetryStrategy`
on `main`):

- 409 → `CommitFailedException`: the engine refreshes and retries the whole operation
  (`commit.retry.num-retries`, bounded).
- 400 → `BadRequestException` (or `IllegalArgumentException` if `type` is that name): fails fast;
  written files are cleaned up.
- **500, 502, 503, 504 → `CommitStateUnknownException`**: the client reloads the table; if the
  snapshot is absent it still reports unknown state and does **not** clean up written files.
- Other codes (e.g. 422, 423) → `RESTException`: definite failure, fails fast.
- HTTP layer: 429 is retried for every request; 503 with `Retry-After` is retried even for POST;
  other 5xx/408 only for idempotent requests or requests with an `Idempotency-Key` header, which the
  client sends only if `/v1/config` advertises idempotency support.

PyIceberg (`commit_table`, `_handle_non_200_response`): 409 → `CommitFailedException`;
500/502/504 → `CommitStateUnknownException`; 503 → `ServiceUnavailableError`; 400 →
`BadRequestError`; anything else → `RESTError`. HTTP retries (429, 5xx) do not apply to POST.

Spec §22 maps "domain degraded / recovery required" to 503. For Java that is "commit state unknown",
although the Plane knows with certainty that the commit was not forwarded.

## Specification

Every response for a decision the Plane made is definite. The gateway never returns 500, 502, 503 or
504 for a commit it did not forward.

| Situation | Codes | HTTP |
|---|---|---|
| Constraint violated | INT-003 … INT-008 | 400 |
| Unsupported operation, invalid request, budget exceeded | INT-001, INT-012, INT-014 | 400 |
| Stale base snapshot (failed requirement, new snapshot not on current `main`) | INT-009 | 409 |
| Transient: unresolved recovery, storage read failure | INT-011, **INT-016** | 409 |
| Operator action needed: index degraded, chain broken | INT-010, INT-015 | 423 |
| Internal error before forwarding | INT-010 (domain set to degraded) | 423 |
| Upstream catalog response after forwarding | — | passed through unchanged |

- **New code INT-016 `STORAGE_READ_FAILED`**: the Plane could not read a manifest or data file of
  the commit. Transient from the client's view; 409 makes engines retry a bounded number of times.
- **409 for transient Plane states** (instead of 503): clients refresh and retry a few times, then
  fail definitely with `CommitFailedException`; written files are cleaned up.
- **423 for operator action** (instead of 503): a definite `RESTException` / `RESTError`; no retry.
- **Body.** A standard Iceberg `ErrorModel`:
  `{"error": {"message": "<INT-xxx> <NAME>: <summary>", "type": "<Type>", "code": <status>,
  "stack": []}}` with `type` = `IntegrityViolationException` (400), `CommitFailedException` (409),
  `IntegrityUnavailableException` (423). `type` is never `IllegalArgumentException`. Key values in
  messages follow spec §24 (`sample_keys` ≤ 10, redaction policy); the full structured error is at
  `/v1/integrity/transactions/{txn_id}`.
- **Idempotency.** `/v1/config` does not advertise `Idempotency-Key` support until request-id
  deduplication exists (Phase 8); then it does, and duplicate keys return the recorded decision.
- **Multi-table commit endpoint** (`/v1/{prefix}/transactions/commit`): 400, INT-012.

Spec §22's table is amended accordingly; §24 gains INT-016.

## Compatibility and migration

No deployed gateway yet. Clients see standard Iceberg error bodies; only the status for Plane-side
unavailability differs from spec v1.

## Test plan

Unit tests for the mapping table; Phase 7 compatibility tests assert, per client (Spark, PyIceberg,
iceberg-rust), the exception type and that a violation causes exactly one commit attempt.

## Alternatives

- *503 as in spec §22*: Java reports "commit state unknown" and leaves orphan files although nothing
  was committed.
- *400 for everything the Plane decides*: loses the retry that transient states deserve.
