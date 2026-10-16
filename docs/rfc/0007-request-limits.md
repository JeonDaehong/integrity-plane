# RFC 0007: Request limits

- Status: Accepted
- Date: 2026-10-16
- Affects: status mapping, error codes (adds INT-017)

## Summary

Bounds how much one writer or one caller of the integrity API can take from the gateway: a bound on
the commits waiting in each domain's queue, an optional commit rate per client, a limit on failed
integrity API authentications per client, and one `verify` at a time. Every refusal is a definite
decision taken before anything is forwarded: new code INT-017 `RATE_LIMITED`, HTTP 429 with
`Retry-After`.

## Motivation

Each domain commits serially (ADR 0015). Nothing bounds how many commits wait for it: a writer that
floods a domain holds connections, request bodies (up to 64 MiB each) and tasks without limit, and
every honest writer of that domain waits behind it. The integrity API token can be guessed at any
rate, and `verify` reads every snapshot of a table, as many at once as callers ask for
(`threat-model.md`, `limitations.md`).

What clients do with 429 (RFC 0003 survey, 2026-10-09): Java's HTTP layer retries 429 for every
request, POST included, with exponential backoff that honours `Retry-After`; once retries are
exhausted the commit fails with `RESTException`, a definite failure whose written files are cleaned
up. PyIceberg does not retry POST and raises `RESTError`, also definite. Neither treats 429 as
"commit state unknown".

## Specification

### Error code and response

- **INT-017 `RATE_LIMITED`**: the gateway refused the request before forwarding or validating it,
  because a limit below was reached. HTTP **429**, header `Retry-After: <seconds>` (an integer,
  at least 1), body the RFC 0003 `ErrorModel` with `type` = `RateLimitedException`.
- A refused request MUST NOT be forwarded upstream, validated, logged in the transaction log or
  counted against any other limit. It MAY be retried unchanged.
- Spec §24 gains INT-017; the RFC 0003 table gains the row *Limit reached (queue full, client rate,
  failed authentications, `verify` busy) — INT-017 — 429*.

### Commit queue bound (`[limits] max_queued_commits`, default 32)

- A commit to a constrained table that would wait for its domain's queue while
  `max_queued_commits` other commits already wait for it is refused with INT-017,
  `Retry-After: 1`. The commit holding the queue does not count. `0` means no bound.
- The check happens after the stale-base pre-check (a commit that fails its requirements is still
  answered 409 without waiting) and before the request waits.
- Commits to tables without constraints are forwarded unchanged and are not counted.

### Commit rate per client (`[limits] commits_per_client`, default off)

- When set to `n` (> 0), each client may send `n` commit requests to constrained tables per
  second, with bursts of up to `n` (a token bucket refilled at `n` per second). A request finding
  the client's bucket empty is refused with INT-017, `Retry-After` = seconds until a token is
  available, rounded up.
- The client is the peer IP address of the connection, or, when `[limits] client_header` names a
  header (e.g. `X-Forwarded-For`), the first comma-separated value of that header. The header is
  only to be configured behind a proxy that sets it; anyone can send it otherwise. Requests
  without a known address share one bucket.
- At most 65 536 clients are tracked; buckets that are full again are forgotten first.

### Integrity API

- **Failed authentications.** When `[server] admin_token` is set, a client (as above) whose
  requests failed authentication 10 times within the last minute is answered INT-017 for every
  integrity API request, valid token or not, until a minute has passed since the first of them;
  `Retry-After` is the remaining seconds. `GET /v1/integrity/status` is never limited.
- **`verify`.** At most one `verify` runs at a time; another is refused with INT-017,
  `Retry-After: 5`.

### Metrics

`integrity_rate_limited_total{reason="queue|client|auth|verify"}` counts refusals.

## Compatibility and migration

New configuration keys with defaults; the queue bound is on by default (32 waiting commits per
domain, far above what serial commits sustain). A client that previously waited behind 32 others
now gets 429 and retries (Java) or fails definitely (PyIceberg). No index, certificate or log
format changes.

## Test plan

- Unit tests: status and type of INT-017, the token bucket (refill, burst, `Retry-After`
  rounding, eviction), the failed-authentication window, client key extraction.
- Gateway tests: with `max_queued_commits = 1` and a domain held, the second waiting commit is
  queued and the third gets 429 with `Retry-After` and nothing forwarded upstream; with
  `commits_per_client = 1`, a second commit within the second gets 429; ten bad tokens lock the
  client out of the API until the window passes while `status` still answers; a second
  concurrent `verify` gets 429.

## Alternatives

- **409 for a full queue.** Engines would refresh the table and retry the whole operation after a
  short backoff, re-reading metadata each time; 429 with `Retry-After` is retried more cheaply by
  Java and says what happened.
- **503 with `Retry-After`.** Java retries it too, but once retries are exhausted reports "commit
  state unknown" and leaves orphan files (RFC 0003).
- **Per-client limits only.** Many clients behind one proxy, or one client with many addresses,
  still flood a domain; the queue bound protects the domain whoever sends.
- **Limiting at a reverse proxy.** Still recommended for connection-level abuse, but a proxy sees
  neither domains nor which requests are commits to constrained tables.
