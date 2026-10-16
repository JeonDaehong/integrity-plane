# 0026. Authenticated actors: named integrity API tokens

- Status: Accepted
- Date: 2026-10-16

## Context

Audit events name an actor (spec §25), but every actor is what the request declares:
`X-Integrity-Actor`, else `User-Agent`. The integrity API has one shared bearer token, so even
with it set the log cannot say *which* operator registered, dropped, rebuilt or disabled
something; anyone holding the token can write any name (`threat-model.md`).

Commits are a different case. Writers authenticate to the upstream catalog, with credentials the
Plane forwards and cannot check itself; the catalog decides who may write. The Plane has no
identity of its own for writers.

## Decision

- **Named tokens.** `[[server.api_tokens]]` entries, each with a `name` and either `token` or
  `token_sha256` (hex SHA-256 of the token, so the configuration file need not hold it). A
  request to the token-protected integrity API is admitted if its bearer token matches any
  entry; the event's actor is that entry's name. `[server] admin_token` stays and is the token
  named `admin`. Names are unique, 1–64 characters of `[A-Za-z0-9._@-]`, `plane` excepted.
- **What an audit event records.** `actor`: the token's name when the request authenticated
  with one, `plane` for the Plane's own actions, otherwise the declared name as before.
  `authenticated`: true exactly in the first two cases. `declared_actor`: the declared name
  (`X-Integrity-Actor`, else `User-Agent`) whenever it differs from `actor`, so operators
  sharing a token still say who they are, now visibly unverified. Events written before this
  ADR read as `authenticated: false`.
- **Commits** keep the declared actor, `authenticated: false`. The catalog's own log is where a
  writer's identity is established; the Plane's commit events correlate with it by time,
  table and snapshot ids.
- Token digests are compared over every entry, so the response time does not depend on which
  entry matched or where a token differs.

## Consequences

- API audit events now attribute changes to an operator who held a specific token. Revoking one
  operator means removing that entry and restarting; the others keep working.
- Without any token configured the API stays open, and every API event says
  `authenticated: false`.
- The audit event record gains two fields with defaults; old records still parse. Neither field
  is part of a certificate, the transaction log or a status mapping.
- Failed authentications are limited per client as before (RFC 0007), whichever token is tried.

## Alternatives considered

- **Trust a proxy-set identity header** (e.g. from an OAuth2 proxy). Useful later; a header is only
  as trustworthy as the network path, and the Plane could not tell a proxy from a client.
- **Verify writers' catalog tokens** (OAuth2 introspection or JWT validation). Ties the Plane to each
  catalog's identity provider; deferred until a deployment needs it.
- **Mutual TLS client certificates.** Stronger, but needs TLS termination in the Plane, which it
  does not do in 0.1.
- **Hashing with BLAKE3** (already a dependency). SHA-256 is what operators can compute with
  standard tools (`sha256sum`); `sha2` was already in the dependency tree (via `ed25519-dalek`).
