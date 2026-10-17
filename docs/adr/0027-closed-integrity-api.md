# 0027. The integrity API is closed unless opened explicitly

- Status: Accepted
- Date: 2026-10-17

## Context

Without `[server] admin_token` or `[[server.api_tokens]]` the integrity API served every request.
Anyone who can reach the Plane could then drop constraints, start rebuilds, read the audit log and
violation reports (key values), and `domain disable` a domain, which forwards its commits
unchecked: protection switched off by an unauthenticated request. The example configuration had
the token commented out, so the default deployment was the open one.

## Decision

The Plane refuses to start when no API token is configured, unless `[server] open_api = true` says
the API is meant to be open (local tests, demos). Setting both a token and `open_api` is an error.
The test and demo configurations in this repository set `open_api = true`; the example
configuration sets a placeholder `admin_token`.

## Consequences

- A configuration without a token that worked before 0.1.0 now fails at start with a message
  naming both options (CHANGELOG).
- Open APIs are explicit in the configuration, where reviews see them.
- No change for configurations with tokens.

## Alternatives considered

- **Open only on loopback:** behind a container or a sidecar proxy every request comes from
  loopback; the bind address does not say who can reach it.
- **Read-only when open:** still exposes key values in violation reports and the audit log.
