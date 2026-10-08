# Security Policy

## Status

This project is pre-alpha. There are no supported releases yet, and it must not be used to protect
production data.

## Reporting a vulnerability

Please **do not** open a public issue for security problems. Use GitHub's private vulnerability
reporting ("Report a vulnerability" under the repository's *Security* tab).

We are especially interested in:

- any way to get a commit published through the Plane that violates a declared constraint;
- ways to forge or splice integrity certificates so that `integrity verify` misses a bypass;
- leaks of key values (which can be PII) through errors, logs or metrics;
- panics or resource exhaustion on malformed REST, manifest or log input.

The threat model lives in `docs/threat-model.md`.
