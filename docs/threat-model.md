# Threat model

> **Status:** partial. Normative source until this document is filled in: spec §10, §18 and §24.

Topics to cover: writers bypassing the Plane by reaching the upstream catalog directly (deployment requirement, detected by certificates), upstream catalog exposure, and PII in key values (errors, logs, metrics, audit).

Notes so far:

- Index files: accidental corruption of an index store is detected on open (full checksum
  verification) and reported as `Corrupt`, never served; storage-engine panics on malformed files
  are contained (ADR 0005). The checksums are not cryptographic: someone who can write the file can
  forge consistent contents, so the control-store directory must be writable only by the Plane.
  Certificates (spec §18) recompute key digests from data files and do not trust the indexes.
- Index errors never include key values.
- Bypass: a writer that reaches the upstream catalog directly can commit anything. The Plane
  notices at the next commit to the domain: every table's `main` must be its anchor or carry a
  certificate, otherwise the commit fails with `BYPASS_DETECTED` and the domain stays degraded until
  an operator rebuilds it (ADR 0011). A bypassing writer that copies a valid-looking certificate into
  its summary is not caught by this check; recomputing the chain from data files (`verify`, spec §18)
  is. Keeping the upstream catalog unreachable to writers remains a deployment requirement.
- Integrity API: registering, dropping and rebuilding change what is enforced. Set
  `[server] admin_token` so these require `Authorization: Bearer <token>`, and keep the API off
  untrusted networks either way; without a token the API is open to anyone who can reach the
  gateway. The token is compared by digest; it is not rate-limited.
- Violation reports and audit events name constraints, tables and codes, never key values.
