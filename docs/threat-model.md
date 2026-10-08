# Threat model

> **Status:** placeholder (Phase 0). Normative source until this document is filled in: spec §10, §18 and §24.
> Nothing described here is implemented yet.

Topics to cover: writers bypassing the Plane by reaching the upstream catalog directly (deployment requirement, detected by certificates), upstream catalog exposure, and PII in key values (errors, logs, metrics, audit).

Notes so far:

- Index files: accidental corruption of an index store is detected on open (full checksum
  verification) and reported as `Corrupt`, never served; storage-engine panics on malformed files
  are contained (ADR 0005). The checksums are not cryptographic: someone who can write the file can
  forge consistent contents, so the control-store directory must be writable only by the Plane.
  Certificates (spec §18) recompute key digests from data files and do not trust the indexes.
- Index errors never include key values.
