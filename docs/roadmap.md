# Roadmap

1.0 is released when the criteria below are met, not on a date. Until then every release is
pre-alpha. Work is tracked in the [milestones](https://github.com/JeonDaehong/integrity-plane/milestones).
Items that change what the Plane accepts, or a stored format, get an RFC first
([`docs/rfc/`](rfc/)).

## 0.2: Flink CDC tables

- Compacting tables with equality delete files through the Plane: a rewrite is accepted when the
  rows readers see in the removed files, after deletes, have the same keys as the added files.
- Flink upsert tables with partitions, where each key determines its partition.
- `verify` recomputes snapshots with equality deletes (now `UNVERIFIABLE`).
- Trino and Flink compatibility tests in CI (#9).

## 0.3: Operating several Planes

- Planes behind one load balancer share their constraints (today each registers them separately,
  in the same order).
- Kubernetes manifests (#8), native TLS on the listener (#10).
- Retention for the audit log and error reports; dropped constraints release their index data.
- Example dashboards for the metrics.

## 0.9: Release candidate

- Every stored and wire format reviewed and frozen: configuration, the integrity API, transaction
  log, index store, certificate format, key encoding, error codes and HTTP mapping.
- `docs/stability.md`: what 1.x keeps compatible, and how anything is deprecated.
- Upgrade tests in CI: stores and logs written by earlier versions are read by later ones.
- A 24-hour soak of several Planes under random kills, and power-loss tests.
- More fuzz targets (deletion vectors #7, Puffin).

A release candidate stays public for at least four weeks. An integrity bug found in that time is
fixed in a new candidate, and the four weeks start again.

## 1.0

- All of the above, with no open integrity bug.
- At least one team running it on production-like data.
- An external security review, with its findings fixed or documented.

## Later (1.x)

CHECK constraints, certified commits to branches other than `main` (write-audit-publish),
multi-table commits from clients, `ON DELETE CASCADE`, AWS KMS and Azure Key Vault signing
(ECDSA), Google Cloud KMS (#11). Feedback from users decides the order.

Have a use case this does not cover? [Open an issue](https://github.com/JeonDaehong/integrity-plane/issues).
