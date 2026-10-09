# RFC 0005: Signed integrity certificates

- Status: Draft
- Date: 2026-10-09
- Affects: certificate format (adds fields; RFC 0002 certificates are unchanged)

## Summary

The Plane signs every certificate it issues with an Ed25519 key and writes the signature into the
snapshot summary next to the certificate. `verify` checks signatures against public keys the
verifier trusts, so a valid chain proves not only that the data matches the certificates but that
the Plane issued them.

## Motivation

A certificate (RFC 0002) is a hash over public inputs. Anyone who can write snapshots to the
catalog and data files to storage can compute valid-looking certificates for data the Plane never
validated (`docs/threat-model.md`, "A compromised upstream catalog"). Certificates prove the chain
is consistent, not who produced it. A signature with a key only the Plane holds closes that gap
(spec §18, "Optional signing").

## Specification

### Algorithm

Ed25519 (RFC 8032), pure, no pre-hashing. The signed message is

```text
msg = "oip-cert-sig-v1" ‖ cert_n          15 ASCII bytes, then the 32-byte certificate
```

The domain separation tag keeps these signatures from being valid for any other message.

### Key identity

```text
key_id = hex( BLAKE3( "oip-key-id-v1" ‖ public_key )[0..16] )      32 lowercase hex characters
```

### Snapshot summary fields

When signing is enabled, each certified snapshot carries, in addition to the RFC 0002 fields:

| Field | Value |
|---|---|
| `integrity.cert-signature` | the 64-byte signature, 128 lowercase hex characters |
| `integrity.cert-key-id` | the signing key's `key_id` |

`integrity.cert-version` stays `1`: the certificate itself is unchanged. A snapshot either has both
fields or neither; one without the other is malformed.

### Keys

- `[signing] key_file = "…"` names a file holding the 32-byte Ed25519 seed. If the file does not
  exist, the Plane generates a key from the operating system's random source and writes it with
  owner-only permissions. Without a `[signing]` section the Plane does not sign (0.1 behaviour).
- The registry records every public key the Plane has used (`key_id`, public key, first use). Keys
  are never removed: old snapshots stay verifiable after rotation. Rotation = pointing `key_file` at
  a new seed.
- `GET /v1/integrity/keys` lists them:
  `{"keys": [{"key_id": "…", "algorithm": "ed25519", "public_key": "<hex>", "active": true}]}`.

### Verification

`verify` reports, for every certified snapshot, a signature status:

| Status | Meaning |
|---|---|
| `VALID` | signed by a trusted key, signature correct |
| `ABSENT` | no signature fields |
| `UNKNOWN_KEY` | signed by a key the verifier does not trust |
| `INVALID` | wrong signature, or only one of the two fields present |

- Trusted keys: those given to the verifier (`integrity verify --trusted-key <hex>`, repeatable),
  else the keys the Plane publishes. Pinning keys is what makes external verification independent
  of the Plane.
- `INVALID` makes the link `MALFORMED`, so it breaks the chain like a forged certificate.
- `ABSENT` and `UNKNOWN_KEY` break the chain only when the verifier requires signatures
  (`--require-signatures`, or `[signing] require = true` for the Plane's own `verify`); otherwise
  they are reported and the report says the chain is unsigned.

### Transaction log and recovery

Ed25519 signatures are deterministic: the signature of a certificate can be recomputed from the
certificate and the key. The transaction log format (RFC 0004) is unchanged; signatures are
injected together with certificates before `VALIDATED` is written, so a recovered transaction
committed upstream carries the same signed summary.

## Compatibility and migration

- Existing certificates and chains are unchanged and stay valid; they verify as `ABSENT`.
- Enabling signing mid-chain is allowed: later snapshots are signed, earlier ones `ABSENT`.
- Clients ignore unknown summary fields (verified for Spark, PyIceberg and iceberg-rust in Phase 7).

## Test plan

- RFC 8032 test vectors for the Ed25519 implementation; the message and key-id derivations pinned
  by fixed vectors in `integrity-core`.
- Property tests: a signature over any certificate verifies; flipping any bit of signature,
  certificate or key fails.
- Gateway tests: signed commits carry both fields; `verify` reports `VALID`; a snapshot whose
  certificate is copied (as in the existing forgery test) with a signature from another key is
  `UNKNOWN_KEY`, and `MALFORMED` when the signature does not match.
- Key rotation: snapshots signed with the old and the new key verify against the published keys.
- Compatibility jobs: `integrity verify --require-signatures` on every client's tables.

## Alternatives

- **HMAC with a shared secret.** Verifiers would need the secret, which lets them forge.
- **Signing the whole chain head only.** Cheaper, but a snapshot could not be checked on its own.
- **Storing signatures only in the Plane.** External verifiers would have to trust the Plane's
  store, which is what signing is meant to avoid.
- **New dependency:** `ed25519-dalek` 2 (BSD-3-Clause, pure Rust, widely used); its license is on
  the `cargo deny` allow list.
