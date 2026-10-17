# RFC 0009: Signing keys held outside the Plane

- Status: Accepted
- Date: 2026-10-17
- Affects: status mapping, error codes (adds INT-018), configuration (`[signing]`)

## Summary

The certificate signing key (RFC 0005) can be kept in HashiCorp Vault's Transit engine or in a
PKCS#11 token (an HSM, a cloud HSM with a PKCS#11 library, SoftHSM) instead of a file in the
control store. The Plane then never holds the private key: it asks the backend to sign each
certificate. Signatures, key ids and verification are unchanged (still Ed25519, RFC 0005). A commit
whose certificate cannot be signed is refused before it is forwarded, with the new transient code
INT-018 `SIGNER_UNAVAILABLE` (409).

## Motivation

With `[signing] key_file`, whoever can read the control store (a backup, a disk snapshot, an
operator of the host) can sign certificates for data the Plane never validated, which is what
signing is meant to prevent (`threat-model.md`). Organisations that already keep keys in Vault or an
HSM expect a service to use them there, with access policy, audit and rotation of their own.

Cloud KMS services (AWS KMS, Google Cloud KMS, Azure Key Vault) are not covered: AWS KMS and Azure
Key Vault do not offer Ed25519 for signing, which would need a second signature algorithm and a
certificate format change (a later RFC). Google Cloud KMS offers Ed25519 and can be added as a
backend later without a format change.

## Specification

### Configuration

```toml
[signing]
backend = "file"            # default; "vault-transit" or "pkcs11"
key_file = "/var/lib/integrity/keys/signing.key"   # backend = "file" only
require = false

[signing.vault]              # backend = "vault-transit" only
address = "https://vault.example:8200"
mount = "transit"            # default
key = "integrity-plane"      # an ed25519 Transit key
key_version = 3              # optional; default: the key's latest version at start
token_file = "/run/secrets/vault-token"   # or the VAULT_TOKEN environment variable
namespace = "team-a"         # optional, Vault Enterprise

[signing.pkcs11]             # backend = "pkcs11" only
module = "/usr/lib/softhsm/libsofthsm2.so"
token_label = "integrity"
key_label = "cert-signing"   # the private key's CKA_LABEL; its public key has the same label
pin_file = "/run/secrets/hsm-pin"         # or the INTEGRITY_PKCS11_PIN environment variable
```

A `[signing]` section written for RFC 0005 (`key_file`, `require`) keeps its meaning. Settings of
another backend than the chosen one are a configuration error. Tokens and PINs are never written
to logs, audit events or error messages.

### Keys

- At start the Plane reads the backend's public key and checks that it is Ed25519 (Vault key type
  `ed25519`; PKCS#11 `CKK_EC_EDWARDS` on curve Ed25519). Anything else fails the start.
- The Plane signs with that one key for its whole run. For Vault, it pins the key version it read
  at start (`key_version` in every sign request), so a rotation in Vault takes effect at the next
  start, never in the middle of a chain of signatures with an unrecorded key.
- The `key_id` and the registry's key records are those of RFC 0005; `GET /v1/integrity/keys`
  lists the key whatever its backend.
- Every signature returned by a backend MUST be checked against the public key before it is put
  into a commit (strict Ed25519 verification). A backend that returns a wrong signature is treated
  as unavailable.

### Signing

The message is RFC 0005's `"oip-cert-sig-v1" ‖ cert_n`, signed with pure Ed25519 (Vault: no
pre-hashing; PKCS#11: `CKM_EDDSA` without parameters). Ed25519 is deterministic, so a backend and
a key file holding the same key produce the same signature.

### Error code

- **INT-018 `SIGNER_UNAVAILABLE`**: signing is configured and the certificate of the commit could
  not be signed (backend unreachable, refused, timed out after `[upstream] timeout_secs`, or a
  signature that does not verify). Nothing was forwarded; the transaction is `ABORTED`. HTTP
  **409** like the other transient Plane states (RFC 0003): engines refresh and retry a bounded
  number of times. Spec §24 gains INT-018 and the RFC 0003 table's transient row lists it.
- A commit is never forwarded unsigned while signing is configured (fail closed).

## Compatibility and migration

- Certificates, signatures, key ids, the transaction log and verification are unchanged.
- Moving from `key_file` to a backend is a key rotation: the new key is recorded beside the old,
  and earlier snapshots stay verifiable (RFC 0005). An operator who wants to keep the same key
  imports the seed into Vault (`transit/keys/<name>/import`) or the token, then removes the file.

## Test plan

- Configuration: each backend's settings parse, a backend's settings without that backend are
  refused, the RFC 0005 form keeps working.
- Vault Transit against an in-process fake implementing `GET /v1/<mount>/keys/<key>` and
  `POST /v1/<mount>/sign/<key>` with a known key: certificates verify with the published key; the
  pinned version is sent; a refused, unreachable or lying signer gives INT-018 and nothing reaches
  upstream; the token is sent as `X-Vault-Token` and never appears in errors.
- Vault Transit against a real Vault dev server (CI): a new `ed25519` key signs certificates that
  verify with the public key Vault publishes. (The fake shows that Vault and a key file holding
  the same seed sign alike.)
- Gateway: a commit is signed through Vault; with Vault sealed, the commit gets INT-018 and nothing
  reaches upstream; once Vault answers again, commits are signed and `verify` reports `VALID`.
- PKCS#11 against SoftHSM2 (CI, Linux): a generated Ed25519 key signs certificates that verify; a
  wrong PIN or label fails the start.

## Alternatives

- **Signing in the backend with a pre-hashed message** (Ed25519ph, or ECDSA over a digest): not
  RFC 0005's algorithm; a format change.
- **Fetching the private key from Vault (KV) at start**: the key would again live in the Plane's
  memory and core dumps; Transit never releases it.
- **Signing asynchronously after the commit**: the signature must be in the snapshot summary that
  is committed, so it has to exist before forwarding.
- **New dependency** `cryptoki` 0.12 (Apache-2.0, maintained by the Parallaxsecond project, MSRV
  1.77, loads the PKCS#11 module at run time with `libloading`; no build-time native
  dependencies). Vault needs none: the existing `reqwest` client.
