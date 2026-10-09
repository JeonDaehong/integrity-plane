//! Certificate signatures (RFC 0005): Ed25519 over `"oip-cert-sig-v1" ‖ cert_n`.

use ed25519_dalek::{Signer, SigningKey, VerifyingKey};

use crate::certificate::Digest;

/// Domain separation tag of the signed message.
pub const SIGNATURE_TAG: &[u8] = b"oip-cert-sig-v1";
/// Snapshot summary field holding the signature (128 hex characters).
pub const SUMMARY_CERT_SIGNATURE: &str = "integrity.cert-signature";
/// Snapshot summary field holding the signing key's id (32 hex characters).
pub const SUMMARY_CERT_KEY_ID: &str = "integrity.cert-key-id";

/// The message signed for a certificate.
pub fn signature_message(cert: &Digest) -> Vec<u8> {
    let mut msg = SIGNATURE_TAG.to_vec();
    msg.extend_from_slice(&cert.0);
    msg
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Parses lowercase or uppercase hex of exactly `N` bytes.
pub fn from_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != 2 * N || !s.is_ascii() {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// `key_id = hex(BLAKE3("oip-key-id-v1" ‖ public_key)[0..16])`.
pub fn key_id(public_key: &[u8; 32]) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"oip-key-id-v1");
    h.update(public_key);
    hex(&h.finalize().as_bytes()[..16])
}

/// A signing key and its identity.
#[derive(Clone)]
pub struct CertSigner {
    key: SigningKey,
    public: [u8; 32],
    id: String,
}

impl std::fmt::Debug for CertSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CertSigner({})", self.id)
    }
}

impl CertSigner {
    /// The signer for a 32-byte Ed25519 seed.
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        let key = SigningKey::from_bytes(seed);
        let public = key.verifying_key().to_bytes();
        Self {
            id: key_id(&public),
            key,
            public,
        }
    }

    /// The public key.
    pub fn public_key(&self) -> [u8; 32] {
        self.public
    }

    /// The key id.
    pub fn key_id(&self) -> &str {
        &self.id
    }

    /// Signs a certificate: the 64-byte signature as hex.
    pub fn sign(&self, cert: &Digest) -> String {
        hex(&self.key.sign(&signature_message(cert)).to_bytes())
    }
}

/// Whether `signature_hex` is a valid signature of `cert` by `public_key` (strict verification:
/// rejects small-order keys and malleable signatures).
pub fn verify(public_key: &[u8; 32], cert: &Digest, signature_hex: &str) -> bool {
    let (Ok(key), Some(sig)) = (
        VerifyingKey::from_bytes(public_key),
        from_hex::<64>(signature_hex),
    ) else {
        return false;
    };
    key.verify_strict(
        &signature_message(cert),
        &ed25519_dalek::Signature::from_bytes(&sig),
    )
    .is_ok()
}

/// The public key of a hex string, if valid.
pub fn public_key_from_hex(s: &str) -> Option<[u8; 32]> {
    from_hex::<32>(s).filter(|k| VerifyingKey::from_bytes(k).is_ok())
}

/// Hex of a public key.
pub fn public_key_hex(key: &[u8; 32]) -> String {
    hex(key)
}
