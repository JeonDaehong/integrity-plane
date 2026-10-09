//! RFC 0005: Ed25519 certificate signatures.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use integrity_core::signature::{self, key_id, signature_message, verify};
use integrity_core::{CertSigner, Digest};
use proptest::prelude::*;

/// RFC 8032 §7.1, test 1: the Ed25519 implementation itself.
#[test]
fn rfc8032_test_vector_1() {
    use ed25519_dalek::{Signer, SigningKey};
    let seed: [u8; 32] =
        signature::from_hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
            .unwrap();
    let key = SigningKey::from_bytes(&seed);
    assert_eq!(
        signature::public_key_hex(&key.verifying_key().to_bytes()),
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    );
    let sig = key.sign(b"").to_bytes();
    assert_eq!(
        sig.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    );
}

/// The derivations of RFC 0005, pinned: any change here is a format change.
#[test]
fn message_key_id_and_signature_are_pinned() {
    let cert = Digest([1u8; 32]);
    let mut expected = b"oip-cert-sig-v1".to_vec();
    expected.extend_from_slice(&[1u8; 32]);
    assert_eq!(signature_message(&cert), expected);
    let signer = CertSigner::from_seed(&[7u8; 32]);
    assert_eq!(signer.key_id(), key_id(&signer.public_key()));
    assert_eq!(signer.key_id().len(), 32);
    assert_eq!(signer.key_id(), "f4221234d7b88fd6719de5a0db3accb0");
    assert_eq!(
        signer.sign(&cert),
        "a561ed1431678720b89101211157b364aa009a52c4afb942bf2a796612949672fa431ce25327f21c27a74d5d0676fa2adca643ec0e764ee55b8b7237d8785300"
    );
}

proptest! {
    #[test]
    fn signatures_verify_and_any_change_breaks_them(
        seed in any::<[u8; 32]>(),
        cert in any::<[u8; 32]>(),
        bit in 0usize..(64 * 8),
    ) {
        let signer = CertSigner::from_seed(&seed);
        let cert = Digest(cert);
        let sig = signer.sign(&cert);
        prop_assert!(verify(&signer.public_key(), &cert, &sig));

        let mut bytes: [u8; 64] = signature::from_hex(&sig).unwrap();
        bytes[bit / 8] ^= 1 << (bit % 8);
        let flipped: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        prop_assert!(!verify(&signer.public_key(), &cert, &flipped));

        let mut other = cert;
        other.0[bit % 32] ^= 1 << (bit % 8);
        prop_assert!(!verify(&signer.public_key(), &other, &sig));

        let stranger = CertSigner::from_seed(&[seed[0].wrapping_add(1); 32]);
        prop_assert!(!verify(&stranger.public_key(), &cert, &sig));
    }
}

#[test]
fn malformed_inputs_do_not_verify() {
    let signer = CertSigner::from_seed(&[3u8; 32]);
    let cert = Digest([9u8; 32]);
    let sig = signer.sign(&cert);
    assert!(!verify(&signer.public_key(), &cert, &sig[..126]));
    assert!(!verify(
        &signer.public_key(),
        &cert,
        &format!("{}zz", &sig[..126])
    ));
    assert!(!verify(&signer.public_key(), &cert, ""));
    assert!(signature::public_key_from_hex("00").is_none());
}
