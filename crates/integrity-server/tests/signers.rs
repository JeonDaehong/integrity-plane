//! Signing keys held outside the Plane (RFC 0009): Vault Transit against an in-process fake and,
//! when the environment names them (the `signers` CI job), against a real Vault dev server and
//! PKCS#11 against SoftHSM2.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::time::Duration;

use integrity_core::signature::{key_id, verify};
use integrity_core::{CertSigner, Digest};
use integrity_server::config::{Pkcs11Config, VaultTransitConfig};
use integrity_server::signing::{Pkcs11Signer, Signer, VaultSigner};

mod support;
use support::{VAULT_TOKEN, VaultMode, serve_vault};

fn transit(address: &str, key_version: Option<u64>) -> VaultTransitConfig {
    VaultTransitConfig {
        address: address.to_owned(),
        mount: "transit".into(),
        key: "oip".into(),
        key_version,
        token_file: None,
        namespace: None,
    }
}

async fn vault_signer(address: &str, key_version: Option<u64>) -> Result<Signer, String> {
    VaultSigner::connect(
        &transit(address, key_version),
        VAULT_TOKEN.into(),
        Duration::from_secs(5),
    )
    .await
    .map(|s| Signer::Vault(std::sync::Arc::new(s)))
}

#[tokio::test]
async fn vault_signs_with_the_pinned_version_and_signatures_verify() {
    let (address, vault) = serve_vault(vec![[1; 32], [2; 32]]).await;
    let latest = CertSigner::from_seed(&[2; 32]);
    let signer = vault_signer(&address, None).await.unwrap();
    assert_eq!(signer.public_key(), latest.public_key());
    assert_eq!(signer.key_id(), key_id(&latest.public_key()));
    let cert = Digest([7; 32]);
    let signature = signer.sign(&cert).await.unwrap();
    assert!(verify(&signer.public_key(), &cert, &signature));
    // Ed25519 is deterministic: the same key in a file signs the same.
    assert_eq!(signature, latest.sign(&cert));

    // A configured version is used even when it is not the latest.
    let first = vault_signer(&address, Some(1)).await.unwrap();
    assert_eq!(
        first.public_key(),
        CertSigner::from_seed(&[1; 32]).public_key()
    );
    first.sign(&cert).await.unwrap();
    // A rotation in Vault does not change the version a running signer uses.
    vault.lock().unwrap().versions.push([3; 32]);
    signer.sign(&cert).await.unwrap();
    assert_eq!(vault.lock().unwrap().signed_with, [2, 1, 2]);
    assert!(vault_signer(&address, Some(9)).await.is_err());
}

#[tokio::test]
async fn vault_failures_are_errors_and_never_name_the_token() {
    let (address, vault) = serve_vault(vec![[1; 32]]).await;
    let signer = vault_signer(&address, None).await.unwrap();
    let cert = Digest([7; 32]);

    vault.lock().unwrap().mode = VaultMode::Sealed;
    let e = signer.sign(&cert).await.unwrap_err();
    assert!(e.contains("503"), "{e}");
    // A signature that does not verify is never used.
    vault.lock().unwrap().mode = VaultMode::Lie;
    let e = signer.sign(&cert).await.unwrap_err();
    assert!(e.contains("does not verify"), "{e}");

    let wrong = VaultSigner::connect(
        &transit(&address, None),
        "s.wrong".into(),
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert!(wrong.contains("403"), "{wrong}");
    assert!(!wrong.contains("s.wrong"), "{wrong}");
    assert!(!format!("{signer:?}").contains(VAULT_TOKEN));

    vault.lock().unwrap().key_type = "ecdsa-p256";
    let e = vault_signer(&address, None).await.unwrap_err();
    assert!(e.contains("not ed25519"), "{e}");

    let e = vault_signer("http://127.0.0.1:1", None).await.unwrap_err();
    assert!(e.starts_with("Vault:"), "{e}");
}

/// Against a real Vault dev server: `INTEGRITY_TEST_VAULT_ADDR` and its root token in
/// `INTEGRITY_TEST_VAULT_TOKEN` (CI).
#[tokio::test]
async fn a_real_vault_transit_key_signs_certificates() {
    let (Ok(address), Ok(token)) = (
        std::env::var("INTEGRITY_TEST_VAULT_ADDR"),
        std::env::var("INTEGRITY_TEST_VAULT_TOKEN"),
    ) else {
        eprintln!("INTEGRITY_TEST_VAULT_ADDR not set: Vault test skipped");
        return;
    };
    let http = reqwest::Client::new();
    let post = |path: &str, body: serde_json::Value| {
        http.post(format!("{address}/v1/{path}"))
            .header("X-Vault-Token", &token)
            .json(&body)
            .send()
    };
    // A mount of its own per run: a dev server keeps earlier runs' mounts.
    let mount = format!(
        "oip-transit-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mounted = post(
        &format!("sys/mounts/{mount}"),
        serde_json::json!({"type": "transit"}),
    )
    .await
    .unwrap();
    assert!(mounted.status().is_success(), "{}", mounted.status());
    let created = post(
        &format!("{mount}/keys/oip"),
        serde_json::json!({"type": "ed25519"}),
    )
    .await
    .unwrap();
    assert!(created.status().is_success(), "{}", created.status());

    let mut config = transit(&address, None);
    config.mount = mount;
    let signer = Signer::Vault(std::sync::Arc::new(
        VaultSigner::connect(&config, token.clone(), Duration::from_secs(10))
            .await
            .unwrap(),
    ));
    for n in 0..3u8 {
        let cert = Digest([n; 32]);
        let signature = signer.sign(&cert).await.unwrap();
        assert!(verify(&signer.public_key(), &cert, &signature));
    }
}

/// Initializes a SoftHSM2 token labelled `label` with an Ed25519 key pair labelled `key`.
fn softhsm_token(module: &str, label: &str, key: &str, pin: &str) {
    use cryptoki::context::{CInitializeArgs, CInitializeFlags, Pkcs11};
    use cryptoki::mechanism::Mechanism;
    use cryptoki::object::Attribute;
    use cryptoki::session::UserType;
    use cryptoki::types::AuthPin;

    let p11 = Pkcs11::new(module).unwrap();
    p11.initialize(CInitializeArgs::new(CInitializeFlags::OS_LOCKING_OK))
        .unwrap();
    let slot = p11
        .get_slots_with_token()
        .unwrap()
        .into_iter()
        .find(|s| !p11.get_token_info(*s).unwrap().token_initialized())
        .expect("a free SoftHSM2 slot");
    let so = AuthPin::from("so-pin".to_owned());
    p11.init_token(slot, &so, label).unwrap();
    let slot = p11
        .get_slots_with_token()
        .unwrap()
        .into_iter()
        .find(|s| p11.get_token_info(*s).unwrap().label().trim_end() == label)
        .unwrap();
    let session = p11.open_rw_session(slot).unwrap();
    session.login(UserType::So, Some(&so)).unwrap();
    session.init_pin(&AuthPin::from(pin.to_owned())).unwrap();
    session.logout().unwrap();
    session
        .login(UserType::User, Some(&AuthPin::from(pin.to_owned())))
        .unwrap();
    let ed25519 = vec![0x06, 0x03, 0x2b, 0x65, 0x70];
    session
        .generate_key_pair(
            &Mechanism::EccEdwardsKeyPairGen,
            &[
                Attribute::Token(true),
                Attribute::Verify(true),
                Attribute::EcParams(ed25519),
                Attribute::Label(key.as_bytes().to_vec()),
            ],
            &[
                Attribute::Token(true),
                Attribute::Private(true),
                Attribute::Sensitive(true),
                Attribute::Sign(true),
                Attribute::Label(key.as_bytes().to_vec()),
            ],
        )
        .unwrap();
}

#[test]
fn pkcs11_signs_with_a_key_that_stays_in_the_token() {
    let Ok(module) = std::env::var("INTEGRITY_TEST_PKCS11_MODULE") else {
        eprintln!("INTEGRITY_TEST_PKCS11_MODULE not set: PKCS#11 test skipped");
        return;
    };
    softhsm_token(&module, "oip-test", "cert-signing", "1234");
    let config = |key: &str| Pkcs11Config {
        module: module.clone().into(),
        token_label: "oip-test".into(),
        key_label: key.into(),
        pin_file: None,
    };
    assert!(Pkcs11Signer::open(&config("cert-signing"), "9999".into()).is_err());
    assert!(Pkcs11Signer::open(&config("other"), "1234".into()).is_err());

    let signer = Signer::Pkcs11(std::sync::Arc::new(
        Pkcs11Signer::open(&config("cert-signing"), "1234".into()).unwrap(),
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for n in 0..3u8 {
        let cert = Digest([n; 32]);
        let signature = runtime.block_on(signer.sign(&cert)).unwrap();
        assert!(verify(&signer.public_key(), &cert, &signature));
    }
}
