//! The Plane's certificate signing key: a 32-byte Ed25519 seed in a file (RFC 0005), or a key held
//! by Vault Transit or a PKCS#11 token (RFC 0009).

use std::io::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use cryptoki::context::{CInitializeArgs, CInitializeFlags, Pkcs11};
use cryptoki::mechanism::Mechanism;
use cryptoki::mechanism::eddsa::{EddsaParams, EddsaSignatureScheme};
use cryptoki::object::{Attribute, AttributeType, KeyType, ObjectClass, ObjectHandle};
use cryptoki::session::{Session, UserType};
use cryptoki::types::AuthPin;
use integrity_core::{CertSigner, Digest};
use serde_json::{Value, json};

use crate::config::{Pkcs11Config, SignerSettings, SigningConfig, VaultTransitConfig};

/// Loads the seed at `path`, or creates it from the operating system's random source (readable by
/// the owner only, on Unix).
pub fn load_or_create(path: &Path) -> Result<CertSigner, String> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                format!(
                    "{}: a signing key file holds exactly 32 bytes, found {}",
                    path.display(),
                    bytes.len()
                )
            })?;
            Ok(CertSigner::from_seed(&seed))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed).map_err(|e| format!("no OS randomness: {e}"))?;
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options
                .open(path)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            file.write_all(&seed)
                .and_then(|()| file.sync_all())
                .map_err(|e| format!("{}: {e}", path.display()))?;
            // The file's directory entry too: without it, power loss can lose the key after
            // certificates were signed with it. Unix only: std cannot open a directory on Windows.
            #[cfg(unix)]
            {
                if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                    std::fs::File::open(dir)
                        .and_then(|d| d.sync_all())
                        .map_err(|e| format!("{}: {e}", dir.display()))?;
                }
            }
            Ok(CertSigner::from_seed(&seed))
        }
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// The Plane's certificate signer: a key file (RFC 0005), or a key that never leaves Vault Transit
/// or a PKCS#11 token (RFC 0009). Every signature is Ed25519 over RFC 0005's message.
#[derive(Debug, Clone)]
pub enum Signer {
    /// A seed file in the control store.
    File(Box<CertSigner>),
    /// A Vault Transit key.
    Vault(Arc<VaultSigner>),
    /// A key in a PKCS#11 token.
    Pkcs11(Arc<Pkcs11Signer>),
}

impl From<CertSigner> for Signer {
    fn from(s: CertSigner) -> Self {
        Self::File(Box::new(s))
    }
}

impl Signer {
    /// Connects to the configured backend and reads its public key. `timeout` bounds every call
    /// to a remote backend.
    pub async fn from_config(config: &SigningConfig, timeout: Duration) -> Result<Self, String> {
        match config.settings().map_err(|e| e.to_string())? {
            SignerSettings::File(path) => load_or_create(path).map(Self::from),
            SignerSettings::VaultTransit(v) => {
                let token = secret(v.token_file.as_deref(), "VAULT_TOKEN", "Vault token")?;
                VaultSigner::connect(v, token, timeout)
                    .await
                    .map(|s| Self::Vault(Arc::new(s)))
            }
            SignerSettings::Pkcs11(p) => {
                let pin = secret(p.pin_file.as_deref(), "INTEGRITY_PKCS11_PIN", "PKCS#11 PIN")?;
                let p = p.clone();
                tokio::task::spawn_blocking(move || Pkcs11Signer::open(&p, pin))
                    .await
                    .map_err(|e| e.to_string())?
                    .map(|s| Self::Pkcs11(Arc::new(s)))
            }
        }
    }

    /// The public key.
    pub fn public_key(&self) -> [u8; 32] {
        match self {
            Self::File(s) => s.public_key(),
            Self::Vault(s) => s.public,
            Self::Pkcs11(s) => s.public,
        }
    }

    /// The key id (RFC 0005).
    pub fn key_id(&self) -> &str {
        match self {
            Self::File(s) => s.key_id(),
            Self::Vault(s) => &s.id,
            Self::Pkcs11(s) => &s.id,
        }
    }

    /// Signs a certificate: the 64-byte signature as hex. A signature from a backend is checked
    /// against the public key first; one that does not verify is an error (RFC 0009).
    pub async fn sign(&self, cert: &Digest) -> Result<String, String> {
        let signature = match self {
            Self::File(s) => return Ok(s.sign(cert)),
            Self::Vault(s) => s.sign(cert).await?,
            Self::Pkcs11(s) => {
                let (s, cert) = (Arc::clone(s), *cert);
                tokio::task::spawn_blocking(move || s.sign(&cert))
                    .await
                    .map_err(|e| e.to_string())??
            }
        };
        if integrity_core::signature::verify(&self.public_key(), cert, &signature) {
            Ok(signature)
        } else {
            Err("the signer returned a signature that does not verify with its public key".into())
        }
    }
}

/// A secret read from `file` (trailing newline removed), else from the environment variable `var`.
fn secret(file: Option<&Path>, var: &str, what: &str) -> Result<String, String> {
    let value = match file {
        Some(f) => std::fs::read_to_string(f).map_err(|e| format!("{}: {e}", f.display()))?,
        None => std::env::var(var).map_err(|_| format!("no {what}: set {var} or a file"))?,
    };
    let value = value.trim_end_matches(['\r', '\n']).to_owned();
    if value.is_empty() {
        return Err(format!("the {what} is empty"));
    }
    Ok(value)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Signs with a HashiCorp Vault Transit key of type `ed25519` (RFC 0009), pinned to one version.
pub struct VaultSigner {
    http: reqwest::Client,
    sign_url: String,
    token: String,
    namespace: Option<String>,
    version: u64,
    public: [u8; 32],
    id: String,
}

impl std::fmt::Debug for VaultSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the token.
        write!(
            f,
            "VaultSigner({} v{}, {})",
            self.sign_url, self.version, self.id
        )
    }
}

impl VaultSigner {
    /// Reads the key's public key and pins its version: the configured one, else the latest.
    pub async fn connect(
        config: &VaultTransitConfig,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| e.to_string())?;
        let base = format!(
            "{}/v1/{}",
            config.address.trim_end_matches('/'),
            config.mount.trim_matches('/')
        );
        let mut signer = Self {
            http,
            sign_url: format!("{base}/sign/{}", config.key),
            token,
            namespace: config.namespace.clone(),
            version: 0,
            public: [0; 32],
            id: String::new(),
        };
        let key_url = format!("{base}/keys/{}", config.key);
        let body = signer.call(signer.http.get(&key_url)).await?;
        let data = &body["data"];
        if data["type"] != "ed25519" {
            return Err(format!(
                "Vault key {} is of type {}, not ed25519",
                config.key, data["type"]
            ));
        }
        let version = match config.key_version {
            Some(v) => v,
            None => data["latest_version"]
                .as_u64()
                .ok_or("Vault key without latest_version")?,
        };
        let encoded = data["keys"][version.to_string()]["public_key"]
            .as_str()
            .ok_or_else(|| format!("Vault key {} has no version {version}", config.key))?;
        let public = BASE64
            .decode(encoded)
            .ok()
            .and_then(|k| <[u8; 32]>::try_from(k).ok())
            .filter(|k| integrity_core::signature::public_key_from_hex(&hex(k)).is_some())
            .ok_or("Vault returned an invalid ed25519 public key")?;
        signer.version = version;
        signer.public = public;
        signer.id = integrity_core::signature::key_id(&public);
        Ok(signer)
    }

    async fn call(&self, request: reqwest::RequestBuilder) -> Result<Value, String> {
        let mut request = request.header("X-Vault-Token", &self.token);
        if let Some(ns) = &self.namespace {
            request = request.header("X-Vault-Namespace", ns);
        }
        // reqwest errors name the URL, never headers: the token stays out of messages.
        let response = request.send().await.map_err(|e| format!("Vault: {e}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("Vault answered {status}"));
        }
        response
            .json()
            .await
            .map_err(|e| format!("Vault response: {e}"))
    }

    async fn sign(&self, cert: &Digest) -> Result<String, String> {
        let input = BASE64.encode(integrity_core::signature::signature_message(cert));
        let body = self
            .call(
                self.http
                    .post(&self.sign_url)
                    .json(&json!({"input": input, "key_version": self.version})),
            )
            .await?;
        let signature = body["data"]["signature"]
            .as_str()
            .ok_or("Vault response without a signature")?;
        let prefix = format!("vault:v{}:", self.version);
        let encoded = signature
            .strip_prefix(&prefix)
            .ok_or("Vault signed with another key version")?;
        let bytes = BASE64
            .decode(encoded)
            .map_err(|_| "Vault returned a malformed signature")?;
        if bytes.len() != 64 {
            return Err("Vault returned a signature that is not 64 bytes".into());
        }
        Ok(hex(&bytes))
    }
}

/// DER of the Ed25519 curve in `CKA_EC_PARAMS`: the OID 1.3.101.112, or the curve name.
const ED25519_OID: &[u8] = &[0x06, 0x03, 0x2b, 0x65, 0x70];
const ED25519_NAME: &[u8] = b"\x13\x0cedwards25519";

/// Signs with an Ed25519 key in a PKCS#11 token (RFC 0009), over one logged-in session.
pub struct Pkcs11Signer {
    // Dropped after the session: finalizes the library.
    session: Mutex<Session>,
    _context: Pkcs11,
    key: ObjectHandle,
    public: [u8; 32],
    id: String,
}

impl std::fmt::Debug for Pkcs11Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pkcs11Signer({})", self.id)
    }
}

impl Pkcs11Signer {
    /// Loads the module, logs in to the token labelled `token_label` and finds the Ed25519 key
    /// pair labelled `key_label`. Blocking.
    pub fn open(config: &Pkcs11Config, pin: String) -> Result<Self, String> {
        let p11 = |e: cryptoki::error::Error| format!("PKCS#11: {e}");
        let context =
            Pkcs11::new(&config.module).map_err(|e| format!("{}: {e}", config.module.display()))?;
        context
            .initialize(CInitializeArgs::new(CInitializeFlags::OS_LOCKING_OK))
            .map_err(p11)?;
        let mut slot = None;
        for s in context.get_slots_with_token().map_err(p11)? {
            if context.get_token_info(s).map_err(p11)?.label().trim_end() == config.token_label {
                slot = Some(s);
                break;
            }
        }
        let slot =
            slot.ok_or_else(|| format!("no PKCS#11 token labelled {}", config.token_label))?;
        let session = context.open_ro_session(slot).map_err(p11)?;
        session
            .login(UserType::User, Some(&AuthPin::from(pin)))
            .map_err(p11)?;
        let one = |class: ObjectClass| -> Result<ObjectHandle, String> {
            let found = session
                .find_objects(&[
                    Attribute::Class(class),
                    Attribute::KeyType(KeyType::EC_EDWARDS),
                    Attribute::Label(config.key_label.as_bytes().to_vec()),
                ])
                .map_err(p11)?;
            match found.as_slice() {
                [h] => Ok(*h),
                [] => Err(format!(
                    "no Ed25519 {class} labelled {} in the token",
                    config.key_label
                )),
                _ => Err(format!(
                    "several Ed25519 {class}s labelled {} in the token",
                    config.key_label
                )),
            }
        };
        let key = one(ObjectClass::PRIVATE_KEY)?;
        let public_handle = one(ObjectClass::PUBLIC_KEY)?;
        let mut params = None;
        let mut point = None;
        for a in session
            .get_attributes(
                public_handle,
                &[AttributeType::EcParams, AttributeType::EcPoint],
            )
            .map_err(p11)?
        {
            match a {
                Attribute::EcParams(p) => params = Some(p),
                Attribute::EcPoint(p) => point = Some(p),
                _ => {}
            }
        }
        if !matches!(params.as_deref(), Some(ED25519_OID | ED25519_NAME)) {
            return Err(format!("key {} is not on curve Ed25519", config.key_label));
        }
        // CKA_EC_POINT is a DER OCTET STRING; some tokens store the raw 32 bytes.
        let public = match point.as_deref() {
            Some([0x04, 0x20, rest @ ..]) | Some(rest) if rest.len() == 32 => {
                <[u8; 32]>::try_from(rest).map_err(|_| "bad public key")?
            }
            _ => {
                return Err(format!(
                    "key {} has no Ed25519 public key",
                    config.key_label
                ));
            }
        };
        if integrity_core::signature::public_key_from_hex(&hex(&public)).is_none() {
            return Err(format!(
                "key {} has an invalid public key",
                config.key_label
            ));
        }
        Ok(Self {
            session: Mutex::new(session),
            _context: context,
            key,
            public,
            id: integrity_core::signature::key_id(&public),
        })
    }

    fn sign(&self, cert: &Digest) -> Result<String, String> {
        let session = self
            .session
            .lock()
            .map_err(|_| "PKCS#11 session lock poisoned".to_owned())?;
        let signature = session
            .sign(
                &Mechanism::Eddsa(EddsaParams::new(EddsaSignatureScheme::Pure)),
                self.key,
                &integrity_core::signature::signature_message(cert),
            )
            .map_err(|e| format!("PKCS#11: {e}"))?;
        if signature.len() != 64 {
            return Err("PKCS#11 returned a signature that is not 64 bytes".into());
        }
        Ok(hex(&signature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_once_then_loads_the_same_key() {
        let dir = std::env::temp_dir().join(format!(
            "oip-signing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        let path = dir.join("keys").join("signing.key");
        let first = load_or_create(&path).unwrap();
        let again = load_or_create(&path).unwrap();
        assert_eq!(first.public_key(), again.public_key());
        std::fs::write(&path, b"short").unwrap();
        assert!(load_or_create(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
