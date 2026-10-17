//! Server configuration (spec §26), read from TOML.
//!
//! Constraints declared here are imported into the registry when it is first created (ADR 0011);
//! afterwards they are managed through the integrity API.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Top-level configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listening socket.
    pub server: ServerConfig,
    /// The catalog writers no longer reach directly.
    pub upstream: UpstreamConfig,
    /// Where indexes (and later the transaction log) live.
    pub control_store: ControlStoreConfig,
    /// Inline validation limits.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Object storage access for reading manifests and data files.
    #[serde(default)]
    pub storage: StorageConfig,
    /// Error reporting.
    #[serde(default)]
    pub errors: ErrorsConfig,
    /// Transaction log retention (RFC 0006).
    #[serde(default)]
    pub txn_log: TxnLogConfig,
    /// Catching up from the table history (RFC 0011).
    #[serde(default)]
    pub catch_up: CatchUpConfig,
    /// Several Planes in front of the same tables (RFC 0012).
    #[serde(default)]
    pub ha: HaConfig,
    /// Certificate signing (RFC 0005); absent = certificates are not signed.
    #[serde(default)]
    pub signing: Option<SigningConfig>,
    /// Constraints imported into the registry on first start.
    #[serde(default, rename = "constraint")]
    pub constraints: Vec<ConstraintConfig>,
}

/// `[ha]` (RFC 0012).
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HaConfig {
    /// How commits are protected against other Planes.
    #[serde(default)]
    pub fence: Fence,
}

/// `[ha] fence`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Fence {
    /// Single-table commits: one Plane per domain.
    #[default]
    None,
    /// Multi-table transactions asserting every table of the domain.
    MultiTable,
}

/// `[catch_up]` (RFC 0011).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatchUpConfig {
    /// Follow certified snapshots the indexes have not seen instead of refusing them.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Most snapshots of one table caught up before a commit; more needs a rebuild.
    #[serde(default = "default_catch_up_snapshots")]
    pub max_snapshots: usize,
}

fn default_true() -> bool {
    true
}

fn default_catch_up_snapshots() -> usize {
    1_000
}

impl Default for CatchUpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_snapshots: default_catch_up_snapshots(),
        }
    }
}

/// `[signing]` (RFC 0005, backends RFC 0009).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningConfig {
    /// Where the key is held.
    #[serde(default)]
    pub backend: SigningBackend,
    /// `backend = "file"`: file holding the 32-byte Ed25519 seed; generated (owner-only) if it
    /// does not exist.
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    /// `verify` treats unsigned snapshots and unknown keys as a broken chain.
    #[serde(default)]
    pub require: bool,
    /// `backend = "vault-transit"`.
    #[serde(default)]
    pub vault: Option<VaultTransitConfig>,
    /// `backend = "pkcs11"`.
    #[serde(default)]
    pub pkcs11: Option<Pkcs11Config>,
}

/// Where the signing key is held (RFC 0009).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SigningBackend {
    /// A seed file in the control store (RFC 0005).
    #[default]
    File,
    /// A HashiCorp Vault Transit key.
    VaultTransit,
    /// A PKCS#11 token (an HSM).
    Pkcs11,
}

/// `[signing.vault]` (RFC 0009).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultTransitConfig {
    /// Vault's address, e.g. `https://vault.example:8200`.
    pub address: String,
    /// The Transit engine's mount path.
    #[serde(default = "default_transit_mount")]
    pub mount: String,
    /// The name of an `ed25519` Transit key.
    pub key: String,
    /// The key version to sign with; default: the latest version at start.
    #[serde(default)]
    pub key_version: Option<u64>,
    /// File holding the Vault token; default: the `VAULT_TOKEN` environment variable.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// Vault Enterprise namespace.
    #[serde(default)]
    pub namespace: Option<String>,
}

fn default_transit_mount() -> String {
    "transit".into()
}

/// `[signing.pkcs11]` (RFC 0009).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pkcs11Config {
    /// The token's PKCS#11 library.
    pub module: PathBuf,
    /// The label of the token holding the key.
    pub token_label: String,
    /// `CKA_LABEL` of the Ed25519 private key and of its public key.
    pub key_label: String,
    /// File holding the user PIN; default: the `INTEGRITY_PKCS11_PIN` environment variable.
    #[serde(default)]
    pub pin_file: Option<PathBuf>,
}

/// The settings of the configured signing backend.
#[derive(Debug, Clone, Copy)]
pub enum SignerSettings<'a> {
    /// `backend = "file"`.
    File(&'a std::path::Path),
    /// `backend = "vault-transit"`.
    VaultTransit(&'a VaultTransitConfig),
    /// `backend = "pkcs11"`.
    Pkcs11(&'a Pkcs11Config),
}

impl SigningConfig {
    /// The chosen backend's settings; settings of another backend are an error (RFC 0009).
    pub fn settings(&self) -> Result<SignerSettings<'_>, ConfigError> {
        let stray = |what: &str| {
            Err(ConfigError(format!(
                "signing.{what} is set but signing.backend is {:?}",
                self.backend
            )))
        };
        match self.backend {
            SigningBackend::File => {}
            _ if self.key_file.is_some() => return stray("key_file"),
            _ => {}
        }
        if self.vault.is_some() && self.backend != SigningBackend::VaultTransit {
            return stray("vault");
        }
        if self.pkcs11.is_some() && self.backend != SigningBackend::Pkcs11 {
            return stray("pkcs11");
        }
        let missing = |what: &str| ConfigError(format!("signing.{what} is required"));
        Ok(match self.backend {
            SigningBackend::File => SignerSettings::File(
                self.key_file
                    .as_deref()
                    .ok_or_else(|| missing("key_file"))?,
            ),
            SigningBackend::VaultTransit => {
                SignerSettings::VaultTransit(self.vault.as_ref().ok_or_else(|| missing("vault"))?)
            }
            SigningBackend::Pkcs11 => {
                SignerSettings::Pkcs11(self.pkcs11.as_ref().ok_or_else(|| missing("pkcs11"))?)
            }
        })
    }
}

/// `[errors]` (spec §24, §26).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorsConfig {
    /// Leave sample key values out of violation reports (keys can be personal data). They are
    /// then neither returned nor stored.
    #[serde(default)]
    pub redact_keys: bool,
}

/// `[server]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// e.g. `0.0.0.0:8181`.
    pub bind: String,
    /// If set, `/v1/integrity/*` requests other than `GET status` must send
    /// `Authorization: Bearer <admin_token>`.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Named integrity API tokens (ADR 0026): a request authenticated with one is attributed to
    /// its name. `admin_token` is the token named `admin`.
    #[serde(default)]
    pub api_tokens: Vec<ApiTokenConfig>,
}

/// `[[server.api_tokens]]` (ADR 0026): `token` or its hex SHA-256 `token_sha256`, not both.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiTokenConfig {
    /// Who holds the token, as audit events name them.
    pub name: String,
    /// The token.
    #[serde(default)]
    pub token: Option<String>,
    /// Hex SHA-256 of the token, so the file need not hold it.
    #[serde(default)]
    pub token_sha256: Option<String>,
}

/// SHA-256 of an integrity API token.
pub fn token_digest(token: &str) -> [u8; 32] {
    use sha2::Digest as _;
    sha2::Sha256::digest(token.as_bytes()).into()
}

fn parse_sha256_hex(text: &str) -> Option<[u8; 32]> {
    let text = text.trim();
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

impl ServerConfig {
    /// Every integrity API token as `(name, SHA-256)`: `admin_token` as `admin`, then
    /// `api_tokens`. Names are unique, 1–64 characters of `[A-Za-z0-9._@-]`, never `plane`.
    pub fn tokens(&self) -> Result<Vec<(String, [u8; 32])>, ConfigError> {
        let mut out = Vec::new();
        if let Some(t) = &self.admin_token {
            out.push(("admin".to_owned(), token_digest(t)));
        }
        for t in &self.api_tokens {
            let err = |why: &str| ConfigError(format!("server.api_tokens {:?}: {why}", t.name));
            let valid_name = (1..=64).contains(&t.name.len())
                && t.name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "._@-".contains(c));
            if !valid_name {
                return Err(err("name must be 1-64 characters of A-Z a-z 0-9 . _ @ -"));
            }
            if t.name == "plane" {
                return Err(err("\"plane\" names the Plane's own actions"));
            }
            if out.iter().any(|(n, _)| *n == t.name) {
                return Err(err("name used twice (admin_token is \"admin\")"));
            }
            let digest = match (&t.token, &t.token_sha256) {
                (Some(token), None) if !token.is_empty() => token_digest(token),
                (None, Some(hex)) => {
                    parse_sha256_hex(hex).ok_or_else(|| err("token_sha256 is not 64 hex digits"))?
                }
                _ => return Err(err("needs exactly one of token, token_sha256")),
            };
            out.push((t.name.clone(), digest));
        }
        Ok(out)
    }
}

/// `[upstream]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    /// Base URL of the upstream REST catalog, e.g. `http://iceberg-rest:8181`.
    pub catalog_uri: String,
    /// Request timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Catalog prefix (`/v1/{prefix}/…`) used by the integrity API to reach tables, if the
    /// upstream uses one.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Credentials the Plane uses for its own upstream requests (reading table metadata for
    /// validation, onboarding, rebuild, verify and recovery). Writers' own commits are forwarded
    /// with their own credentials.
    #[serde(default)]
    pub auth: Option<UpstreamAuth>,
}

/// `[upstream.auth]`: a static bearer token, or OAuth2 client credentials (e.g. Polaris).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "type")]
pub enum UpstreamAuth {
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The token.
        token: String,
    },
    /// Client-credentials grant against the catalog's token endpoint.
    #[serde(rename = "oauth2")]
    OAuth2 {
        /// Client id.
        client_id: String,
        /// Client secret.
        client_secret: String,
        /// Requested scope, e.g. `PRINCIPAL_ROLE:ALL`.
        #[serde(default)]
        scope: Option<String>,
        /// Token endpoint: absolute URL, or a path under the catalog URI. Default
        /// `/v1/oauth/tokens`.
        #[serde(default)]
        token_uri: Option<String>,
    },
}

fn default_timeout() -> u64 {
    60
}

/// `[control_store]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlStoreConfig {
    /// Directory.
    pub path: PathBuf,
}

/// `[limits]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    /// Maximum bytes read from storage to validate one commit.
    #[serde(default = "default_budget", deserialize_with = "byte_size")]
    pub max_inline_validation_bytes: u64,
    /// Bytes of keys an onboarding or rebuild scan keeps in memory per table, and a commit
    /// validation per commit (one per domain at a time); beyond it, sorted runs spill to the
    /// control store directory.
    #[serde(default = "default_scan_memory", deserialize_with = "byte_size")]
    pub scan_memory: u64,
    /// Commits that may wait for one domain's queue at once; more are refused with 429 (RFC
    /// 0007). `0`: no bound.
    #[serde(default = "default_max_queued_commits")]
    pub max_queued_commits: usize,
    /// Commits to constrained tables each client may send per second (bursts of as many); off
    /// when absent (RFC 0007).
    #[serde(default)]
    pub commits_per_client: Option<u32>,
    /// Header naming the client (first comma-separated value) instead of the peer address, e.g.
    /// `X-Forwarded-For`; only behind a proxy that sets it.
    #[serde(default)]
    pub client_header: Option<String>,
}

/// Default `limits.max_queued_commits`.
pub const DEFAULT_MAX_QUEUED_COMMITS: usize = 32;

fn default_max_queued_commits() -> usize {
    DEFAULT_MAX_QUEUED_COMMITS
}

fn default_budget() -> u64 {
    2 << 30
}

/// Default `limits.scan_memory`: 512 MiB.
pub const DEFAULT_SCAN_MEMORY: u64 = 512 << 20;

fn default_scan_memory() -> u64 {
    DEFAULT_SCAN_MEMORY
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_inline_validation_bytes: default_budget(),
            scan_memory: default_scan_memory(),
            max_queued_commits: default_max_queued_commits(),
            commits_per_client: None,
            client_header: None,
        }
    }
}

/// `[txn_log]` (RFC 0006).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxnLogConfig {
    /// How long a finished transaction (and its idempotency key) is kept, e.g. `"24h"`; at least
    /// `"30m"`, the advertised `idempotency-key-lifetime`.
    #[serde(default = "default_retention", deserialize_with = "retention")]
    pub retention: Duration,
    /// Writes per staged chunk in the log (RFC 0006); a tuning knob, at least 1.
    #[serde(default = "default_chunk_writes", deserialize_with = "chunk_writes")]
    pub chunk_writes: usize,
}

fn default_chunk_writes() -> usize {
    integrity_txn::log::DEFAULT_CHUNK_WRITES
}

fn chunk_writes<'de, D: serde::Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    let n = usize::deserialize(d)?;
    if n == 0 {
        return Err(serde::de::Error::custom(
            "txn_log.chunk_writes must be at least 1",
        ));
    }
    Ok(n)
}

fn default_retention() -> Duration {
    integrity_txn::log::DEFAULT_RETENTION
}

impl Default for TxnLogConfig {
    fn default() -> Self {
        Self {
            retention: default_retention(),
            chunk_writes: default_chunk_writes(),
        }
    }
}

fn retention<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(d)?;
    let retention = parse_duration(&text).map_err(serde::de::Error::custom)?;
    if retention < integrity_txn::log::MIN_RETENTION {
        return Err(serde::de::Error::custom(format!(
            "txn_log.retention {text:?} is shorter than 30m, the advertised idempotency-key-lifetime"
        )));
    }
    Ok(retention)
}

/// Parses `"90s"`, `"30m"`, `"24h"`, `"7d"`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let t = text.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("not a duration: {text:?}"))?;
    let factor: u64 = match unit.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        other => return Err(format!("unknown duration unit {other:?} in {text:?}")),
    };
    n.checked_mul(factor)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("duration too large: {text:?}"))
}

/// `[storage]`: S3-compatible object storage (credentials may also come from the environment).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Endpoint override (MinIO), e.g. `http://minio:9000`.
    pub endpoint: Option<String>,
    /// Region.
    pub region: Option<String>,
    /// Access key id.
    pub access_key_id: Option<String>,
    /// Secret access key.
    pub secret_access_key: Option<String>,
    /// Allow plain HTTP (local MinIO only).
    #[serde(default)]
    pub allow_http: bool,
}

/// One `[[constraint]]` entry (also the registry's stored form).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConstraintConfig {
    /// Unique, never reused.
    pub id: u64,
    /// Table identifier `namespace.table` (multi-level namespaces joined with `.`).
    pub table: String,
    /// User-facing name.
    pub name: String,
    /// `primary_key`, `unique`, `foreign_key` or `not_null`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Iceberg field ids, in key order (one for `not_null`).
    pub columns: Vec<i32>,
    /// UNIQUE: `distinct` (default) or `not_distinct`.
    #[serde(default)]
    pub nulls: Option<String>,
    /// FK: the referenced constraint (`table` and constraint `id`).
    #[serde(default)]
    pub references: Option<ReferenceConfig>,
    /// FK: `simple` (default) or `full`.
    #[serde(default, rename = "match")]
    pub match_mode: Option<String>,
    /// Column names at registration (informational; the Plane matches by field id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_names: Option<Vec<String>>,
}

/// FK target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceConfig {
    /// Parent table identifier.
    pub table: String,
    /// Parent PK/UNIQUE constraint id.
    pub constraint: u64,
}

/// A byte count: an integer, or a string such as `"2GiB"`, `"512 MiB"`, `"10MB"` (spec §26).
fn byte_size<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Int(u64),
        Text(String),
    }
    match Raw::deserialize(d)? {
        Raw::Int(n) => Ok(n),
        Raw::Text(t) => parse_byte_size(&t).map_err(serde::de::Error::custom),
    }
}

/// Parses `"2GiB"` and friends; binary (KiB, MiB, GiB, TiB) and decimal (KB, MB, GB, TB) units.
pub fn parse_byte_size(text: &str) -> Result<u64, String> {
    let t = text.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("not a byte size: {text:?}"))?;
    let factor: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        other => return Err(format!("unknown byte unit {other:?} in {text:?}")),
    };
    n.checked_mul(factor)
        .ok_or_else(|| format!("byte size too large: {text:?}"))
}

/// Why a configuration could not be loaded.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Parses TOML.
    pub fn from_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }

    /// Parses TOML, then applies `INTEGRITY__SECTION__KEY=value` overrides from `vars` (spec §26),
    /// e.g. `INTEGRITY__UPSTREAM__CATALOG_URI`. A value that parses as a TOML value (number,
    /// boolean, quoted string, array) is used as such, anything else as a string. Overrides can set
    /// keys of `[sections]`, not `[[constraint]]` entries.
    pub fn from_toml_with_env(
        text: &str,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let mut doc: toml::Table = toml::from_str(text).map_err(|e| ConfigError(e.to_string()))?;
        for (name, raw) in vars {
            let Some(path) = name.strip_prefix("INTEGRITY__") else {
                continue;
            };
            let parts: Vec<String> = path.split("__").map(str::to_ascii_lowercase).collect();
            let [section, key] = parts.as_slice() else {
                return Err(ConfigError(format!(
                    "{name}: expected INTEGRITY__SECTION__KEY"
                )));
            };
            if section == "constraint" {
                return Err(ConfigError(format!(
                    "{name}: constraints cannot be set from the environment"
                )));
            }
            let value = format!("v = {raw}")
                .parse::<toml::Table>()
                .ok()
                .and_then(|mut t| t.remove("v"))
                .unwrap_or(toml::Value::String(raw));
            let table = doc
                .entry(section.clone())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
            let toml::Value::Table(table) = table else {
                return Err(ConfigError(format!("{name}: [{section}] is not a section")));
            };
            table.insert(key.clone(), value);
        }
        Config::deserialize(doc).map_err(|e| ConfigError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
[server]
bind = "127.0.0.1:8181"
[upstream]
catalog_uri = "http://a:8181"
[control_store]
path = "/tmp/x"
[limits]
max_inline_validation_bytes = "2GiB"
"#;

    /// `BASE` with an `admin_token` and `[[server.api_tokens]]` entries.
    fn with_tokens(entries: &str) -> Result<Vec<(String, [u8; 32])>, String> {
        let text = BASE.replace(
            "bind = \"127.0.0.1:8181\"",
            "bind = \"127.0.0.1:8181\"\nadmin_token = \"shared\"",
        ) + entries;
        Config::from_toml(&text)
            .map_err(|e| e.to_string())?
            .server
            .tokens()
            .map_err(|e| e.0)
    }

    #[test]
    fn api_tokens_have_names_and_one_secret_each() {
        // Any 64 hex digits (the digest of whatever bob holds).
        let bob = "4fd0c7d2b3d9ff06a8fa3e7a3ba7ff5b0a2b8c3c5f4e1d2b1d6a0e7f9c8b7a6d";
        let tokens = with_tokens(&format!(
            "[[server.api_tokens]]\nname = \"alice\"\ntoken = \"alice-token\"\n\
             [[server.api_tokens]]\nname = \"bob\"\ntoken_sha256 = \"{bob}\"\n"
        ))
        .unwrap();
        let names: Vec<&str> = tokens.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["admin", "alice", "bob"]);
        assert_eq!(tokens[0].1, token_digest("shared"));
        assert_eq!(tokens[1].1, token_digest("alice-token"));
        assert_eq!(tokens[2].1, parse_sha256_hex(bob).unwrap());
        // The digest operators compute with `sha256sum`.
        assert_eq!(
            token_digest("abc"),
            parse_sha256_hex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                .unwrap()
        );

        for (entry, why) in [
            ("name = \"admin\"\ntoken = \"x\"", "name used twice"),
            ("name = \"plane\"\ntoken = \"x\"", "Plane's own"),
            ("name = \"a b\"\ntoken = \"x\"", "name must be"),
            ("name = \"\"\ntoken = \"x\"", "name must be"),
            ("name = \"c\"", "exactly one"),
            ("name = \"c\"\ntoken = \"\"", "exactly one"),
            (
                "name = \"c\"\ntoken = \"x\"\ntoken_sha256 = \"00\"",
                "exactly one",
            ),
            ("name = \"c\"\ntoken_sha256 = \"zz\"", "64 hex"),
        ] {
            let e = with_tokens(&format!("[[server.api_tokens]]\n{entry}\n")).unwrap_err();
            assert!(e.contains(why), "{entry}: {e}");
        }
        let twice = "[[server.api_tokens]]\nname = \"c\"\ntoken = \"x\"\n";
        let e = with_tokens(&format!("{twice}{twice}")).unwrap_err();
        assert!(e.contains("name used twice"), "{e}");
    }

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn shipped_configurations_parse() {
        for (name, text) in [
            (
                "deploy/integrity.example.toml",
                include_str!("../../../deploy/integrity.example.toml"),
            ),
            (
                "deploy/integrity.compose.toml",
                include_str!("../../../deploy/integrity.compose.toml"),
            ),
            (
                "compat/integrity.toml",
                include_str!("../../../compat/integrity.toml"),
            ),
            (
                "compat/integrity.polaris.toml",
                include_str!("../../../compat/integrity.polaris.toml"),
            ),
            (
                "compat/integrity.lakekeeper.toml",
                include_str!("../../../compat/integrity.lakekeeper.toml"),
            ),
            (
                "compat/integrity.nessie.toml",
                include_str!("../../../compat/integrity.nessie.toml"),
            ),
        ] {
            if let Err(e) = Config::from_toml(text) {
                panic!("{name}: {e}");
            }
        }
    }

    #[test]
    fn signing_backends_take_only_their_own_settings() {
        let base = "[server]\nbind = \"0.0.0.0:8181\"\n[upstream]\ncatalog_uri = \"http://c\"\n\
                    [control_store]\npath = \"/tmp/x\"\n";
        let settings = |signing: &str| -> Result<String, String> {
            let c = Config::from_toml(&format!("{base}{signing}")).map_err(|e| e.to_string())?;
            let s = c.signing.unwrap();
            match s.settings().map_err(|e| e.0)? {
                SignerSettings::File(p) => Ok(format!("file {}", p.display())),
                SignerSettings::VaultTransit(v) => Ok(format!(
                    "vault {} {} {} {:?}",
                    v.address, v.mount, v.key, v.key_version
                )),
                SignerSettings::Pkcs11(p) => {
                    Ok(format!("pkcs11 {} {}", p.token_label, p.key_label))
                }
            }
        };
        // RFC 0005's form keeps its meaning.
        assert_eq!(
            settings("[signing]\nkey_file = \"k\"\nrequire = true\n").unwrap(),
            "file k"
        );
        assert_eq!(
            settings(
                "[signing]\nbackend = \"vault-transit\"\n[signing.vault]\naddress = \"https://v\"\nkey = \"oip\"\n"
            )
            .unwrap(),
            "vault https://v transit oip None"
        );
        assert_eq!(
            settings(
                "[signing]\nbackend = \"pkcs11\"\n[signing.pkcs11]\nmodule = \"/m.so\"\ntoken_label = \"t\"\nkey_label = \"k\"\n"
            )
            .unwrap(),
            "pkcs11 t k"
        );
        for (bad, expect) in [
            (
                "[signing]\nrequire = true\n",
                "signing.key_file is required",
            ),
            (
                "[signing]\nbackend = \"vault-transit\"\nkey_file = \"k\"\n",
                "signing.key_file is set",
            ),
            (
                "[signing]\nkey_file = \"k\"\n[signing.vault]\naddress = \"a\"\nkey = \"k\"\n",
                "signing.vault is set",
            ),
            (
                "[signing]\nbackend = \"pkcs11\"\n",
                "signing.pkcs11 is required",
            ),
            ("[signing]\nbackend = \"kms\"\n", "unknown variant"),
            (
                "[signing]\nbackend = \"vault-transit\"\n[signing.vault]\naddress = \"a\"\nkey = \"k\"\ntoken = \"t\"\n",
                "unknown field",
            ),
        ] {
            let e = settings(bad).unwrap_err();
            assert!(e.contains(expect), "{bad}: {e}");
        }
    }

    #[test]
    fn byte_sizes_accept_units() {
        assert_eq!(parse_byte_size("2GiB").unwrap(), 2 << 30);
        assert_eq!(parse_byte_size("512 MiB").unwrap(), 512 << 20);
        assert_eq!(parse_byte_size("10MB").unwrap(), 10_000_000);
        assert_eq!(parse_byte_size("42").unwrap(), 42);
        assert!(parse_byte_size("2 parsecs").is_err());
        assert!(parse_byte_size("GiB").is_err());
        let c = Config::from_toml(BASE).unwrap();
        assert_eq!(c.limits.max_inline_validation_bytes, 2 << 30);
    }

    #[test]
    fn txn_log_retention_is_a_duration_of_at_least_30_minutes() {
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1_800));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
        assert!(parse_duration("7").is_err());
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("1w").is_err());
        let c = Config::from_toml(BASE).unwrap();
        assert_eq!(c.txn_log.retention, Duration::from_secs(86_400));
        let c = Config::from_toml(&format!("{BASE}\n[txn_log]\nretention = \"7d\"\n")).unwrap();
        assert_eq!(c.txn_log.retention, Duration::from_secs(604_800));
        let short = Config::from_toml(&format!("{BASE}\n[txn_log]\nretention = \"29m\"\n"));
        assert!(short.unwrap_err().to_string().contains("shorter than 30m"));
        assert_eq!(c.txn_log.chunk_writes, 100_000);
        let zero = Config::from_toml(&format!(
            "{BASE}
[txn_log]
chunk_writes = 0
"
        ));
        assert!(zero.is_err());
    }

    #[test]
    fn environment_overrides_sections() {
        let c = Config::from_toml_with_env(
            BASE,
            env(&[
                ("INTEGRITY__UPSTREAM__CATALOG_URI", "http://b:8181"),
                ("INTEGRITY__UPSTREAM__TIMEOUT_SECS", "7"),
                ("INTEGRITY__ERRORS__REDACT_KEYS", "true"),
                ("INTEGRITY__LIMITS__MAX_INLINE_VALIDATION_BYTES", "1MiB"),
                ("INTEGRITY__SERVER__ADMIN_TOKEN", "s3cret"),
                ("INTEGRITY__HA__FENCE", "multi-table"),
                ("PATH", "/usr/bin"),
            ]),
        )
        .unwrap();
        assert_eq!(c.ha.fence, Fence::MultiTable);
        assert_eq!(c.upstream.catalog_uri, "http://b:8181");
        assert_eq!(c.upstream.timeout_secs, 7);
        assert!(c.errors.redact_keys);
        assert_eq!(c.limits.max_inline_validation_bytes, 1 << 20);
        assert_eq!(c.server.admin_token.as_deref(), Some("s3cret"));
    }

    #[test]
    fn bad_overrides_are_errors() {
        for (k, v) in [
            ("INTEGRITY__UPSTREAM", "x"),
            ("INTEGRITY__CONSTRAINT__ID", "1"),
            ("INTEGRITY__UPSTREAM__NO_SUCH_KEY", "1"),
            ("INTEGRITY__UPSTREAM__TIMEOUT_SECS", "soon"),
        ] {
            assert!(
                Config::from_toml_with_env(BASE, env(&[(k, v)])).is_err(),
                "{k}={v} should be refused"
            );
        }
    }
}
