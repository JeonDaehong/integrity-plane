//! Server configuration (spec §26), read from TOML.
//!
//! Constraints declared here are imported into the registry when it is first created (ADR 0011);
//! afterwards they are managed through the integrity API.

use std::path::PathBuf;

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
    /// Certificate signing (RFC 0005); absent = certificates are not signed.
    #[serde(default)]
    pub signing: Option<SigningConfig>,
    /// Constraints imported into the registry on first start.
    #[serde(default, rename = "constraint")]
    pub constraints: Vec<ConstraintConfig>,
}

/// `[signing]` (RFC 0005).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningConfig {
    /// File holding the 32-byte Ed25519 seed; generated (owner-only) if it does not exist.
    pub key_file: PathBuf,
    /// `verify` treats unsigned snapshots and unknown keys as a broken chain.
    #[serde(default)]
    pub require: bool,
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
}

fn default_budget() -> u64 {
    2 << 30
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_inline_validation_bytes: default_budget(),
        }
    }
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
    fn environment_overrides_sections() {
        let c = Config::from_toml_with_env(
            BASE,
            env(&[
                ("INTEGRITY__UPSTREAM__CATALOG_URI", "http://b:8181"),
                ("INTEGRITY__UPSTREAM__TIMEOUT_SECS", "7"),
                ("INTEGRITY__ERRORS__REDACT_KEYS", "true"),
                ("INTEGRITY__LIMITS__MAX_INLINE_VALIDATION_BYTES", "1MiB"),
                ("INTEGRITY__SERVER__ADMIN_TOKEN", "s3cret"),
                ("PATH", "/usr/bin"),
            ]),
        )
        .unwrap();
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
