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
    /// Constraints imported into the registry on first start.
    #[serde(default, rename = "constraint")]
    pub constraints: Vec<ConstraintConfig>,
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
    #[serde(default = "default_budget")]
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

impl Config {
    /// Parses TOML.
    pub fn from_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}
