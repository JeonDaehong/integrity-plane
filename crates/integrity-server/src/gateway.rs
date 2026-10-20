//! The REST gateway (spec §14, §16, §22, ADR 0010, RFC 0003, RFC 0004).
//!
//! Commits to tables with constraints go through the integrity pipeline, serialized per integrity
//! domain (the FK-connected component of the table, spec §11) and recorded in the transaction log so
//! that a crash at any point is recoverable. Commits in different domains run in parallel. Every
//! other request is forwarded to the upstream catalog unchanged.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use integrity_core::{
    CertificateInput, Digest, certificate, constraint_set_digest, parse_table_uuid,
};
use integrity_iceberg::{
    Classification, CommitRequest, FileIo, Rejection, TableMetadata, check_requirements, classify,
    inject_certificate, snapshot_certificate,
};
use integrity_index::{IndexKind, KeyIndex, PersistentIndex, PersistentStore, TableHead};
use integrity_txn::{
    Decision as TxnDecision, FaultPoint, Prepared, StagedEntry, TxnId, TxnLog, TxnState, Validated,
    fault,
};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, SnapshotId};
use integrity_validator::Validator;
use serde_json::Value;
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, RwLock};

use crate::config::ConstraintConfig;
use crate::error::ApiError;
use crate::metrics::Metrics;
use crate::pipeline::{self, Job, Outcome};
use crate::registry;
use crate::report;
use crate::store::{Anchor, AuditEvent, Registry, RegistryDoc, StoreError};

mod catchup;
mod fence;

/// A table with data this Plane has not anchored and cannot follow from its first snapshot.
fn not_onboarded(identifier: &str, why: Option<&str>) -> ApiError {
    let why = why.map(|w| format!(" ({w})")).unwrap_or_default();
    ApiError::new(
        ErrorCode::IndexDegraded,
        format!(
            "{identifier} already has data{why}; register its constraints through the integrity API to onboard it"
        ),
    )
}

/// A table commit route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePath {
    /// `/v1` or `/v1/{prefix}`.
    pub base: String,
    /// Namespace levels.
    pub namespace: Vec<String>,
    /// Table name.
    pub table: String,
}

impl TablePath {
    /// `ns1.ns2.table`.
    pub fn identifier(&self) -> String {
        let mut parts = self.namespace.clone();
        parts.push(self.table.clone());
        parts.join(".")
    }

    /// The REST path of another table under the same prefix.
    pub fn sibling(&self, identifier: &str) -> Option<String> {
        let (ns, table) = identifier.rsplit_once('.')?;
        let ns: Vec<String> = ns.split('.').map(encode).collect();
        Some(format!(
            "{}/namespaces/{}/tables/{}",
            self.base,
            ns.join("%1F"),
            encode(table)
        ))
    }

    /// The REST path of `identifier` under `base` (`/v1` or `/v1/{prefix}`).
    pub fn of(base: &str, identifier: &str) -> Option<Self> {
        let (ns, table) = identifier.rsplit_once('.')?;
        Some(Self {
            base: base.to_owned(),
            namespace: ns.split('.').map(str::to_owned).collect(),
            table: table.to_owned(),
        })
    }

    /// This table's REST path.
    pub fn path(&self) -> String {
        let ns: Vec<String> = self.namespace.iter().map(|n| encode(n)).collect();
        format!(
            "{}/namespaces/{}/tables/{}",
            self.base,
            ns.join("%1F"),
            encode(&self.table)
        )
    }
}

fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// What a request is, from its method and path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// `GET /v1/config`.
    Config,
    /// `POST …/namespaces/{ns}/tables/{table}`: a table commit.
    Commit(TablePath),
    /// `POST …/transactions/commit`: multi-table commit.
    MultiTableCommit,
    /// Anything else: forwarded unchanged.
    Proxy,
}

/// True if forwarding `path` could reach another resource than the one it routes as: the upstream
/// URL is parsed per WHATWG, which resolves `.`/`..` segments (also as `%2e`) and reads `\` as `/`.
pub fn ambiguous_path(path: &str) -> bool {
    path.contains('\\')
        || path.split('/').any(|seg| {
            let seg = seg.to_ascii_lowercase().replace("%2e", ".");
            seg == "." || seg == ".."
        })
}

/// Classifies a request.
pub fn route(method: &Method, path: &str) -> Route {
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    if segs.len() < 2 || !segs[0].is_empty() || segs[1] != "v1" {
        return Route::Proxy;
    }
    if *method == Method::GET && segs.len() == 3 && segs[2] == "config" {
        return Route::Config;
    }
    if *method != Method::POST {
        return Route::Proxy;
    }
    if segs.ends_with(&["transactions", "commit"]) {
        return Route::MultiTableCommit;
    }
    let (base, rest) = match segs.get(2) {
        Some(&"namespaces") => ("/v1".to_owned(), &segs[2..]),
        Some(prefix) if segs.get(3) == Some(&"namespaces") => (format!("/v1/{prefix}"), &segs[3..]),
        _ => return Route::Proxy,
    };
    match rest {
        ["namespaces", ns, "tables", table] => {
            let (Some(ns), Some(table)) = (decode(ns), decode(table)) else {
                return Route::Proxy;
            };
            Route::Commit(TablePath {
                base,
                namespace: ns.split('\u{1f}').map(str::to_owned).collect(),
                table,
            })
        }
        _ => Route::Proxy,
    }
}

const HOP_BY_HOP: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "upgrade",
    "accept-encoding",
];

fn forwardable(name: &HeaderName) -> bool {
    !HOP_BY_HOP.contains(&name.as_str())
}

/// An upstream response, read completely so it can be both returned and recorded.
#[derive(Debug, Clone)]
struct Captured {
    status: u16,
    headers: Vec<(HeaderName, axum::http::HeaderValue)>,
    body: Bytes,
}

impl Captured {
    async fn read(resp: reqwest::Response) -> Result<Self, reqwest::Error> {
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .filter(|(n, _)| forwardable(n))
            .map(|(n, v)| (n.clone(), v.clone()))
            .collect();
        let body = resp.bytes().await?;
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    fn response(&self) -> Response {
        let mut out = Response::builder().status(self.status);
        for (name, value) in &self.headers {
            out = out.header(name, value);
        }
        out.body(Body::from(self.body.clone()))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
    }

    fn decision(&self) -> TxnDecision {
        TxnDecision {
            status: self.status,
            body: serde_json::from_slice(&self.body).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&self.body).into_owned())
            }),
        }
    }
}

fn error_decision(e: &ApiError) -> TxnDecision {
    TxnDecision {
        status: e.status().as_u16(),
        body: serde_json::json!({
            "error": {
                "message": format!("{} {}: {}", e.code.code(), e.code.name(), e.message),
                "type": e.error_type(),
                "code": e.status().as_u16(),
                "stack": [],
            }
        }),
    }
}

fn replay(d: &TxnDecision) -> Response {
    let status = StatusCode::from_u16(d.status).unwrap_or(StatusCode::BAD_GATEWAY);
    (status, axum::Json(d.body.clone())).into_response()
}

fn log_error(e: integrity_txn::TxnError) -> ApiError {
    ApiError::new(ErrorCode::IndexDegraded, format!("transaction log: {e}"))
}

/// One integrity domain's commit queue (spec §11). Holding its lock is holding the queue:
/// validation, publication and index application of one commit at a time.
#[derive(Debug, Default)]
struct DomainQueue {
    lock: Arc<Mutex<()>>,
    /// Commits waiting for the lock (RFC 0007 bounds them).
    waiting: AtomicUsize,
}

type Queue = Arc<DomainQueue>;

/// A commit counted among a queue's waiting commits until dropped.
struct Waiting<'q>(&'q AtomicUsize);

impl<'q> Waiting<'q> {
    /// Counts one more waiting commit, unless `max` (non-zero) already wait.
    fn enter(waiting: &'q AtomicUsize, max: usize) -> Option<Self> {
        let before = waiting.fetch_add(1, Ordering::AcqRel);
        if max != 0 && before >= max {
            waiting.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Self(waiting))
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Request limits (RFC 0007).
#[derive(Debug)]
pub(crate) struct Limits {
    /// Commits that may wait for one domain's queue; `0`: no bound.
    max_queued_commits: usize,
    /// Commit rate per client, if limited.
    commit_rate: Option<std::sync::Mutex<crate::limits::TokenBuckets>>,
    /// Header naming the client instead of the peer address.
    client_header: Option<HeaderName>,
    /// Failed integrity API authentications per client.
    pub(crate) auth_failures: std::sync::Mutex<crate::limits::AuthFailures>,
    /// Held by the one `verify` that may run.
    pub(crate) verifying: tokio::sync::Semaphore,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_queued_commits: crate::config::DEFAULT_MAX_QUEUED_COMMITS,
            commit_rate: None,
            client_header: None,
            auth_failures: std::sync::Mutex::default(),
            verifying: tokio::sync::Semaphore::new(1),
        }
    }
}

/// The gateway.
pub struct Gateway {
    http: reqwest::Client,
    upstream: String,
    pub(crate) io: Arc<dyn FileIo + Send + Sync>,
    pub(crate) store: PersistentStore,
    pub(crate) log: TxnLog,
    budget: u64,
    pub(crate) registry: Registry,
    /// `/v1` or `/v1/{prefix}`: where the integrity API finds tables upstream.
    pub(crate) base: String,
    /// Integrity API tokens as `(name, SHA-256)` (ADR 0026); none: the API is open.
    pub(crate) tokens: Vec<(String, [u8; 32])>,
    /// Credentials for the Plane's own upstream requests.
    upstream_auth: Option<crate::config::UpstreamAuth>,
    /// Cached OAuth2 access token and when to refresh it.
    upstream_token: Mutex<Option<(String, std::time::Instant)>>,
    /// Leave key values out of violation reports.
    pub(crate) redact_keys: bool,
    /// Bytes of keys a scan (per table) or a commit validation keeps in memory.
    pub(crate) scan_memory: usize,
    /// Certificate signer (RFC 0005), if signing is enabled.
    pub(crate) signer: Option<crate::signing::Signer>,
    /// `verify` requires signatures.
    pub(crate) require_signatures: bool,
    /// Held shared by every commit request, exclusively by constraint changes and rebuilds
    /// (ADR 0011).
    /// Held shared by every commit (until it is finished, ADR 0022) and exclusively by constraint
    /// changes (ADR 0011).
    pub(crate) admin: Arc<RwLock<()>>,
    /// One queue per integrity domain, keyed by the domain's smallest table identifier.
    domains: std::sync::Mutex<BTreeMap<String, Queue>>,
    /// Tables degraded by a failed index apply in this process (recovery completes it).
    pub(crate) transient: std::sync::Mutex<BTreeMap<String, String>>,
    /// Accepted commits answered and still being finished (ADR 0022): unresolved in the log, but
    /// not waiting for recovery.
    finishing: std::sync::Mutex<BTreeSet<u64>>,
    /// Commit requests received per table identifier (observability; detects retry storms).
    commit_requests: std::sync::Mutex<BTreeMap<String, u64>>,
    /// Prometheus metrics.
    pub(crate) metrics: Metrics,
    /// Request limits (RFC 0007).
    pub(crate) limits: Limits,
    /// Catching up from the table history (RFC 0011).
    pub(crate) catch_up: crate::config::CatchUpConfig,
    /// Forward commits as multi-table transactions asserting the domain (RFC 0012).
    pub(crate) fence: bool,
    /// Locations readable besides each table's own (RFC 0013).
    pub(crate) allowed_locations: Vec<String>,
}

pub(crate) enum Loaded {
    Table(Box<TableMetadata>, Value),
    Missing,
}

impl Gateway {
    /// A gateway forwarding to `upstream`.
    pub fn new(
        upstream: &str,
        timeout: std::time::Duration,
        io: Arc<dyn FileIo + Send + Sync>,
        store: PersistentStore,
        log: TxnLog,
        budget: u64,
        registry: Registry,
    ) -> Result<Self, reqwest::Error> {
        let gateway = Self {
            http: reqwest::Client::builder().timeout(timeout).build()?,
            upstream: upstream.trim_end_matches('/').to_owned(),
            io,
            store,
            log,
            budget,
            registry,
            base: "/v1".to_owned(),
            tokens: Vec::new(),
            upstream_auth: None,
            upstream_token: Mutex::new(None),
            redact_keys: false,
            scan_memory: crate::config::DEFAULT_SCAN_MEMORY as usize,
            signer: None,
            require_signatures: false,
            admin: Arc::new(RwLock::new(())),
            domains: std::sync::Mutex::new(BTreeMap::new()),
            transient: std::sync::Mutex::new(BTreeMap::new()),
            finishing: std::sync::Mutex::new(BTreeSet::new()),
            commit_requests: std::sync::Mutex::new(BTreeMap::new()),
            metrics: Metrics::default(),
            limits: Limits::default(),
            catch_up: crate::config::CatchUpConfig::default(),
            fence: false,
            allowed_locations: Vec::new(),
        };
        gateway.check_control_store();
        Ok(gateway)
    }

    /// Fails closed when a control-store file was lost and recreated (ADR 0011): an empty index
    /// store would accept duplicates, and a fresh transaction log would forget commits whose
    /// index changes were never applied. Every enforced table is degraded until rebuilt.
    fn check_control_store(&self) {
        let doc = self.registry.snapshot();
        let mut reasons = Vec::new();
        match &doc.index_store {
            Some(id) if id != self.store.id() => reasons.push(
                "the index store was replaced since the indexes were built; rebuild every domain",
            ),
            _ => {}
        }
        match self.registry.max_audited_txn() {
            Ok(Some(t)) if t >= self.log.next_txn_id() => reasons.push(
                "the transaction log is missing transactions recorded in the audit log; rebuild every domain",
            ),
            Ok(_) => {}
            Err(_) => reasons.push("the audit log cannot be read"),
        }
        let store_id = self.store.id().to_owned();
        let updated = self.registry.update(|d| {
            d.index_store = Some(store_id.clone());
            if let Some(reason) = reasons.first() {
                let tables: Vec<String> = d.anchors.keys().cloned().collect();
                for t in tables {
                    d.degraded.insert(t, (*reason).to_owned());
                }
            }
            Ok::<_, StoreError>(())
        });
        if let Some(reason) = reasons.first() {
            tracing::error!("{reason}");
            let mut event = AuditEvent::new("DOMAIN_DEGRADED", None);
            event.detail = Some((*reason).to_owned());
            self.audit(event);
        }
        if updated.is_err() {
            // Cannot record it: refuse everything in this process instead.
            let doc = self.registry.snapshot();
            if let Ok(mut t) = self.transient.lock() {
                for table in doc.anchors.keys() {
                    t.insert(table.clone(), "registry unwritable at start".into());
                }
            }
        }
    }

    /// Sets the upstream catalog prefix used by the integrity API.
    pub fn with_prefix(mut self, prefix: Option<&str>) -> Self {
        self.base = match prefix {
            Some(p) if !p.is_empty() => format!("/v1/{p}"),
            _ => "/v1".to_owned(),
        };
        self
    }

    /// Requires `Authorization: Bearer <token>` on the integrity API (except `status`); the token
    /// is named `admin` in audit events (ADR 0026). Replaces any token named `admin`.
    pub fn with_admin_token(mut self, token: Option<String>) -> Self {
        self.tokens.retain(|(name, _)| name != "admin");
        if let Some(t) = token {
            self.tokens
                .insert(0, ("admin".to_owned(), crate::config::token_digest(&t)));
        }
        self
    }

    /// Integrity API tokens as `(name, SHA-256)` (`ServerConfig::tokens`, ADR 0026); replaces
    /// every token set before.
    pub fn with_api_tokens(mut self, tokens: Vec<(String, [u8; 32])>) -> Self {
        self.tokens = tokens;
        self
    }

    /// The name of the token `headers` authenticate with, if any. Every entry is compared, so the
    /// time taken does not say which one matched.
    pub(crate) fn token_name(&self, headers: &HeaderMap) -> Option<String> {
        let given = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))?;
        let digest = crate::config::token_digest(given);
        let mut found = None;
        for (name, expected) in &self.tokens {
            let same = expected
                .iter()
                .zip(&digest)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
            if same && found.is_none() {
                found = Some(name.clone());
            }
        }
        found
    }

    /// Credentials for the Plane's own upstream requests (`[upstream.auth]`).
    pub fn with_upstream_auth(mut self, auth: Option<crate::config::UpstreamAuth>) -> Self {
        self.upstream_auth = auth;
        self
    }

    /// Headers for a request the Plane makes on its own behalf. With `[upstream.auth]`, its
    /// credentials; otherwise the caller's headers (a writer's own, for requests made while
    /// handling its commit), or none.
    async fn upstream_headers(&self, caller: &HeaderMap) -> Result<HeaderMap, ApiError> {
        use crate::config::UpstreamAuth;
        let token = match &self.upstream_auth {
            None => return Ok(caller.clone()),
            Some(UpstreamAuth::Bearer { token }) => token.clone(),
            Some(UpstreamAuth::OAuth2 {
                client_id,
                client_secret,
                scope,
                token_uri,
            }) => {
                let mut cached = self.upstream_token.lock().await;
                match cached.as_ref() {
                    Some((t, until)) if std::time::Instant::now() < *until => t.clone(),
                    _ => {
                        let (t, ttl) = self
                            .fetch_token(
                                client_id,
                                client_secret,
                                scope.as_deref(),
                                token_uri.as_deref(),
                            )
                            .await?;
                        let refresh = ttl.saturating_sub(60).max(30);
                        *cached = Some((
                            t.clone(),
                            std::time::Instant::now() + std::time::Duration::from_secs(refresh),
                        ));
                        t
                    }
                }
            }
        };
        let mut out = HeaderMap::new();
        let value =
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                ApiError::new(
                    ErrorCode::RecoveryRequired,
                    "upstream token is not a valid header",
                )
            })?;
        out.insert(axum::http::header::AUTHORIZATION, value);
        Ok(out)
    }

    /// OAuth2 client-credentials grant; returns the token and its lifetime in seconds.
    async fn fetch_token(
        &self,
        client_id: &str,
        client_secret: &str,
        scope: Option<&str>,
        token_uri: Option<&str>,
    ) -> Result<(String, u64), ApiError> {
        let uri = match token_uri {
            Some(u) if u.starts_with("http://") || u.starts_with("https://") => u.to_owned(),
            Some(path) => format!("{}{path}", self.upstream),
            None => format!("{}/v1/oauth/tokens", self.upstream),
        };
        let mut form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}",
            encode(client_id),
            encode(client_secret)
        );
        if let Some(s) = scope {
            form.push_str(&format!("&scope={}", encode(s)));
        }
        let failed = |m: String| ApiError::new(ErrorCode::RecoveryRequired, m);
        let resp = self
            .http
            .post(&uri)
            .header(
                axum::http::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(form)
            .send()
            .await
            .map_err(|e| failed(format!("upstream token endpoint unreachable: {e}")))?;
        if !resp.status().is_success() {
            return Err(failed(format!(
                "upstream token endpoint answered {}",
                resp.status()
            )));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| failed(format!("bad token response: {e}")))?;
        let token = body["access_token"]
            .as_str()
            .ok_or_else(|| failed("token response without access_token".into()))?;
        Ok((
            token.to_owned(),
            body["expires_in"].as_u64().unwrap_or(3600),
        ))
    }

    /// Lets the Plane read `locations` besides each table's own (RFC 0013).
    pub fn with_allowed_locations(mut self, locations: Vec<String>) -> Self {
        self.allowed_locations = locations;
        self
    }

    /// Forwards commits as multi-table transactions that assert every table of the domain
    /// (RFC 0012, `[ha] fence = "multi-table"`).
    pub fn with_fence(mut self, fence: bool) -> Self {
        self.fence = fence;
        self
    }

    /// Catches up from the table history as `config` says (RFC 0011).
    pub fn with_catch_up(mut self, config: crate::config::CatchUpConfig) -> Self {
        self.catch_up = config;
        self
    }

    /// Signs certificates with `signer` (RFC 0005, RFC 0009) and records its public key in the
    /// registry; `require` makes `verify` treat unsigned snapshots as a broken chain.
    pub fn with_signer(mut self, signer: Option<crate::signing::Signer>, require: bool) -> Self {
        if let Some(s) = &signer {
            let id = s.key_id().to_owned();
            let public = integrity_core::signature::public_key_hex(&s.public_key());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            let recorded = self.registry.update(|d| {
                d.keys.entry(id.clone()).or_insert(crate::store::KeyRecord {
                    public_key: public.clone(),
                    first_used_ms: now,
                });
                d.active_key = Some(id.clone());
                Ok::<_, StoreError>(())
            });
            if let Err(e) = recorded {
                tracing::error!("cannot record the signing key: {e}");
            }
        }
        self.signer = signer;
        self.require_signatures = require;
        self
    }

    /// `GET /v1/integrity/keys`: every public key the Plane has signed with (RFC 0005).
    pub fn keys(&self) -> Value {
        let doc = self.registry.snapshot();
        let keys: Vec<Value> = doc
            .keys
            .iter()
            .map(|(id, k)| {
                serde_json::json!({
                    "key_id": id,
                    "algorithm": "ed25519",
                    "public_key": k.public_key,
                    "active": doc.active_key.as_deref() == Some(id.as_str()),
                    "first_used_ms": k.first_used_ms,
                })
            })
            .collect();
        serde_json::json!({ "keys": keys })
    }

    /// Bytes of keys an onboarding or rebuild scan keeps in memory per table, and a commit
    /// validation per commit, before spilling sorted runs to disk (`limits.scan_memory`).
    pub fn with_scan_memory(mut self, bytes: u64) -> Self {
        self.scan_memory = usize::try_from(bytes).unwrap_or(usize::MAX);
        self
    }

    /// Request limits (RFC 0007): commits that may wait for one domain (`0`: no bound), commits
    /// per client per second, and the header naming clients instead of the peer address.
    pub fn with_request_limits(
        mut self,
        max_queued_commits: usize,
        commits_per_client: Option<u32>,
        client_header: Option<HeaderName>,
    ) -> Self {
        self.limits.max_queued_commits = max_queued_commits;
        self.limits.commit_rate = commits_per_client
            .filter(|n| *n > 0)
            .map(|n| std::sync::Mutex::new(crate::limits::TokenBuckets::new(n)));
        self.limits.client_header = client_header;
        self
    }

    /// Who sent a request (RFC 0007).
    pub(crate) fn client(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> String {
        crate::limits::client_key(headers, peer, self.limits.client_header.as_ref())
    }

    /// Takes a commit token of `client`, or refuses the commit (RFC 0007).
    fn take_commit_token(&self, client: &str) -> Result<(), ApiError> {
        let Some(rate) = &self.limits.commit_rate else {
            return Ok(());
        };
        let taken = match rate.lock() {
            Ok(mut b) => b.take(client, Instant::now()),
            Err(p) => p.into_inner().take(client, Instant::now()),
        };
        taken.map_err(|seconds| {
            self.metrics.limited_client.fetch_add(1, Ordering::Relaxed);
            ApiError::rate_limited("commit rate of this client exceeded", seconds)
        })
    }

    /// Leaves sample key values out of violation reports (`errors.redact_keys`).
    pub fn with_redact_keys(mut self, redact: bool) -> Self {
        self.redact_keys = redact;
        self
    }

    /// Appends an audit event; a failure is logged, never turned into a decision.
    pub(crate) fn audit(&self, mut event: AuditEvent) {
        if event.actor.is_none() {
            event.actor = Some("plane".to_owned());
            event.authenticated = true;
        }
        if let Err(e) = self.registry.audit(event) {
            tracing::warn!("audit write failed: {e}");
        }
    }

    /// Forgets every domain queue; only called under the exclusive admin lock, when no commit
    /// holds a queue and domain membership may just have changed.
    pub(crate) fn reset_domains(&self) {
        match self.domains.lock() {
            Ok(mut d) => d.clear(),
            Err(p) => p.into_inner().clear(),
        }
    }

    /// Why the domain of `members` refuses commits, if it does.
    fn degraded(&self, doc: &RegistryDoc, members: &BTreeSet<String>) -> Option<String> {
        let transient = match self.transient.lock() {
            Ok(t) => t.clone(),
            Err(p) => p.into_inner().clone(),
        };
        members
            .iter()
            .find_map(|m| doc.degraded.get(m).or_else(|| transient.get(m)).cloned())
    }

    fn mark_transient(&self, identifier: &str, reason: &str) {
        if let Ok(mut t) = self.transient.lock() {
            t.insert(identifier.to_owned(), reason.to_owned());
        }
        let mut e = AuditEvent::new("DOMAIN_DEGRADED", Some(identifier));
        e.detail = Some(reason.to_owned());
        self.audit(e);
    }

    /// Serves one request.
    pub async fn handle(
        self: &Arc<Self>,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        peer: Option<IpAddr>,
    ) -> Response {
        let path = path_and_query.split('?').next().unwrap_or("");
        if ambiguous_path(path) {
            return ApiError::new(
                ErrorCode::UnsupportedCommitOperation,
                "request path with `.` or `..` segments or `\\`",
            )
            .into_response();
        }
        let route = route(&method, path);
        if let Route::Commit(table) = &route
            && let Ok(mut counts) = self.commit_requests.lock()
        {
            *counts.entry(table.identifier()).or_insert(0) += 1;
        }
        match route {
            Route::Config => self.config(path_and_query, headers).await,
            Route::MultiTableCommit => ApiError::new(
                ErrorCode::UnsupportedCommitOperation,
                "multi-table commits are not supported in 0.1",
            )
            .into_response(),
            Route::Commit(table) => {
                // Constraints cannot change between this check and the end of the commit (ADR 0011,
                // ADR 0022).
                let shared = Arc::clone(&self.admin).read_owned().await;
                let doc = self.registry.snapshot();
                let identifier = table.identifier();
                if !doc.is_constrained(&identifier) {
                    self.proxy(method, path_and_query, &headers, body).await
                } else if let Some(reason) = disabled_reason(&doc, &identifier) {
                    // An operator disabled the domain (spec §19): forwarded unchecked.
                    let mut event = AuditEvent::new("COMMIT_UNCHECKED", Some(&identifier));
                    Actor::declared(&headers).stamp(&mut event);
                    event.detail = Some(format!("domain disabled: {reason}"));
                    self.audit(event);
                    self.proxy(method, path_and_query, &headers, body).await
                } else if let Err(e) = self.take_commit_token(&self.client(&headers, peer)) {
                    e.into_response()
                } else {
                    self.commit(&table, path_and_query, headers, body, shared)
                        .await
                }
            }
            Route::Proxy => self.proxy(method, path_and_query, &headers, body).await,
        }
    }

    /// The queue of the domain `identifier` belongs to, and the domain's tables.
    fn domain(
        &self,
        constraints: &[ConstraintConfig],
        identifier: &str,
    ) -> (Queue, BTreeSet<String>) {
        let members = registry::component(constraints, identifier);
        let key = members
            .first()
            .cloned()
            .unwrap_or_else(|| identifier.to_owned());
        let mut domains = match self.domains.lock() {
            Ok(d) => d,
            Err(poisoned) => poisoned.into_inner(),
        };
        let queue = Arc::clone(domains.entry(key).or_default());
        (queue, members)
    }

    /// Resolves transactions left unfinished by a previous process (spec §16), before any request
    /// is served. Leaves them for later if upstream cannot be reached.
    pub async fn recover_on_start(&self) -> Result<(), String> {
        self.recover(&HeaderMap::new(), None)
            .await
            .map_err(|e| e.message)
    }

    /// Integrity status for operators.
    pub async fn status(&self) -> Value {
        let commit_requests = self
            .commit_requests
            .lock()
            .map(|c| c.clone())
            .unwrap_or_default();
        let doc = self.registry.snapshot();
        let mut degraded = doc.degraded.clone();
        if let Ok(t) = self.transient.lock() {
            for (k, v) in t.iter() {
                degraded.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        let bound: BTreeMap<&String, &String> = doc
            .anchors
            .iter()
            .map(|(k, a)| (k, &a.table_uuid))
            .collect();
        serde_json::json!({
            "commit_requests": commit_requests,
            "degraded": if degraded.is_empty() { Value::Null } else { serde_json::json!(degraded) },
            "unresolved_transactions": self.awaiting_recovery().map(|u| u.len()).ok(),
            "bound_tables": bound,
            "constraint_set_versions": doc.versions,
            "disabled": if doc.disabled.is_empty() { Value::Null } else { serde_json::json!(doc.disabled) },
        })
    }

    async fn send(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = self
            .http
            .request(method, format!("{}{path_and_query}", self.upstream))
            .body(body);
        for (name, value) in headers {
            if forwardable(name) {
                req = req.header(name, value);
            }
        }
        req.send().await
    }

    async fn proxy(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response {
        match self.send(method, path_and_query, headers, body).await {
            Ok(resp) => match Captured::read(resp).await {
                Ok(c) => c.response(),
                Err(_) => StatusCode::BAD_GATEWAY.into_response(),
            },
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("upstream unreachable: {e}"),
            )
                .into_response(),
        }
    }

    /// `GET /v1/config`: forwarded, minus anything that would send clients around the Plane, and
    /// advertising idempotency keys, which the transaction log honours (RFC 0003, RFC 0004).
    async fn config(&self, path_and_query: &str, headers: HeaderMap) -> Response {
        let resp = match self
            .send(Method::GET, path_and_query, &headers, Bytes::new())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("upstream unreachable: {e}"),
                )
                    .into_response();
            }
        };
        if !resp.status().is_success() {
            return match Captured::read(resp).await {
                Ok(c) => c.response(),
                Err(_) => StatusCode::BAD_GATEWAY.into_response(),
            };
        }
        let Ok(mut config) = resp.json::<Value>().await else {
            return StatusCode::BAD_GATEWAY.into_response();
        };
        for section in ["defaults", "overrides"] {
            if let Some(map) = config.get_mut(section).and_then(Value::as_object_mut) {
                map.remove("uri");
                map.remove("idempotency-key-lifetime");
            }
        }
        if let Some(map) = config.as_object_mut() {
            map.insert(
                "idempotency-key-lifetime".into(),
                Value::String("PT30M".into()),
            );
        }
        axum::Json(config).into_response()
    }

    pub(crate) async fn load(&self, path: &str, headers: &HeaderMap) -> Result<Loaded, ApiError> {
        let headers = self.upstream_headers(headers).await?;
        let resp = self
            .send(Method::GET, path, &headers, Bytes::new())
            .await
            .map_err(|e| {
                ApiError::new(
                    ErrorCode::RecoveryRequired,
                    format!("upstream unreachable: {e}"),
                )
            })?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(Loaded::Missing);
        }
        if !resp.status().is_success() {
            return Err(ApiError::new(
                ErrorCode::RecoveryRequired,
                format!("upstream returned {} loading {path}", resp.status()),
            ));
        }
        let body: Value = resp.json().await.map_err(|e| {
            ApiError::new(
                ErrorCode::RecoveryRequired,
                format!("bad upstream response: {e}"),
            )
        })?;
        let meta = serde_json::from_value(body.get("metadata").cloned().unwrap_or(Value::Null))
            .map_err(|e| {
                ApiError::new(
                    ErrorCode::IndexDegraded,
                    format!("unreadable table metadata: {e}"),
                )
            })?;
        Ok(Loaded::Table(Box::new(meta), body))
    }

    fn index_kind(&self, id: ConstraintId) -> Option<IndexKind> {
        let doc = self.registry.snapshot();
        let c = doc.constraints.get(&id.0)?;
        match c.kind.as_str() {
            "primary_key" | "unique" => Some(IndexKind::Unique),
            "foreign_key" => Some(IndexKind::Reference),
            _ => None,
        }
    }

    fn indexes(
        &self,
        resolved: &[integrity_validator::ResolvedConstraint],
    ) -> Result<BTreeMap<ConstraintId, PersistentIndex>, ApiError> {
        let mut out = BTreeMap::new();
        for rc in resolved {
            if let Some(kind) = rc.index_kind() {
                let index = self
                    .store
                    .index(rc.constraint.id, kind)
                    .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, e.to_string()))?;
                out.insert(rc.constraint.id, index);
            }
        }
        Ok(out)
    }

    /// Resolves unfinished transactions (spec §16, RFC 0004). `Validated` was never forwarded and
    /// aborts without asking upstream; `Committing` is decided by whether its final snapshot is in
    /// the table. Fails with `RECOVERY_REQUIRED` while upstream cannot answer.
    ///
    /// `scope` limits recovery to one domain's tables: another domain may be forwarding its own
    /// transaction right now, and resolving it from here would race with its outcome.
    pub(crate) async fn recover(
        &self,
        headers: &HeaderMap,
        scope: Option<&BTreeSet<String>>,
    ) -> Result<(), ApiError> {
        let interrupted = |what: &str| {
            error_decision(&ApiError::new(
                ErrorCode::RecoveryRequired,
                format!("the commit was interrupted {what} and not applied; retry"),
            ))
        };
        for u in self.log.unresolved().map_err(log_error)? {
            if scope.is_some_and(|members| !members.contains(&u.prepared.identifier)) {
                continue;
            }
            let mut recovered = AuditEvent::new("TXN_RECOVERED", Some(&u.prepared.identifier));
            recovered.txn = Some(u.txn.0);
            match u.state {
                TxnState::Prepared => self
                    .log
                    .finish(u.txn, TxnState::Aborted, interrupted("during validation"))
                    .map_err(log_error)?,
                TxnState::Validated => self
                    .log
                    .finish(u.txn, TxnState::Aborted, interrupted("before publication"))
                    .map_err(log_error)?,
                TxnState::Committing => {
                    let v = u
                        .validated
                        .clone()
                        .ok_or_else(|| log_error(integrity_txn::TxnError::Corrupt))?;
                    let loaded = self.load(&u.prepared.load_path, headers).await?;
                    match loaded {
                        Loaded::Table(meta, body)
                            if meta.snapshot(SnapshotId(v.final_snapshot)).is_some() =>
                        {
                            let mut indexes = BTreeMap::new();
                            for id in u.staged.iter().map(StagedEntry::constraint_id) {
                                let kind = self.index_kind(id).ok_or_else(|| {
                                    ApiError::new(
                                        ErrorCode::IndexDegraded,
                                        format!("no configured index for {id}"),
                                    )
                                })?;
                                let index = self.store.index(id, kind).map_err(|e| {
                                    ApiError::new(ErrorCode::IndexDegraded, e.to_string())
                                })?;
                                indexes.insert(id, index);
                            }
                            let head = TableHead {
                                table_uuid: meta.table_uuid.to_lowercase(),
                                snapshot: Some(v.final_snapshot),
                            };
                            if let Err(e) = pipeline::apply(
                                &self.store,
                                &self.log,
                                u.txn,
                                &u.staged,
                                &indexes,
                                v.epoch,
                                (&u.prepared.identifier, head),
                            ) {
                                self.mark_transient(&u.prepared.identifier, &e.message);
                                return Err(e);
                            }
                            self.log
                                .finish(
                                    u.txn,
                                    TxnState::Committed,
                                    TxnDecision { status: 200, body },
                                )
                                .map_err(log_error)?;
                            recovered.verdict = Some("COMMITTED".into());
                            self.metrics
                                .recovered_committed
                                .fetch_add(1, Ordering::Relaxed);
                            recovered.result_snapshot = Some(v.final_snapshot);
                            recovered.certificate = v.certificates.last().cloned();
                            self.audit(recovered);
                        }
                        Loaded::Table(..) | Loaded::Missing => {
                            self.log
                                .finish(u.txn, TxnState::Aborted, interrupted("upstream"))
                                .map_err(log_error)?;
                            recovered.verdict = Some("ABORTED".into());
                            self.metrics
                                .recovered_aborted
                                .fetch_add(1, Ordering::Relaxed);
                            self.audit(recovered);
                        }
                    }
                }
                TxnState::Committed | TxnState::Aborted | TxnState::Rejected => {}
            }
        }
        Ok(())
    }

    /// Checks a domain member against its anchor (ADR 0011) and the indexes' head (RFC 0008): an
    /// empty table is anchored on first sight; a table with data needs onboarding; a replaced
    /// table is refused. `Some((head, fresh))` when `main` is not the snapshot the indexes reflect:
    /// the caller catches up from `head` or refuses (RFC 0011). `fresh`: this Plane never anchored
    /// the table, which another Plane filled; it can only follow its whole certified history.
    fn check_chain_head(
        &self,
        doc: &mut RegistryDoc,
        identifier: &str,
        meta: &TableMetadata,
    ) -> Result<Option<(Option<i64>, bool)>, ApiError> {
        let uuid = meta.table_uuid.to_lowercase();
        let head = meta.main_snapshot_id();
        let degraded = |e: integrity_index::IndexError| {
            ApiError::new(ErrorCode::IndexDegraded, format!("index store: {e}"))
        };
        match doc.anchors.get(identifier) {
            None if head.is_none() => {
                // The head first: an anchor never exists without one.
                self.store
                    .set_head(
                        identifier,
                        TableHead {
                            table_uuid: uuid.clone(),
                            snapshot: None,
                        },
                    )
                    .map_err(degraded)?;
                *doc = self
                    .registry
                    .update(|d| {
                        let anchor = Anchor {
                            table_uuid: uuid.clone(),
                            snapshot: None,
                            version: d.version(identifier),
                        };
                        d.set_anchor(identifier, anchor);
                        Ok::<_, StoreError>(d.clone())
                    })
                    .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, e.to_string()))?;
                Ok(None)
            }
            // Another Plane may have taken every commit so far (RFC 0012): follow them all.
            None if self.catch_up.enabled => {
                let indexed = match self.store.head(identifier).map_err(degraded)? {
                    Some(h) if h.table_uuid == uuid => h.snapshot,
                    _ => None,
                };
                Ok(Some((indexed, true)))
            }
            None => Err(not_onboarded(identifier, None)),
            Some(a) if a.table_uuid != uuid => Err(ApiError::new(
                ErrorCode::IndexDegraded,
                format!("{identifier} was replaced by another table; rebuild its domain"),
            )),
            Some(_) => match self.store.head(identifier).map_err(degraded)? {
                Some(h) if h.table_uuid == uuid && h.snapshot == head.map(|h| h.0) => Ok(None),
                Some(h) if h.table_uuid == uuid => Ok(Some((h.snapshot, false))),
                None => Err(ApiError::new(
                    ErrorCode::IndexDegraded,
                    format!(
                        "the index store does not record which snapshot of {identifier} it reflects (written before RFC 0008); rebuild its domain"
                    ),
                )),
                Some(_) => Err(ApiError::new(
                    ErrorCode::IndexDegraded,
                    format!("{identifier} was replaced by another table; rebuild its domain"),
                )),
            },
        }
    }

    /// Refuses a table that `catch_up` could not follow: a table this Plane never anchored is
    /// not enforced yet (onboarding), any other one was moved around the indexes (RFC 0008).
    fn refuse_behind(
        &self,
        doc: &mut RegistryDoc,
        b: &catchup::Behind,
        why: Option<&str>,
    ) -> ApiError {
        if b.fresh {
            not_onboarded(&b.identifier, why)
        } else {
            self.refuse_head(doc, &b.identifier, &b.meta, b.indexed, why)
        }
    }

    /// Refuses a domain whose table `identifier` moved where its indexes cannot follow
    /// (RFC 0008, RFC 0011): degrades it until a rebuild and audits `BYPASS_DETECTED`.
    pub(crate) fn refuse_head(
        &self,
        doc: &mut RegistryDoc,
        identifier: &str,
        meta: &TableMetadata,
        indexed: Option<i64>,
        why: Option<&str>,
    ) -> ApiError {
        let head = meta.main_snapshot_id();
        let main = head.map_or_else(|| "(none)".to_owned(), |h| h.to_string());
        let indexed = indexed.map_or_else(|| "(none)".to_owned(), |s| s.to_string());
        let reason = match (why, head.map(|h| snapshot_certificate(meta, h))) {
            (Some(why), _) => format!(
                "{identifier}: snapshot {main} of main cannot be caught up from snapshot {indexed}, which the indexes reflect: {why}"
            ),
            (None, Some(Ok(Some(_)))) => format!(
                "{identifier}: snapshot {main} of main is certified, but the indexes reflect snapshot {indexed}: it was committed through another Plane, or the control store was restored from an older copy"
            ),
            (None, _) => {
                format!("{identifier}: snapshot {main} of main was not committed through the Plane")
            }
        };
        if let Ok(d) = self.registry.update(|d| {
            d.degraded.insert(identifier.to_owned(), reason.clone());
            Ok::<_, StoreError>(d.clone())
        }) {
            *doc = d;
        }
        self.metrics.bypass_detected.fetch_add(1, Ordering::Relaxed);
        let mut event = AuditEvent::new("BYPASS_DETECTED", Some(identifier));
        event.result_snapshot = head.map(|h| h.0);
        event.detail = Some(reason.clone());
        self.audit(event);
        ApiError::new(ErrorCode::BypassDetected, reason)
    }

    async fn commit(
        self: &Arc<Self>,
        table: &TablePath,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        shared: OwnedRwLockReadGuard<()>,
    ) -> Response {
        let outcome = self
            .commit_inner(table, path_and_query, headers, body, shared)
            .await;
        let counter = match &outcome {
            Ok(r) if r.status().is_success() => &self.metrics.accepted,
            Err(e) if is_violation(e.code) => &self.metrics.rejected,
            _ => &self.metrics.aborted,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        match outcome {
            Ok(r) => r,
            Err(e) => e.into_response(),
        }
    }

    async fn commit_inner(
        self: &Arc<Self>,
        table: &TablePath,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        shared: OwnedRwLockReadGuard<()>,
    ) -> Result<Response, ApiError> {
        let request_id = headers
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let json: Value = serde_json::from_slice(&body).map_err(|e| {
            ApiError::new(
                ErrorCode::UnsupportedCommitOperation,
                format!("body is not JSON: {e}"),
            )
        })?;
        let mut request = CommitRequest::from_json(json)
            .map_err(|e| ApiError::new(ErrorCode::UnsupportedCommitOperation, e.to_string()))?;

        let identifier = table.identifier();
        let mut doc = self.registry.snapshot();
        let constraints = doc.list();
        let (queue, members) = self.domain(&constraints, &identifier);

        // A commit built on a stale base fails its requirements whatever happens in the queue:
        // answer 409 without occupying it, so writers racing on one table do not starve the
        // domain (the check is repeated authoritatively inside the queue). A request whose
        // idempotency key the log knows goes to the queue to get its recorded answer.
        let known = request_id
            .as_deref()
            .is_some_and(|r| self.log.knows_request(r));
        if !known
            && !request.requirements.is_empty()
            && let Loaded::Table(meta, _) = self.load(&table.path(), &headers).await?
        {
            check_requirements(&meta, &request).map_err(rejection)?;
        }

        let waiting = Instant::now();
        let held = match Arc::clone(&queue.lock).try_lock_owned() {
            Ok(held) => held,
            Err(_) => {
                // Bounded (RFC 0007): refused before waiting, nothing forwarded.
                let Some(_counted) = Waiting::enter(&queue.waiting, self.limits.max_queued_commits)
                else {
                    self.metrics.limited_queue.fetch_add(1, Ordering::Relaxed);
                    return Err(ApiError::rate_limited(
                        "too many commits are waiting for this integrity domain",
                        1,
                    ));
                };
                Arc::clone(&queue.lock).lock_owned().await
            }
        };
        self.metrics.queue_wait.observe(waiting.elapsed());
        self.recover(&headers, Some(&members)).await?;
        if let Some(r) = request_id.as_deref()
            && let Some(d) = self.log.decision_for(r).map_err(log_error)?
        {
            return Ok(replay(&d));
        }
        if let Some(reason) = self.degraded(&doc, &members) {
            return Err(ApiError::new(ErrorCode::IndexDegraded, reason));
        }

        // Load and bind every table of the integrity domain; check each one's chain head.
        let mut bindings = BTreeMap::new();
        let mut names = report::ColumnNames::new();
        let mut target_meta = None;
        let mut behind = Vec::new();
        // Every table of the domain: identifier → (uuid, `main`).
        let mut domain_state = BTreeMap::new();
        // The written table first: a stale commit fails its requirements before anything else
        // is loaded, keeping the queue short while writers race.
        let order = std::iter::once(identifier.clone())
            .chain(members.iter().filter(|m| **m != identifier).cloned());
        for ident in order {
            let path = if ident == identifier {
                table.path()
            } else {
                table.sibling(&ident).ok_or_else(|| {
                    ApiError::new(ErrorCode::IndexDegraded, format!("bad identifier {ident}"))
                })?
            };
            let meta = match self.load(&path, &headers).await? {
                Loaded::Table(m, _) => *m,
                Loaded::Missing if ident == identifier => {
                    return Err(ApiError::new(
                        ErrorCode::UnsupportedCommitOperation,
                        "creating a constrained table with data requires onboarding",
                    ));
                }
                Loaded::Missing => {
                    return Err(ApiError::new(
                        ErrorCode::IndexDegraded,
                        format!("{ident} is constrained but does not exist"),
                    ));
                }
            };
            if ident == identifier {
                check_requirements(&meta, &request).map_err(rejection)?;
            }
            let binding = registry::bind(&constraints, &ident, &meta)?;
            report::add_names(&mut names, &ident, &meta);
            let indexed = self.check_chain_head(&mut doc, &ident, &meta)?;
            domain_state.insert(
                ident.clone(),
                (
                    meta.table_uuid.to_lowercase(),
                    meta.main_snapshot_id().map(|s| s.0),
                ),
            );
            bindings.insert(ident.clone(), binding);
            if let Some((indexed, fresh)) = indexed {
                behind.push(catchup::Behind {
                    identifier: ident.clone(),
                    meta: meta.clone(),
                    indexed,
                    fresh,
                });
            }
            if ident == identifier {
                target_meta = Some(meta);
            }
        }
        let meta = target_meta
            .ok_or_else(|| ApiError::new(ErrorCode::IndexDegraded, "table not loaded"))?;

        let resolved = registry::resolve(&constraints, &bindings)?;
        let indexes = self.indexes(&resolved)?;
        if !behind.is_empty() {
            // Indexes behind `main`: follow the table history first (RFC 0011).
            let ctx = catchup::Ctx {
                bindings: &bindings,
                resolved: &resolved,
                indexes: &indexes,
            };
            self.catch_up(&mut doc, &ctx, &constraints, &domain_state, behind)
                .await?;
        }
        let validator = Validator::new(resolved);
        let table_id = bindings[&identifier].table.clone();
        let binding_columns = bindings[&identifier].columns.clone();
        let version = ConstraintSetVersion(doc.version(&identifier));
        let anchor = doc.anchors.get(&identifier).and_then(|a| a.snapshot);

        let constrained = validator.projection(&table_id).into_iter().collect();
        let change = match classify(&meta, &request, &constrained).map_err(rejection)? {
            Classification::PassThrough => {
                return Ok(self
                    .proxy(Method::POST, path_and_query, &headers, body)
                    .await);
            }
            Classification::MainChange(change) => change,
        };

        let txn = self
            .log
            .begin(Prepared {
                request_id,
                table: table_id.to_string(),
                identifier: identifier.clone(),
                load_path: table.path(),
            })
            .map_err(log_error)?;
        fault::hit(FaultPoint::AfterPreparedLog);
        let base_snapshot = change.parent.map(|s| s.0);
        let actor = Actor::declared(&headers);
        let abort = |mut e: ApiError, state: TxnState| -> ApiError {
            // The structured report goes to the audit log (`/v1/integrity/transactions/{id}`), not
            // into the Iceberg error body (RFC 0003).
            let report = e.report.take();
            if self.log.finish(txn, state, error_decision(&e)).is_err() {
                return ApiError::new(ErrorCode::IndexDegraded, "transaction log write failed");
            }
            let kind = if state == TxnState::Rejected {
                "COMMIT_REJECTED"
            } else {
                "COMMIT_ABORTED"
            };
            let mut event = AuditEvent::new(kind, Some(&identifier));
            actor.stamp(&mut event);
            event.txn = Some(txn.0);
            event.base_snapshot = base_snapshot;
            event.verdict = Some(e.code.code().to_owned());
            event.detail = Some(e.message.clone());
            event.error = report.map(|r| {
                serde_json::json!({
                    "code": e.code.code(),
                    "message": e.message,
                    "transaction_id": txn.0,
                    "violations": r["violations"],
                })
            });
            self.audit(event);
            e
        };

        let columns = validator
            .projection(&table_id)
            .into_iter()
            .map(|f| {
                binding_columns
                    .get(&f)
                    .cloned()
                    .map(|t| (f, t))
                    .ok_or_else(|| {
                        ApiError::new(ErrorCode::IndexDegraded, format!("no type for {f}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| abort(e, TxnState::Aborted))?;
        let set_digest =
            constraint_set_digest(version, &validator.governing(&table_id)).map_err(|e| {
                abort(
                    ApiError::new(ErrorCode::IndexDegraded, e.to_string()),
                    TxnState::Aborted,
                )
            })?;
        let uuid = parse_table_uuid(&meta.table_uuid).map_err(|_| {
            abort(
                ApiError::new(
                    ErrorCode::UnsupportedCommitOperation,
                    "table uuid is not a UUID",
                ),
                TxnState::Aborted,
            )
        })?;
        let mut previous = match change.parent {
            None => Digest::ZERO,
            Some(parent) => match snapshot_certificate(&meta, parent) {
                Ok(Some(cert)) => cert,
                // The anchor starts the chain (RFC 0002 chain root, ADR 0011).
                Ok(None) if anchor == Some(parent.0) => Digest::ZERO,
                Ok(None) => {
                    return Err(abort(
                        ApiError::new(
                            ErrorCode::BypassDetected,
                            format!("parent snapshot {parent} was not committed through the Plane"),
                        ),
                        TxnState::Aborted,
                    ));
                }
                Err(_) => {
                    return Err(abort(
                        ApiError::new(
                            ErrorCode::BypassDetected,
                            "parent snapshot has a malformed certificate",
                        ),
                        TxnState::Aborted,
                    ));
                }
            },
        };
        let epoch = 1 + indexes
            .values()
            .filter_map(|i| i.epoch().ok())
            .map(|e| e.0)
            .max()
            .unwrap_or(0);

        let governing_ids: Vec<u64> = validator
            .governing(&table_id)
            .iter()
            .map(|c| c.id.0)
            .collect();
        let job = Job {
            io: Arc::clone(&self.io),
            budget: self.budget,
            meta: meta.clone(),
            table: table_id,
            change: change.clone(),
            columns,
            validator,
            indexes: indexes.clone(),
            stats: Arc::new(pipeline::Stats::default()),
            scratch: self.store.scratch_dir().to_path_buf(),
            memory: self.scan_memory,
            allowed: self.allowed_locations.clone(),
        };
        let stats = Arc::clone(&job.stats);
        let validating = Instant::now();
        let outcome = tokio::task::spawn_blocking(move || pipeline::run(&job))
            .await
            .inspect(|_| self.metrics.validation.observe(validating.elapsed()))
            .map_err(|e| {
                ApiError::new(
                    ErrorCode::IndexDegraded,
                    format!("validation task failed: {e}"),
                )
            })
            .and_then(|r| r);
        self.metrics
            .bytes_read
            .fetch_add(stats.bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        self.metrics
            .keys_validated
            .fetch_add(stats.rows.load(Ordering::Relaxed), Ordering::Relaxed);
        self.metrics
            .index_probe
            .observe(std::time::Duration::from_nanos(
                stats.probe_nanos.load(Ordering::Relaxed),
            ));
        let outcome = outcome.map_err(|e| abort(e, TxnState::Aborted))?;
        let (plans, staged) = match outcome {
            Outcome::Rejected(details) => {
                let entries = report::violations(&details, &constraints, &names, self.redact_keys);
                let code = details
                    .keys()
                    .min_by_key(|v| (v.code.number(), v.constraint))
                    .map_or(ErrorCode::UnsupportedCommitOperation, |v| v.code);
                let e = ApiError::new(
                    code,
                    format!(
                        "commit rejected: {}; details: GET /v1/integrity/transactions/{}",
                        report::summary(&entries),
                        txn.0
                    ),
                )
                .with_report(serde_json::json!({ "violations": entries }));
                return Err(abort(e, TxnState::Rejected));
            }
            Outcome::Accepted { plans, staged } => (plans, staged),
        };

        let mut certificates = Vec::new();
        for plan in &plans {
            let key_delta = plan.key_delta.map_err(|e| {
                abort(
                    ApiError::new(ErrorCode::IndexDegraded, e.to_string()),
                    TxnState::Aborted,
                )
            })?;
            let cert = certificate(&CertificateInput {
                table_uuid: uuid,
                snapshot: plan.step.snapshot,
                parent: plan.step.parent,
                constraint_set: set_digest,
                key_delta,
                previous,
            });
            inject_certificate(&mut request, &plan.step, cert, version).map_err(|e| {
                abort(
                    ApiError::new(ErrorCode::UnsupportedCommitOperation, e.to_string()),
                    TxnState::Aborted,
                )
            })?;
            if let Some(signer) = &self.signer {
                // Never forwarded unsigned while signing is configured (RFC 0009).
                let signature = signer.sign(&cert).await.map_err(|e| {
                    self.metrics.signer_failures.fetch_add(1, Ordering::Relaxed);
                    abort(
                        ApiError::new(
                            ErrorCode::SignerUnavailable,
                            format!("the certificate could not be signed: {e}"),
                        ),
                        TxnState::Aborted,
                    )
                })?;
                integrity_iceberg::inject_signature(
                    &mut request,
                    &plan.step,
                    signer.key_id(),
                    &signature,
                )
                .map_err(|e| {
                    abort(
                        ApiError::new(ErrorCode::UnsupportedCommitOperation, e.to_string()),
                        TxnState::Aborted,
                    )
                })?;
            }
            certificates.push(cert.to_hex());
            previous = cert;
        }
        // The rest of the domain as validated, for the fence and the record (RFC 0012).
        let mut others = domain_state.clone();
        others.remove(&identifier);
        if self.fence {
            let recorded = fence::domain_state(
                &others
                    .values()
                    .map(|(uuid, at)| (uuid.clone(), *at))
                    .collect(),
            );
            for plan in &plans {
                request
                    .set_summary_fields(
                        plan.step.update_index,
                        &[(fence::SUMMARY_DOMAIN_STATE, recorded.clone())],
                    )
                    .map_err(|e| {
                        abort(
                            ApiError::new(ErrorCode::UnsupportedCommitOperation, e.to_string()),
                            TxnState::Aborted,
                        )
                    })?;
            }
        }
        let snapshots: Vec<i64> = plans.iter().map(|p| p.step.snapshot.0).collect();
        let final_snapshot = *snapshots.last().ok_or_else(|| {
            abort(
                ApiError::new(ErrorCode::IndexDegraded, "empty main change"),
                TxnState::Aborted,
            )
        })?;
        let forwarded = Bytes::from(serde_json::to_vec(request.json()).map_err(|e| {
            abort(
                ApiError::new(ErrorCode::IndexDegraded, e.to_string()),
                TxnState::Aborted,
            )
        })?);

        // VALIDATED (with the staged deltas) and COMMITTING in one durable write (RFC 0004
        // records, staged deltas in chunks: RFC 0006).
        let sources = staged.sources().map_err(|e| abort(e, TxnState::Aborted))?;
        self.log
            .validated_and_committing_streamed(
                txn,
                Validated {
                    base_snapshot,
                    snapshots,
                    final_snapshot,
                    constraint_set_version: version.0,
                    epoch,
                    certificates: certificates.clone(),
                },
                sources,
            )
            .map_err(log_error)?;
        fault::hit(FaultPoint::AfterValidatedLog);
        fault::hit(FaultPoint::BeforeUpstream);

        let captured = if self.fence {
            let fence = fence::Fence {
                table,
                uuid: &meta.table_uuid.to_lowercase(),
                base: base_snapshot,
                snapshot: final_snapshot,
                others: &others,
            };
            self.forward_fenced(&fence, &headers, request.json())
                .await
                .map_err(|e| abort(e, TxnState::Aborted))?
        } else {
            match self
                .send(Method::POST, path_and_query, &headers, forwarded)
                .await
            {
                Ok(resp) => Captured::read(resp).await.ok(),
                Err(_) => None,
            }
        };
        match captured {
            Some(c) if (200..300).contains(&c.status) => {
                // The commit happened: answer now and write the indexes after, still holding the
                // domain queue and the shared constraint lock (ADR 0022).
                let mut event = AuditEvent::new("COMMIT_ACCEPTED", Some(&identifier));
                actor.stamp(&mut event);
                event.txn = Some(txn.0);
                event.base_snapshot = base_snapshot;
                event.result_snapshot = Some(final_snapshot);
                event.verdict = Some("ACCEPTED".into());
                event.certificate = certificates.last().cloned();
                event.constraints = governing_ids;
                let accepted = Accepted {
                    txn,
                    epoch,
                    staged,
                    indexes,
                    head: TableHead {
                        table_uuid: meta.table_uuid.to_lowercase(),
                        snapshot: Some(final_snapshot),
                    },
                    decision: c.decision(),
                    identifier,
                    event,
                    _queue: held,
                    _shared: shared,
                };
                if let Ok(mut f) = self.finishing.lock() {
                    f.insert(txn.0);
                }
                tokio::spawn(Arc::clone(self).finish(accepted));
                Ok(c.response())
            }
            Some(c) if (400..500).contains(&c.status) => {
                self.log
                    .finish(txn, TxnState::Aborted, c.decision())
                    .map_err(log_error)?;
                let mut event = AuditEvent::new("COMMIT_ABORTED", Some(&identifier));
                actor.stamp(&mut event);
                event.txn = Some(txn.0);
                event.base_snapshot = base_snapshot;
                event.verdict = Some(format!("upstream {}", c.status));
                self.audit(event);
                Ok(c.response())
            }
            unknown => {
                fault::hit(FaultPoint::AfterUpstreamUnknown);
                match self.recover(&headers, Some(&members)).await {
                    Ok(()) => Ok(self.log.decision(txn).map_or_else(
                        || StatusCode::GATEWAY_TIMEOUT.into_response(),
                        |d| replay(&d),
                    )),
                    // Still unknown: the client cannot know either.
                    Err(_) => Ok(unknown.map_or_else(
                        || (StatusCode::GATEWAY_TIMEOUT, "upstream did not answer").into_response(),
                        |c| c.response(),
                    )),
                }
            }
        }
    }
}

/// An accepted commit whose client was answered, still to be finished (ADR 0022). Holds the domain
/// queue and the shared constraint lock until it is dropped.
struct Accepted {
    txn: TxnId,
    epoch: u64,
    staged: pipeline::Staged,
    indexes: BTreeMap<ConstraintId, PersistentIndex>,
    /// What the indexes reflect once written (RFC 0008).
    head: TableHead,
    decision: TxnDecision,
    identifier: String,
    event: AuditEvent,
    _queue: OwnedMutexGuard<()>,
    _shared: OwnedRwLockReadGuard<()>,
}

impl Gateway {
    /// Writes an accepted commit's indexes, logs `COMMITTED` and audits it. A failure leaves the
    /// transaction to recovery and the domain refusing commits until then, as a crash would.
    async fn finish(self: Arc<Self>, accepted: Accepted) {
        let _finished = Finished(&self.finishing, accepted.txn.0);
        fault::hit(FaultPoint::AfterUpstreamBeforeLog);
        let Accepted {
            txn,
            epoch,
            staged,
            indexes,
            head,
            decision,
            identifier,
            event,
            _queue,
            _shared,
        } = accepted;
        let gateway = Arc::clone(&self);
        let table = identifier.clone();
        let applied = tokio::task::spawn_blocking(move || {
            pipeline::apply_staged(&gateway.store, &staged, &indexes, epoch, (&table, head))
        })
        .await;
        match applied {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return self.mark_transient(&identifier, &e.message),
            Err(e) => return self.mark_transient(&identifier, &format!("index write: {e}")),
        }
        fault::hit(FaultPoint::BeforeCommittedLog);
        if let Err(e) = self.log.finish(txn, TxnState::Committed, decision) {
            return self.mark_transient(&identifier, &format!("transaction log: {e}"));
        }
        self.audit(event);
    }

    /// Unfinished transactions waiting for recovery: those of the log, without the commits being
    /// finished right now.
    pub(crate) fn awaiting_recovery(
        &self,
    ) -> Result<Vec<integrity_txn::Unresolved>, integrity_txn::TxnError> {
        let finishing = match self.finishing.lock() {
            Ok(f) => f.clone(),
            Err(p) => p.into_inner().clone(),
        };
        Ok(self
            .log
            .unresolved()?
            .into_iter()
            .filter(|u| !finishing.contains(&u.txn.0))
            .collect())
    }

    /// Waits until every commit answered so far is finished (ADR 0022): constraint changes wait
    /// for the same.
    pub async fn settled(&self) {
        drop(self.admin.write().await);
    }
}

/// Removes a commit from the finishing set when its task ends, however it ends.
struct Finished<'a>(&'a std::sync::Mutex<BTreeSet<u64>>, u64);

impl Drop for Finished<'_> {
    fn drop(&mut self) {
        match self.0.lock() {
            Ok(mut f) => f.remove(&self.1),
            Err(p) => p.into_inner().remove(&self.1),
        };
    }
}

/// Who caused an audited request (ADR 0026).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    /// The token's name, or the declared name.
    pub name: String,
    /// `name` is a token's.
    pub authenticated: bool,
    /// The declared name, when it is not `name`.
    pub declared: Option<String>,
}

impl Actor {
    /// What the request declares, unverified (commits, and an integrity API without tokens).
    pub fn declared(headers: &HeaderMap) -> Self {
        Self {
            name: actor_of(headers),
            authenticated: false,
            declared: None,
        }
    }

    /// The holder of the token named `name`; the request's declared name is kept beside it.
    pub fn token(name: String, headers: &HeaderMap) -> Self {
        let declared = declared_of(headers).filter(|d| *d != name);
        Self {
            name,
            authenticated: true,
            declared,
        }
    }

    /// Records this actor in `event`.
    pub(crate) fn stamp(&self, event: &mut AuditEvent) {
        event.actor = Some(self.name.clone());
        event.authenticated = self.authenticated;
        event.declared_actor = self.declared.clone();
    }
}

/// What a request declares about who sent it: `X-Integrity-Actor`, else `User-Agent`.
fn declared_of(headers: &HeaderMap) -> Option<String> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| v.chars().take(200).collect::<String>())
    };
    header("x-integrity-actor").or_else(|| header("user-agent"))
}

/// Who a request says it comes from: `X-Integrity-Actor`, else `User-Agent`. Not authenticated.
fn actor_of(headers: &HeaderMap) -> String {
    declared_of(headers).unwrap_or_else(|| "unknown".to_owned())
}

/// The reason the domain of `identifier` is disabled, if it is.
pub(crate) fn disabled_reason(doc: &RegistryDoc, identifier: &str) -> Option<String> {
    if doc.disabled.is_empty() {
        return None;
    }
    registry::component(&doc.list(), identifier)
        .iter()
        .find_map(|m| doc.disabled.get(m).cloned())
}

/// Whether `code` is a constraint verdict (as opposed to a refusal for another reason).
fn is_violation(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::DuplicatePrimaryKey
            | ErrorCode::DuplicateUniqueKey
            | ErrorCode::ForeignKeyViolation
            | ErrorCode::ReferencedRowDelete
            | ErrorCode::NotNullViolation
            | ErrorCode::CheckViolation
    )
}

fn rejection(r: Rejection) -> ApiError {
    ApiError::new(r.code(), r.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes() {
        let post = Method::POST;
        let commit = |base: &str, ns: &[&str], t: &str| {
            Route::Commit(TablePath {
                base: base.into(),
                namespace: ns.iter().map(|s| s.to_string()).collect(),
                table: t.into(),
            })
        };
        assert_eq!(route(&Method::GET, "/v1/config"), Route::Config);
        assert_eq!(
            route(&post, "/v1/namespaces/db/tables/orders"),
            commit("/v1", &["db"], "orders")
        );
        assert_eq!(
            route(&post, "/v1/wh/namespaces/a%1Fb/tables/t"),
            commit("/v1/wh", &["a", "b"], "t")
        );
        assert_eq!(
            route(&post, "/v1/wh/transactions/commit"),
            Route::MultiTableCommit
        );
        assert_eq!(
            route(&post, "/v1/namespaces/db/tables"),
            Route::Proxy,
            "create table"
        );
        assert_eq!(
            route(&post, "/v1/namespaces/db/tables/t/metrics"),
            Route::Proxy
        );
        assert_eq!(
            route(&Method::GET, "/v1/namespaces/db/tables/orders"),
            Route::Proxy,
            "load"
        );
        assert_eq!(route(&post, "/v1/tables/rename"), Route::Proxy);
    }

    #[test]
    fn ambiguous_paths() {
        for p in [
            "/v1/namespaces/db/tables/orders/../orders",
            "/v1/namespaces/db/tables/x/%2e%2e/orders",
            "/v1/namespaces/db/tables/x/.%2E/orders",
            "/v1/namespaces/db/tables/orders/.",
            "/v1/./namespaces/db/tables/orders",
            "/v1/namespaces/db/tables/x\\..\\orders",
        ] {
            assert!(ambiguous_path(p), "{p}");
        }
        for p in [
            "/v1/namespaces/db/tables/orders",
            "/v1/namespaces/db/tables/a.b",
            "/v1/namespaces/db/tables/...",
            "/v1/namespaces/db/tables/%2e%2e%2e",
            "/v1/namespaces/db/tables/orders/",
        ] {
            assert!(!ambiguous_path(p), "{p}");
        }
    }

    #[test]
    fn paths_round_trip() {
        let p = TablePath {
            base: "/v1/wh".into(),
            namespace: vec!["a".into(), "b c".into()],
            table: "t".into(),
        };
        assert_eq!(p.identifier(), "a.b c.t");
        assert_eq!(p.path(), "/v1/wh/namespaces/a%1Fb%20c/tables/t");
        assert_eq!(route(&Method::POST, &p.path()), Route::Commit(p.clone()));
        assert_eq!(
            p.sibling("db.customer").unwrap(),
            "/v1/wh/namespaces/db/tables/customer"
        );
    }
}
