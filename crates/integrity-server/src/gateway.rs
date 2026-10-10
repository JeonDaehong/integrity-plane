//! The REST gateway (spec §14, §16, §22, ADR 0010, RFC 0003, RFC 0004).
//!
//! Commits to tables with constraints go through the integrity pipeline, serialized per integrity
//! domain (the FK-connected component of the table, spec §11) and recorded in the transaction log so
//! that a crash at any point is recoverable. Commits in different domains run in parallel. Every
//! other request is forwarded to the upstream catalog unchanged.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
use integrity_index::{IndexKind, KeyIndex, PersistentIndex, PersistentStore};
use integrity_txn::{
    Decision as TxnDecision, FaultPoint, Prepared, TxnLog, TxnState, Validated, fault,
};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, SnapshotId};
use integrity_validator::Validator;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::ConstraintConfig;
use crate::error::ApiError;
use crate::metrics::Metrics;
use crate::pipeline::{self, Job, Outcome};
use crate::registry;
use crate::report;
use crate::store::{Anchor, AuditEvent, Registry, RegistryDoc, StoreError};

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
type Queue = Arc<Mutex<()>>;

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
    /// Bearer token required by the integrity API, if any.
    pub(crate) admin_token: Option<String>,
    /// Credentials for the Plane's own upstream requests.
    upstream_auth: Option<crate::config::UpstreamAuth>,
    /// Cached OAuth2 access token and when to refresh it.
    upstream_token: Mutex<Option<(String, std::time::Instant)>>,
    /// Leave key values out of violation reports.
    pub(crate) redact_keys: bool,
    /// Bytes of keys a scan (per table) or a commit validation keeps in memory.
    pub(crate) scan_memory: usize,
    /// Certificate signer (RFC 0005), if signing is enabled.
    pub(crate) signer: Option<integrity_core::CertSigner>,
    /// `verify` requires signatures.
    pub(crate) require_signatures: bool,
    /// Held shared by every commit request, exclusively by constraint changes and rebuilds
    /// (ADR 0011).
    pub(crate) admin: tokio::sync::RwLock<()>,
    /// One queue per integrity domain, keyed by the domain's smallest table identifier.
    domains: std::sync::Mutex<BTreeMap<String, Queue>>,
    /// Tables degraded by a failed index apply in this process (recovery completes it).
    pub(crate) transient: std::sync::Mutex<BTreeMap<String, String>>,
    /// Commit requests received per table identifier (observability; detects retry storms).
    commit_requests: std::sync::Mutex<BTreeMap<String, u64>>,
    /// Prometheus metrics.
    pub(crate) metrics: Metrics,
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
            admin_token: None,
            upstream_auth: None,
            upstream_token: Mutex::new(None),
            redact_keys: false,
            scan_memory: crate::config::DEFAULT_SCAN_MEMORY as usize,
            signer: None,
            require_signatures: false,
            admin: tokio::sync::RwLock::new(()),
            domains: std::sync::Mutex::new(BTreeMap::new()),
            transient: std::sync::Mutex::new(BTreeMap::new()),
            commit_requests: std::sync::Mutex::new(BTreeMap::new()),
            metrics: Metrics::default(),
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

    /// Requires `Authorization: Bearer <token>` on the integrity API.
    pub fn with_admin_token(mut self, token: Option<String>) -> Self {
        self.admin_token = token;
        self
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

    /// Signs certificates with `signer` (RFC 0005) and records its public key in the registry;
    /// `require` makes `verify` treat unsigned snapshots as a broken chain.
    pub fn with_signer(
        mut self,
        signer: Option<integrity_core::CertSigner>,
        require: bool,
    ) -> Self {
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

    /// Leaves sample key values out of violation reports (`errors.redact_keys`).
    pub fn with_redact_keys(mut self, redact: bool) -> Self {
        self.redact_keys = redact;
        self
    }

    /// Appends an audit event; a failure is logged, never turned into a decision.
    pub(crate) fn audit(&self, mut event: AuditEvent) {
        if event.actor.is_none() {
            event.actor = Some("plane".to_owned());
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
        &self,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let path = path_and_query.split('?').next().unwrap_or("");
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
                // Constraints cannot change between this check and the response (ADR 0011).
                let _shared = self.admin.read().await;
                let doc = self.registry.snapshot();
                let identifier = table.identifier();
                if !doc.is_constrained(&identifier) {
                    self.proxy(method, path_and_query, &headers, body).await
                } else if let Some(reason) = disabled_reason(&doc, &identifier) {
                    // An operator disabled the domain (spec §19): forwarded unchecked.
                    let mut event = AuditEvent::new("COMMIT_UNCHECKED", Some(&identifier));
                    event.actor = Some(actor_of(&headers));
                    event.detail = Some(format!("domain disabled: {reason}"));
                    self.audit(event);
                    self.proxy(method, path_and_query, &headers, body).await
                } else {
                    self.commit(&table, path_and_query, headers, body).await
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
            "unresolved_transactions": self.log.unresolved().map(|u| u.len()).ok(),
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
                            for (id, _) in &u.staged {
                                let kind = self.index_kind(*id).ok_or_else(|| {
                                    ApiError::new(
                                        ErrorCode::IndexDegraded,
                                        format!("no configured index for {id}"),
                                    )
                                })?;
                                let index = self.store.index(*id, kind).map_err(|e| {
                                    ApiError::new(ErrorCode::IndexDegraded, e.to_string())
                                })?;
                                indexes.insert(*id, index);
                            }
                            if let Err(e) =
                                pipeline::apply(&self.store, &u.staged, &indexes, v.epoch)
                            {
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

    /// Checks a domain member against its anchor (ADR 0011): an empty table is anchored on first
    /// sight; a table with data needs onboarding; a replaced table is refused; and `main` must be
    /// the anchor or a certified snapshot, otherwise a writer bypassed the Plane.
    fn check_chain_head(
        &self,
        doc: &mut RegistryDoc,
        identifier: &str,
        meta: &TableMetadata,
    ) -> Result<(), ApiError> {
        let uuid = meta.table_uuid.to_lowercase();
        let head = meta.main_snapshot_id();
        match doc.anchors.get(identifier) {
            None if head.is_none() => {
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
                Ok(())
            }
            None => Err(ApiError::new(
                ErrorCode::IndexDegraded,
                format!(
                    "{identifier} already has data; register its constraints through the integrity API to onboard it"
                ),
            )),
            Some(a) if a.table_uuid != uuid => Err(ApiError::new(
                ErrorCode::IndexDegraded,
                format!("{identifier} was replaced by another table; rebuild its domain"),
            )),
            Some(a) if head.map(|h| h.0) == a.snapshot => Ok(()),
            Some(_) => match head.map(|h| snapshot_certificate(meta, h)) {
                Some(Ok(Some(_))) => Ok(()),
                _ => {
                    let reason = format!(
                        "{identifier}: snapshot {} of main was not committed through the Plane",
                        head.map_or_else(|| "(none)".to_owned(), |h| h.to_string())
                    );
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
                    Err(ApiError::new(ErrorCode::BypassDetected, reason))
                }
            },
        }
    }

    async fn commit(
        &self,
        table: &TablePath,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let outcome = self
            .commit_inner(table, path_and_query, headers, body)
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
        &self,
        table: &TablePath,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
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
        let _queue = queue.lock().await;
        self.metrics.queue_wait.observe(waiting.elapsed());
        self.recover(&headers, Some(&members)).await?;
        if let Some(d) = request_id.as_deref().and_then(|r| self.log.decision_for(r)) {
            return Ok(replay(&d));
        }
        if let Some(reason) = self.degraded(&doc, &members) {
            return Err(ApiError::new(ErrorCode::IndexDegraded, reason));
        }

        // Load and bind every table of the integrity domain; check each one's chain head.
        let mut bindings = BTreeMap::new();
        let mut names = report::ColumnNames::new();
        let mut target_meta = None;
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
            self.check_chain_head(&mut doc, &ident, &meta)?;
            bindings.insert(ident.clone(), binding);
            if ident == identifier {
                target_meta = Some(meta);
            }
        }
        let meta = target_meta
            .ok_or_else(|| ApiError::new(ErrorCode::IndexDegraded, "table not loaded"))?;

        let resolved = registry::resolve(&constraints, &bindings)?;
        let indexes = self.indexes(&resolved)?;
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
        let actor = actor_of(&headers);
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
            event.actor = Some(actor.clone());
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
            let key_delta = plan.validated.key_delta_digest().map_err(|e| {
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
                integrity_iceberg::inject_signature(
                    &mut request,
                    &plan.step,
                    signer.key_id(),
                    &signer.sign(&cert),
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

        // VALIDATED (with the staged deltas) and COMMITTING in one durable write (RFC 0004 records).
        self.log
            .validated_and_committing(
                txn,
                Validated {
                    base_snapshot,
                    snapshots,
                    final_snapshot,
                    constraint_set_version: version.0,
                    epoch,
                    certificates: certificates.clone(),
                },
                &staged,
            )
            .map_err(log_error)?;
        fault::hit(FaultPoint::AfterValidatedLog);
        fault::hit(FaultPoint::BeforeUpstream);

        let captured = match self
            .send(Method::POST, path_and_query, &headers, forwarded)
            .await
        {
            Ok(resp) => Captured::read(resp).await.ok(),
            Err(_) => None,
        };
        match captured {
            Some(c) if (200..300).contains(&c.status) => {
                fault::hit(FaultPoint::AfterUpstreamBeforeLog);
                if let Err(e) = pipeline::apply(&self.store, &staged, &indexes, epoch) {
                    // The commit happened; recovery re-applies, and nothing else commits meanwhile.
                    self.mark_transient(&identifier, &e.message);
                    return Ok(c.response());
                }
                fault::hit(FaultPoint::BeforeCommittedLog);
                self.log
                    .finish(txn, TxnState::Committed, c.decision())
                    .map_err(log_error)?;
                let mut event = AuditEvent::new("COMMIT_ACCEPTED", Some(&identifier));
                event.actor = Some(actor.clone());
                event.txn = Some(txn.0);
                event.base_snapshot = base_snapshot;
                event.result_snapshot = Some(final_snapshot);
                event.verdict = Some("ACCEPTED".into());
                event.certificate = certificates.last().cloned();
                event.constraints = governing_ids;
                self.audit(event);
                Ok(c.response())
            }
            Some(c) if (400..500).contains(&c.status) => {
                self.log
                    .finish(txn, TxnState::Aborted, c.decision())
                    .map_err(log_error)?;
                let mut event = AuditEvent::new("COMMIT_ABORTED", Some(&identifier));
                event.actor = Some(actor.clone());
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

/// Who a request says it comes from: `X-Integrity-Actor`, else `User-Agent`. Not authenticated.
pub(crate) fn actor_of(headers: &HeaderMap) -> String {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| v.chars().take(200).collect::<String>())
    };
    header("x-integrity-actor")
        .or_else(|| header("user-agent"))
        .unwrap_or_else(|| "unknown".to_owned())
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
