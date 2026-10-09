//! The REST gateway (spec §14, §16, §22, ADR 0010, RFC 0003, RFC 0004).
//!
//! Commits to tables with constraints go through the integrity pipeline under one lock, with every
//! step recorded in the transaction log so that a crash at any point is recoverable; every other
//! request is forwarded to the upstream catalog unchanged. Per-domain queues arrive in Phase 9.

use std::collections::BTreeMap;
use std::sync::Arc;

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
use crate::pipeline::{self, Job, Outcome};
use crate::registry::{self, Binding};

/// Constraint set version until versioned registration exists (Phase 10).
const VERSION: ConstraintSetVersion = ConstraintSetVersion(1);

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

#[derive(Debug, Default)]
struct State {
    bindings: BTreeMap<String, Binding>,
    degraded: Option<String>,
}

/// The gateway.
pub struct Gateway {
    http: reqwest::Client,
    upstream: String,
    io: Arc<dyn FileIo + Send + Sync>,
    store: PersistentStore,
    log: TxnLog,
    budget: u64,
    constraints: Vec<ConstraintConfig>,
    state: Mutex<State>,
    /// Commit requests received per table identifier (observability; detects retry storms).
    commit_requests: std::sync::Mutex<BTreeMap<String, u64>>,
}

enum Loaded {
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
        constraints: Vec<ConstraintConfig>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: reqwest::Client::builder().timeout(timeout).build()?,
            upstream: upstream.trim_end_matches('/').to_owned(),
            io,
            store,
            log,
            budget,
            constraints,
            state: Mutex::new(State::default()),
            commit_requests: std::sync::Mutex::new(BTreeMap::new()),
        })
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
            Route::Commit(table)
                if registry::is_constrained(&self.constraints, &table.identifier()) =>
            {
                self.commit(&table, path_and_query, headers, body).await
            }
            Route::Commit(_) | Route::Proxy => {
                self.proxy(method, path_and_query, &headers, body).await
            }
        }
    }

    /// Resolves transactions left unfinished by a previous process (spec §16). Leaves them for
    /// later if upstream cannot be reached.
    pub async fn recover_on_start(&self) -> Result<(), String> {
        let mut st = self.state.lock().await;
        self.recover(&mut st, &HeaderMap::new())
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
        let st = self.state.lock().await;
        serde_json::json!({
            "commit_requests": commit_requests,
            "degraded": st.degraded,
            "unresolved_transactions": self.log.unresolved().map(|u| u.len()).ok(),
            "bound_tables": st.bindings.iter().map(|(k, b)| (k.clone(), b.table.to_string())).collect::<BTreeMap<_, _>>(),
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

    async fn load(&self, path: &str, headers: &HeaderMap) -> Result<Loaded, ApiError> {
        let resp = self
            .send(Method::GET, path, headers, Bytes::new())
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
        let c = self.constraints.iter().find(|c| c.id == id.0)?;
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

    /// Resolves every unfinished transaction (spec §16, RFC 0004). `Validated` was never forwarded
    /// and aborts without asking upstream; `Committing` is decided by whether its final snapshot is
    /// in the table. Fails with `RECOVERY_REQUIRED` while upstream cannot answer.
    async fn recover(&self, st: &mut State, headers: &HeaderMap) -> Result<(), ApiError> {
        let interrupted = |what: &str| {
            error_decision(&ApiError::new(
                ErrorCode::RecoveryRequired,
                format!("the commit was interrupted {what} and not applied; retry"),
            ))
        };
        for u in self.log.unresolved().map_err(log_error)? {
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
                            if let Err(e) = pipeline::apply(&u.staged, &indexes, v.epoch) {
                                st.degraded = Some(e.message.clone());
                                return Err(e);
                            }
                            self.log
                                .finish(
                                    u.txn,
                                    TxnState::Committed,
                                    TxnDecision { status: 200, body },
                                )
                                .map_err(log_error)?;
                        }
                        Loaded::Table(..) | Loaded::Missing => self
                            .log
                            .finish(u.txn, TxnState::Aborted, interrupted("upstream"))
                            .map_err(log_error)?,
                    }
                }
                TxnState::Committed | TxnState::Aborted | TxnState::Rejected => {}
            }
        }
        Ok(())
    }

    async fn commit(
        &self,
        table: &TablePath,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        match self
            .commit_inner(table, path_and_query, headers, body)
            .await
        {
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

        let mut st = self.state.lock().await;
        self.recover(&mut st, &headers).await?;
        if let Some(d) = request_id.as_deref().and_then(|r| self.log.decision_for(r)) {
            return Ok(replay(&d));
        }
        if let Some(reason) = &st.degraded {
            return Err(ApiError::new(ErrorCode::IndexDegraded, reason.clone()));
        }

        // Bind every table of the integrity domain.
        let identifier = table.identifier();
        let mut target_meta = None;
        for ident in registry::component(&self.constraints, &identifier) {
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
            let binding = registry::bind(&self.constraints, &ident, &meta)?;
            match st.bindings.get(&ident) {
                Some(existing) if existing.table != binding.table => {
                    return Err(ApiError::new(
                        ErrorCode::IndexDegraded,
                        format!("{ident} was replaced by another table"),
                    ));
                }
                Some(_) => {}
                None => {
                    st.bindings.insert(ident.clone(), binding);
                    let resolved = registry::resolve(&self.constraints, &st.bindings)?;
                    let indexes = self.indexes(&resolved)?;
                    let own_indexed = resolved
                        .iter()
                        .filter(|rc| {
                            rc.constraint.table == st.bindings[&ident].table
                                && rc.index_kind().is_some()
                        })
                        .count();
                    let has_history = indexes
                        .values()
                        .any(|i| i.epoch().map(|e| e.0 > 0).unwrap_or(false));
                    if meta.main_snapshot_id().is_some() && !(own_indexed > 0 && has_history) {
                        st.bindings.remove(&ident);
                        return Err(ApiError::new(
                            ErrorCode::IndexDegraded,
                            format!(
                                "{ident} already has data; onboarding is required before enforcement"
                            ),
                        ));
                    }
                }
            }
            if ident == identifier {
                target_meta = Some(meta);
            }
        }
        let meta = target_meta
            .ok_or_else(|| ApiError::new(ErrorCode::IndexDegraded, "table not loaded"))?;

        let resolved = registry::resolve(&self.constraints, &st.bindings)?;
        let indexes = self.indexes(&resolved)?;
        let validator = Validator::new(resolved);
        let table_id = st.bindings[&identifier].table.clone();
        let binding_columns = st.bindings[&identifier].columns.clone();

        check_requirements(&meta, &request).map_err(rejection)?;
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
        let abort = |e: ApiError, state: TxnState| -> ApiError {
            if self.log.finish(txn, state, error_decision(&e)).is_err() {
                return ApiError::new(ErrorCode::IndexDegraded, "transaction log write failed");
            }
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
            constraint_set_digest(VERSION, &validator.governing(&table_id)).map_err(|e| {
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
                // Chain root until onboarding records exist (Phase 10).
                Ok(None) => Digest::ZERO,
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

        let job = Job {
            io: Arc::clone(&self.io),
            budget: self.budget,
            meta: meta.clone(),
            table: table_id,
            change: change.clone(),
            columns,
            validator,
            indexes: indexes.clone(),
        };
        let outcome = tokio::task::spawn_blocking(move || pipeline::run(&job))
            .await
            .map_err(|e| {
                ApiError::new(
                    ErrorCode::IndexDegraded,
                    format!("validation task failed: {e}"),
                )
            })
            .and_then(|r| r)
            .map_err(|e| abort(e, TxnState::Aborted))?;
        let (plans, staged) = match outcome {
            Outcome::Rejected(violations) => {
                return Err(abort(
                    violation_error(&violations, &self.constraints),
                    TxnState::Rejected,
                ));
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
            inject_certificate(&mut request, &plan.step, cert, VERSION).map_err(|e| {
                abort(
                    ApiError::new(ErrorCode::UnsupportedCommitOperation, e.to_string()),
                    TxnState::Aborted,
                )
            })?;
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

        self.log
            .validated(
                txn,
                Validated {
                    base_snapshot: change.parent.map(|s| s.0),
                    snapshots,
                    final_snapshot,
                    constraint_set_version: VERSION.0,
                    epoch,
                    certificates,
                },
                &staged,
            )
            .map_err(log_error)?;
        fault::hit(FaultPoint::AfterValidatedLog);
        self.log.committing(txn).map_err(log_error)?;
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
                if let Err(e) = pipeline::apply(&staged, &indexes, epoch) {
                    // The commit happened; recovery re-applies, and nothing else commits meanwhile.
                    st.degraded = Some(e.message);
                    return Ok(c.response());
                }
                fault::hit(FaultPoint::BeforeCommittedLog);
                self.log
                    .finish(txn, TxnState::Committed, c.decision())
                    .map_err(log_error)?;
                Ok(c.response())
            }
            Some(c) if (400..500).contains(&c.status) => {
                self.log
                    .finish(txn, TxnState::Aborted, c.decision())
                    .map_err(log_error)?;
                Ok(c.response())
            }
            unknown => {
                fault::hit(FaultPoint::AfterUpstreamUnknown);
                match self.recover(&mut st, &headers).await {
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

fn rejection(r: Rejection) -> ApiError {
    ApiError::new(r.code(), r.to_string())
}

fn violation_error(
    violations: &std::collections::BTreeSet<integrity_core::Violation>,
    configs: &[ConstraintConfig],
) -> ApiError {
    let name = |id: ConstraintId| {
        configs
            .iter()
            .find(|c| c.id == id.0)
            .map_or_else(|| id.to_string(), |c| c.name.clone())
    };
    let first = violations
        .iter()
        .min_by_key(|v| (v.code.number(), v.constraint));
    let code = first.map_or(ErrorCode::UnsupportedCommitOperation, |v| v.code);
    let details: Vec<String> = violations
        .iter()
        .map(|v| format!("{} on {}", v.code.code(), name(v.constraint)))
        .collect();
    ApiError::new(code, format!("commit rejected: {}", details.join(", ")))
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
