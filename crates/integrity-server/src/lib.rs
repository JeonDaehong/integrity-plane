//! Iceberg REST gateway with commit-time integrity enforcement (spec §14, §22, ADR 0010).
//!
//! Writers use the gateway as their REST catalog. Commits to constrained tables are validated,
//! certified and forwarded; everything else is forwarded unchanged. `/v1/integrity/*` is the
//! integrity API (spec §23).

pub mod admin;
pub mod config;
pub mod error;
pub mod fileio;
pub mod gateway;
pub mod limits;
pub mod metrics;
pub mod onboard;
pub mod pipeline;
pub mod registry;
pub mod report;
pub mod signing;
pub mod store;
pub mod verify;

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Extension, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};

pub use gateway::Gateway;

/// The HTTP router: the integrity API plus the catalog API (everything else). Serve it with
/// `into_make_service_with_connect_info::<SocketAddr>()` so request limits know each client's
/// address (RFC 0007).
pub fn router(gateway: Arc<Gateway>) -> Router {
    // Behind the bearer token, if one is set.
    let api = Router::new()
        .route(
            "/v1/integrity/constraints",
            post(register).get(list_constraints),
        )
        .route("/v1/integrity/constraints/{id}", delete(drop_constraint))
        .route("/v1/integrity/indexes/{id}/rebuild", post(rebuild))
        .route("/v1/integrity/audit", get(audit))
        .route("/v1/integrity/verify", get(verify))
        .route("/v1/integrity/transactions/{id}", get(transaction))
        .route("/v1/integrity/domains/{table}", get(domain))
        .route("/v1/integrity/domains/{table}/disable", post(disable))
        .route_layer(axum::middleware::from_fn_with_state(
            Arc::clone(&gateway),
            admit,
        ));
    Router::new()
        .route("/v1/integrity/status", get(status))
        .route("/metrics", get(metrics))
        .route("/v1/integrity/keys", get(keys))
        .merge(api)
        .fallback(catalog)
        .with_state(gateway)
}

/// The connection's peer address, when the router is served with connect info.
fn peer_of(request: &Request) -> Option<IpAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip())
}

/// Lets an integrity API request through if it carries a token, unless its client failed to
/// authenticate too often lately (RFC 0007), and tells the handler who sent it (ADR 0026).
async fn admit(State(g): State<Arc<Gateway>>, mut request: Request, next: Next) -> Response {
    let actor = if g.tokens.is_empty() {
        gateway::Actor::declared(request.headers())
    } else {
        let client = g.client(request.headers(), peer_of(&request));
        let now = Instant::now();
        let admitted = {
            let mut failures = match g.limits.auth_failures.lock() {
                Ok(f) => f,
                Err(p) => p.into_inner(),
            };
            if let Some(seconds) = failures.locked(&client, now) {
                g.metrics
                    .limited_auth
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(
                    error::ApiError::rate_limited("too many failed authentications", seconds)
                        .into_response(),
                )
            } else if let Some(name) = g.token_name(request.headers()) {
                Ok(name)
            } else {
                failures.failed(&client, now);
                Err(unauthorized())
            }
        };
        match admitted {
            Ok(name) => gateway::Actor::token(name, request.headers()),
            Err(response) => return response,
        }
    };
    request.extensions_mut().insert(actor);
    next.run(request).await
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "integrity API token required").into_response()
}

fn reply(r: Result<serde_json::Value, error::ApiError>) -> Response {
    match r {
        Ok(v) => axum::Json(v).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn status(State(g): State<Arc<Gateway>>) -> Response {
    axum::Json(g.status().await).into_response()
}

async fn register(
    State(g): State<Arc<Gateway>>,
    Extension(actor): Extension<gateway::Actor>,
    body: Bytes,
) -> Response {
    let req: admin::RegisterRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error::ApiError::new(
                integrity_types::ErrorCode::InvalidConstraint,
                format!("bad request body: {e}"),
            )
            .into_response();
        }
    };
    reply(g.register(req, &actor).await)
}

async fn list_constraints(
    State(g): State<Arc<Gateway>>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    axum::Json(g.constraints(q.get("table").map(String::as_str))).into_response()
}

async fn drop_constraint(
    State(g): State<Arc<Gateway>>,
    Extension(actor): Extension<gateway::Actor>,
    Path(id): Path<u64>,
) -> Response {
    reply(g.drop_constraint(id, &actor).await)
}

async fn rebuild(
    State(g): State<Arc<Gateway>>,
    Extension(actor): Extension<gateway::Actor>,
    Path(id): Path<u64>,
) -> Response {
    reply(g.rebuild(id, &actor).await)
}

async fn audit(
    State(g): State<Arc<Gateway>>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    let since = q.get("since").and_then(|s| s.parse().ok()).unwrap_or(0);
    // Commits already answered are in the log once finished (ADR 0022).
    g.settled().await;
    reply(g.audit_events(q.get("table").map(String::as_str), since))
}

async fn verify(
    State(g): State<Arc<Gateway>>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(table) = q.get("table") else {
        return (
            StatusCode::BAD_REQUEST,
            "verify needs ?table=namespace.table",
        )
            .into_response();
    };
    let require = q
        .get("require_signatures")
        .is_some_and(|v| v == "true" || v == "1");
    reply(
        g.verify(table, q.get("trusted_keys").map(String::as_str), require)
            .await,
    )
}

fn not_found(what: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({ "error": format!("no such {what}") })),
    )
        .into_response()
}

async fn transaction(State(g): State<Arc<Gateway>>, Path(id): Path<u64>) -> Response {
    g.settled().await;
    g.transaction(id).map_or_else(
        || not_found("transaction"),
        |v| axum::Json(v).into_response(),
    )
}

async fn domain(State(g): State<Arc<Gateway>>, Path(table): Path<String>) -> Response {
    g.domain_state(&table)
        .map_or_else(|| not_found("domain"), |v| axum::Json(v).into_response())
}

async fn disable(
    State(g): State<Arc<Gateway>>,
    Extension(actor): Extension<gateway::Actor>,
    Path(table): Path<String>,
    body: Bytes,
) -> Response {
    let reason = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["reason"].as_str().map(str::to_owned))
        .unwrap_or_default();
    reply(g.disable(&table, &reason, &actor).await)
}

async fn keys(State(g): State<Arc<Gateway>>) -> Response {
    axum::Json(g.keys()).into_response()
}

async fn metrics(State(g): State<Arc<Gateway>>) -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        g.metrics_text(),
    )
        .into_response()
}

async fn catalog(State(g): State<Arc<Gateway>>, request: Request) -> Response {
    let peer = peer_of(&request);
    let (parts, body) = request.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_owned(), |p| p.as_str().to_owned());
    let headers: HeaderMap = parts.headers;
    let body: Bytes = match axum::body::to_bytes(body, 64 << 20).await {
        Ok(b) => b,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    g.handle(parts.method, &path_and_query, headers, body, peer)
        .await
}
