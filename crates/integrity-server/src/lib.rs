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
pub mod metrics;
pub mod onboard;
pub mod pipeline;
pub mod registry;
pub mod report;
pub mod store;
pub mod verify;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};

pub use gateway::Gateway;

/// The HTTP router: the integrity API plus the catalog API (everything else).
pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/v1/integrity/status", get(status))
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
        .route("/metrics", get(metrics))
        .fallback(catalog)
        .with_state(gateway)
}

/// Checks the bearer token, comparing digests so the comparison time does not depend on where
/// the token differs.
fn authorized(g: &Gateway, headers: &HeaderMap) -> bool {
    let Some(token) = &g.admin_token else {
        return true;
    };
    let given = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    blake3::hash(given.as_bytes()) == blake3::hash(token.as_bytes())
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

async fn register(State(g): State<Arc<Gateway>>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
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
    reply(g.register(req, &headers).await)
}

async fn list_constraints(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    axum::Json(g.constraints(q.get("table").map(String::as_str))).into_response()
}

async fn drop_constraint(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    reply(g.drop_constraint(id, &headers).await)
}

async fn rebuild(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    reply(g.rebuild(id, &headers).await)
}

async fn audit(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    let since = q.get("since").and_then(|s| s.parse().ok()).unwrap_or(0);
    reply(g.audit_events(q.get("table").map(String::as_str), since))
}

async fn verify(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    let Some(table) = q.get("table") else {
        return (
            StatusCode::BAD_REQUEST,
            "verify needs ?table=namespace.table",
        )
            .into_response();
    };
    reply(g.verify(table, &headers).await)
}

fn not_found(what: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({ "error": format!("no such {what}") })),
    )
        .into_response()
}

async fn transaction(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    g.transaction(id).map_or_else(
        || not_found("transaction"),
        |v| axum::Json(v).into_response(),
    )
}

async fn domain(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Path(table): Path<String>,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    g.domain_state(&table)
        .map_or_else(|| not_found("domain"), |v| axum::Json(v).into_response())
}

async fn disable(
    State(g): State<Arc<Gateway>>,
    headers: HeaderMap,
    Path(table): Path<String>,
    body: Bytes,
) -> Response {
    if !authorized(&g, &headers) {
        return unauthorized();
    }
    let reason = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["reason"].as_str().map(str::to_owned))
        .unwrap_or_default();
    reply(g.disable(&table, &reason, &headers).await)
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
    g.handle(parts.method, &path_and_query, headers, body).await
}
