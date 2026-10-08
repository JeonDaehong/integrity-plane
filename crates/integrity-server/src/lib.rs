//! Iceberg REST gateway with commit-time integrity enforcement (spec §14, §22, ADR 0010).
//!
//! Writers use the gateway as their REST catalog. Commits to constrained tables are validated,
//! certified and forwarded; everything else is forwarded unchanged.

pub mod config;
pub mod error;
pub mod fileio;
pub mod gateway;
pub mod pipeline;
pub mod registry;

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;

pub use gateway::Gateway;

/// The HTTP router: `/v1/integrity/status` plus the catalog API (everything else).
pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/v1/integrity/status", get(status))
        .fallback(catalog)
        .with_state(gateway)
}

async fn status(State(g): State<Arc<Gateway>>) -> Response {
    axum::Json(g.status().await).into_response()
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
        Err(_) => return axum::http::StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    g.handle(parts.method, &path_and_query, headers, body).await
}
