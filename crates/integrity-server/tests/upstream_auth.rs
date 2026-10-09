//! An upstream catalog that requires authentication (Polaris-style OAuth2 client credentials):
//! the Plane uses its own credentials for its own requests, forwards writers' commits with the
//! writers' credentials, and never sends the integrity API token upstream.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::post;
use integrity_index::PersistentStore;
use integrity_server::config::UpstreamAuth;
use integrity_server::fileio::ObjectStoreIo;
use integrity_server::store::Registry;
use integrity_server::{Gateway, router};
use integrity_txn::TxnLog;
use serde_json::{Value, json};

mod support;
use support::{Dir, Files, Shared, Upstream, empty_table, upstream};

type Seen = Arc<Mutex<Vec<String>>>;

async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn the_plane_authenticates_itself_and_never_leaks_the_admin_token() {
    let files = Arc::new(Files {
        dir: Dir::new(),
        next: AtomicI64::new(1),
        manifests_of: Mutex::new(HashMap::new()),
        rows_of: Mutex::new(HashMap::new()),
    });
    let shared: Shared = Arc::new(Mutex::new(Upstream::default()));
    shared.lock().unwrap().tables.insert(
        "/v1/namespaces/db/tables/customer".into(),
        empty_table("11111111-1111-1111-1111-111111111111"),
    );
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let tokens_issued = Arc::new(Mutex::new(0));

    let issued = Arc::clone(&tokens_issued);
    let token_endpoint = post(move |body: String| {
        let issued = Arc::clone(&issued);
        async move {
            if body.contains("client_id=plane") && body.contains("client_secret=s%26cret") {
                *issued.lock().unwrap() += 1;
                axum::Json(json!({"access_token": "plane-tok", "token_type": "bearer", "expires_in": 3600}))
                    .into_response()
            } else {
                StatusCode::UNAUTHORIZED.into_response()
            }
        }
    });
    let record = Arc::clone(&seen);
    let guard = move |req: Request, next: Next| {
        let record = Arc::clone(&record);
        async move {
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            record.lock().unwrap().push(auth.clone());
            if auth == "Bearer plane-tok" || auth == "Bearer writer-tok" {
                next.run(req).await
            } else {
                StatusCode::UNAUTHORIZED.into_response()
            }
        }
    };
    let up = serve(
        Router::new()
            .fallback(upstream)
            .with_state(Arc::clone(&shared))
            .layer(middleware::from_fn(guard))
            .route("/v1/oauth/tokens", token_endpoint),
    )
    .await;

    let control = files.dir.0.join("control");
    std::fs::create_dir_all(&control).unwrap();
    let gateway = Gateway::new(
        &format!("http://{up}"),
        Duration::from_secs(30),
        Arc::new(ObjectStoreIo::new(
            Default::default(),
            tokio::runtime::Handle::current(),
        )),
        PersistentStore::open(control.join("indexes.redb")).unwrap(),
        TxnLog::open(control.join("txn.redb")).unwrap(),
        1 << 30,
        Registry::open(control.join("registry.redb"), &[]).unwrap(),
    )
    .unwrap()
    .with_admin_token(Some("admin-secret".into()))
    .with_upstream_auth(Some(UpstreamAuth::OAuth2 {
        client_id: "plane".into(),
        client_secret: "s&cret".into(),
        scope: Some("PRINCIPAL_ROLE:ALL".into()),
        token_uri: None,
    }));
    let gw = format!("http://{}", serve(router(Arc::new(gateway))).await);
    let http = reqwest::Client::new();

    // The operator registers a constraint: the Plane reads the table with its own token.
    let r = http
        .post(format!("{gw}/v1/integrity/constraints"))
        .bearer_auth("admin-secret")
        .json(&json!({"table": "db.customer", "name": "pk", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());

    // A writer commits with its own token.
    let commit = |rows: Vec<(i64, Option<i64>)>, parent: Option<i64>| {
        let mut manifests = parent
            .map(|p| files.manifests_of.lock().unwrap()[&p].clone())
            .unwrap_or_default();
        manifests.push(files.manifest(&rows));
        let (id, list) = files.list(&manifests);
        json!({
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent}],
            "updates": [
                {"action": "add-snapshot", "snapshot": {"snapshot-id": id, "parent-snapshot-id": parent,
                    "sequence-number": if parent.is_some() { 2 } else { 1 },
                    "timestamp-ms": id, "manifest-list": list, "summary": {"operation": "append"}}},
                {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": id, "type": "branch"}
            ]
        })
    };
    let r = http
        .post(format!("{gw}/v1/namespaces/db/tables/customer"))
        .bearer_auth("writer-tok")
        .json(&commit(vec![(1, None), (1, None)], None))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "duplicate refused");
    let r = http
        .post(format!("{gw}/v1/namespaces/db/tables/customer"))
        .bearer_auth("writer-tok")
        .json(&commit(vec![(1, None)], None))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let head = r.json::<Value>().await.unwrap()["metadata"]["refs"]["main"]["snapshot-id"].as_i64();
    // Without credentials the upstream refuses the forwarded commit itself.
    let r = http
        .post(format!("{gw}/v1/namespaces/db/tables/customer"))
        .json(&commit(vec![(2, None)], head))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    let r = http
        .get(format!("{gw}/v1/integrity/verify?table=db.customer"))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap();
    let report: Value = r.json().await.unwrap();
    assert_eq!(report["ok"], true, "{report}");

    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter().all(|a| !a.contains("admin-secret")),
        "integrity API token sent upstream: {seen:?}"
    );
    assert!(seen.iter().any(|a| a == "Bearer plane-tok"));
    assert!(seen.iter().any(|a| a == "Bearer writer-tok"));
    assert_eq!(*tokens_issued.lock().unwrap(), 1, "token cached");
}

/// Without `[upstream.auth]` the Plane's own requests carry no credentials: in particular, not the
/// operator's integrity API token.
#[tokio::test(flavor = "multi_thread")]
async fn without_upstream_credentials_operator_headers_stay_local() {
    let files = Files {
        dir: Dir::new(),
        next: AtomicI64::new(1),
        manifests_of: Mutex::new(HashMap::new()),
        rows_of: Mutex::new(HashMap::new()),
    };
    let shared: Shared = Arc::new(Mutex::new(Upstream::default()));
    shared.lock().unwrap().tables.insert(
        "/v1/namespaces/db/tables/customer".into(),
        empty_table("11111111-1111-1111-1111-111111111111"),
    );
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    let recorder = move |req: Request, next: Next| {
        let record = Arc::clone(&record);
        async move {
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            record.lock().unwrap().push(auth);
            next.run(req).await
        }
    };
    let up = serve(
        Router::new()
            .fallback(upstream)
            .with_state(Arc::clone(&shared))
            .layer(middleware::from_fn(recorder)),
    )
    .await;
    let control = files.dir.0.join("control");
    std::fs::create_dir_all(&control).unwrap();
    let gateway = Gateway::new(
        &format!("http://{up}"),
        Duration::from_secs(30),
        Arc::new(ObjectStoreIo::new(
            Default::default(),
            tokio::runtime::Handle::current(),
        )),
        PersistentStore::open(control.join("indexes.redb")).unwrap(),
        TxnLog::open(control.join("txn.redb")).unwrap(),
        1 << 30,
        Registry::open(control.join("registry.redb"), &[]).unwrap(),
    )
    .unwrap()
    .with_admin_token(Some("admin-secret".into()));
    let gw = format!("http://{}", serve(router(Arc::new(gateway))).await);
    let http = reqwest::Client::new();
    for (method, path, body) in [
        (
            "POST",
            "constraints",
            Some(
                json!({"table": "db.customer", "name": "pk", "type": "PRIMARY_KEY", "columns": ["id"]}),
            ),
        ),
        ("POST", "indexes/1/rebuild", None),
        ("GET", "verify?table=db.customer", None),
    ] {
        let url = format!("{gw}/v1/integrity/{path}");
        let req = if method == "POST" {
            http.post(url)
        } else {
            http.get(url)
        };
        let req = match body {
            Some(b) => req.json(&b),
            None => req,
        };
        let r = req.bearer_auth("admin-secret").send().await.unwrap();
        assert!(r.status().is_success(), "{path}: {}", r.status());
    }
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty());
    assert!(
        seen.iter().all(|a| a.is_empty()),
        "operator credentials sent upstream: {seen:?}"
    );
}
