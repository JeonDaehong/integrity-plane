//! The integrity API (spec §19, §20, §23, ADR 0011): registration with an onboarding scan of
//! existing data, violation reports, drop, bypass detection through chain anchors, and rebuild.
//!
//! Tables hold real Parquet files. "Direct" writes go straight to the fake upstream catalog, the
//! way a writer that bypasses the Plane would.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use integrity_index::PersistentStore;
use integrity_server::fileio::ObjectStoreIo;
use integrity_server::store::Registry;
use integrity_server::{Gateway, router};
use integrity_txn::TxnLog;
use serde_json::{Value, json};

mod support;
use support::{Dir, Files, Row, Shared, Upstream, empty_table, upstream};

struct Env {
    http: reqwest::Client,
    gateway: String,
    upstream: String,
    files: Arc<Files>,
    _shared: Shared,
    up: SocketAddr,
    token: Option<String>,
    redact: bool,
    running: Option<Running>,
}

/// A gateway served in-process; stopping it releases its control-store files.
struct Running {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_gateway(
    files: &Files,
    up: SocketAddr,
    token: Option<&str>,
    redact: bool,
    index_file: &str,
    log_file: &str,
) -> (String, Running) {
    let control = files.dir.0.join("control");
    std::fs::create_dir_all(&control).unwrap();
    // After a restart the previous gateway's files can stay locked for a moment (Windows).
    let mut opened = None;
    for _ in 0..100 {
        let attempt = (
            PersistentStore::open(control.join(index_file)),
            TxnLog::open(control.join(log_file)),
            Registry::open(control.join("registry.redb"), &[]),
        );
        if let (Ok(store), Ok(log), Ok(registry)) = attempt {
            opened = Some((store, log, registry));
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (store, log, registry) = opened.expect("control-store files stay locked");
    let gateway = Gateway::new(
        &format!("http://{up}"),
        Duration::from_secs(30),
        Arc::new(ObjectStoreIo::new(
            Default::default(),
            tokio::runtime::Handle::current(),
        )),
        store,
        log,
        1 << 30,
        registry,
    )
    .unwrap()
    .with_admin_token(token.map(str::to_owned))
    .with_redact_keys(redact);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let app = router(Arc::new(gateway));
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    (format!("http://{addr}"), Running { stop, task })
}

async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn env(token: Option<&str>) -> Env {
    env_with(token, false).await
}

async fn env_with(token: Option<&str>, redact: bool) -> Env {
    let files = Arc::new(Files {
        dir: Dir::new(),
        next: AtomicI64::new(1),
        manifests_of: Mutex::new(HashMap::new()),
        rows_of: Mutex::new(HashMap::new()),
    });
    let shared: Shared = Arc::new(Mutex::new(Upstream::default()));
    for (name, uuid) in [
        ("customer", "11111111-1111-1111-1111-111111111111"),
        ("orders", "22222222-2222-2222-2222-222222222222"),
    ] {
        shared.lock().unwrap().tables.insert(
            format!("/v1/namespaces/db/tables/{name}"),
            empty_table(uuid),
        );
    }
    let up = serve(
        Router::new()
            .fallback(upstream)
            .with_state(Arc::clone(&shared)),
    )
    .await;
    let (gateway, running) =
        start_gateway(&files, up, token, redact, "indexes.redb", "txn.redb").await;
    Env {
        http: reqwest::Client::new(),
        gateway,
        upstream: format!("http://{up}"),
        files,
        _shared: shared,
        up,
        token: token.map(str::to_owned),
        redact,
        running: Some(running),
    }
}

impl Env {
    /// Stops the gateway and starts another on the same registry, with the given index store and
    /// transaction log files (a new name plays a file that was deleted and recreated).
    async fn restart(&mut self, index_file: &str, log_file: &str) {
        self.http = reqwest::Client::new(); // drop pooled connections to the old server
        let old = self.running.take().unwrap();
        let _ = old.stop.send(());
        old.task.await.unwrap();
        let (gateway, running) = start_gateway(
            &self.files,
            self.up,
            self.token.as_deref(),
            self.redact,
            index_file,
            log_file,
        )
        .await;
        self.gateway = gateway;
        self.running = Some(running);
    }

    /// Appends `rows` to `table` through `base` (the gateway, or upstream for a bypass).
    async fn append(&self, base: &str, table: &str, rows: &[Row]) -> (u16, Value) {
        self.append_with(base, table, rows, |_| json!({"operation": "append"}))
            .await
    }

    /// Like `append`, with the new snapshot's summary computed from the current metadata.
    async fn append_with(
        &self,
        base: &str,
        table: &str,
        rows: &[Row],
        summary: impl Fn(&Value) -> Value,
    ) -> (u16, Value) {
        let url = format!("{base}/v1/namespaces/db/tables/{table}");
        let meta: Value = self
            .http
            .get(&url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let head = meta["metadata"]["refs"]["main"]["snapshot-id"].as_i64();
        // Sequence numbers as Iceberg assigns them: one more than the table's last.
        let seq = meta["metadata"]["snapshots"].as_array().map_or(0, |s| {
            s.iter()
                .filter_map(|s| s["sequence-number"].as_i64())
                .max()
                .unwrap_or(0)
        }) + 1;
        let mut manifests = head
            .map(|h| self.files.manifests_of.lock().unwrap()[&h].clone())
            .unwrap_or_default();
        manifests.push(self.files.manifest(rows));
        let (id, list) = self.files.list(&manifests);
        let body = json!({
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": head}],
            "updates": [
                {"action": "add-snapshot", "snapshot": {"snapshot-id": id, "parent-snapshot-id": head,
                    "sequence-number": seq, "timestamp-ms": id, "manifest-list": list,
                    "summary": summary(&meta["metadata"])}},
                {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": id, "type": "branch"}
            ]
        });
        let r = self.http.post(&url).json(&body).send().await.unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn through_plane(&self, table: &str, rows: &[Row]) -> (u16, Value) {
        let base = self.gateway.clone();
        self.append(&base, table, rows).await
    }

    async fn bypassing(&self, table: &str, rows: &[Row]) -> (u16, Value) {
        let base = self.upstream.clone();
        self.append(&base, table, rows).await
    }

    async fn api(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        let url = format!("{}/v1/integrity/{path}", self.gateway);
        let req = match method {
            "POST" => self.http.post(url),
            "DELETE" => self.http.delete(url),
            _ => self.http.get(url),
        };
        let req = match body {
            Some(b) => req.json(&b),
            None => req,
        };
        let r = req.send().await.unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn register(&self, body: Value) -> (u16, Value) {
        self.api("POST", "constraints", Some(body)).await
    }

    async fn register_demo(&self) {
        for body in [
            json!({"table": "db.customer", "name": "pk_customer", "type": "PRIMARY_KEY", "columns": ["id"]}),
            json!({"table": "db.orders", "name": "pk_orders", "type": "PRIMARY_KEY", "columns": ["id"]}),
            json!({"table": "db.orders", "name": "fk_orders_customer", "type": "FOREIGN_KEY",
                   "columns": ["ref"], "references": {"table": "db.customer", "constraint": "pk_customer"}}),
        ] {
            let (status, out) = self.register(body).await;
            assert_eq!(status, 200, "{out}");
        }
    }

    async fn audit_kinds(&self) -> Vec<String> {
        let (_, events) = self.api("GET", "audit", None).await;
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["kind"].as_str().unwrap().to_owned())
            .collect()
    }

    async fn status(&self) -> Value {
        self.api("GET", "status", None).await.1
    }
}

fn code(body: &Value) -> String {
    body["error"]["message"]
        .as_str()
        .unwrap_or("")
        .split(' ')
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn onboarding_existing_data_then_enforcing() {
    let e = env(None).await;
    // Data written before the Plane knew about the tables.
    assert_eq!(
        e.bypassing("customer", &[(1, None), (2, None)]).await.0,
        200
    );
    assert_eq!(e.bypassing("orders", &[(10, Some(1))]).await.0, 200);

    e.register_demo().await;
    let (_, listed) = e.api("GET", "constraints?table=db.orders", None).await;
    let names: Vec<&str> = listed["constraints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["pk_orders", "fk_orders_customer"]);
    assert_eq!(
        listed["constraints"][1]["columns"],
        json!([2]),
        "by field id"
    );

    // The indexes reflect the onboarded data.
    let (status, body) = e.through_plane("orders", &[(11, Some(999))]).await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-005"), "{body}");
    let (status, body) = e.through_plane("orders", &[(10, Some(2))]).await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-003"), "{body}");
    let (status, body) = e.through_plane("orders", &[(11, Some(2))]).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body["metadata"]["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["summary"]["integrity.cert"]
            .is_string()
    );
    // The onboarded (uncertified) head was the anchor; later heads are certified.
    let (status, body) = e.through_plane("customer", &[(3, None)]).await;
    assert_eq!(status, 200, "{body}");

    let status = e.status().await;
    assert_eq!(status["constraint_set_versions"]["db.customer"], 2);
    assert_eq!(status["constraint_set_versions"]["db.orders"], 2);
    let kinds = e.audit_kinds().await;
    assert_eq!(
        kinds
            .iter()
            .filter(|k| *k == "CONSTRAINT_REGISTERED")
            .count(),
        3
    );
    assert!(kinds.contains(&"COMMIT_REJECTED".to_owned()));
    assert!(kinds.contains(&"COMMIT_ACCEPTED".to_owned()));
}

#[tokio::test(flavor = "multi_thread")]
async fn violations_in_existing_data_refuse_registration() {
    let e = env(None).await;
    assert_eq!(
        e.bypassing("customer", &[(1, None), (1, None)]).await.0,
        200
    );
    let (status, body) = e
        .register(json!({"table": "db.customer", "name": "pk_customer", "type": "primary_key", "columns": ["id"]}))
        .await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-013"), "{body}");
    assert_eq!(
        body["integrity"]["violations"],
        json!([{"table": "db.customer", "constraint": "pk_customer", "constraint_id": 1,
                "code": "INT-003", "violation_count": 1, "sample_keys": [{"id": 1}]}])
    );
    let (_, listed) = e.api("GET", "constraints", None).await;
    assert_eq!(listed["constraints"], json!([]), "nothing registered");
    // Still unconstrained: commits are forwarded unchecked.
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);

    // An orphan child key refuses the FK, not the parent's PK.
    let e = env(None).await;
    assert_eq!(e.bypassing("customer", &[(1, None)]).await.0, 200);
    assert_eq!(e.bypassing("orders", &[(10, Some(7))]).await.0, 200);
    let (status, _) = e
        .register(json!({"table": "db.customer", "name": "pk_customer", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .await;
    assert_eq!(status, 200);
    let (status, body) = e
        .register(
            json!({"table": "db.orders", "name": "fk", "type": "FOREIGN_KEY", "columns": ["ref"],
                         "references": {"table": "db.customer", "constraint": 1}}),
        )
        .await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-013"), "{body}");
    assert_eq!(body["integrity"]["violations"][0]["code"], "INT-005");
    assert!(
        e.audit_kinds()
            .await
            .contains(&"CONSTRAINT_REJECTED".to_owned())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_registrations_are_refused() {
    let e = env(None).await;
    let cases = [
        (
            json!({"table": "db.customer", "name": "x", "type": "PRIMARY_KEY", "columns": ["nope"]}),
            400,
            "INT-001",
        ),
        (
            json!({"table": "db.missing", "name": "x", "type": "PRIMARY_KEY", "columns": ["id"]}),
            400,
            "INT-001",
        ),
        (
            json!({"table": "db.customer", "name": "x", "type": "CHECK", "columns": ["id"]}),
            400,
            "INT-001",
        ),
        (
            json!({"table": "db.orders", "name": "x", "type": "FOREIGN_KEY", "columns": ["ref"],
                   "references": {"table": "db.customer", "constraint": "pk_customer"}}),
            404,
            "INT-002",
        ),
        (
            json!({"table": "db.customer", "name": "x", "type": "NOT_NULL", "columns": ["id"], "nulls": "DISTINCT"}),
            400,
            "INT-001",
        ),
    ];
    for (body, status, expected) in cases {
        let (s, out) = e.register(body.clone()).await;
        assert_eq!(
            (s, code(&out).as_str()),
            (status, expected),
            "{body} → {out}"
        );
    }
    let (s, _) = e
        .register(
            json!({"table": "db.customer", "name": "pk", "type": "PRIMARY_KEY", "columns": ["id"]}),
        )
        .await;
    assert_eq!(s, 200);
    let (s, out) = e
        .register(
            json!({"table": "db.customer", "name": "pk", "type": "UNIQUE", "columns": ["ref"]}),
        )
        .await;
    assert_eq!((s, code(&out).as_str()), (400, "INT-001"), "duplicate name");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bypass_is_detected_and_a_rebuild_resumes_enforcement() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    assert_eq!(e.through_plane("orders", &[(10, Some(1))]).await.0, 200);

    // A writer appends to the parent without the Plane.
    assert_eq!(e.bypassing("customer", &[(2, None)]).await.0, 200);
    // The next commit anywhere in the domain sees the uncertified head.
    let (status, body) = e.through_plane("orders", &[(11, Some(1))]).await;
    assert_eq!((status, code(&body).as_str()), (423, "INT-015"), "{body}");
    let (status, body) = e.through_plane("customer", &[(3, None)]).await;
    assert_eq!((status, code(&body).as_str()), (423, "INT-010"), "{body}");
    assert!(e.status().await["degraded"]["db.customer"].is_string());
    assert!(
        e.audit_kinds()
            .await
            .contains(&"BYPASS_DETECTED".to_owned())
    );

    // Rebuild rescans the data (customer 2 included) and re-anchors.
    let (status, body) = e.api("POST", "indexes/1/rebuild", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(e.status().await["degraded"], Value::Null);
    let (status, body) = e.through_plane("orders", &[(11, Some(2))]).await;
    assert_eq!(status, 200, "customer 2 is in the rebuilt index: {body}");
    let (status, body) = e.through_plane("customer", &[(2, None)]).await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-003"), "{body}");
    assert!(e.audit_kinds().await.contains(&"INDEX_REBUILT".to_owned()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_over_violating_data_keeps_the_domain_degraded() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    assert_eq!(e.bypassing("customer", &[(1, None)]).await.0, 200);
    assert_eq!(e.through_plane("customer", &[(5, None)]).await.0, 423);

    let (status, body) = e.api("POST", "indexes/1/rebuild", None).await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-013"), "{body}");
    assert_eq!(body["integrity"]["violations"][0]["code"], "INT-003");
    let (status, body) = e.through_plane("orders", &[(10, Some(1))]).await;
    assert_eq!((status, code(&body).as_str()), (423, "INT-010"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_constraints() {
    let e = env(None).await;
    e.register_demo().await;
    let (status, body) = e.api("DELETE", "constraints/1", None).await;
    assert_eq!(
        (status, code(&body).as_str()),
        (400, "INT-001"),
        "referenced by the FK: {body}"
    );
    let (status, body) = e.api("DELETE", "constraints/99", None).await;
    assert_eq!((status, code(&body).as_str()), (404, "INT-002"));

    assert_eq!(e.api("DELETE", "constraints/3", None).await.0, 200);
    assert_eq!(e.api("DELETE", "constraints/1", None).await.0, 200);
    // customer is unconstrained now: anything goes, uncertified.
    assert_eq!(
        e.through_plane("customer", &[(1, None), (1, None)]).await.0,
        200
    );
    // orders keeps its PK and no longer checks the FK.
    assert_eq!(e.through_plane("orders", &[(10, Some(999))]).await.0, 200);
    let status = e.status().await;
    assert_eq!(status["constraint_set_versions"]["db.customer"], 4);
    assert_eq!(status["constraint_set_versions"]["db.orders"], 3);
    assert!(
        status["bound_tables"]["db.customer"].is_null(),
        "re-registering onboards again"
    );
    // Re-registering the PK onboards customer and finds the duplicate.
    let (status, body) = e
        .register(json!({"table": "db.customer", "name": "pk_customer", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-013"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_api_requires_the_admin_token_when_configured() {
    let e = env(Some("s3cret")).await;
    let (status, _) = e.api("GET", "constraints", None).await;
    assert_eq!(status, 401);
    let (status, _) = e.api("GET", "status", None).await;
    assert_eq!(status, 200, "status stays open");
    let r = e
        .http
        .get(format!("{}/v1/integrity/constraints", e.gateway))
        .bearer_auth("s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

fn head_of(meta: &Value) -> i64 {
    meta["metadata"]["refs"]["main"]["snapshot-id"]
        .as_i64()
        .unwrap()
}

fn statuses(report: &Value) -> Vec<&str> {
    report["chain"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["status"].as_str().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_pinpoints_the_snapshot_written_around_the_plane() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    assert_eq!(e.through_plane("orders", &[(10, Some(1))]).await.0, 200);
    assert_eq!(e.through_plane("orders", &[(11, Some(1))]).await.0, 200);
    let (_, report) = e.api("GET", "verify?table=db.orders", None).await;
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(statuses(&report), ["OK", "OK"]);

    let (status, bypass) = e.bypassing("orders", &[(12, Some(999))]).await;
    assert_eq!(status, 200);
    let bypassed = head_of(&bypass);
    let (status, report) = e.api("GET", "verify?table=db.orders", None).await;
    assert_eq!(status, 200);
    assert_eq!(report["ok"], false);
    assert_eq!(report["first_broken"], bypassed, "{report}");
    assert_eq!(statuses(&report), ["OK", "OK", "MISSING"]);
    // Verification found a bypass: the domain stops accepting commits until rebuilt.
    let (_, domain) = e.api("GET", "domains/db.customer", None).await;
    assert_eq!(domain["state"], "Degraded", "{domain}");
    assert_eq!(domain["tables"], json!(["db.customer", "db.orders"]));
    assert!(
        e.audit_kinds()
            .await
            .contains(&"BYPASS_DETECTED".to_owned())
    );
    let (_, customer) = e.api("GET", "verify?table=db.customer", None).await;
    assert_eq!(customer["ok"], true, "{customer}");
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_recomputes_certificates_instead_of_trusting_them() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    // A bypassing writer copies the head's certificate fields into its own snapshot.
    let (status, forged) = e
        .append_with(&e.upstream.clone(), "customer", &[(2, None)], |meta| {
            let head = meta["refs"]["main"]["snapshot-id"].clone();
            let snap = meta["snapshots"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["snapshot-id"] == head)
                .unwrap()
                .clone();
            let mut summary = snap["summary"].clone();
            summary["operation"] = json!("append");
            summary
        })
        .await;
    assert_eq!(status, 200);
    // The head looks certified, so commit-time checks let the next commit through ...
    let (status, body) = e.through_plane("customer", &[(3, None)]).await;
    assert_eq!(status, 200, "{body}");
    // ... but verify recomputes every certificate from the data files.
    let (_, report) = e.api("GET", "verify?table=db.customer", None).await;
    assert_eq!(statuses(&report), ["OK", "MISMATCH", "OK"], "{report}");
    assert_eq!(report["first_broken"], head_of(&forged));
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_follows_onboarding_anchors_and_constraint_set_versions() {
    let e = env(None).await;
    assert_eq!(e.bypassing("customer", &[(1, Some(5))]).await.0, 200);
    let (s, _) = e
        .register(
            json!({"table": "db.customer", "name": "pk", "type": "PRIMARY_KEY", "columns": ["id"]}),
        )
        .await;
    assert_eq!(s, 200);
    assert_eq!(e.through_plane("customer", &[(2, Some(6))]).await.0, 200);
    // Version 2 adds a UNIQUE on another column; the chain continues across versions.
    let (s, _) = e
        .register(
            json!({"table": "db.customer", "name": "uq_ref", "type": "UNIQUE", "columns": ["ref"]}),
        )
        .await;
    assert_eq!(s, 200);
    let (status, body) = e.through_plane("customer", &[(3, Some(6))]).await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-004"), "{body}");
    assert_eq!(e.through_plane("customer", &[(3, Some(7))]).await.0, 200);
    let (_, report) = e.api("GET", "verify?table=db.customer", None).await;
    assert_eq!(statuses(&report), ["ANCHOR", "OK", "OK"], "{report}");
    assert_eq!(report["ok"], true);
    let (s, _) = e.api("GET", "verify?table=db.orders", None).await;
    assert_eq!(s, 404, "never constrained");
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_domains_and_metrics() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    assert_eq!(e.through_plane("orders", &[(10, Some(9))]).await.0, 400);

    let (_, audit) = e.api("GET", "audit?table=db.orders", None).await;
    let rejected = audit["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ev| ev["kind"] == "COMMIT_REJECTED")
        .unwrap()
        .clone();
    assert_eq!(rejected["verdict"], "INT-005");
    let (status, txn) = e
        .api("GET", &format!("transactions/{}", rejected["txn"]), None)
        .await;
    assert_eq!(status, 200);
    assert_eq!(txn["state"], "REJECTED");
    assert_eq!(txn["table"], "db.orders");
    assert_eq!(txn["decision"]["status"], 400);
    assert!(
        txn["decision"]["body"]["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("INT-005")
    );
    assert_eq!(e.api("GET", "transactions/999999", None).await.0, 404);

    let (_, domain) = e.api("GET", "domains/db.orders", None).await;
    assert_eq!(domain["state"], "Healthy");
    assert_eq!(domain["id"], "db.customer");
    assert_eq!(domain["constraints"], json!([1, 2, 3]));
    assert_eq!(e.api("GET", "domains/db.nothing", None).await.0, 404);

    let text = e
        .http
        .get(format!("{}/metrics", e.gateway))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        text.contains("integrity_commits_total{verdict=\"accepted\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains("integrity_commits_total{verdict=\"rejected\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains("integrity_domain_state{state=\"healthy\"} 2\n"),
        "{text}"
    );
    assert!(
        text.contains("integrity_domain_queue_wait_seconds_count 2\n"),
        "{text}"
    );
    assert!(
        text.contains("integrity_keys_validated_total 2\n"),
        "{text}"
    );
    assert!(
        text.contains("integrity_index_probe_seconds_count 2\n"),
        "{text}"
    );
    let bytes: u64 = text
        .lines()
        .find_map(|l| l.strip_prefix("integrity_bytes_read_total "))
        .unwrap()
        .parse()
        .unwrap();
    assert!(bytes > 0, "{text}");
}

async fn probe_verdicts(e: &Env) -> Vec<(u16, String)> {
    let probes: [(&str, &[Row]); 4] = [
        ("customer", &[(1, None)]),
        ("orders", &[(12, Some(9))]),
        ("orders", &[(10, Some(1))]),
        ("orders", &[(12, Some(2)), (12, Some(1))]),
    ];
    let mut out = Vec::new();
    for (table, rows) in probes {
        let (status, body) = e.through_plane(table, rows).await;
        out.push((status, code(&body)));
    }
    out
}

/// Spec §27 DoD 6: delete the index store, rebuild, identical verdicts. A recreated (empty) index
/// store must not be trusted: it would accept every duplicate.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_index_store_is_refused_until_rebuilt_with_identical_verdicts() {
    let mut e = env(None).await;
    e.register_demo().await;
    assert_eq!(
        e.through_plane("customer", &[(1, None), (2, None)]).await.0,
        200
    );
    assert_eq!(
        e.through_plane("orders", &[(10, Some(1)), (11, Some(2))])
            .await
            .0,
        200
    );
    let before = probe_verdicts(&e).await;
    assert!(before.iter().all(|(s, _)| *s == 400), "{before:?}");

    e.restart("indexes-recreated.redb", "txn.redb").await;
    let (status, body) = e.through_plane("customer", &[(1, None)]).await;
    assert_eq!((status, code(&body).as_str()), (423, "INT-010"), "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("index store was replaced"),
        "{body}"
    );

    let (status, body) = e.api("POST", "indexes/1/rebuild", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(probe_verdicts(&e).await, before);
    assert_eq!(e.through_plane("customer", &[(3, None)]).await.0, 200);

    // Restarting with the rebuilt store is fine.
    e.restart("indexes-recreated.redb", "txn.redb").await;
    assert_eq!(e.through_plane("customer", &[(4, None)]).await.0, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_transaction_log_is_refused_until_rebuilt() {
    let mut e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    e.restart("indexes.redb", "txn-recreated.redb").await;
    let (status, body) = e.through_plane("customer", &[(2, None)]).await;
    assert_eq!((status, code(&body).as_str()), (423, "INT-010"), "{body}");
    assert_eq!(e.api("POST", "indexes/1/rebuild", None).await.0, 200);
    assert_eq!(e.through_plane("customer", &[(2, None)]).await.0, 200);
}

fn txn_of_rejection(body: &Value) -> u64 {
    body["error"]["message"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

/// Spec §22/§24: a rejected commit names the violation count and its transaction in the Iceberg
/// error message; the structured error with sample keys is served by the integrity API.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_commits_have_a_structured_error() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    let rows: Vec<Row> = (0..12).map(|i| (100 + i, Some(900 + i % 11))).collect();
    let (status, body) = e.through_plane("orders", &rows).await;
    assert_eq!(status, 400);
    let message = body["error"]["message"].as_str().unwrap();
    assert!(
        message.starts_with("INT-005 FOREIGN_KEY_VIOLATION: commit rejected: INT-005 on fk_orders_customer (11 keys); details: GET /v1/integrity/transactions/"),
        "{message}"
    );
    assert!(
        body.get("integrity").is_none(),
        "no key values in the Iceberg error body"
    );
    let (_, txn) = e
        .api(
            "GET",
            &format!("transactions/{}", txn_of_rejection(&body)),
            None,
        )
        .await;
    let error = &txn["error"];
    assert_eq!(error["code"], "INT-005");
    assert_eq!(error["transaction_id"], txn["txn"]);
    let v = &error["violations"][0];
    assert_eq!(v["constraint"], "fk_orders_customer");
    assert_eq!(v["table"], "db.orders");
    assert_eq!(v["violation_count"], 11);
    let samples = v["sample_keys"].as_array().unwrap();
    assert_eq!(samples.len(), 10);
    assert_eq!(samples[0], json!({"ref": 900}));

    // Duplicates and the parent side of a referenced delete.
    let (_, body) = e
        .through_plane("customer", &[(1, None), (2, None), (2, None)])
        .await;
    let (_, txn) = e
        .api(
            "GET",
            &format!("transactions/{}", txn_of_rejection(&body)),
            None,
        )
        .await;
    assert_eq!(
        txn["error"]["violations"][0]["sample_keys"],
        json!([{"id": 2}, {"id": 1}])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn redacted_reports_keep_counts_but_no_key_values() {
    let e = env_with(None, true).await;
    e.register_demo().await;
    let (status, body) = e
        .through_plane("orders", &[(1, Some(7)), (2, Some(8))])
        .await;
    assert_eq!(status, 400);
    let (_, txn) = e
        .api(
            "GET",
            &format!("transactions/{}", txn_of_rejection(&body)),
            None,
        )
        .await;
    let v = &txn["error"]["violations"][0];
    assert_eq!(v["violation_count"], 2);
    assert_eq!(v["sample_keys_redacted"], true);
    assert!(v.get("sample_keys").is_none());
    let (_, audit) = e.api("GET", "audit", None).await;
    assert!(
        !audit.to_string().contains("\"ref\""),
        "no key values stored: {audit}"
    );
}

/// Spec §19: an operator can disable a domain (e.g. a degraded one that must accept writes);
/// its commits are then forwarded unchecked and uncertified, audited, until a rebuild.
#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_domain_forwards_unchecked_until_rebuilt() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    let (status, _) = e
        .api("POST", "domains/db.orders/disable", Some(json!({})))
        .await;
    assert_eq!(status, 400, "a reason is required");
    let (status, body) = e
        .api(
            "POST",
            "domains/db.orders/disable",
            Some(json!({"reason": "incident 42"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["domain"], json!(["db.customer", "db.orders"]));

    // Violations go through, without certificates.
    let (status, body) = e.through_plane("orders", &[(10, Some(999))]).await;
    assert_eq!(status, 200, "{body}");
    let snaps = body["metadata"]["snapshots"].as_array().unwrap();
    assert!(snaps.last().unwrap()["summary"]["integrity.cert"].is_null());
    let (_, domain) = e.api("GET", "domains/db.orders", None).await;
    assert_eq!(domain["state"], "Disabled");
    let kinds = e.audit_kinds().await;
    assert!(kinds.contains(&"DOMAIN_DISABLED".to_owned()));
    assert!(kinds.contains(&"COMMIT_UNCHECKED".to_owned()));

    // Rebuilding validates what was written meanwhile: the orphan keeps it degraded...
    let (status, body) = e.api("POST", "indexes/1/rebuild", None).await;
    assert_eq!((status, code(&body).as_str()), (400, "INT-013"), "{body}");
    let (_, domain) = e.api("GET", "domains/db.orders", None).await;
    assert_eq!(
        domain["state"], "Disabled",
        "still disabled, nothing re-enabled"
    );
    // ... until the data is fixed.
    assert_eq!(e.bypassing("customer", &[(999, None)]).await.0, 200);
    assert_eq!(e.api("POST", "indexes/1/rebuild", None).await.0, 200);
    let (_, domain) = e.api("GET", "domains/db.orders", None).await;
    assert_eq!(domain["state"], "Healthy");
    let (status, body) = e.through_plane("orders", &[(11, Some(5))]).await;
    assert_eq!(
        (status, code(&body).as_str()),
        (400, "INT-005"),
        "enforced again"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn audit_events_name_their_actor() {
    let e = env(None).await;
    let r = e
        .http
        .post(format!("{}/v1/integrity/constraints", e.gateway))
        .header("x-integrity-actor", "alice")
        .json(&json!({"table": "db.customer", "name": "pk", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    let (_, audit) = e.api("GET", "audit", None).await;
    let actor = |kind: &str| {
        audit["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|ev| ev["kind"] == kind)
            .unwrap()["actor"]
            .clone()
    };
    assert_eq!(actor("CONSTRAINT_REGISTERED"), "alice");
    // Without the header, the client's User-Agent (reqwest sends none by default: "unknown").
    assert!(actor("COMMIT_ACCEPTED").is_string());
}

/// A catalog that serves only recent snapshots (Nessie) or expired history: `verify` cannot
/// recompute a certificate without the parent, and must say so instead of reporting a bypass.
#[tokio::test(flavor = "multi_thread")]
async fn verify_without_the_parent_snapshot_is_unverifiable_not_broken() {
    let e = env(None).await;
    e.register_demo().await;
    assert_eq!(e.through_plane("customer", &[(1, None)]).await.0, 200);
    let (_, body) = e.through_plane("customer", &[(2, None)]).await;
    let head = head_of(&body);
    // Trim the catalog's metadata to the head snapshot without its parent link, as Nessie does.
    {
        let mut up = e._shared.lock().unwrap();
        let meta = up
            .tables
            .get_mut("/v1/namespaces/db/tables/customer")
            .unwrap();
        let snaps = meta["snapshots"].as_array_mut().unwrap();
        snaps.retain(|s| s["snapshot-id"] == head);
        snaps[0]
            .as_object_mut()
            .unwrap()
            .remove("parent-snapshot-id");
    }
    let (_, report) = e.api("GET", "verify?table=db.customer", None).await;
    assert_eq!(statuses(&report), ["UNVERIFIABLE"], "{report}");
    assert_eq!(report["first_broken"], Value::Null);
    let (_, domain) = e.api("GET", "domains/db.customer", None).await;
    assert_eq!(domain["state"], "Healthy", "no false alarm");
}
