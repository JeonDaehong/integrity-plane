//! The gateway against an in-process fake upstream catalog, replaying the real PyIceberg table of
//! `integrity-iceberg/tests/fixtures`: valid commits are certified and forwarded; violations,
//! stale bases and unsupported endpoints are answered by the Plane without reaching upstream.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apache_avro::types::Value as Avro;
use apache_avro::{Codec, DeflateSettings, Schema, Writer};
use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use integrity_iceberg::manifest::{read_manifest, read_manifest_list};
use integrity_iceberg::{FileIo, MemoryIo, ReadError};
use integrity_index::PersistentStore;
use integrity_server::config::ConstraintConfig;
use integrity_server::store::Registry;
use integrity_server::{Gateway, router};
use serde_json::{Value, json};

const TABLE_PATH: &str = "/v1/namespaces/db/tables/orders";

fn fixture_dir() -> String {
    format!(
        "{}/../integrity-iceberg/tests/fixtures/table",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// Fixture files by `…/warehouse/<rest>`, plus files written by the test.
struct TestIo {
    extra: MemoryIo,
}

impl FileIo for TestIo {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        if let Ok(b) = self.extra.read(location) {
            return Ok(b);
        }
        let rest = location
            .split_once("/warehouse/")
            .map(|(_, r)| r)
            .ok_or_else(|| ReadError::Io(location.to_owned()))?;
        std::fs::read(format!("{}/warehouse/{rest}", fixture_dir()))
            .map(Bytes::from)
            .map_err(|e| ReadError::Io(e.to_string()))
    }
}

fn metadata_file(name: &str) -> Value {
    serde_json::from_slice(
        &std::fs::read(format!(
            "{}/warehouse/db/orders/metadata/{name}",
            fixture_dir()
        ))
        .unwrap(),
    )
    .unwrap()
}

fn main_commits() -> Vec<Value> {
    let list: Value =
        serde_json::from_slice(&std::fs::read(format!("{}/commits.json", fixture_dir())).unwrap())
            .unwrap();
    list.as_array()
        .unwrap()
        .iter()
        .filter(|c| c["branch"] == "main")
        .map(|c| metadata_file(c["metadata"].as_str().unwrap()))
        .collect()
}

// ---------- fake upstream ----------

#[derive(Default)]
struct Upstream {
    tables: BTreeMap<String, Value>,
    commits: Vec<(String, Value)>,
}

type Shared = Arc<Mutex<Upstream>>;

async fn upstream_handler(State(s): State<Shared>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let body = axum::body::to_bytes(req.into_body(), 1 << 26)
        .await
        .unwrap();
    let mut up = s.lock().unwrap();
    if path == "/v1/config" {
        return axum::Json(json!({
            "defaults": {"uri": "http://upstream:8181"},
            "overrides": {"uri": "http://upstream:8181", "warehouse": "w"},
            "idempotency-key-lifetime": "PT30M"
        }))
        .into_response();
    }
    let Some(meta) = up.tables.get(&path).cloned() else {
        return (StatusCode::NOT_FOUND, axum::Json(json!({"error": {"message": "no table", "type": "NoSuchTableException", "code": 404}}))).into_response();
    };
    match method.as_str() {
        "GET" => {
            axum::Json(json!({"metadata-location": "m.json", "metadata": meta})).into_response()
        }
        "POST" => {
            let req: Value = serde_json::from_slice(&body).unwrap();
            up.commits.push((path.clone(), req.clone()));
            let mut meta = meta;
            for r in req["requirements"].as_array().unwrap() {
                if r["type"] == "assert-ref-snapshot-id" && r["ref"] == "main" {
                    let current = meta["refs"]["main"]["snapshot-id"].clone();
                    if current != r["snapshot-id"] {
                        return (StatusCode::CONFLICT, axum::Json(json!({"error": {"message": "stale", "type": "CommitFailedException", "code": 409}}))).into_response();
                    }
                }
            }
            for u in req["updates"].as_array().unwrap() {
                match u["action"].as_str().unwrap() {
                    "add-snapshot" => meta["snapshots"]
                        .as_array_mut()
                        .unwrap()
                        .push(u["snapshot"].clone()),
                    "set-snapshot-ref" => {
                        let name = u["ref-name"].as_str().unwrap().to_owned();
                        if !meta["refs"].is_object() {
                            meta["refs"] = json!({});
                        }
                        meta["refs"][&name] =
                            json!({"snapshot-id": u["snapshot-id"], "type": u["type"]});
                        if name == "main" {
                            meta["current-snapshot-id"] = u["snapshot-id"].clone();
                        }
                    }
                    _ => {}
                }
            }
            up.tables.insert(path, meta.clone());
            axum::Json(json!({"metadata-location": "m.json", "metadata": meta})).into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn constraints() -> Vec<ConstraintConfig> {
    let c = |id, name: &str, kind: &str, columns: Vec<i32>| ConstraintConfig {
        id,
        table: "db.orders".into(),
        name: name.into(),
        kind: kind.into(),
        columns,
        nulls: None,
        references: None,
        match_mode: None,
        column_names: None,
    };
    vec![
        c(1, "pk_orders", "primary_key", vec![1]),
        c(2, "uq_amount", "unique", vec![4]),
    ]
}

struct Harness {
    client: reqwest::Client,
    gateway: String,
    upstream: Shared,
    _dir: tempdir::Dir,
}

mod tempdir {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub struct Dir(pub std::path::PathBuf);
    impl Dir {
        pub fn new() -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            // Parallel tests can read the same clock value; the counter keeps directories apart.
            let p = std::env::temp_dir().join(format!(
                "oip-gateway-{}-{nanos}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

async fn harness(extra: MemoryIo) -> Harness {
    let commits = main_commits();
    let upstream: Shared = Arc::new(Mutex::new(Upstream::default()));
    {
        let mut up = upstream.lock().unwrap();
        up.tables.insert(TABLE_PATH.into(), commits[0].clone());
        up.tables
            .insert("/v1/namespaces/db/tables/free".into(), commits[0].clone());
    }
    let up_addr = serve(
        Router::new()
            .fallback(upstream_handler)
            .with_state(Arc::clone(&upstream)),
    )
    .await;
    let dir = tempdir::Dir::new();
    let store = PersistentStore::open(dir.0.join("indexes.redb")).unwrap();
    let log = integrity_txn::TxnLog::open(dir.0.join("txn.redb")).unwrap();
    let gateway = Gateway::new(
        &format!("http://{up_addr}"),
        Duration::from_secs(10),
        Arc::new(TestIo { extra }),
        store,
        log,
        1 << 30,
        Registry::open(dir.0.join("registry.redb"), &constraints()).unwrap(),
    )
    .unwrap();
    let gw_addr = serve(router(Arc::new(gateway))).await;
    Harness {
        client: reqwest::Client::new(),
        gateway: format!("http://{gw_addr}"),
        upstream,
        _dir: dir,
    }
}

/// The request that turns `before` into `after` on main.
fn request(before: &Value, after: &Value) -> Value {
    let known: BTreeSet<i64> = before["snapshots"]
        .as_array()
        .map_or_else(BTreeSet::new, |s| {
            s.iter()
                .map(|s| s["snapshot-id"].as_i64().unwrap())
                .collect()
        });
    let mut updates: Vec<Value> = after["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| !known.contains(&s["snapshot-id"].as_i64().unwrap()))
        .map(|s| json!({"action": "add-snapshot", "snapshot": s}))
        .collect();
    updates.sort_by_key(|u| u["snapshot"]["sequence-number"].as_i64());
    updates.push(json!({"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": after["refs"]["main"]["snapshot-id"], "type": "branch"}));
    json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": before["refs"]["main"]["snapshot-id"]}],
        "updates": updates
    })
}

impl Harness {
    async fn post(&self, path: &str, body: &Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.gateway))
            .json(body)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    fn upstream_commits(&self) -> usize {
        self.upstream.lock().unwrap().commits.len()
    }

    fn upstream_table(&self) -> Value {
        self.upstream.lock().unwrap().tables[TABLE_PATH].clone()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_commits_are_certified_and_forwarded() {
    let h = harness(MemoryIo::new()).await;
    let commits = main_commits();
    for pair in commits.windows(2) {
        let (status, body) = h.post(TABLE_PATH, &request(&pair[0], &pair[1])).await;
        assert_eq!(status, 200, "{body}");
    }
    // Every snapshot on main now carries a certificate, chained.
    let meta = h.upstream_table();
    let certified: Vec<&Value> = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["summary"]["integrity.cert"].is_string())
        .collect();
    assert_eq!(certified.len(), 6);
    assert!(
        certified
            .iter()
            .all(|s| s["summary"]["integrity.cert-version"] == "1")
    );
    assert!(
        certified
            .iter()
            .all(|s| s["summary"]["operation"].is_string()),
        "client fields kept"
    );

    let status: Value = h
        .client
        .get(format!("{}/v1/integrity/status", h.gateway))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["degraded"], Value::Null);
    assert_eq!(
        status["commit_requests"]["db.orders"],
        commits.len() as u64 - 1
    );
    assert!(status["bound_tables"]["db.orders"].is_string());
}

/// Builds a commit on top of `parent` (fixture metadata) that re-lists the data file of the first
/// append in a new manifest: readers would see its rows twice.
fn relisting_commit(parent: &Value, io: &mut MemoryIo) -> Value {
    let test_io = TestIo {
        extra: MemoryIo::new(),
    };
    let head = parent["refs"]["main"]["snapshot-id"].as_i64().unwrap();
    let snapshot = parent["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["snapshot-id"] == head)
        .unwrap();
    let list_path = snapshot["manifest-list"].as_str().unwrap();
    let manifests = read_manifest_list(&test_io.read(list_path).unwrap()).unwrap();
    let file = read_manifest(&test_io.read(&manifests[0].path).unwrap()).unwrap()[0]
        .file
        .clone();

    let entry_schema = Schema::parse_str(r#"{"type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int"},
        {"name": "snapshot_id", "type": ["null", "long"]},
        {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
            {"name": "content", "type": "int"}, {"name": "file_path", "type": "string"},
            {"name": "file_format", "type": "string"}, {"name": "record_count", "type": "long"}]}}]}"#).unwrap();
    let mut w = Writer::with_codec(
        &entry_schema,
        Vec::new(),
        Codec::Deflate(DeflateSettings::default()),
    )
    .unwrap();
    w.append_value(Avro::Record(vec![
        ("status".into(), Avro::Int(1)),
        (
            "snapshot_id".into(),
            Avro::Union(1, Box::new(Avro::Long(999))),
        ),
        (
            "data_file".into(),
            Avro::Record(vec![
                ("content".into(), Avro::Int(0)),
                ("file_path".into(), Avro::String(file.path.clone())),
                ("file_format".into(), Avro::String("PARQUET".into())),
                ("record_count".into(), Avro::Long(file.record_count)),
            ]),
        ),
    ]))
    .unwrap();
    io.insert("mem://relist-manifest.avro", w.into_inner().unwrap());

    let list_schema = Schema::parse_str(
        r#"{"type": "record", "name": "manifest_file", "fields": [
        {"name": "manifest_path", "type": "string"}, {"name": "content", "type": "int"},
        {"name": "added_snapshot_id", "type": "long"}]}"#,
    )
    .unwrap();
    let mut w = Writer::with_codec(
        &list_schema,
        Vec::new(),
        Codec::Deflate(DeflateSettings::default()),
    )
    .unwrap();
    for m in manifests
        .iter()
        .map(|m| m.path.clone())
        .chain(["mem://relist-manifest.avro".to_owned()])
    {
        w.append_value(Avro::Record(vec![
            ("manifest_path".into(), Avro::String(m)),
            ("content".into(), Avro::Int(0)),
            ("added_snapshot_id".into(), Avro::Long(999)),
        ]))
        .unwrap();
    }
    io.insert("mem://relist-list.avro", w.into_inner().unwrap());

    json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": head}],
        "updates": [
            {"action": "add-snapshot", "snapshot": {"snapshot-id": 999, "parent-snapshot-id": head,
                "sequence-number": 99, "timestamp-ms": 1, "manifest-list": "mem://relist-list.avro",
                "summary": {"operation": "append"}}},
            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 999, "type": "branch"}
        ]
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn violations_are_rejected_without_reaching_upstream() {
    let commits = main_commits();
    let mut extra = MemoryIo::new();
    let bad = relisting_commit(&commits[1], &mut extra);
    let h = harness(extra).await;

    let (status, _) = h.post(TABLE_PATH, &request(&commits[0], &commits[1])).await;
    assert_eq!(status, 200);
    let forwarded = h.upstream_commits();

    let (status, body) = h.post(TABLE_PATH, &bad).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["type"], "IntegrityViolationException");
    let message = body["error"]["message"].as_str().unwrap();
    assert!(
        message.starts_with("INT-003 DUPLICATE_PRIMARY_KEY"),
        "{message}"
    );
    assert!(
        message.contains("pk_orders") && message.contains("uq_amount"),
        "{message}"
    );
    assert_eq!(
        h.upstream_commits(),
        forwarded,
        "a rejected commit is never forwarded"
    );

    // The table is unchanged: the next real commit still applies.
    let (status, body) = h.post(TABLE_PATH, &request(&commits[1], &commits[2])).await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_base_and_unsupported_endpoints() {
    let h = harness(MemoryIo::new()).await;
    let commits = main_commits();
    let (status, _) = h.post(TABLE_PATH, &request(&commits[0], &commits[1])).await;
    assert_eq!(status, 200);

    // Replaying the same commit: its base is no longer current.
    let (status, body) = h.post(TABLE_PATH, &request(&commits[0], &commits[1])).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["type"], "CommitFailedException");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("INT-009")
    );

    let (status, body) = h
        .post("/v1/transactions/commit", &json!({"table-changes": []}))
        .await;
    assert_eq!(status, 400);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("INT-012")
    );

    let (status, body) = h.post(TABLE_PATH, &json!({"no": "updates"})).await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn unconstrained_tables_and_config_are_proxied() {
    let h = harness(MemoryIo::new()).await;
    let commits = main_commits();
    // `db.free` has no constraints: forwarded as is, no certificate.
    let before = h.upstream_commits();
    let (status, _) = h
        .post(
            "/v1/namespaces/db/tables/free",
            &request(&commits[0], &commits[1]),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(h.upstream_commits(), before + 1);
    {
        let up = h.upstream.lock().unwrap();
        let (_, forwarded) = up.commits.last().unwrap();
        assert!(forwarded["updates"][0]["snapshot"]["summary"]["integrity.cert"].is_null());
    }

    let config: Value = h
        .client
        .get(format!("{}/v1/config", h.gateway))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        config["overrides"]["uri"].is_null(),
        "clients must not be sent around the Plane"
    );
    assert!(config["defaults"]["uri"].is_null());
    assert_eq!(config["overrides"]["warehouse"], "w");
    assert_eq!(
        config["idempotency-key-lifetime"], "PT30M",
        "idempotency keys are honoured"
    );

    let r = h
        .client
        .get(format!("{}{TABLE_PATH}", h.gateway))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "loads are proxied");
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_idempotency_keys_return_the_recorded_decision() {
    let commits = main_commits();
    let mut extra = MemoryIo::new();
    let bad = relisting_commit(&commits[1], &mut extra);
    let h = harness(extra).await;
    let post = |body: &Value, key: &str| {
        h.client
            .post(format!("{}{TABLE_PATH}", h.gateway))
            .header("Idempotency-Key", key)
            .json(body)
            .send()
    };

    let first = post(&request(&commits[0], &commits[1]), "k-1")
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    let first: Value = first.json().await.unwrap();
    let forwarded = h.upstream_commits();

    // The same key again: the recorded response, not a stale-base rejection, and nothing forwarded.
    let again = post(&request(&commits[0], &commits[1]), "k-1")
        .await
        .unwrap();
    assert_eq!(again.status(), 200);
    assert_eq!(again.json::<Value>().await.unwrap(), first);
    assert_eq!(h.upstream_commits(), forwarded);

    // Rejections are recorded too.
    let rejected = post(&bad, "k-2").await.unwrap();
    assert_eq!(rejected.status(), 400);
    let body: Value = rejected.json().await.unwrap();
    let again = post(&bad, "k-2").await.unwrap();
    assert_eq!(again.status(), 400);
    assert_eq!(again.json::<Value>().await.unwrap(), body);
}

/// Spec §28: malformed REST payloads to a constrained table never panic the gateway or produce a
/// 5xx; the gateway keeps serving afterwards. Mutations of a real commit request plus raw garbage.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_commit_payloads_get_4xx_and_the_gateway_survives() {
    let h = harness(MemoryIo::new()).await;
    let commits = main_commits();
    let valid = serde_json::to_vec(&request(&commits[0], &commits[1])).unwrap();
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for round in 0..300 {
        let mut body = valid.clone();
        match round % 3 {
            0 => {
                for _ in 0..1 + next() % 8 {
                    let i = (next() as usize) % body.len();
                    body[i] = (next() % 256) as u8;
                }
            }
            1 => body.truncate((next() as usize) % body.len()),
            _ => {
                body = (0..next() % 64).map(|_| (next() % 256) as u8).collect();
            }
        }
        let r = h
            .client
            .post(format!("{}{TABLE_PATH}", h.gateway))
            .body(body.clone())
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        assert!(
            status < 500,
            "round {round}: {status} for {:?}",
            String::from_utf8_lossy(&body)
        );
    }
    // Some mutations are still valid commits and were accepted; the gateway must simply still be
    // serving and healthy.
    let status: Value = h
        .client
        .get(format!("{}/v1/integrity/status", h.gateway))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["degraded"], Value::Null, "{status}");
    assert_eq!(status["unresolved_transactions"], 0);
}
