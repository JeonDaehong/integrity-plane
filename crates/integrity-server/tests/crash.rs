//! Phase 8 exit criterion: kill the gateway at every fault point (spec §28), restart it, and check
//! that no invalid state was committed and no accepted commit was lost: afterwards the persistent
//! indexes hold exactly the keys of the snapshots the upstream table contains, no transaction is
//! left unresolved, and the next commit succeeds.
//!
//! The gateway runs as a separate process (the `integrity-server` binary built with the
//! `fault-injection` feature); `OIP_FAULT` makes it abort at one point. The upstream catalog is an
//! in-process fake whose answer to commits can be forced to "500 after applying" or "500 without
//! applying". Data files and manifests are written by the test.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apache_avro::types::Value as Avro;
use apache_avro::{Codec, DeflateSettings, Schema, Writer};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field};
use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use integrity_core::{EncodedKey, KeySchema, KeyValue, TypeFamily};
use integrity_index::{IndexKind, KeyIndex, PersistentStore};
use integrity_txn::{FaultPoint, TxnLog};
use integrity_types::ConstraintId;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};

const TABLE_PATH: &str = "/v1/namespaces/db/tables/t";
const UUID: &str = "11111111-2222-4333-8444-555555555555";

// ---------- fake upstream ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    Normal,
    /// Apply the commit, then answer 500 (outcome unknown to the gateway).
    ApplyThen500,
    /// Do not apply; answer 500.
    Refuse500,
}

struct Upstream {
    meta: Value,
    answer: Answer,
}

type Shared = Arc<Mutex<Upstream>>;

async fn upstream(State(s): State<Shared>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let body = axum::body::to_bytes(req.into_body(), 1 << 26)
        .await
        .unwrap();
    let mut up = s.lock().unwrap();
    if path == "/v1/config" {
        return axum::Json(json!({"defaults": {}, "overrides": {}})).into_response();
    }
    if path != TABLE_PATH {
        return StatusCode::NOT_FOUND.into_response();
    }
    if method == "GET" {
        return axum::Json(json!({"metadata-location": "m", "metadata": up.meta})).into_response();
    }
    let req: Value = serde_json::from_slice(&body).unwrap();
    if up.answer == Answer::Refuse500 {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let base = req["requirements"][0]["snapshot-id"].clone();
    if up.meta["refs"]["main"]["snapshot-id"] != base
        && !(base.is_null() && up.meta["refs"].get("main").is_none())
    {
        return (StatusCode::CONFLICT, axum::Json(json!({"error": {"message": "stale", "type": "CommitFailedException", "code": 409}}))).into_response();
    }
    for u in req["updates"].as_array().unwrap() {
        match u["action"].as_str().unwrap() {
            "add-snapshot" => up.meta["snapshots"]
                .as_array_mut()
                .unwrap()
                .push(u["snapshot"].clone()),
            "set-snapshot-ref" => {
                up.meta["refs"]["main"] =
                    json!({"snapshot-id": u["snapshot-id"], "type": "branch"});
                up.meta["current-snapshot-id"] = u["snapshot-id"].clone();
            }
            _ => {}
        }
    }
    if up.answer == Answer::ApplyThen500 {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    axum::Json(json!({"metadata-location": "m", "metadata": up.meta})).into_response()
}

fn empty_table() -> Value {
    json!({
        "format-version": 2, "table-uuid": UUID, "location": "x",
        "current-schema-id": 0, "current-snapshot-id": -1,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "code", "required": false, "type": "long"}
        ]}],
        "snapshots": [], "refs": {}
    })
}

// ---------- files ----------

struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "oip-crash-{}-{nanos}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn location(&self, name: &str) -> String {
        self.0.join(name).to_string_lossy().replace('\\', "/")
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn avro(schema: &str, records: Vec<Vec<(&str, Avro)>>) -> Vec<u8> {
    let schema = Schema::parse_str(schema).unwrap();
    let mut w = Writer::with_codec(
        &schema,
        Vec::new(),
        Codec::Deflate(DeflateSettings::default()),
    )
    .unwrap();
    for r in records {
        w.append_value(Avro::Record(
            r.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        ))
        .unwrap();
    }
    w.into_inner().unwrap()
}

/// Keys written by commit `n`: ids and codes `n*10 .. n*10+2`.
fn keys(n: i64) -> Vec<i64> {
    (n * 10..n * 10 + 3).collect()
}

/// Writes commit `n`'s data file, manifest and manifest list (which keeps all earlier manifests);
/// returns the commit request on top of `parent`.
fn commit_request(dir: &Dir, n: i64, parent: Option<i64>, manifests: &mut Vec<String>) -> Value {
    let meta = |id: &str| {
        std::collections::HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    };
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(meta("1")),
        Field::new("code", DataType::Int64, true).with_metadata(meta("2")),
    ]));
    let ks = keys(n);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ks.clone())),
            Arc::new(Int64Array::from(ks.clone())),
        ],
    )
    .unwrap();
    let mut data = Vec::new();
    let mut w = ArrowWriter::try_new(&mut data, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let data_path = dir.location(&format!("data-{n}.parquet"));
    std::fs::write(&data_path, data).unwrap();

    let manifest = avro(
        r#"{"type": "record", "name": "manifest_entry", "fields": [
            {"name": "status", "type": "int"},
            {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
                {"name": "content", "type": "int"}, {"name": "file_path", "type": "string"},
                {"name": "file_format", "type": "string"}, {"name": "record_count", "type": "long"}]}}]}"#,
        vec![vec![
            ("status", Avro::Int(1)),
            (
                "data_file",
                Avro::Record(vec![
                    ("content".into(), Avro::Int(0)),
                    ("file_path".into(), Avro::String(data_path)),
                    ("file_format".into(), Avro::String("PARQUET".into())),
                    ("record_count".into(), Avro::Long(ks.len() as i64)),
                ]),
            ),
        ]],
    );
    let manifest_path = dir.location(&format!("manifest-{n}.avro"));
    std::fs::write(&manifest_path, manifest).unwrap();
    manifests.push(manifest_path);

    let list = avro(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"}, {"name": "content", "type": "int"},
            {"name": "added_snapshot_id", "type": "long"}]}"#,
        manifests
            .iter()
            .map(|m| {
                vec![
                    ("manifest_path", Avro::String(m.clone())),
                    ("content", Avro::Int(0)),
                    ("added_snapshot_id", Avro::Long(n)),
                ]
            })
            .collect(),
    );
    let list_path = dir.location(&format!("list-{n}.avro"));
    std::fs::write(&list_path, list).unwrap();

    json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent}],
        "updates": [
            {"action": "add-snapshot", "snapshot": {"snapshot-id": n, "parent-snapshot-id": parent,
                "sequence-number": n, "timestamp-ms": n, "manifest-list": list_path,
                "summary": {"operation": "append"}}},
            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": n, "type": "branch"}
        ]
    })
}

// ---------- gateway process ----------

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Gateway {
    child: Child,
    url: String,
}

impl Gateway {
    async fn start(dir: &Dir, upstream: SocketAddr, fault: Option<FaultPoint>) -> Self {
        let port = free_port();
        let control = dir.location("control");
        let config = format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
[upstream]
catalog_uri = "http://{upstream}"
timeout_secs = 5
[control_store]
path = "{control}"
[[constraint]]
id = 1
table = "db.t"
name = "pk_t"
type = "primary_key"
columns = [1]
[[constraint]]
id = 2
table = "db.t"
name = "uq_code"
type = "unique"
columns = [2]
"#
        );
        let config_path = dir.0.join("integrity.toml");
        std::fs::write(&config_path, config).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_integrity-server"));
        cmd.arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match fault {
            Some(f) => cmd.env("OIP_FAULT", f.name()),
            None => cmd.env_remove("OIP_FAULT"),
        };
        let mut child = cmd.spawn().unwrap();
        let url = format!("http://127.0.0.1:{port}");
        for _ in 0..200 {
            if reqwest::get(format!("{url}/v1/integrity/status"))
                .await
                .is_ok()
            {
                return Self { child, url };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("gateway did not start");
    }

    async fn commit(&self, body: &Value) -> Result<u16, reqwest::Error> {
        let r = reqwest::Client::new()
            .post(format!("{}{TABLE_PATH}", self.url))
            .json(body)
            .send()
            .await?;
        Ok(r.status().as_u16())
    }

    async fn rebuild(&self, constraint: u64) -> Result<u16, reqwest::Error> {
        let r = reqwest::Client::new()
            .post(format!(
                "{}/v1/integrity/indexes/{constraint}/rebuild",
                self.url
            ))
            .send()
            .await?;
        Ok(r.status().as_u16())
    }

    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Waits for a process that is expected to abort.
    fn expect_crash(mut self) {
        for _ in 0..200 {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    !status.success(),
                    "gateway exited cleanly instead of aborting"
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = self.child.kill();
        panic!("gateway did not abort at the fault point");
    }
}

/// Snapshot ids on main, oldest first.
fn main_history(meta: &Value) -> Vec<i64> {
    meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["snapshot-id"].as_i64().unwrap())
        .collect()
}

fn index_keys(control: &Path, id: u64, kind: IndexKind) -> BTreeSet<Vec<u8>> {
    let store = PersistentStore::open(control.join("indexes.redb")).unwrap();
    let index = store.index(ConstraintId(id), kind).unwrap();
    index
        .entries()
        .unwrap()
        .into_iter()
        .map(|(k, _)| k.as_bytes().to_vec())
        .collect()
}

fn expected_keys(history: &[i64]) -> BTreeSet<Vec<u8>> {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    history
        .iter()
        .flat_map(|&n| keys(n))
        .map(|k| {
            EncodedKey::encode(&s, &[Some(KeyValue::Integer(k))])
                .unwrap()
                .as_bytes()
                .to_vec()
        })
        .collect()
}

async fn scenario(fault: FaultPoint, answer: Answer) -> (Vec<i64>, BTreeMap<&'static str, u16>) {
    let dir = Dir::new();
    let shared: Shared = Arc::new(Mutex::new(Upstream {
        meta: empty_table(),
        answer: Answer::Normal,
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(upstream)
        .with_state(Arc::clone(&shared));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut manifests = Vec::new();
    let mut statuses = BTreeMap::new();

    // 1. A normal commit.
    let g = Gateway::start(&dir, addr, None).await;
    statuses.insert(
        "first",
        g.commit(&commit_request(&dir, 1, None, &mut manifests))
            .await
            .unwrap(),
    );
    g.stop();

    // 2. The commit that crashes the gateway.
    shared.lock().unwrap().answer = answer;
    let g = Gateway::start(&dir, addr, Some(fault)).await;
    let second = commit_request(&dir, 2, Some(1), &mut manifests);
    let _ = g.commit(&second).await; // the connection drops when the process aborts
    g.expect_crash();
    shared.lock().unwrap().answer = Answer::Normal;

    // 3. Restart (recovery runs at start and before every commit), commit on top of whatever main is.
    let g = Gateway::start(&dir, addr, None).await;
    let head = *main_history(&shared.lock().unwrap().meta).last().unwrap();
    if head == 1 {
        manifests.pop(); // commit 2 never happened; build commit 3 on commit 1
    }
    statuses.insert(
        "third",
        g.commit(&commit_request(&dir, 3, Some(head), &mut manifests))
            .await
            .unwrap(),
    );
    g.stop();

    // 4. Invariants.
    let history = main_history(&shared.lock().unwrap().meta);
    let control = dir.0.join("control");
    let expected = expected_keys(&history);
    assert_eq!(
        index_keys(&control, 1, IndexKind::Unique),
        expected,
        "{fault:?}/{answer:?}: PK index != upstream data"
    );
    assert_eq!(
        index_keys(&control, 2, IndexKind::Unique),
        expected,
        "{fault:?}/{answer:?}: UNIQUE index != upstream data"
    );
    let log = TxnLog::open(control.join("txn.redb")).unwrap();
    assert!(
        log.unresolved().unwrap().is_empty(),
        "{fault:?}: unresolved transactions remain"
    );
    (history, statuses)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_fault_point_recovers_to_the_upstream_state() {
    let cases = [
        (FaultPoint::AfterPreparedLog, Answer::Normal, vec![1, 3]),
        (FaultPoint::AfterValidatedLog, Answer::Normal, vec![1, 3]),
        (FaultPoint::BeforeUpstream, Answer::Normal, vec![1, 3]),
        (
            FaultPoint::AfterUpstreamBeforeLog,
            Answer::Normal,
            vec![1, 2, 3],
        ),
        (FaultPoint::DuringIndexApply, Answer::Normal, vec![1, 2, 3]),
        (
            FaultPoint::BeforeCommittedLog,
            Answer::Normal,
            vec![1, 2, 3],
        ),
        (
            FaultPoint::AfterUpstreamUnknown,
            Answer::ApplyThen500,
            vec![1, 2, 3],
        ),
        (
            FaultPoint::AfterUpstreamUnknown,
            Answer::Refuse500,
            vec![1, 3],
        ),
    ];
    let mut covered: BTreeSet<&str> = cases.iter().map(|(f, _, _)| f.name()).collect();
    // Exercised by `a_crash_between_index_swaps_of_a_rebuild_is_safe`.
    covered.insert(FaultPoint::DuringRebuildSwap.name());
    assert_eq!(
        covered.len(),
        FaultPoint::ALL.len(),
        "every fault point is exercised"
    );
    for (fault, answer, history) in cases {
        let (actual, statuses) = scenario(fault, answer).await;
        assert_eq!(actual, history, "{fault:?}/{answer:?}: upstream history");
        assert_eq!(statuses["first"], 200);
        assert_eq!(
            statuses["third"], 200,
            "{fault:?}/{answer:?}: the next commit succeeds"
        );
    }
}

/// Spec §19 step 5 under a crash: the gateway dies after building both indexes, just before
/// swapping them in. The live indexes and the registry are unchanged and the build tables are left
/// behind, so commits continue correctly and a second rebuild completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_index_swaps_of_a_rebuild_is_safe() {
    let dir = Dir::new();
    let shared: Shared = Arc::new(Mutex::new(Upstream {
        meta: empty_table(),
        answer: Answer::Normal,
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(upstream)
        .with_state(Arc::clone(&shared));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut manifests = Vec::new();

    let g = Gateway::start(&dir, addr, None).await;
    let first = commit_request(&dir, 1, None, &mut manifests);
    assert_eq!(g.commit(&first).await.unwrap(), 200);
    g.stop();

    let g = Gateway::start(&dir, addr, Some(FaultPoint::DuringRebuildSwap)).await;
    let _ = g.rebuild(1).await; // the connection drops when the process aborts
    g.expect_crash();

    let g = Gateway::start(&dir, addr, None).await;
    let second = commit_request(&dir, 2, Some(1), &mut manifests);
    assert_eq!(g.commit(&second).await.unwrap(), 200);
    assert_eq!(g.rebuild(1).await.unwrap(), 200);
    let third = commit_request(&dir, 3, Some(2), &mut manifests);
    assert_eq!(g.commit(&third).await.unwrap(), 200);
    g.stop();

    let history = main_history(&shared.lock().unwrap().meta);
    assert_eq!(history, vec![1, 2, 3]);
    let control = dir.0.join("control");
    let expected = expected_keys(&history);
    assert_eq!(index_keys(&control, 1, IndexKind::Unique), expected);
    assert_eq!(index_keys(&control, 2, IndexKind::Unique), expected);
    let log = TxnLog::open(control.join("txn.redb")).unwrap();
    assert!(log.unresolved().unwrap().is_empty());
}
