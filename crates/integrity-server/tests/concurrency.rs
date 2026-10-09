//! Phase 9 exit criterion: concurrent FK inserts and parent deletes on a hot parent key never
//! publish a state that violates PK or FK, over many runs; and commits in different integrity
//! domains proceed in parallel while one domain's queue is busy (spec §11).
//!
//! Clients behave like engines: read the table, build a commit on its current `main`, and retry on
//! 409. The fake upstream records the global order of accepted commits; after each run that order is
//! replayed and both constraints are checked after every single commit.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
use integrity_index::{IndexKind, IndexValue, KeyIndex, PersistentStore};
use integrity_server::config::{ConstraintConfig, ReferenceConfig};
use integrity_server::fileio::ObjectStoreIo;
use integrity_server::{Gateway, router};
use integrity_txn::TxnLog;
use integrity_types::ConstraintId;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

type Row = (i64, Option<i64>);

// ---------- fake upstream with several tables ----------

#[derive(Default)]
struct Upstream {
    tables: BTreeMap<String, Value>,
    /// Accepted commits in order: (table path, snapshot id).
    history: Vec<(String, i64)>,
    /// Tables whose commits wait for a permit.
    gates: HashMap<String, Arc<Semaphore>>,
}

type Shared = Arc<Mutex<Upstream>>;

async fn upstream(State(s): State<Shared>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let body = axum::body::to_bytes(req.into_body(), 1 << 26)
        .await
        .unwrap();
    if path == "/v1/config" {
        return axum::Json(json!({"defaults": {}, "overrides": {}})).into_response();
    }
    if method == "POST" {
        let gate = s.lock().unwrap().gates.get(&path).cloned();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
    }
    let mut up = s.lock().unwrap();
    let Some(meta) = up.tables.get(&path).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if method == "GET" {
        return axum::Json(json!({"metadata-location": "m", "metadata": meta})).into_response();
    }
    let req: Value = serde_json::from_slice(&body).unwrap();
    let mut meta = meta;
    let base = req["requirements"][0]["snapshot-id"].clone();
    let current = meta["refs"]["main"]["snapshot-id"].clone();
    if current != base {
        return (StatusCode::CONFLICT, axum::Json(json!({"error": {"message": "stale", "type": "CommitFailedException", "code": 409}}))).into_response();
    }
    let mut head = None;
    for u in req["updates"].as_array().unwrap() {
        match u["action"].as_str().unwrap() {
            "add-snapshot" => meta["snapshots"]
                .as_array_mut()
                .unwrap()
                .push(u["snapshot"].clone()),
            "set-snapshot-ref" => {
                meta["refs"]["main"] = json!({"snapshot-id": u["snapshot-id"], "type": "branch"});
                meta["current-snapshot-id"] = u["snapshot-id"].clone();
                head = u["snapshot-id"].as_i64();
            }
            _ => {}
        }
    }
    up.tables.insert(path.clone(), meta.clone());
    up.history.push((path, head.unwrap()));
    axum::Json(json!({"metadata-location": "m", "metadata": meta})).into_response()
}

fn empty_table(uuid: &str) -> Value {
    json!({
        "format-version": 2, "table-uuid": uuid, "location": "x",
        "current-schema-id": 0, "current-snapshot-id": -1,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "ref", "required": false, "type": "long"}
        ]}],
        "snapshots": [], "refs": {}
    })
}

// ---------- files and the clients' view of them ----------

struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "oip-conc-{}-{nanos}-{}",
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

/// Files written by clients, and the manifests of every snapshot they built.
struct Files {
    dir: Dir,
    next: AtomicI64,
    manifests_of: Mutex<HashMap<i64, Vec<String>>>,
    rows_of: Mutex<HashMap<String, Vec<Row>>>,
}

impl Files {
    fn location(&self, name: &str) -> String {
        self.dir.0.join(name).to_string_lossy().replace('\\', "/")
    }

    /// A data file and its manifest holding `rows`; returns the manifest location.
    fn manifest(&self, rows: &[Row]) -> String {
        let n = self.next.fetch_add(1, Ordering::SeqCst);
        let meta =
            |id: &str| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(meta("1")),
            Field::new("ref", DataType::Int64, true).with_metadata(meta("2")),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut data = Vec::new();
        let mut w = ArrowWriter::try_new(&mut data, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let data_path = self.location(&format!("d{n}.parquet"));
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
                        ("record_count".into(), Avro::Long(rows.len() as i64)),
                    ]),
                ),
            ]],
        );
        let path = self.location(&format!("m{n}.avro"));
        std::fs::write(&path, manifest).unwrap();
        self.rows_of
            .lock()
            .unwrap()
            .insert(path.clone(), rows.to_vec());
        path
    }

    /// A manifest list; returns (snapshot id, its location).
    fn list(&self, manifests: &[String]) -> (i64, String) {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let bytes = avro(
            r#"{"type": "record", "name": "manifest_file", "fields": [
                {"name": "manifest_path", "type": "string"}, {"name": "content", "type": "int"},
                {"name": "added_snapshot_id", "type": "long"}]}"#,
            manifests
                .iter()
                .map(|m| {
                    vec![
                        ("manifest_path", Avro::String(m.clone())),
                        ("content", Avro::Int(0)),
                        ("added_snapshot_id", Avro::Long(id)),
                    ]
                })
                .collect(),
        );
        let path = self.location(&format!("l{id}.avro"));
        std::fs::write(&path, bytes).unwrap();
        self.manifests_of
            .lock()
            .unwrap()
            .insert(id, manifests.to_vec());
        (id, path)
    }

    fn rows(&self, manifests: &[String]) -> Vec<Row> {
        let rows = self.rows_of.lock().unwrap();
        manifests.iter().flat_map(|m| rows[m].clone()).collect()
    }
}

// ---------- clients ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Accepted,
    Rejected,
    Nothing,
}

/// What a client wants to do, given the table's current live manifests.
enum Plan {
    Append(Vec<Row>),
    /// Drop these manifests (whole-file delete).
    Drop(Vec<String>),
    Nothing,
}

struct Client {
    http: reqwest::Client,
    gateway: String,
    files: Arc<Files>,
}

impl Client {
    async fn head(&self, table: &str) -> Option<i64> {
        let meta: Value = self
            .http
            .get(format!("{}/v1/namespaces/db/tables/{table}", self.gateway))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        meta["metadata"]["refs"]["main"]["snapshot-id"].as_i64()
    }

    async fn run(&self, table: &str, plan: impl Fn(&[String]) -> Plan) -> Outcome {
        for _ in 0..200 {
            let head = self.head(table).await;
            let current = head
                .map(|h| self.files.manifests_of.lock().unwrap()[&h].clone())
                .unwrap_or_default();
            let (manifests, op) = match plan(&current) {
                Plan::Nothing => return Outcome::Nothing,
                Plan::Append(rows) => {
                    let mut m = current.clone();
                    m.push(self.files.manifest(&rows));
                    (m, "append")
                }
                Plan::Drop(drop) => (
                    current
                        .iter()
                        .filter(|m| !drop.contains(m))
                        .cloned()
                        .collect(),
                    "delete",
                ),
            };
            let (id, list) = self.files.list(&manifests);
            let body = json!({
                "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": head}],
                "updates": [
                    {"action": "add-snapshot", "snapshot": {"snapshot-id": id, "parent-snapshot-id": head,
                        "sequence-number": id, "timestamp-ms": id, "manifest-list": list,
                        "summary": {"operation": op}}},
                    {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": id, "type": "branch"}
                ]
            });
            let status = self
                .http
                .post(format!("{}/v1/namespaces/db/tables/{table}", self.gateway))
                .json(&body)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16();
            match status {
                200 => return Outcome::Accepted,
                400 => return Outcome::Rejected,
                409 => tokio::task::yield_now().await,
                other => panic!("unexpected status {other}"),
            }
        }
        panic!("too many retries on {table}");
    }
}

fn manifests_with(files: &Files, current: &[String], pred: impl Fn(&Row) -> bool) -> Vec<String> {
    let rows = files.rows_of.lock().unwrap();
    current
        .iter()
        .filter(|m| rows[*m].iter().any(&pred))
        .cloned()
        .collect()
}

// ---------- setup ----------

fn constraint(
    id: u64,
    table: &str,
    kind: &str,
    columns: Vec<i32>,
    references: Option<(&str, u64)>,
) -> ConstraintConfig {
    ConstraintConfig {
        id,
        table: table.into(),
        name: format!("c{id}"),
        kind: kind.into(),
        columns,
        nulls: None,
        references: references.map(|(t, c)| ReferenceConfig {
            table: t.into(),
            constraint: c,
        }),
        match_mode: None,
    }
}

struct Env {
    upstream: Shared,
    client: Client,
    store: PersistentStore,
    log_path: PathBuf,
    gateway: Arc<Gateway>,
}

async fn env(tables: &[(&str, &str)], constraints: Vec<ConstraintConfig>) -> Env {
    let files = Arc::new(Files {
        dir: Dir::new(),
        next: AtomicI64::new(1),
        manifests_of: Mutex::new(HashMap::new()),
        rows_of: Mutex::new(HashMap::new()),
    });
    let shared: Shared = Arc::new(Mutex::new(Upstream::default()));
    for (name, uuid) in tables {
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
    let control = files.dir.0.join("control");
    std::fs::create_dir_all(&control).unwrap();
    let store = PersistentStore::open(control.join("indexes.redb")).unwrap();
    let log_path = control.join("txn.redb");
    let gateway = Arc::new(
        Gateway::new(
            &format!("http://{up}"),
            Duration::from_secs(30),
            Arc::new(ObjectStoreIo::new(
                Default::default(),
                tokio::runtime::Handle::current(),
            )),
            store.clone(),
            TxnLog::open(&log_path).unwrap(),
            1 << 30,
            constraints,
        )
        .unwrap(),
    );
    let gw = serve(router(Arc::clone(&gateway))).await;
    Env {
        upstream: shared,
        client: Client {
            http: reqwest::Client::new(),
            gateway: format!("http://{gw}"),
            files,
        },
        store,
        log_path,
        gateway,
    }
}

async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn key(v: i64) -> Vec<u8> {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))])
        .unwrap()
        .as_bytes()
        .to_vec()
}

/// A tiny deterministic generator, so each run interleaves differently but reproducibly.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

// ---------- tests ----------

const CUSTOMER: &str = "/v1/namespaces/db/tables/customer";

async fn hot_key_run(seed: u64) -> BTreeMap<&'static str, usize> {
    let env = env(
        &[
            ("customer", "00000000-0000-4000-8000-00000000000c"),
            ("orders", "00000000-0000-4000-8000-00000000000d"),
        ],
        vec![
            constraint(1, "db.customer", "primary_key", vec![1], None),
            constraint(2, "db.orders", "primary_key", vec![1], None),
            constraint(
                3,
                "db.orders",
                "foreign_key",
                vec![2],
                Some(("db.customer", 1)),
            ),
        ],
    )
    .await;
    let client = Arc::new(env.client);
    assert_eq!(
        client
            .run("customer", |_| Plan::Append(vec![(1, None), (2, None)]))
            .await,
        Outcome::Accepted
    );

    let next_oid = Arc::new(AtomicI64::new(100));
    let mut tasks = Vec::new();
    // Order inserters, mostly on the hot customer 1.
    for w in 0..4u64 {
        let (client, next_oid) = (Arc::clone(&client), Arc::clone(&next_oid));
        tasks.push(tokio::spawn(async move {
            let mut rng = Lcg(seed * 31 + w);
            let mut out = Vec::new();
            for _ in 0..6 {
                let cust = if rng.next(4) == 0 { 2 } else { 1 };
                let oid = next_oid.fetch_add(1, Ordering::SeqCst);
                out.push(
                    client
                        .run("orders", |_| Plan::Append(vec![(oid, Some(cust))]))
                        .await,
                );
                tokio::time::sleep(Duration::from_millis(rng.next(4))).await;
            }
            out
        }));
    }
    // Churn on the hot parent: delete customer 1, re-insert it.
    {
        let client = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            let mut rng = Lcg(seed * 17 + 99);
            let mut out = Vec::new();
            for _ in 0..6 {
                let files = Arc::clone(&client.files);
                out.push(
                    client
                        .run("customer", |cur| {
                            let drop = manifests_with(&files, cur, |r| r.0 == 1);
                            if drop.is_empty() {
                                Plan::Nothing
                            } else {
                                Plan::Drop(drop)
                            }
                        })
                        .await,
                );
                tokio::time::sleep(Duration::from_millis(rng.next(4))).await;
                let files = Arc::clone(&client.files);
                out.push(
                    client
                        .run("customer", |cur| {
                            if files.rows(cur).iter().any(|r| r.0 == 1) {
                                Plan::Nothing
                            } else {
                                Plan::Append(vec![(1, None)])
                            }
                        })
                        .await,
                );
            }
            out
        }));
    }
    // Child cleanup so that parent deletes can sometimes succeed.
    {
        let client = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            let mut rng = Lcg(seed * 7 + 5);
            let mut out = Vec::new();
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_millis(rng.next(6))).await;
                let files = Arc::clone(&client.files);
                out.push(
                    client
                        .run("orders", |cur| {
                            let drop = manifests_with(&files, cur, |r| r.1 == Some(1));
                            if drop.is_empty() {
                                Plan::Nothing
                            } else {
                                Plan::Drop(drop)
                            }
                        })
                        .await,
                );
            }
            out
        }));
    }
    let mut outcomes: BTreeMap<&'static str, usize> = BTreeMap::new();
    for t in tasks {
        for o in t.await.unwrap() {
            *outcomes
                .entry(match o {
                    Outcome::Accepted => "accepted",
                    Outcome::Rejected => "rejected",
                    Outcome::Nothing => "nothing",
                })
                .or_default() += 1;
        }
    }

    // Replay the global order of published snapshots; check PK and FK after every commit.
    let (history, metas) = {
        let up = env.upstream.lock().unwrap();
        (up.history.clone(), up.tables.clone())
    };
    let files = &client.files;
    let mut customers: Vec<Row> = Vec::new();
    let mut orders: Vec<Row> = Vec::new();
    for (step, (path, snapshot)) in history.iter().enumerate() {
        let rows = files.rows(&files.manifests_of.lock().unwrap()[snapshot]);
        if path == CUSTOMER {
            customers = rows
        } else {
            orders = rows
        }
        let ids: BTreeSet<i64> = customers.iter().map(|r| r.0).collect();
        assert_eq!(
            ids.len(),
            customers.len(),
            "seed {seed} step {step}: duplicate customer"
        );
        let oids: BTreeSet<i64> = orders.iter().map(|r| r.0).collect();
        assert_eq!(
            oids.len(),
            orders.len(),
            "seed {seed} step {step}: duplicate order"
        );
        for o in &orders {
            if let Some(c) = o.1 {
                assert!(
                    ids.contains(&c),
                    "seed {seed} step {step}: order {} references missing customer {c}",
                    o.0
                );
            }
        }
    }
    // Every main snapshot carries a certificate.
    for meta in metas.values() {
        for s in meta["snapshots"].as_array().unwrap() {
            assert!(
                s["summary"]["integrity.cert"].is_string(),
                "uncertified snapshot"
            );
        }
    }
    // Indexes equal the final data.
    let entries = |id: u64, kind| {
        env.store
            .index(ConstraintId(id), kind)
            .unwrap()
            .entries()
            .unwrap()
    };
    let pk_customer: BTreeSet<_> = entries(1, IndexKind::Unique)
        .into_iter()
        .map(|(k, _)| k.as_bytes().to_vec())
        .collect();
    assert_eq!(
        pk_customer,
        customers.iter().map(|r| key(r.0)).collect(),
        "seed {seed}"
    );
    let pk_orders: BTreeSet<_> = entries(2, IndexKind::Unique)
        .into_iter()
        .map(|(k, _)| k.as_bytes().to_vec())
        .collect();
    assert_eq!(
        pk_orders,
        orders.iter().map(|r| key(r.0)).collect(),
        "seed {seed}"
    );
    let mut refs: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    for o in &orders {
        if let Some(c) = o.1 {
            *refs.entry(key(c)).or_default() += 1;
        }
    }
    let fk: BTreeMap<_, _> = entries(3, IndexKind::Reference)
        .into_iter()
        .map(|(k, v)| match v {
            IndexValue::Reference { child_count } => (k.as_bytes().to_vec(), child_count),
            IndexValue::Unique { .. } => panic!(),
        })
        .collect();
    assert_eq!(fk, refs, "seed {seed}: reference counts");
    let status = env.gateway.status().await;
    assert_eq!(status["degraded"], Value::Null);
    drop(env.gateway);
    assert!(
        TxnLog::open(&env.log_path)
            .map(|l| l.unresolved().unwrap().is_empty())
            .unwrap_or(true)
    );
    outcomes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fk_inserts_and_parent_deletes_never_publish_violations() {
    let mut totals: BTreeMap<&str, usize> = BTreeMap::new();
    for seed in 1..=12 {
        for (k, v) in hot_key_run(seed).await {
            *totals.entry(k).or_default() += v;
        }
    }
    eprintln!("outcomes over all runs: {totals:?}");
    // Not vacuous: both accepted commits and integrity rejections happened.
    assert!(
        totals.get("accepted").copied().unwrap_or(0) > 100,
        "{totals:?}"
    );
    assert!(
        totals.get("rejected").copied().unwrap_or(0) > 10,
        "{totals:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn domains_commit_in_parallel_and_each_domain_is_serial() {
    let env = env(
        &[
            ("a", "00000000-0000-4000-8000-00000000000a"),
            ("b", "00000000-0000-4000-8000-00000000000b"),
        ],
        vec![
            constraint(1, "db.a", "primary_key", vec![1], None),
            constraint(2, "db.b", "primary_key", vec![1], None),
        ],
    )
    .await;
    let client = Arc::new(env.client);
    let gate = Arc::new(Semaphore::new(0));
    env.upstream
        .lock()
        .unwrap()
        .gates
        .insert("/v1/namespaces/db/tables/a".into(), Arc::clone(&gate));

    // A commit to `a` is held inside upstream while holding domain a's queue.
    let c = Arc::clone(&client);
    let first_a = tokio::spawn(async move { c.run("a", |_| Plan::Append(vec![(1, None)])).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    // A second commit to `a` must wait for the first.
    let c = Arc::clone(&client);
    let second_a = tokio::spawn(async move { c.run("a", |_| Plan::Append(vec![(2, None)])).await });
    // Domain b is independent and completes meanwhile.
    let b = tokio::time::timeout(
        Duration::from_secs(10),
        client.run("b", |_| Plan::Append(vec![(1, None)])),
    )
    .await;
    assert_eq!(
        b.expect("domain b was blocked by domain a"),
        Outcome::Accepted
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !first_a.is_finished() && !second_a.is_finished(),
        "domain a is not serial"
    );

    gate.add_permits(100);
    assert_eq!(first_a.await.unwrap(), Outcome::Accepted);
    assert_eq!(second_a.await.unwrap(), Outcome::Accepted);
    let history: Vec<String> = env
        .upstream
        .lock()
        .unwrap()
        .history
        .iter()
        .map(|(p, _)| p.clone())
        .collect();
    assert_eq!(
        history.first().map(String::as_str),
        Some("/v1/namespaces/db/tables/b"),
        "b committed first: {history:?}"
    );
}
