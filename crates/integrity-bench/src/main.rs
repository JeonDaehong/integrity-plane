//! `integrity-bench`: end-to-end benchmarks of the Open Integrity Plane (spec §29).
//!
//! Runs the real gateway in-process against an in-process fake REST catalog, with real Parquet
//! data files, Avro manifests and the persistent (redb) control store on local disk. Commits go
//! through HTTP exactly as an engine's would; validation time, bytes read and queue wait come from
//! the gateway's own `/metrics`.
//!
//! Usage: `integrity-bench [--parent-rows N] [--dir PATH] [--appends N] [--append-rows N]
//!                         [--writers N] [--hot-commits N]`
//!
//! Prints a Markdown report to stdout.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use apache_avro::types::Value as Avro;
use apache_avro::{Codec, DeflateSettings, Schema, Writer};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field};
use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use integrity_index::PersistentStore;
use integrity_server::fileio::ObjectStoreIo;
use integrity_server::store::Registry;
use integrity_server::{Gateway, router};
use integrity_txn::TxnLog;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

mod scale;

// ---------- options ----------

struct Options {
    parent_rows: i64,
    dir: PathBuf,
    appends: usize,
    append_rows: i64,
    writers: usize,
    hot_commits: usize,
    /// Large-table mode: parent rows (0 = the standard scenarios).
    scale: i64,
    /// Rows per data file in large-table mode.
    file_rows: i64,
    /// Merge-on-read deletes in large-table mode.
    mor_deletes: usize,
}

fn options() -> Result<Options, String> {
    let mut o = Options {
        parent_rows: 1_000_000,
        dir: std::env::temp_dir().join("integrity-bench"),
        appends: 20,
        append_rows: 10_000,
        writers: 16,
        hot_commits: 20,
        scale: 0,
        file_rows: 1_000_000,
        mor_deletes: 50,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let n = || {
            value
                .parse::<i64>()
                .map_err(|_| format!("{flag}: not a number"))
        };
        match flag.as_str() {
            "--parent-rows" => o.parent_rows = n()?,
            "--dir" => o.dir = PathBuf::from(value),
            "--appends" => o.appends = n()? as usize,
            "--append-rows" => o.append_rows = n()?,
            "--writers" => o.writers = n()? as usize,
            "--hot-commits" => o.hot_commits = n()? as usize,
            "--scale" => o.scale = n()?,
            "--file-rows" => o.file_rows = n()?,
            "--mor-deletes" => o.mor_deletes = n()? as usize,
            other => return Err(format!("unknown option {other}")),
        }
    }
    Ok(o)
}

// ---------- fake upstream catalog ----------

#[derive(Default)]
struct Upstream {
    tables: BTreeMap<String, Value>,
}

type Shared = Arc<Mutex<Upstream>>;

async fn upstream(State(s): State<Shared>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let Ok(body) = axum::body::to_bytes(req.into_body(), 1 << 28).await else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(mut up) = s.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(mut meta) = up.tables.get(&path).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if method == "GET" {
        return axum::Json(json!({"metadata-location": "m", "metadata": meta})).into_response();
    }
    let Ok(req) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if req["requirements"][0]["snapshot-id"] != meta["refs"]["main"]["snapshot-id"] {
        return (
            StatusCode::CONFLICT,
            axum::Json(json!({"error": {"message": "stale", "type": "CommitFailedException", "code": 409}})),
        )
            .into_response();
    }
    for u in req["updates"].as_array().into_iter().flatten() {
        match u["action"].as_str() {
            Some("add-snapshot") => {
                if let Some(s) = meta["snapshots"].as_array_mut() {
                    s.push(u["snapshot"].clone());
                }
            }
            Some("set-snapshot-ref") => {
                meta["refs"]["main"] = json!({"snapshot-id": u["snapshot-id"], "type": "branch"});
                meta["current-snapshot-id"] = u["snapshot-id"].clone();
            }
            _ => {}
        }
    }
    up.tables.insert(path, meta.clone());
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

// ---------- files written the way engines write them ----------

struct Files {
    dir: PathBuf,
    next: AtomicI64,
    manifests_of: Mutex<HashMap<i64, Vec<String>>>,
    rows_of: Mutex<HashMap<String, i64>>,
    /// Data file of each data manifest.
    data_of: Mutex<HashMap<String, String>>,
    /// Manifests that hold delete files.
    deletes: Mutex<std::collections::HashSet<String>>,
}

fn avro(schema: &str, records: Vec<Vec<(&str, Avro)>>) -> Result<Vec<u8>, BoxError> {
    let schema = Schema::parse_str(schema)?;
    let mut w = Writer::with_codec(
        &schema,
        Vec::new(),
        Codec::Deflate(DeflateSettings::default()),
    )?;
    for r in records {
        w.append_value(Avro::Record(
            r.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        ))?;
    }
    Ok(w.into_inner()?)
}

impl Files {
    fn location(&self, name: &str) -> String {
        self.dir.join(name).to_string_lossy().replace('\\', "/")
    }

    /// One Parquet file with columns `id` and `ref`, and a manifest listing it.
    fn manifest(&self, ids: &[i64], refs: &[Option<i64>]) -> Result<String, BoxError> {
        self.manifest_with_payload(ids, refs, 0)
    }

    /// Like `manifest`, with an extra non-key column of `payload` incompressible bytes per row
    /// (a wide row, as real tables have).
    fn manifest_with_payload(
        &self,
        ids: &[i64],
        refs: &[Option<i64>],
        payload: usize,
    ) -> Result<String, BoxError> {
        let n = self.next.fetch_add(1, Ordering::SeqCst);
        let meta =
            |id: &str| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
        let mut fields = vec![
            Field::new("id", DataType::Int64, false).with_metadata(meta("1")),
            Field::new("ref", DataType::Int64, true).with_metadata(meta("2")),
        ];
        let mut columns: Vec<arrow_array::ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(refs.to_vec())),
        ];
        if payload > 0 {
            fields.push(Field::new("payload", DataType::Utf8, false).with_metadata(meta("3")));
            let mut rng = Rng(n as u64 ^ 0xdead_beef);
            let values: Vec<String> = ids
                .iter()
                .map(|_| {
                    (0..payload / 16)
                        .map(|_| format!("{:016x}", rng.next()))
                        .collect()
                })
                .collect();
            columns.push(Arc::new(arrow_array::StringArray::from(values)));
        }
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let data_path = data_path_for(&self.dir, n);
        let file = std::fs::File::create(&data_path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        w.write(&batch)?;
        w.close()?;
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
                        ("record_count".into(), Avro::Long(ids.len() as i64)),
                    ]),
                ),
            ]],
        )?;
        let path = self.location(&format!("m{n}.avro"));
        std::fs::write(&path, manifest)?;
        if let Ok(mut rows) = self.rows_of.lock() {
            rows.insert(path.clone(), ids.len() as i64);
        }
        if let Ok(mut d) = self.data_of.lock() {
            d.insert(path.clone(), data_path_for(&self.dir, n));
        }
        Ok(path)
    }

    /// A Parquet position delete file of `(data file, row position)` and a delete manifest listing
    /// it; returns the manifest location.
    fn position_deletes(&self, deletes: &[(String, i64)]) -> Result<String, BoxError> {
        let n = self.next.fetch_add(1, Ordering::SeqCst);
        let meta =
            |id: &str| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(meta("2147483546")),
            Field::new("pos", DataType::Int64, false).with_metadata(meta("2147483545")),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(
                    deletes.iter().map(|d| d.0.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    deletes.iter().map(|d| d.1).collect::<Vec<_>>(),
                )),
            ],
        )?;
        let file_path = self.location(&format!("pd{n}.parquet"));
        let file = std::fs::File::create(&file_path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        w.write(&batch)?;
        w.close()?;
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
                        ("content".into(), Avro::Int(1)),
                        ("file_path".into(), Avro::String(file_path)),
                        ("file_format".into(), Avro::String("PARQUET".into())),
                        ("record_count".into(), Avro::Long(deletes.len() as i64)),
                    ]),
                ),
            ]],
        )?;
        let path = self.location(&format!("dm{n}.avro"));
        std::fs::write(&path, manifest)?;
        if let Ok(mut d) = self.deletes.lock() {
            d.insert(path.clone());
        }
        Ok(path)
    }

    /// A manifest list; returns (snapshot id, location).
    fn list(&self, manifests: &[String]) -> Result<(i64, String), BoxError> {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let bytes = avro(
            r#"{"type": "record", "name": "manifest_file", "fields": [
                {"name": "manifest_path", "type": "string"}, {"name": "content", "type": "int"},
                {"name": "added_snapshot_id", "type": "long"}]}"#,
            manifests
                .iter()
                .map(|m| {
                    let content = i32::from(self.deletes.lock().is_ok_and(|d| d.contains(m)));
                    vec![
                        ("manifest_path", Avro::String(m.clone())),
                        ("content", Avro::Int(content)),
                        ("added_snapshot_id", Avro::Long(id)),
                    ]
                })
                .collect(),
        )?;
        let path = self.location(&format!("l{id}.avro"));
        std::fs::write(&path, bytes)?;
        if let Ok(mut m) = self.manifests_of.lock() {
            m.insert(id, manifests.to_vec());
        }
        Ok((id, path))
    }
}

fn data_path_for(dir: &Path, n: i64) -> String {
    dir.join(format!("d{n}.parquet"))
        .to_string_lossy()
        .replace('\\', "/")
}

// ---------- client ----------

struct Bench {
    http: reqwest::Client,
    gateway: String,
    upstream: String,
    files: Arc<Files>,
}

enum Change {
    Append(Vec<i64>, Vec<Option<i64>>),
    /// Append rows carrying `usize` payload bytes each.
    Wide(Vec<i64>, usize),
    /// Replace every current manifest with one file holding the same rows.
    Compact(Vec<i64>, Vec<Option<i64>>),
    /// An explicit new manifest list and operation (scale scenarios).
    Raw(Vec<String>, &'static str),
}

impl Bench {
    async fn head(&self, base: &str, table: &str) -> Result<Option<i64>, BoxError> {
        let meta: Value = self
            .http
            .get(format!("{base}/v1/namespaces/bench/tables/{table}"))
            .send()
            .await?
            .json()
            .await?;
        Ok(meta["metadata"]["refs"]["main"]["snapshot-id"].as_i64())
    }

    /// One commit attempt on the current head; returns the HTTP status.
    async fn commit(&self, base: &str, table: &str, change: &Change) -> Result<u16, BoxError> {
        let head = self.head(base, table).await?;
        let current = match head {
            Some(h) => self
                .files
                .manifests_of
                .lock()
                .map_err(|_| "lock")?
                .get(&h)
                .cloned()
                .unwrap_or_default(),
            None => Vec::new(),
        };
        let (manifests, op) = match change {
            Change::Append(ids, refs) => {
                let mut m = current;
                m.push(self.files.manifest(ids, refs)?);
                (m, "append")
            }
            Change::Compact(ids, refs) => (vec![self.files.manifest(ids, refs)?], "replace"),
            Change::Raw(list, op) => (list.clone(), *op),
            Change::Wide(ids, payload) => {
                let mut m = current;
                m.push(
                    self.files
                        .manifest_with_payload(ids, &vec![None; ids.len()], *payload)?,
                );
                (m, "append")
            }
        };
        let (id, list) = self.files.list(&manifests)?;
        let body = json!({
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": head}],
            "updates": [
                {"action": "add-snapshot", "snapshot": {"snapshot-id": id, "parent-snapshot-id": head,
                    "sequence-number": id, "timestamp-ms": id, "manifest-list": list,
                    "summary": {"operation": op}}},
                {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": id, "type": "branch"}
            ]
        });
        let r = self
            .http
            .post(format!("{base}/v1/namespaces/bench/tables/{table}"))
            .json(&body)
            .send()
            .await?;
        Ok(r.status().as_u16())
    }

    async fn register(&self, body: Value) -> Result<(), BoxError> {
        let r = self
            .http
            .post(format!("{}/v1/integrity/constraints", self.gateway))
            .json(&body)
            .send()
            .await?;
        if !r.status().is_success() {
            return Err(format!("register failed: {}", r.text().await?).into());
        }
        Ok(())
    }

    async fn metrics(&self) -> Result<Metrics, BoxError> {
        let text = self
            .http
            .get(format!("{}/metrics", self.gateway))
            .send()
            .await?
            .text()
            .await?;
        Ok(Metrics::parse(&text))
    }
}

/// The few series the report needs.
#[derive(Debug, Clone, Default)]
struct Metrics {
    values: BTreeMap<String, f64>,
}

impl Metrics {
    fn parse(text: &str) -> Self {
        let values = text
            .lines()
            .filter(|l| !l.starts_with('#'))
            .filter_map(|l| {
                let (name, v) = l.rsplit_once(' ')?;
                Some((name.to_owned(), v.parse().ok()?))
            })
            .collect();
        Self { values }
    }

    fn get(&self, name: &str) -> f64 {
        self.values.get(name).copied().unwrap_or(0.0)
    }

    /// Upper bound of the bucket holding the `q` quantile of the observations made between
    /// `before` and `self`.
    fn quantile_bound(&self, before: &Metrics, histogram: &str, q: f64) -> String {
        let count =
            self.get(&format!("{histogram}_count")) - before.get(&format!("{histogram}_count"));
        if count <= 0.0 {
            return "-".into();
        }
        for bound in integrity_server::metrics::BUCKETS {
            let key = format!("{histogram}_bucket{{le=\"{bound}\"}}");
            if self.get(&key) - before.get(&key) >= q * count {
                return format!("≤ {}", seconds(bound));
            }
        }
        "> 10 s".into()
    }
}

fn seconds(s: f64) -> String {
    if s < 1.0 {
        format!("{:.1} ms", s * 1e3)
    } else {
        format!("{s:.2} s")
    }
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn mib(bytes: f64) -> String {
    format!("{:.1} MiB", bytes / (1024.0 * 1024.0))
}

/// Deterministic pseudo-random numbers (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

async fn serve(app: Router) -> Result<SocketAddr, BoxError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(addr)
}

fn dir_size(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BoxError> {
    let o = options().map_err(|e| format!("{e}\nsee the module documentation for usage"))?;
    let _ = std::fs::remove_dir_all(&o.dir);
    let data = o.dir.join("data");
    let control = o.dir.join("control");
    std::fs::create_dir_all(&data)?;
    std::fs::create_dir_all(&control)?;

    let shared: Shared = Arc::new(Mutex::new(Upstream::default()));
    for (name, uuid) in [
        ("customer", "11111111-1111-1111-1111-111111111111"),
        ("orders", "22222222-2222-2222-2222-222222222222"),
        ("hot", "44444444-4444-4444-4444-444444444444"),
        ("baseline", "33333333-3333-3333-3333-333333333333"),
        ("direct", "55555555-5555-5555-5555-555555555555"),
        ("free", "66666666-6666-6666-6666-666666666666"),
        ("wide", "77777777-7777-7777-7777-777777777777"),
        ("big", "88888888-8888-8888-8888-888888888888"),
    ] {
        shared.lock().map_err(|_| "lock")?.tables.insert(
            format!("/v1/namespaces/bench/tables/{name}"),
            empty_table(uuid),
        );
    }
    let up = serve(
        Router::new()
            .fallback(upstream)
            .with_state(Arc::clone(&shared)),
    )
    .await?;
    let gateway = Gateway::new(
        &format!("http://{up}"),
        Duration::from_secs(600),
        Arc::new(ObjectStoreIo::new(
            Default::default(),
            tokio::runtime::Handle::current(),
        )),
        PersistentStore::open(control.join("indexes.redb"))?,
        TxnLog::open(control.join("txn.redb"))?,
        u64::MAX,
        Registry::open(control.join("registry.redb"), &[])?,
    )?;
    let gw = serve(router(Arc::new(gateway))).await?;
    let b = Bench {
        http: reqwest::Client::new(),
        gateway: format!("http://{gw}"),
        upstream: format!("http://{up}"),
        files: Arc::new(Files {
            dir: data.clone(),
            next: AtomicI64::new(1),
            manifests_of: Mutex::new(HashMap::new()),
            rows_of: Mutex::new(HashMap::new()),
            data_of: Mutex::new(HashMap::new()),
            deletes: Mutex::new(std::collections::HashSet::new()),
        }),
    };
    let mut report = String::new();
    let mut line = |s: String| {
        eprintln!("{s}");
        report.push_str(&s);
        report.push('\n');
    };

    if o.scale > 0 {
        let report = scale::run(&b, &o).await?;
        println!("| Scenario | Time | Data | Notes |\n|---|---|---|---|\n{report}");
        let _ = std::fs::remove_dir_all(&o.dir);
        return Ok(());
    }

    // 1. Parent data written before the Plane (1 M rows per file), then onboarded.
    let chunk = 1_000_000;
    let mut start = 0;
    while start < o.parent_rows {
        let end = (start + chunk).min(o.parent_rows);
        let ids: Vec<i64> = (start..end).collect();
        let refs = vec![None; ids.len()];
        let status = b
            .commit(&b.upstream, "customer", &Change::Append(ids, refs))
            .await?;
        assert_eq!(status, 200, "upstream append");
        start = end;
    }
    let t = Instant::now();
    b.register(json!({"table": "bench.customer", "name": "pk_customer", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .await?;
    let onboard = t.elapsed();
    b.register(json!({"table": "bench.orders", "name": "pk_orders", "type": "PRIMARY_KEY", "columns": ["id"]}))
        .await?;
    b.register(json!({"table": "bench.orders", "name": "fk_orders_customer", "type": "FOREIGN_KEY",
                      "columns": ["ref"], "references": {"table": "bench.customer", "constraint": "pk_customer"}}))
        .await?;
    line(format!(
        "| Onboarding scan of {} parent rows (PRIMARY KEY) | {} | {:.0} keys/s | parent data {} |",
        o.parent_rows,
        seconds(onboard.as_secs_f64()),
        o.parent_rows as f64 / onboard.as_secs_f64(),
        mib(dir_size(&data) as f64),
    ));

    // 2. Child appends: append_rows rows each, FK into random parent keys, warm index.
    let mut rng = Rng(0x5eed);
    let mut next_order = 0i64;
    let mut latencies = Vec::new();
    let mut validation = Vec::new();
    let mut bytes = Vec::new();
    for _ in 0..o.appends {
        let ids: Vec<i64> = (next_order..next_order + o.append_rows).collect();
        next_order += o.append_rows;
        let refs: Vec<Option<i64>> = ids
            .iter()
            .map(|_| Some((rng.next() % o.parent_rows as u64) as i64))
            .collect();
        let before = b.metrics().await?;
        let t = Instant::now();
        let status = b
            .commit(&b.gateway, "orders", &Change::Append(ids, refs))
            .await?;
        latencies.push(t.elapsed());
        assert_eq!(status, 200, "child append");
        let after = b.metrics().await?;
        validation.push(
            after.get("integrity_validation_seconds_sum")
                - before.get("integrity_validation_seconds_sum"),
        );
        bytes.push(
            after.get("integrity_bytes_read_total") - before.get("integrity_bytes_read_total"),
        );
    }
    latencies.sort();
    let mut sorted_validation = validation.clone();
    sorted_validation.sort_by(f64::total_cmp);
    let median = |v: &[f64]| v.get(v.len() / 2).copied().unwrap_or(0.0);
    line(format!(
        "| Child append of {} rows (PK + FK into {} parent keys), {} commits | validation p50 {} / max {}; end-to-end p50 {} / p99 {} | {:.0} keys/s | read per commit p50 {} |",
        o.append_rows,
        o.parent_rows,
        o.appends,
        seconds(median(&sorted_validation)),
        seconds(sorted_validation.last().copied().unwrap_or(0.0)),
        seconds(percentile(&latencies, 0.5).as_secs_f64()),
        seconds(percentile(&latencies, 0.99).as_secs_f64()),
        o.append_rows as f64 / median(&sorted_validation),
        mib({
            let mut s = bytes.clone();
            s.sort_by(f64::total_cmp);
            median(&s)
        }),
    ));

    // 3. Compaction: every child file rewritten into one, keys unchanged.
    let total = next_order;
    let ids: Vec<i64> = (0..total).collect();
    let mut rng = Rng(0x5eed);
    let refs: Vec<Option<i64>> = ids
        .iter()
        .map(|_| Some((rng.next() % o.parent_rows as u64) as i64))
        .collect();
    let before = b.metrics().await?;
    let t = Instant::now();
    let status = b
        .commit(&b.gateway, "orders", &Change::Compact(ids, refs))
        .await?;
    let compaction = t.elapsed();
    assert_eq!(status, 200, "compaction");
    let after = b.metrics().await?;
    let v = after.get("integrity_validation_seconds_sum")
        - before.get("integrity_validation_seconds_sum");
    let keys =
        after.get("integrity_keys_validated_total") - before.get("integrity_keys_validated_total");
    line(format!(
        "| Compaction of {} child rows ({} files → 1, `replace`) | validation {}; end-to-end {} | {:.0} keys/s | read {} |",
        total,
        o.appends,
        seconds(v),
        seconds(compaction.as_secs_f64()),
        keys / v,
        mib(after.get("integrity_bytes_read_total") - before.get("integrity_bytes_read_total")),
    ));

    // Overhead against the catalog alone, one writer at a time.
    let p50_of = |mut v: Vec<Duration>| {
        v.sort();
        percentile(&v, 0.5).as_secs_f64()
    };
    let mut loads = (Vec::new(), Vec::new());
    for _ in 0..200 {
        let t = Instant::now();
        b.head(&b.upstream, "customer").await?;
        loads.0.push(t.elapsed());
        let t = Instant::now();
        b.head(&b.gateway, "customer").await?;
        loads.1.push(t.elapsed());
    }
    let (direct_load, plane_load) = (p50_of(loads.0), p50_of(loads.1));
    line(format!(
        "| Overhead: table load (what readers do before reading data files) | direct p50 {}; through the Plane p50 {} | +{} | data files are read from storage directly |",
        seconds(direct_load),
        seconds(plane_load),
        seconds(plane_load - direct_load),
    ));
    let mut commits = (Vec::new(), Vec::new());
    for i in 0..100 {
        let t = Instant::now();
        let status = b
            .commit(&b.upstream, "direct", &Change::Append(vec![i], vec![None]))
            .await?;
        assert_eq!(status, 200);
        commits.0.push(t.elapsed());
        let t = Instant::now();
        let status = b
            .commit(&b.gateway, "free", &Change::Append(vec![i], vec![None]))
            .await?;
        assert_eq!(status, 200);
        commits.1.push(t.elapsed());
    }
    let (direct_commit, proxied_commit) = (p50_of(commits.0), p50_of(commits.1));
    line(format!(
        "| Overhead: single-row commit, client side included | straight to the catalog p50 {}; unconstrained table through the Plane p50 {} | +{} | constrained: see the next row |",
        seconds(direct_commit),
        seconds(proxied_commit),
        seconds(proxied_commit - direct_commit),
    ));

    // Wide rows: the Plane reads the key column chunks, not whole data files.
    b.register(
        json!({"table": "bench.wide", "name": "pk_wide", "type": "PRIMARY_KEY", "columns": ["id"]}),
    )
    .await?;
    let (rows, payload) = (10_000i64, 1024usize);
    let before = b.metrics().await?;
    let mut direct = Vec::new();
    let mut plane = Vec::new();
    for round in 0..5i64 {
        let ids: Vec<i64> = (round * rows..(round + 1) * rows).collect();
        let t = Instant::now();
        assert_eq!(
            b.commit(&b.upstream, "baseline", &Change::Wide(ids.clone(), payload))
                .await?,
            200
        );
        direct.push(t.elapsed());
        let t = Instant::now();
        assert_eq!(
            b.commit(&b.gateway, "wide", &Change::Wide(ids, payload))
                .await?,
            200
        );
        plane.push(t.elapsed());
    }
    let after = b.metrics().await?;
    let read_per_commit =
        (after.get("integrity_bytes_read_total") - before.get("integrity_bytes_read_total")) / 5.0;
    let file_size = std::fs::read_dir(&data)?
        .filter_map(Result::ok)
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .max()
        .unwrap_or(0) as f64;
    let (direct_p50, plane_p50) = (p50_of(direct), p50_of(plane));
    line(format!(
        "| Wide append: {rows} rows × {payload} B payload, PK on id, 5 commits | client writing the file and committing: straight to the catalog p50 {}; through the Plane p50 {} | +{} | Plane read per commit {} of a {} data file ({:.1} %) |",
        seconds(direct_p50),
        seconds(plane_p50),
        seconds(plane_p50 - direct_p50),
        mib(read_per_commit),
        mib(file_size),
        100.0 * read_per_commit / file_size.max(1.0),
    ));

    // 4. One writer, sequential single-row FK appends: the per-commit cost without contention.
    let before = b.metrics().await?;
    let mut latencies = Vec::new();
    let t = Instant::now();
    let sequential = 100;
    for _ in 0..sequential {
        let id = next_order;
        next_order += 1;
        let start = Instant::now();
        let status = b
            .commit(
                &b.gateway,
                "orders",
                &Change::Append(vec![id], vec![Some(0)]),
            )
            .await?;
        assert_eq!(status, 200, "sequential append");
        latencies.push(start.elapsed());
    }
    let elapsed = t.elapsed();
    latencies.sort();
    let after = b.metrics().await?;
    line(format!(
        "| One writer: {sequential} sequential single-row FK appends | end-to-end p50 {} / p99 {}; server validation p50 {} | {:.0} commits/s | |",
        seconds(percentile(&latencies, 0.5).as_secs_f64()),
        seconds(percentile(&latencies, 0.99).as_secs_f64()),
        after.quantile_bound(&before, "integrity_validation_seconds", 0.5),
        sequential as f64 / elapsed.as_secs_f64(),
    ));

    // 5. Hot key: concurrent writers appending one order each for parent key 0, retrying on 409;
    //    and the same workload sent straight to the catalog (no Plane, no constraints) as baseline.
    let b = Arc::new(b);
    let hot = |base: String, table: &'static str, first_id: i64| {
        let b = Arc::clone(&b);
        let writers = o.writers;
        let commits = o.hot_commits;
        async move {
            let next = Arc::new(AtomicI64::new(first_id));
            let t = Instant::now();
            let mut tasks = Vec::new();
            for _ in 0..writers {
                let b = Arc::clone(&b);
                let next = Arc::clone(&next);
                let base = base.clone();
                tasks.push(tokio::spawn(async move {
                    let mut latencies = Vec::new();
                    let mut attempts = 0usize;
                    for _ in 0..commits {
                        let id = next.fetch_add(1, Ordering::SeqCst);
                        let t = Instant::now();
                        loop {
                            attempts += 1;
                            let status = b
                                .commit(&base, table, &Change::Append(vec![id], vec![Some(0)]))
                                .await?;
                            match status {
                                200 => break,
                                409 => continue,
                                other => {
                                    return Err::<_, BoxError>(format!("hot key: {other}").into());
                                }
                            }
                        }
                        latencies.push(t.elapsed());
                    }
                    Ok((latencies, attempts))
                }));
            }
            let mut latencies = Vec::new();
            let mut attempts = 0;
            for task in tasks {
                let (l, a) = task.await??;
                latencies.extend(l);
                attempts += a;
            }
            let elapsed = t.elapsed();
            latencies.sort();
            Ok::<_, BoxError>((latencies, attempts, elapsed))
        }
    };
    // Both runs start from an empty table: metadata grows with every snapshot.
    b.register(
        json!({"table": "bench.hot", "name": "pk_hot", "type": "PRIMARY_KEY", "columns": ["id"]}),
    )
    .await?;
    b.register(json!({"table": "bench.hot", "name": "fk_hot_customer", "type": "FOREIGN_KEY",
                      "columns": ["ref"], "references": {"table": "bench.customer", "constraint": "pk_customer"}}))
        .await?;
    let before = b.metrics().await?;
    let (latencies, attempts, elapsed) = hot(b.gateway.clone(), "hot", 0).await?;
    let after = b.metrics().await?;
    line(format!(
        "| Hot parent key: {} writers × {} single-row FK appends to one new table | queue wait p50 {} / p99 {}; end-to-end incl. retries p50 {} / p99 {} | {:.0} commits/s | {} attempts for {} commits (409 retries) |",
        o.writers,
        o.hot_commits,
        after.quantile_bound(&before, "integrity_domain_queue_wait_seconds", 0.5),
        after.quantile_bound(&before, "integrity_domain_queue_wait_seconds", 0.99),
        seconds(percentile(&latencies, 0.5).as_secs_f64()),
        seconds(percentile(&latencies, 0.99).as_secs_f64()),
        latencies.len() as f64 / elapsed.as_secs_f64(),
        attempts,
        latencies.len(),
    ));
    let (latencies, attempts, elapsed) = hot(b.upstream.clone(), "baseline", 0).await?;
    line(format!(
        "| Baseline: the same writers straight to the catalog (no Plane) | end-to-end incl. retries p50 {} / p99 {} | {:.0} commits/s | {} attempts for {} commits |",
        seconds(percentile(&latencies, 0.5).as_secs_f64()),
        seconds(percentile(&latencies, 0.99).as_secs_f64()),
        latencies.len() as f64 / elapsed.as_secs_f64(),
        attempts,
        latencies.len(),
    ));
    line(format!(
        "| Control store after the run | indexes {} / txn log {} / registry {} | | |",
        mib(std::fs::metadata(control.join("indexes.redb"))?.len() as f64),
        mib(std::fs::metadata(control.join("txn.redb"))?.len() as f64),
        mib(std::fs::metadata(control.join("registry.redb"))?.len() as f64),
    ));

    println!("| Scenario | Time | Throughput | Notes |\n|---|---|---|---|\n{report}");
    let _ = std::fs::remove_dir_all(&o.dir);
    Ok(())
}
