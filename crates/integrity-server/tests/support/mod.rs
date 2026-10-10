//! Shared by the integration tests: a fake multi-table upstream catalog and real Parquet/Avro
//! files written the way engines write them.

#![allow(clippy::unwrap_used, dead_code)] // test helpers outside #[test] fns; not every test uses all

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use apache_avro::types::Value as Avro;
use apache_avro::{Codec, DeflateSettings, Schema, Writer};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

pub type Row = (i64, Option<i64>);

// ---------- fake upstream with several tables ----------

#[derive(Default)]
pub struct Upstream {
    pub tables: BTreeMap<String, Value>,
    /// Accepted commits in order: (table path, snapshot id).
    pub history: Vec<(String, i64)>,
    /// Tables whose commits wait for a permit.
    pub gates: HashMap<String, Arc<Semaphore>>,
}

pub type Shared = Arc<Mutex<Upstream>>;

pub async fn upstream(State(s): State<Shared>, req: Request) -> Response {
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

pub fn empty_table(uuid: &str) -> Value {
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

pub struct Dir(pub PathBuf);

impl Dir {
    pub fn new() -> Self {
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

pub fn avro(schema: &str, records: Vec<Vec<(&str, Avro)>>) -> Vec<u8> {
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
pub struct Files {
    pub dir: Dir,
    pub next: AtomicI64,
    pub manifests_of: Mutex<HashMap<i64, Vec<String>>>,
    pub rows_of: Mutex<HashMap<String, Vec<Row>>>,
}

impl Files {
    pub fn location(&self, name: &str) -> String {
        self.dir.0.join(name).to_string_lossy().replace('\\', "/")
    }

    /// A data file and its manifest holding `rows`; returns the manifest location.
    pub fn manifest(&self, rows: &[Row]) -> String {
        self.manifest_with_row_groups(rows, None)
    }

    /// Like `manifest`, with at most `row_group` rows per Parquet row group.
    pub fn manifest_with_row_groups(&self, rows: &[Row], row_group: Option<usize>) -> String {
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
        let props = row_group.map(|r| {
            parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_row_count(Some(r))
                .build()
        });
        let mut w = ArrowWriter::try_new(&mut data, schema, props).unwrap();
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
    pub fn list(&self, manifests: &[String]) -> (i64, String) {
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

    pub fn rows(&self, manifests: &[String]) -> Vec<Row> {
        let rows = self.rows_of.lock().unwrap();
        manifests.iter().flat_map(|m| rows[m].clone()).collect()
    }
}
