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
    /// Tables whose reads wait for a permit.
    pub read_gates: HashMap<String, Arc<Semaphore>>,
    /// Multi-table commits received (RFC 0012).
    pub transactions: u64,
    /// Requests that reached a gate.
    pub gated: u64,
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
    if method == "GET" {
        let gate = s.lock().unwrap().read_gates.get(&path).cloned();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
    }
    if method == "POST" {
        let gate = s.lock().unwrap().gates.get(&path).cloned();
        if let Some(gate) = gate {
            s.lock().unwrap().gated += 1;
            gate.acquire().await.unwrap().forget();
        }
    }
    let mut up = s.lock().unwrap();
    if method == "POST" && path.ends_with("/transactions/commit") {
        return commit_transaction(&mut up, &body);
    }
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
        return conflict("stale");
    }
    let head = apply_updates(&mut meta, &req["updates"]);
    up.tables.insert(path.clone(), meta.clone());
    up.history.push((path, head.unwrap()));
    axum::Json(json!({"metadata-location": "m", "metadata": meta})).into_response()
}

fn conflict(message: &str) -> Response {
    (
        StatusCode::CONFLICT,
        axum::Json(
            json!({"error": {"message": message, "type": "CommitFailedException", "code": 409}}),
        ),
    )
        .into_response()
}

/// Applies a commit's updates; the new `main`, if it moved.
fn apply_updates(meta: &mut Value, updates: &Value) -> Option<i64> {
    let mut head = None;
    for u in updates.as_array().unwrap() {
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
    head
}

/// `POST /v1/transactions/commit`: every requirement of every table first, then all updates, or
/// nothing (as the catalogs RFC 0012 relies on do).
fn commit_transaction(up: &mut Upstream, body: &[u8]) -> Response {
    up.transactions += 1;
    let req: Value = serde_json::from_slice(body).unwrap();
    let changes = req["table-changes"].as_array().unwrap();
    let path_of = |c: &Value| {
        let ns: Vec<&str> = c["identifier"]["namespace"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap())
            .collect();
        format!(
            "/v1/namespaces/{}/tables/{}",
            ns.join("%1F"),
            c["identifier"]["name"].as_str().unwrap()
        )
    };
    for c in changes {
        let Some(meta) = up.tables.get(&path_of(c)) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        for r in c["requirements"].as_array().unwrap() {
            let holds = match r["type"].as_str().unwrap() {
                "assert-table-uuid" => meta["table-uuid"] == r["uuid"],
                "assert-ref-snapshot-id" => {
                    let current = &meta["refs"][r["ref"].as_str().unwrap()]["snapshot-id"];
                    *current == r["snapshot-id"]
                }
                _ => true,
            };
            if !holds {
                return conflict(&format!("requirement failed: {r}"));
            }
        }
    }
    for c in changes {
        let path = path_of(c);
        let mut meta = up.tables[&path].clone();
        if let Some(head) = apply_updates(&mut meta, &c["updates"]) {
            up.history.push((path.clone(), head));
        }
        up.tables.insert(path, meta);
    }
    StatusCode::NO_CONTENT.into_response()
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

// ---------- fake Vault Transit (RFC 0009) ----------

/// The token the fake Vault accepts.
pub const VAULT_TOKEN: &str = "s.fake-vault-token";

/// How the fake Vault answers sign requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultMode {
    Sign,
    /// 503, as a sealed Vault does.
    Sealed,
    /// A well-formed signature of another certificate.
    Lie,
}

/// A Transit engine at `transit/` with one key named `oip`, of `key_type`, whose versions are
/// Ed25519 seeds (version 1 first).
pub struct FakeVault {
    pub key_type: &'static str,
    pub versions: Vec<[u8; 32]>,
    pub mode: VaultMode,
    /// The `key_version` of every sign request.
    pub signed_with: Vec<u64>,
}

pub type SharedVault = Arc<Mutex<FakeVault>>;

pub async fn vault(State(s): State<SharedVault>, req: Request) -> Response {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    let token = req.headers().get("x-vault-token").cloned();
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let body = axum::body::to_bytes(req.into_body(), 1 << 20)
        .await
        .unwrap();
    if token.as_ref().and_then(|t| t.to_str().ok()) != Some(VAULT_TOKEN) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(json!({"errors": ["permission denied"]})),
        )
            .into_response();
    }
    let mut v = s.lock().unwrap();
    let versions = v.versions.clone();
    let signer = move |version: u64| {
        integrity_core::CertSigner::from_seed(&versions[usize::try_from(version - 1).unwrap()])
    };
    match (method.as_str(), path.as_str()) {
        ("GET", "/v1/transit/keys/oip") => {
            let keys: serde_json::Map<String, Value> = (1..=v.versions.len() as u64)
                .map(|n| {
                    let public = STANDARD.encode(signer(n).public_key());
                    (
                        n.to_string(),
                        json!({"public_key": public, "name": "ed25519"}),
                    )
                })
                .collect();
            axum::Json(json!({"data": {
                "type": v.key_type, "latest_version": v.versions.len(), "keys": keys
            }}))
            .into_response()
        }
        ("POST", "/v1/transit/sign/oip") => {
            if v.mode == VaultMode::Sealed {
                return (StatusCode::SERVICE_UNAVAILABLE, "sealed").into_response();
            }
            let req: Value = serde_json::from_slice(&body).unwrap();
            let version = req["key_version"].as_u64().unwrap();
            v.signed_with.push(version);
            let input = STANDARD.decode(req["input"].as_str().unwrap()).unwrap();
            let (tag, cert) = input.split_at(input.len() - 32);
            assert_eq!(tag, b"oip-cert-sig-v1");
            let mut cert = integrity_core::Digest(cert.try_into().unwrap());
            if v.mode == VaultMode::Lie {
                cert.0[0] ^= 1;
            }
            let hex = signer(version).sign(&cert);
            let bytes: Vec<u8> = (0..64)
                .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap())
                .collect();
            let signature = format!("vault:v{version}:{}", STANDARD.encode(bytes));
            axum::Json(json!({"data": {"signature": signature, "key_version": version}}))
                .into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Serves a fake Vault; returns its address and state.
pub async fn serve_vault(versions: Vec<[u8; 32]>) -> (String, SharedVault) {
    let state: SharedVault = Arc::new(Mutex::new(FakeVault {
        key_type: "ed25519",
        versions,
        mode: VaultMode::Sign,
        signed_with: Vec::new(),
    }));
    let app = axum::Router::new()
        .fallback(vault)
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
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
