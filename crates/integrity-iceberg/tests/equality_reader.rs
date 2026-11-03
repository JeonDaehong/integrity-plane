//! Tables with equality deletes against a naive Iceberg reader (ADR 0031).
//!
//! The oracle below reads a snapshot as the Iceberg spec's scan planning describes it, file by
//! file, sharing nothing with `changes.rs` but the Avro manifest parsing: a data file's row is
//! live unless a position delete with a data sequence number at least the file's names it, or an
//! equality delete with a greater one holds its key. Random Flink-shaped histories (upserts, a key
//! twice in one checkpoint, deletes of missing keys) are mixed with the commits ADR 0031 accepts:
//! compactions with inherited, starting and stale sequence numbers (some losing or duplicating a
//! row), copy-on-write and merge-on-read deletes of a key, and removed equality delete files. For
//! every commit the Plane accepts, the rows it reads must be exactly what a reader sees change;
//! the commits ADR 0031 accepts must be accepted, and removing a delete that still hides rows must
//! not be.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use apache_avro::types::Value as Avro;
use apache_avro::{Codec, DeflateSettings, Schema, Writer};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use integrity_core::{Datum, KeyValue, LogicalType, RowBatch, Side};
use integrity_iceberg::manifest::{FileContent, read_manifest, read_manifest_list};
use integrity_iceberg::{
    FileIo, InspectError, MemoryIo, Operation, check_operation, commit_rows, diff_snapshots,
    for_each_commit_batch, for_each_live_batch,
};
use integrity_types::{FieldId, SnapshotId, TableId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use proptest::prelude::*;

const LIST_SCHEMA: &str = r#"{"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int"},
    {"name": "sequence_number", "type": "long"},
    {"name": "added_snapshot_id", "type": "long"}
]}"#;

const ENTRY_SCHEMA: &str = r#"{"type": "record", "name": "manifest_entry", "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"]},
    {"name": "sequence_number", "type": ["null", "long"]},
    {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
        {"name": "content", "type": "int"},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "record_count", "type": "long"},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}]},
        {"name": "referenced_data_file", "type": ["null", "string"], "default": null},
        {"name": "content_offset", "type": ["null", "long"], "default": null},
        {"name": "content_size_in_bytes", "type": ["null", "long"], "default": null}
    ]}}
]}"#;

/// A row of the table `(id long #1, region string #2)`; the upsert key is `id`.
type Row = (i64, String);

const COLUMNS: [(FieldId, LogicalType); 2] = [
    (FieldId(1), LogicalType::Long),
    (FieldId(2), LogicalType::String),
];

// ---------- the oracle: a naive reader ----------

/// A live data file as a reader sees it.
#[derive(Debug, Clone)]
struct DataView {
    path: String,
    seq: i64,
    rows: Vec<Row>,
    /// Per row: not deleted by any delete file.
    live: Vec<bool>,
}

impl DataView {
    fn live_rows(&self) -> Vec<Row> {
        self.rows
            .iter()
            .zip(&self.live)
            .filter(|(_, l)| **l)
            .map(|(r, _)| r.clone())
            .collect()
    }
}

/// A snapshot as a reader sees it.
#[derive(Debug, Clone, Default)]
struct View {
    data: Vec<DataView>,
    /// Live equality delete files: `(path, data sequence number)`.
    equality: Vec<(String, i64)>,
}

impl View {
    /// Every live row, sorted.
    fn rows(&self) -> Vec<Row> {
        sorted(self.data.iter().flat_map(DataView::live_rows).collect())
    }
}

fn batches(io: &MemoryIo, path: &str) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(io.read(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn longs(b: &RecordBatch, name: &str) -> Vec<i64> {
    let c = b.column_by_name(name).unwrap();
    let a = c.as_any().downcast_ref::<Int64Array>().unwrap();
    a.values().to_vec()
}

fn strings(b: &RecordBatch, name: &str) -> Vec<String> {
    let c = b.column_by_name(name).unwrap();
    let a = c.as_any().downcast_ref::<StringArray>().unwrap();
    (0..a.len()).map(|i| a.value(i).to_owned()).collect()
}

/// Reads the snapshot whose manifest list is `list` as the Iceberg spec says readers do
/// (unpartitioned table, so every delete file is global).
fn read(io: &MemoryIo, list: Option<&str>) -> View {
    let Some(list) = list else {
        return View::default();
    };
    let mut files = Vec::new();
    for m in read_manifest_list(&io.read(list).unwrap()).unwrap() {
        for e in read_manifest(&io.read(&m.path).unwrap()).unwrap() {
            if e.status.is_live() {
                let seq = e.sequence_number.or(m.sequence_number).unwrap();
                files.push((e.file, seq));
            }
        }
    }
    // Position deletes: data file -> (position, delete sequence number).
    let mut positions: HashMap<String, Vec<(i64, i64)>> = HashMap::new();
    // Equality deletes on `id`: id -> delete sequence numbers.
    let mut keys: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut view = View::default();
    for (f, seq) in &files {
        match f.content {
            FileContent::PositionDeletes => {
                for b in batches(io, &f.path) {
                    for (p, pos) in strings(&b, "file_path").into_iter().zip(longs(&b, "pos")) {
                        positions.entry(p).or_default().push((pos, *seq));
                    }
                }
            }
            FileContent::EqualityDeletes => {
                assert_eq!(f.equality_ids.as_deref(), Some(&[1][..]));
                for b in batches(io, &f.path) {
                    for id in longs(&b, "id") {
                        keys.entry(id).or_default().push(*seq);
                    }
                }
                view.equality.push((f.path.clone(), *seq));
            }
            FileContent::Data => {}
        }
    }
    for (f, seq) in &files {
        if f.content != FileContent::Data {
            continue;
        }
        let mut rows = Vec::new();
        for b in batches(io, &f.path) {
            rows.extend(longs(&b, "id").into_iter().zip(strings(&b, "region")));
        }
        let gone = positions.get(&f.path);
        let live = rows
            .iter()
            .enumerate()
            .map(|(i, (id, _))| {
                let by_position =
                    gone.is_some_and(|g| g.iter().any(|&(p, s)| p == i as i64 && s >= *seq));
                let by_key = keys.get(id).is_some_and(|s| s.iter().any(|s| s > seq));
                !by_position && !by_key
            })
            .collect();
        view.data.push(DataView {
            path: f.path.clone(),
            seq: *seq,
            rows,
            live,
        });
    }
    view.data.sort_by(|a, b| a.path.cmp(&b.path));
    view.equality.sort();
    view
}

fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

fn plain(batch: &RowBatch) -> Vec<Row> {
    batch.rows().iter().map(|r| plain_row(r)).collect()
}

fn plain_row(r: &[Datum]) -> Row {
    match (&r[0], &r[1]) {
        (Datum::Value(KeyValue::Integer(id)), Datum::Value(KeyValue::String(s))) => {
            (*id, s.clone())
        }
        other => panic!("unexpected row {other:?}"),
    }
}

/// `a ⊎ b`, sorted.
fn union(a: &[Row], b: &[Row]) -> Vec<Row> {
    sorted(a.iter().chain(b).cloned().collect())
}

// ---------- writing a table ----------

fn avro(schema: &str, records: Vec<Vec<(&str, Avro)>>) -> Bytes {
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
    Bytes::from(w.into_inner().unwrap())
}

fn parquet(schema: Vec<Field>, columns: Vec<Arc<dyn Array>>) -> Vec<u8> {
    let schema = Arc::new(arrow_schema::Schema::new(schema));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    out
}

fn field(name: &str, ty: DataType, id: &str) -> Field {
    Field::new(name, ty, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

/// A table's history: one manifest per content file, each listed with the sequence number of the
/// commit that wrote it.
struct History {
    io: MemoryIo,
    files: usize,
    seq: i64,
    /// Manifests of the current snapshot: file path -> (manifest path, manifest content).
    head: BTreeMap<String, (String, i32, i64)>,
    /// Manifest list of every snapshot, oldest first.
    lists: Vec<String>,
}

impl History {
    fn new() -> Self {
        Self {
            io: MemoryIo::new(),
            files: 0,
            seq: 0,
            head: BTreeMap::new(),
            lists: Vec::new(),
        }
    }

    fn head_list(&self) -> Option<&str> {
        self.lists.last().map(String::as_str)
    }

    fn name(&mut self, kind: &str) -> String {
        self.files += 1;
        format!("{kind}-{}.parquet", self.files)
    }

    fn data(&mut self, rows: &[Row]) -> String {
        let path = self.name("data");
        let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let regions: Vec<&str> = rows.iter().map(|r| r.1.as_str()).collect();
        let bytes = parquet(
            vec![
                field("id", DataType::Int64, "1"),
                field("region", DataType::Utf8, "2"),
            ],
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(regions)),
            ],
        );
        self.io.insert(path.clone(), bytes);
        path
    }

    fn equality(&mut self, ids: &[i64]) -> String {
        let path = self.name("eq");
        let bytes = parquet(
            vec![field("id", DataType::Int64, "1")],
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        );
        self.io.insert(path.clone(), bytes);
        path
    }

    fn positions(&mut self, deletes: &[(String, i64)]) -> String {
        let path = self.name("pos");
        let bytes = parquet(
            vec![
                field("file_path", DataType::Utf8, "2147483546"),
                field("pos", DataType::Int64, "2147483545"),
            ],
            vec![
                Arc::new(StringArray::from(
                    deletes.iter().map(|d| d.0.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    deletes.iter().map(|d| d.1).collect::<Vec<_>>(),
                )),
            ],
        );
        self.io.insert(path.clone(), bytes);
        path
    }

    /// Commits a snapshot that adds `added` files `(path, content 0/1/2, record count, explicit
    /// data sequence number)` and drops the manifests of `removed` files. Returns the parent's and
    /// the new manifest list, and the sequence number before the commit (to undo it).
    fn commit(
        &mut self,
        added: &[(String, i32, i64, Option<i64>)],
        removed: &[String],
    ) -> (Option<String>, String, Undo) {
        let undo = Undo {
            seq: self.seq,
            head: self.head.clone(),
            lists: self.lists.len(),
        };
        self.seq += 1;
        let seq = self.seq;
        for f in removed {
            self.head.remove(f).unwrap();
        }
        for (path, content, count, explicit) in added {
            let manifest = format!("m-{path}.avro");
            let record = vec![
                ("status", Avro::Int(1)),
                ("snapshot_id", Avro::Union(1, Box::new(Avro::Long(seq)))),
                (
                    "sequence_number",
                    match explicit {
                        Some(n) => Avro::Union(1, Box::new(Avro::Long(*n))),
                        None => Avro::Union(0, Box::new(Avro::Null)),
                    },
                ),
                (
                    "data_file",
                    Avro::Record(vec![
                        ("content".into(), Avro::Int(*content)),
                        ("file_path".into(), Avro::String(path.clone())),
                        ("file_format".into(), Avro::String("PARQUET".into())),
                        ("record_count".into(), Avro::Long(*count)),
                        (
                            "equality_ids".into(),
                            if *content == 2 {
                                Avro::Union(1, Box::new(Avro::Array(vec![Avro::Int(1)])))
                            } else {
                                Avro::Union(0, Box::new(Avro::Null))
                            },
                        ),
                        (
                            "referenced_data_file".into(),
                            Avro::Union(0, Box::new(Avro::Null)),
                        ),
                        (
                            "content_offset".into(),
                            Avro::Union(0, Box::new(Avro::Null)),
                        ),
                        (
                            "content_size_in_bytes".into(),
                            Avro::Union(0, Box::new(Avro::Null)),
                        ),
                    ]),
                ),
            ];
            self.io
                .insert(manifest.clone(), avro(ENTRY_SCHEMA, vec![record]));
            let kind = if *content == 0 { 0 } else { 1 };
            self.head.insert(path.clone(), (manifest, kind, seq));
        }
        let records = self
            .head
            .values()
            .map(|(m, kind, seq)| {
                vec![
                    ("manifest_path", Avro::String(m.clone())),
                    ("manifest_length", Avro::Long(1)),
                    ("partition_spec_id", Avro::Int(0)),
                    ("content", Avro::Int(*kind)),
                    ("sequence_number", Avro::Long(*seq)),
                    ("added_snapshot_id", Avro::Long(*seq)),
                ]
            })
            .collect();
        let list = format!("list-{seq}.avro");
        self.io.insert(list.clone(), avro(LIST_SCHEMA, records));
        let parent = self.lists.last().cloned();
        self.lists.push(list.clone());
        (parent, list, undo)
    }

    fn undo(&mut self, u: Undo) {
        self.seq = u.seq;
        self.head = u.head;
        self.lists.truncate(u.lists);
    }
}

struct Undo {
    seq: i64,
    head: BTreeMap<String, (String, i32, i64)>,
    lists: usize,
}

// ---------- operations ----------

#[derive(Debug, Clone, Copy)]
enum SeqChoice {
    /// The commit's own (Spark `use-starting-sequence-number=false`).
    Inherited,
    /// The sequence number of the snapshot the rewrite read.
    Starting,
    /// Rows read from the snapshot before the current one, committed with its sequence number:
    /// a rewrite that started before the last commit.
    Stale,
}

#[derive(Debug, Clone, Copy)]
enum Flaw {
    None,
    /// The rewrite drops a row.
    Lose,
    /// The rewrite writes a row twice.
    Duplicate,
}

#[derive(Debug, Clone)]
enum Op {
    /// A Flink checkpoint in upsert mode: a data file, an equality delete of its keys, and a
    /// position delete of the earlier rows of a key written twice.
    Upsert(Vec<(i64, usize)>),
    /// A Flink checkpoint deleting keys: an equality delete only.
    Delete(Vec<i64>),
    /// A rewrite of some data files into one.
    Compact {
        pick: Vec<bool>,
        seq: SeqChoice,
        flaw: Flaw,
    },
    /// Spark copy-on-write `DELETE WHERE id = ..` on one data file.
    CowDelete { pick: usize, id: i64 },
    /// Spark merge-on-read `DELETE WHERE id = ..` on one data file: a position delete naming every
    /// row of the key, including rows equality deletes already hide.
    MorDelete { pick: usize, id: i64 },
    /// Removes one equality delete file.
    RemoveEquality { pick: usize },
}

const REGIONS: [&str; 3] = ["eu", "us", "ap"];

fn op() -> impl Strategy<Value = Op> {
    let id = 0i64..5;
    prop_oneof![
        4 => proptest::collection::vec((id.clone(), 0usize..3), 1..5).prop_map(Op::Upsert),
        1 => proptest::collection::vec(id.clone(), 1..3).prop_map(Op::Delete),
        2 => (
            proptest::collection::vec(any::<bool>(), 4),
            prop_oneof![
                Just(SeqChoice::Inherited),
                Just(SeqChoice::Starting),
                Just(SeqChoice::Stale)
            ],
            prop_oneof![6 => Just(Flaw::None), 1 => Just(Flaw::Lose), 1 => Just(Flaw::Duplicate)],
        )
            .prop_map(|(pick, seq, flaw)| Op::Compact { pick, seq, flaw }),
        1 => (0usize..8, id.clone()).prop_map(|(pick, id)| Op::CowDelete { pick, id }),
        1 => (0usize..8, id).prop_map(|(pick, id)| Op::MorDelete { pick, id }),
        1 => (0usize..8).prop_map(|pick| Op::RemoveEquality { pick }),
    ]
}

/// What ADR 0031 says about a commit.
#[derive(Debug, PartialEq, Eq)]
enum Expect {
    /// Must be accepted.
    Accept,
    /// Must be refused.
    Refuse,
}

/// A commit to check: the files it adds and removes, its operation and what must happen.
struct Planned {
    added: Vec<(String, i32, i64, Option<i64>)>,
    removed: Vec<String>,
    operation: Operation,
    expect: Expect,
}

/// Turns `op` into a commit on the current snapshot, or `None` if it does not apply.
fn plan(h: &mut History, op: &Op) -> Option<Planned> {
    let view = read(&h.io, h.head_list());
    let pick = |n: usize, i: usize| (n > 0).then(|| i % n);
    match op {
        Op::Upsert(rows) => {
            let rows: Vec<Row> = rows
                .iter()
                .map(|&(id, r)| (id, REGIONS[r].to_owned()))
                .collect();
            let data = h.data(&rows);
            let ids: BTreeSet<i64> = rows.iter().map(|r| r.0).collect();
            let ids: Vec<i64> = ids.into_iter().collect();
            let eq = h.equality(&ids);
            let mut added = vec![
                (data.clone(), 0, rows.len() as i64, None),
                (eq, 2, ids.len() as i64, None),
            ];
            // Earlier rows of a key written twice in this checkpoint.
            let dups: Vec<(String, i64)> = (0..rows.len())
                .filter(|&i| rows[i + 1..].iter().any(|r| r.0 == rows[i].0))
                .map(|i| (data.clone(), i as i64))
                .collect();
            if !dups.is_empty() {
                let n = dups.len() as i64;
                added.push((h.positions(&dups), 1, n, None));
            }
            Some(Planned {
                added,
                removed: Vec::new(),
                operation: Operation::Overwrite,
                expect: Expect::Accept,
            })
        }
        Op::Delete(ids) => {
            let ids: BTreeSet<i64> = ids.iter().copied().collect();
            let ids: Vec<i64> = ids.into_iter().collect();
            let eq = h.equality(&ids);
            Some(Planned {
                added: vec![(eq, 2, ids.len() as i64, None)],
                removed: Vec::new(),
                operation: Operation::Delete,
                expect: Expect::Accept,
            })
        }
        Op::Compact { pick, seq, flaw } => {
            let chosen: Vec<&DataView> = view
                .data
                .iter()
                .enumerate()
                .filter(|(i, _)| pick[i % pick.len()])
                .map(|(_, d)| d)
                .collect();
            if chosen.is_empty() {
                return None;
            }
            let paths: Vec<String> = chosen.iter().map(|d| d.path.clone()).collect();
            // The snapshot the rewrite read, and its sequence number.
            let previous = h
                .lists
                .len()
                .checked_sub(2)
                .map(|i| read(&h.io, Some(&h.lists[i])));
            let (source, explicit) = match (seq, previous) {
                (SeqChoice::Stale, Some(p))
                    if paths.iter().all(|f| p.data.iter().any(|d| &d.path == f)) =>
                {
                    (p, Some(h.seq - 1))
                }
                (SeqChoice::Inherited, _) => (view.clone(), None),
                _ => (view.clone(), Some(h.seq)),
            };
            let mut rows: Vec<Row> = source
                .data
                .iter()
                .filter(|d| paths.contains(&d.path))
                .flat_map(DataView::live_rows)
                .collect();
            match flaw {
                Flaw::None => {}
                Flaw::Lose => {
                    rows.pop();
                }
                Flaw::Duplicate => {
                    if let Some(r) = rows.first().cloned() {
                        rows.push(r);
                    }
                }
            }
            let mut added = Vec::new();
            if !rows.is_empty() {
                added.push((h.data(&rows), 0, rows.len() as i64, explicit));
            }
            Some(Planned {
                added,
                removed: paths,
                operation: Operation::Replace,
                expect: Expect::Accept,
            })
        }
        Op::CowDelete { pick: i, id } => {
            let d = &view.data[pick(view.data.len(), *i)?];
            let rows: Vec<Row> = d.live_rows().into_iter().filter(|r| r.0 != *id).collect();
            let mut added = Vec::new();
            let path = d.path.clone();
            if !rows.is_empty() {
                added.push((h.data(&rows), 0, rows.len() as i64, None));
            }
            Some(Planned {
                added,
                removed: vec![path],
                operation: Operation::Overwrite,
                expect: Expect::Accept,
            })
        }
        Op::MorDelete { pick: i, id } => {
            let d = &view.data[pick(view.data.len(), *i)?];
            let gone: Vec<(String, i64)> = d
                .rows
                .iter()
                .enumerate()
                .filter(|(_, r)| r.0 == *id)
                .map(|(p, _)| (d.path.clone(), p as i64))
                .collect();
            if gone.is_empty() {
                return None;
            }
            let n = gone.len() as i64;
            let pos = h.positions(&gone);
            Some(Planned {
                added: vec![(pos, 1, n, None)],
                removed: Vec::new(),
                operation: Operation::Delete,
                expect: Expect::Accept,
            })
        }
        Op::RemoveEquality { pick: i } => {
            let (path, seq) = view.equality[pick(view.equality.len(), *i)?].clone();
            // Dangling: no live data file is old enough for it to apply to.
            let dangling = view.data.iter().all(|d| d.seq >= seq);
            Some(Planned {
                added: Vec::new(),
                removed: vec![path],
                operation: Operation::Replace,
                expect: if dangling {
                    Expect::Accept
                } else {
                    Expect::Refuse
                },
            })
        }
    }
}

/// What the Plane reads for the commit `parent` -> `new`, if it accepts it: rows added, rows
/// removed, equality delete keys.
#[allow(clippy::type_complexity)]
fn plane_rows(
    io: &MemoryIo,
    parent: Option<&str>,
    new: &str,
) -> Result<
    (
        Vec<Row>,
        Vec<Row>,
        Option<Vec<i64>>,
        integrity_core::CommitRows,
    ),
    InspectError,
> {
    let changes = diff_snapshots(io, parent, new)?;
    let rows = commit_rows(io, TableId::new("t"), SnapshotId(2), &changes, &COLUMNS)?;
    let keys = rows.equality_deletes.as_ref().map(|b| {
        b.rows()
            .iter()
            .map(|r| match &r[0] {
                Datum::Value(KeyValue::Integer(id)) => *id,
                other => panic!("unexpected key {other:?}"),
            })
            .collect()
    });
    if keys.is_none() {
        // Streamed, the same rows (ADR 0019).
        let (mut added, mut removed) = (Vec::new(), Vec::new());
        for_each_commit_batch(io, &changes, &COLUMNS, |side, b| {
            match side {
                Side::Added => added.extend(plain(&b)),
                Side::Removed => removed.extend(plain(&b)),
            }
            Ok::<_, InspectError>(())
        })?;
        assert_eq!(sorted(added), sorted(plain(&rows.added)), "streamed added");
        assert_eq!(
            sorted(removed),
            sorted(plain(&rows.removed)),
            "streamed removed"
        );
    }
    Ok((
        sorted(plain(&rows.added)),
        sorted(plain(&rows.removed)),
        keys,
        rows,
    ))
}

/// Every live row of the snapshot, as onboarding reads it.
fn whole_table(io: &MemoryIo, list: &str) -> Result<Vec<Row>, InspectError> {
    let changes = diff_snapshots(io, None, list)?;
    let mut rows = Vec::new();
    for_each_live_batch(io, &changes, &COLUMNS, usize::MAX, |b| {
        rows.extend(plain(&b));
        Ok::<_, InspectError>(())
    })?;
    Ok(sorted(rows))
}

/// Applies `ops` in order, checking every commit; returns how many commits of each kind were
/// accepted (to show the generator reaches them).
fn run(ops: &[Op]) -> Result<BTreeMap<&'static str, usize>, TestCaseError> {
    let mut h = History::new();
    let mut accepted = BTreeMap::new();
    for op in ops {
        let Some(planned) = plan(&mut h, op) else {
            continue;
        };
        let before = read(&h.io, h.head_list());
        let (parent, new, undo) = h.commit(&planned.added, &planned.removed);
        let after = read(&h.io, Some(&new));
        match plane_rows(&h.io, parent.as_deref(), &new) {
            Err(e) => {
                prop_assert!(
                    planned.expect == Expect::Refuse,
                    "{op:?} refused: {e}\nbefore {before:?}\nafter {after:?}"
                );
                h.undo(undo);
                continue;
            }
            Ok((added, removed, keys, rows)) => {
                prop_assert!(
                    planned.expect == Expect::Accept,
                    "{op:?} accepted\nbefore {before:?}\nafter {after:?}"
                );
                match keys {
                    // ADR 0009: existing rows of the keys go, then the commit's rows come.
                    Some(keys) => {
                        prop_assert!(removed.is_empty(), "{op:?} removed {removed:?}");
                        let kept: Vec<Row> = before
                            .rows()
                            .into_iter()
                            .filter(|r| !keys.contains(&r.0))
                            .collect();
                        prop_assert_eq!(union(&kept, &added), after.rows(), "{:?}", op);
                    }
                    // Exact rows: what leaves and what comes balance what readers see.
                    None => {
                        prop_assert_eq!(
                            union(&before.rows(), &added),
                            union(&after.rows(), &removed),
                            "{:?}",
                            op
                        );
                    }
                }
                if planned.operation == Operation::Replace {
                    // A rewrite is valid exactly when readers see the same rows.
                    let same = before.rows() == after.rows();
                    let checked = check_operation(Operation::Replace, &rows).is_ok();
                    prop_assert_eq!(checked, same, "replace check of {:?}", op);
                    if !same {
                        // Refused by the validator: not part of the history.
                        h.undo(undo);
                        continue;
                    }
                }
                *accepted.entry(kind(op)).or_insert(0) += 1;
            }
        }
        // Onboarding reads what readers read.
        let whole = whole_table(&h.io, &new);
        prop_assert!(whole.is_ok(), "whole table after {op:?}: {whole:?}");
        prop_assert_eq!(whole.unwrap(), after.rows(), "whole table after {:?}", op);
    }
    Ok(accepted)
}

fn kind(op: &Op) -> &'static str {
    match op {
        Op::Upsert(_) => "upsert",
        Op::Delete(_) => "delete",
        Op::Compact { .. } => "compact",
        Op::CowDelete { .. } => "cow-delete",
        Op::MorDelete { .. } => "mor-delete",
        Op::RemoveEquality { .. } => "remove-equality",
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    #[test]
    fn equality_delete_tables_read_as_readers_read_them(
        ops in proptest::collection::vec(op(), 1..14)
    ) {
        run(&ops)?;
    }
}

/// The generator reaches every kind of commit, and the shapes the ADR names.
#[test]
fn random_histories_reach_every_commit_kind() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let mut total: BTreeMap<&str, usize> = BTreeMap::new();
    for _ in 0..64 {
        let ops = proptest::collection::vec(op(), 1..14)
            .new_tree(&mut runner)
            .unwrap()
            .current();
        for (k, n) in run(&ops).unwrap() {
            *total.entry(k).or_insert(0) += n;
        }
    }
    for k in [
        "upsert",
        "delete",
        "compact",
        "cow-delete",
        "mor-delete",
        "remove-equality",
    ] {
        assert!(total.get(k).copied().unwrap_or(0) > 0, "{k}: {total:?}");
    }
}

// ---------- the ADR's named cases ----------

/// Upserts (1, eu), (2, eu) (sequence number 1), then (1, us) (2).
fn two_checkpoints() -> History {
    let mut h = History::new();
    for op in [Op::Upsert(vec![(1, 0), (2, 0)]), Op::Upsert(vec![(1, 1)])] {
        let p = plan(&mut h, &op).unwrap();
        h.commit(&p.added, &p.removed);
    }
    h
}

#[test]
fn a_compaction_applies_equality_deletes_to_the_rows_it_removes() {
    let mut h = two_checkpoints();
    let p = plan(
        &mut h,
        &Op::Compact {
            pick: vec![true],
            seq: SeqChoice::Starting,
            flaw: Flaw::None,
        },
    )
    .unwrap();
    let (parent, new, _) = h.commit(&p.added, &p.removed);
    let (added, removed, keys, rows) = plane_rows(&h.io, parent.as_deref(), &new).unwrap();
    assert_eq!(keys, None);
    let expected = vec![(1, "us".to_owned()), (2, "eu".to_owned())];
    assert_eq!(added, expected);
    // (1, eu) is hidden by the second checkpoint's delete: not removed again.
    assert_eq!(removed, expected);
    check_operation(Operation::Replace, &rows).unwrap();
}

#[test]
fn a_later_delete_hides_rows_of_a_rewrite_that_started_before_it() {
    let mut h = two_checkpoints();
    // The rewrite reads the head (sequence number 2) ...
    let view = read(&h.io, h.head_list());
    let rows: Vec<Row> = view.data.iter().flat_map(DataView::live_rows).collect();
    let removed: Vec<String> = view.data.iter().map(|d| d.path.clone()).collect();
    // ... a checkpoint upserting 2 commits (sequence number 3) ...
    let p = plan(&mut h, &Op::Upsert(vec![(2, 2)])).unwrap();
    h.commit(&p.added, &p.removed);
    // ... and the rewrite commits with its starting sequence number, removing only the files it
    // read.
    let data = h.data(&rows);
    let (parent, new, _) = h.commit(&[(data, 0, rows.len() as i64, Some(2))], &removed);
    let after = read(&h.io, Some(&new));
    assert_eq!(
        after.rows(),
        vec![(1, "us".to_owned()), (2, "ap".to_owned())]
    );
    let (added, removed, _, rows) = plane_rows(&h.io, parent.as_deref(), &new).unwrap();
    // The rewritten (2, eu) is hidden by the later delete on both sides.
    assert_eq!(added, vec![(1, "us".to_owned())]);
    assert_eq!(removed, vec![(1, "us".to_owned())]);
    check_operation(Operation::Replace, &rows).unwrap();
}

#[test]
fn a_rewrite_with_an_inherited_sequence_number_brings_hidden_rows_back() {
    // The same rewrite committed with its own sequence number: the later delete no longer
    // applies to (2, eu), which readers see again. Not a valid replace.
    let mut h = two_checkpoints();
    let view = read(&h.io, h.head_list());
    let rows: Vec<Row> = view.data.iter().flat_map(DataView::live_rows).collect();
    let removed: Vec<String> = view.data.iter().map(|d| d.path.clone()).collect();
    let p = plan(&mut h, &Op::Upsert(vec![(2, 2)])).unwrap();
    h.commit(&p.added, &p.removed);
    let data = h.data(&rows);
    let (parent, new, _) = h.commit(&[(data, 0, rows.len() as i64, None)], &removed);
    let (added, _, _, rows) = plane_rows(&h.io, parent.as_deref(), &new).unwrap();
    assert!(added.contains(&(2, "eu".to_owned())), "{added:?}");
    assert!(check_operation(Operation::Replace, &rows).is_err());
}

#[test]
fn only_dangling_equality_deletes_may_be_removed() {
    let mut h = two_checkpoints();
    let view = read(&h.io, h.head_list());
    // The second checkpoint's delete still hides (1, eu).
    let (applies, _) = view.equality.iter().max_by_key(|e| e.1).unwrap().clone();
    let (parent, new, undo) = h.commit(&[], std::slice::from_ref(&applies));
    assert!(plane_rows(&h.io, parent.as_deref(), &new).is_err());
    h.undo(undo);
    // After a compaction with an inherited sequence number, no data file is older than any
    // delete: both are dangling.
    let p = plan(
        &mut h,
        &Op::Compact {
            pick: vec![true],
            seq: SeqChoice::Inherited,
            flaw: Flaw::None,
        },
    )
    .unwrap();
    h.commit(&p.added, &p.removed);
    let deletes: Vec<String> = read(&h.io, h.head_list())
        .equality
        .into_iter()
        .map(|e| e.0)
        .collect();
    let (parent, new, _) = h.commit(&[], &deletes);
    let (added, removed, keys, rows) = plane_rows(&h.io, parent.as_deref(), &new).unwrap();
    assert_eq!((added.len(), removed.len(), keys), (0, 0, None));
    check_operation(Operation::Replace, &rows).unwrap();
}

#[test]
fn a_commit_adding_equality_deletes_may_not_remove_data_files() {
    let mut h = two_checkpoints();
    let view = read(&h.io, h.head_list());
    let eq = h.equality(&[2]);
    let (parent, new, _) = h.commit(&[(eq, 2, 1, None)], &[view.data[0].path.clone()]);
    let e = plane_rows(&h.io, parent.as_deref(), &new).unwrap_err();
    assert_eq!(
        e.code(),
        integrity_types::ErrorCode::UnsupportedCommitOperation
    );
}
