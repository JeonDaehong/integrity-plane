//! Spec §15 data-level rows and adversarial manifests, with manifests written here in Avro and
//! data files in Parquet: compaction (replace), delete files, manifest rewrites, re-listed files,
//! client-declared status that lies, wrong formats and record counts.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value as Avro;
use apache_avro::{Codec, DeflateSettings, Schema, Writer};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use integrity_core::{Datum, KeyValue, LogicalType, Side};
use integrity_iceberg::{
    CommitDeletes, FileChanges, FileIo, InspectError, MemoryIo, Operation, check_operation,
    commit_rows, diff_snapshots, for_each_commit_batch, for_each_live_batch, for_each_side_batch,
};
use integrity_types::{ErrorCode, FieldId, SnapshotId, TableId};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

const LIST_SCHEMA: &str = r#"{"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int"},
    {"name": "added_snapshot_id", "type": "long"}
]}"#;

/// Manifest entries with a partition tuple `(region)`.
const PART_ENTRY_SCHEMA: &str = r#"{"type": "record", "name": "manifest_entry", "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"]},
    {"name": "sequence_number", "type": ["null", "long"]},
    {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
        {"name": "content", "type": "int"},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": [
            {"name": "region", "type": ["null", "string"]}
        ]}},
        {"name": "record_count", "type": "long"},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}]},
        {"name": "referenced_data_file", "type": ["null", "string"], "default": null},
        {"name": "content_offset", "type": ["null", "long"], "default": null},
        {"name": "content_size_in_bytes", "type": ["null", "long"], "default": null}
    ]}}
]}"#;

const LIST_SEQ_SCHEMA: &str = r#"{"type": "record", "name": "manifest_file", "fields": [
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

/// `(status, content, path, format, record_count)` with status 0 existing, 1 added, 2 deleted;
/// content 0 data, 1 position deletes, 2 equality deletes.
type Entry = (i32, i32, &'static str, &'static str, i64);

/// A Parquet entry of a table partitioned by `region`: `(entry, explicit sequence number, region,
/// referenced data file)`.
type PartEntry = (
    Entry,
    Option<i64>,
    Option<&'static str>,
    Option<&'static str>,
);

struct Table {
    io: MemoryIo,
    /// Rows per Parquet row group of data files (`None`: the writer's default, one group).
    row_group: Option<usize>,
}

impl Table {
    fn new() -> Self {
        Self {
            io: MemoryIo::new(),
            row_group: None,
        }
    }

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

    /// A manifest whose entries inherit their sequence numbers.
    fn manifest(&mut self, path: &str, entries: &[Entry]) {
        let with_seq: Vec<_> = entries.iter().map(|&e| (e, None)).collect();
        self.manifest_seq(path, &with_seq);
    }

    /// A manifest with an optional explicit sequence number per entry.
    fn manifest_seq(&mut self, path: &str, entries: &[(Entry, Option<i64>)]) {
        let records = entries
            .iter()
            .map(|&((status, content, file, format, count), seq)| {
                vec![
                    ("status", Avro::Int(status)),
                    ("snapshot_id", Avro::Union(1, Box::new(Avro::Long(1)))),
                    (
                        "sequence_number",
                        match seq {
                            Some(n) => Avro::Union(1, Box::new(Avro::Long(n))),
                            None => Avro::Union(0, Box::new(Avro::Null)),
                        },
                    ),
                    (
                        "data_file",
                        Avro::Record(vec![
                            ("content".into(), Avro::Int(content)),
                            ("file_path".into(), Avro::String(file.into())),
                            ("file_format".into(), Avro::String(format.into())),
                            ("record_count".into(), Avro::Long(count)),
                            (
                                "equality_ids".into(),
                                if content == 2 {
                                    let ids = if file.contains("region") { 2 } else { 1 };
                                    Avro::Union(1, Box::new(Avro::Array(vec![Avro::Int(ids)])))
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
                ]
            })
            .collect();
        self.io.insert(path, Self::avro(ENTRY_SCHEMA, records));
    }

    /// A Parquet equality delete file on `id` (#1).
    fn id_deletes(&mut self, path: &str, ids: &[i64]) {
        let meta = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(meta),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut w = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        self.io.insert(path, out);
    }

    /// A Parquet position delete file of `(data file path, row position)`.
    fn positions(&mut self, path: &str, deletes: &[(&str, i64)]) {
        let meta =
            |id: &str| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(meta("2147483546")),
            Field::new("pos", DataType::Int64, false).with_metadata(meta("2147483545")),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    deletes.iter().map(|d| d.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    deletes.iter().map(|d| d.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut w = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        self.io.insert(path, out);
    }

    /// A Puffin file holding one deletion vector for `data` (positions), behind a few bytes of
    /// other content; returns the manifest entry fields `(offset, size)`.
    fn dv(&mut self, path: &str, positions: &[u64]) -> (i64, i64) {
        let blob = integrity_iceberg::deletion_vector::encode(positions);
        let mut file = b"PFA1....".to_vec();
        let offset = file.len() as i64;
        file.extend_from_slice(&blob);
        file.extend_from_slice(b"footer");
        self.io.insert(path, file);
        (offset, blob.len() as i64)
    }

    /// A delete manifest of deletion vectors:
    /// `(status, puffin path, referenced data file, offset, size, deleted rows)`.
    fn dv_manifest(&mut self, path: &str, entries: &[(i32, &str, &str, i64, i64, i64)]) {
        let records = entries
            .iter()
            .map(|&(status, file, target, offset, size, count)| {
                vec![
                    ("status", Avro::Int(status)),
                    ("snapshot_id", Avro::Union(1, Box::new(Avro::Long(1)))),
                    ("sequence_number", Avro::Union(0, Box::new(Avro::Null))),
                    (
                        "data_file",
                        Avro::Record(vec![
                            ("content".into(), Avro::Int(1)),
                            ("file_path".into(), Avro::String(file.into())),
                            ("file_format".into(), Avro::String("PUFFIN".into())),
                            ("record_count".into(), Avro::Long(count)),
                            ("equality_ids".into(), Avro::Union(0, Box::new(Avro::Null))),
                            (
                                "referenced_data_file".into(),
                                Avro::Union(1, Box::new(Avro::String(target.into()))),
                            ),
                            (
                                "content_offset".into(),
                                Avro::Union(1, Box::new(Avro::Long(offset))),
                            ),
                            (
                                "content_size_in_bytes".into(),
                                Avro::Union(1, Box::new(Avro::Long(size))),
                            ),
                        ]),
                    ),
                ]
            })
            .collect();
        self.io.insert(path, Self::avro(ENTRY_SCHEMA, records));
    }

    /// A manifest list of `(manifest path, content)`.
    fn list(&mut self, path: &str, manifests: &[(&str, i32)]) {
        let records = manifests
            .iter()
            .map(|&(m, content)| {
                vec![
                    ("manifest_path", Avro::String(m.into())),
                    ("manifest_length", Avro::Long(1)),
                    ("partition_spec_id", Avro::Int(0)),
                    ("content", Avro::Int(content)),
                    ("added_snapshot_id", Avro::Long(1)),
                ]
            })
            .collect();
        self.io.insert(path, Self::avro(LIST_SCHEMA, records));
    }

    /// A manifest of a table partitioned by `region`.
    fn part_manifest(&mut self, path: &str, entries: &[PartEntry]) {
        let opt_string = |v: Option<&str>| match v {
            Some(s) => Avro::Union(1, Box::new(Avro::String(s.into()))),
            None => Avro::Union(0, Box::new(Avro::Null)),
        };
        let records = entries
            .iter()
            .map(
                |&((status, content, file, format, count), seq, region, target)| {
                    vec![
                        ("status", Avro::Int(status)),
                        ("snapshot_id", Avro::Union(1, Box::new(Avro::Long(1)))),
                        (
                            "sequence_number",
                            match seq {
                                Some(n) => Avro::Union(1, Box::new(Avro::Long(n))),
                                None => Avro::Union(0, Box::new(Avro::Null)),
                            },
                        ),
                        (
                            "data_file",
                            Avro::Record(vec![
                                ("content".into(), Avro::Int(content)),
                                ("file_path".into(), Avro::String(file.into())),
                                ("file_format".into(), Avro::String(format.into())),
                                (
                                    "partition".into(),
                                    Avro::Record(vec![("region".into(), opt_string(region))]),
                                ),
                                ("record_count".into(), Avro::Long(count)),
                                (
                                    "equality_ids".into(),
                                    if content == 2 {
                                        Avro::Union(1, Box::new(Avro::Array(vec![Avro::Int(1)])))
                                    } else {
                                        Avro::Union(0, Box::new(Avro::Null))
                                    },
                                ),
                                ("referenced_data_file".into(), opt_string(target)),
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
                    ]
                },
            )
            .collect();
        self.io.insert(path, Self::avro(PART_ENTRY_SCHEMA, records));
    }

    /// A v2 manifest list: `(manifest, content, sequence number its added entries inherit)`.
    fn list_seq(&mut self, path: &str, manifests: &[(&str, i32, i64)]) {
        let with_spec: Vec<_> = manifests
            .iter()
            .map(|&(m, c, seq)| (m, c, seq, 0))
            .collect();
        self.list_spec(path, &with_spec);
    }

    /// A v2 manifest list: `(manifest, content, sequence number, partition spec id)`.
    fn list_spec(&mut self, path: &str, manifests: &[(&str, i32, i64, i32)]) {
        let records = manifests
            .iter()
            .map(|&(m, content, seq, spec)| {
                vec![
                    ("manifest_path", Avro::String(m.into())),
                    ("manifest_length", Avro::Long(1)),
                    ("partition_spec_id", Avro::Int(spec)),
                    ("content", Avro::Int(content)),
                    ("sequence_number", Avro::Long(seq)),
                    ("added_snapshot_id", Avro::Long(1)),
                ]
            })
            .collect();
        self.io.insert(path, Self::avro(LIST_SEQ_SCHEMA, records));
    }

    /// A Parquet data file `(id long #1, region string #2)`.
    fn data(&mut self, path: &str, rows: &[(i64, &str)]) {
        let meta =
            |id: &str| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(meta("1")),
            Field::new("region", DataType::Utf8, true).with_metadata(meta("2")),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        let props = self.row_group.map(|r| {
            parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_row_count(Some(r))
                .build()
        });
        let mut w = ArrowWriter::try_new(&mut out, schema, props).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        self.io.insert(path, out);
    }

    fn diff(&self, parent: Option<&str>, new: &str) -> Result<FileChanges, InspectError> {
        diff_snapshots(&self.io, parent, new)
    }

    fn rows(&self, changes: &FileChanges) -> Result<integrity_core::CommitRows, InspectError> {
        commit_rows(
            &self.io,
            TableId::new("t"),
            SnapshotId(2),
            changes,
            &[
                (FieldId(1), LogicalType::Long),
                (FieldId(2), LogicalType::String),
            ],
        )
    }
}

fn paths(files: &[integrity_iceberg::manifest::DataFile]) -> Vec<&str> {
    let mut v: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    v.sort();
    v
}

fn unsupported(r: Result<impl std::fmt::Debug, InspectError>) -> String {
    match r {
        Err(e @ InspectError::Unsupported(_)) => {
            assert_eq!(e.code(), ErrorCode::UnsupportedCommitOperation);
            e.to_string()
        }
        other => panic!("expected unsupported, got {other:?}"),
    }
}

/// Parent snapshot: files a.parquet and b.parquet in manifest m1.
fn parent() -> Table {
    let mut t = Table::new();
    t.data("a.parquet", &[(1, "eu"), (2, "us")]);
    t.data("b.parquet", &[(3, "eu")]);
    t.manifest(
        "m1.avro",
        &[
            (1, 0, "a.parquet", "PARQUET", 2),
            (1, 0, "b.parquet", "PARQUET", 1),
        ],
    );
    t.list("parent.avro", &[("m1.avro", 0)]);
    t
}

#[test]
fn compaction_with_identical_rows_is_a_valid_replace() {
    let mut t = parent();
    t.data("ab.parquet", &[(2, "us"), (3, "eu"), (1, "eu")]);
    t.manifest(
        "m2.avro",
        &[
            (1, 0, "ab.parquet", "PARQUET", 3),
            (2, 0, "a.parquet", "PARQUET", 2),
            (2, 0, "b.parquet", "PARQUET", 1),
        ],
    );
    t.list("new.avro", &[("m2.avro", 0)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert_eq!(paths(&changes.added), ["ab.parquet"]);
    assert_eq!(paths(&changes.removed), ["a.parquet", "b.parquet"]);
    let rows = t.rows(&changes).unwrap();
    check_operation(Operation::Replace, &rows).unwrap();
}

#[test]
fn compaction_that_changes_rows_is_rejected() {
    let mut t = parent();
    t.data("ab.parquet", &[(1, "eu"), (2, "us"), (3, "us")]); // region of 3 changed
    t.manifest("m2.avro", &[(1, 0, "ab.parquet", "PARQUET", 3)]);
    t.list("new.avro", &[("m2.avro", 0)]);
    let rows = t
        .rows(&t.diff(Some("parent.avro"), "new.avro").unwrap())
        .unwrap();
    assert!(unsupported(check_operation(Operation::Replace, &rows)).contains("replace"));
    // The same change declared as an overwrite is fine at this level (the validator decides).
    check_operation(Operation::Overwrite, &rows).unwrap();
}

#[test]
fn manifest_rewrite_changes_nothing() {
    let mut t = parent();
    t.manifest(
        "m1-rewritten.avro",
        &[
            (0, 0, "a.parquet", "PARQUET", 2),
            (0, 0, "b.parquet", "PARQUET", 1),
        ],
    );
    t.list("new.avro", &[("m1-rewritten.avro", 0)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert_eq!(changes, FileChanges::default());
    check_operation(Operation::Replace, &t.rows(&changes).unwrap()).unwrap();
}

#[test]
fn append_reads_only_new_manifests() {
    let mut t = parent();
    t.data("c.parquet", &[(4, "eu")]);
    t.manifest("m2.avro", &[(1, 0, "c.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("m2.avro", 0)]);
    // m1 is in both lists, so it is never read: corrupting it changes nothing.
    let mut io = t.io.clone();
    io.insert("m1.avro", Bytes::from_static(b"not avro"));
    let changes = diff_snapshots(&io, Some("parent.avro"), "new.avro").unwrap();
    assert_eq!(paths(&changes.added), ["c.parquet"]);
    assert!(changes.removed.is_empty());
}

#[test]
fn declared_status_is_not_trusted() {
    // A new manifest lists a new file as EXISTING (as if old): it is still an added file.
    let mut t = parent();
    t.data("c.parquet", &[(4, "eu")]);
    t.manifest("m2.avro", &[(0, 0, "c.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("m2.avro", 0)]);
    assert_eq!(
        paths(&t.diff(Some("parent.avro"), "new.avro").unwrap().added),
        ["c.parquet"]
    );
}

#[test]
fn relisting_a_live_file_counts_as_adding_it_again() {
    // b.parquet stays live in m1 and is listed again in m2: readers would see its rows twice.
    let mut t = parent();
    t.manifest("m2.avro", &[(1, 0, "b.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("m2.avro", 0)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert_eq!(paths(&changes.added), ["b.parquet"]);
    let rows = t.rows(&changes).unwrap();
    assert_eq!(
        rows.added.rows(),
        &[vec![
            Datum::Value(KeyValue::Integer(3)),
            Datum::Value(KeyValue::String("eu".into()))
        ]]
    );
}

#[test]
fn equality_delete_files_limit_what_a_commit_may_remove() {
    let mut t = parent();
    t.manifest("d2.avro", &[(1, 2, "eq.parquet", "PARQUET", 1)]);
    t.list(
        "parent-with-deletes.avro",
        &[("m1.avro", 0), ("d2.avro", 1)],
    );
    // Removing a data file while equality delete files exist.
    t.manifest("m2.avro", &[(1, 0, "a.parquet", "PARQUET", 2)]);
    t.list("drop-b.avro", &[("m2.avro", 0), ("d2.avro", 1)]);
    assert!(
        unsupported(t.diff(Some("parent-with-deletes.avro"), "drop-b.avro"))
            .contains("equality delete files")
    );
    // Removing an equality delete file.
    t.list("no-deletes.avro", &[("m1.avro", 0)]);
    assert!(
        unsupported(t.diff(Some("parent-with-deletes.avro"), "no-deletes.avro"))
            .contains("removes equality delete files")
    );
    // Appending to a table with delete files is fine.
    t.data("c.parquet", &[(4, "eu")]);
    t.manifest("m3.avro", &[(1, 0, "c.parquet", "PARQUET", 1)]);
    t.list(
        "append.avro",
        &[("m1.avro", 0), ("d2.avro", 1), ("m3.avro", 0)],
    );
    let changes = t
        .diff(Some("parent-with-deletes.avro"), "append.avro")
        .unwrap();
    assert_eq!(paths(&changes.added), ["c.parquet"]);
    assert!(changes.equality_deletes.is_empty());
}

// ---------- merge-on-read position deletes (ADR 0017) ----------

fn row(id: i64, region: &str) -> Vec<Datum> {
    vec![
        Datum::Value(KeyValue::Integer(id)),
        Datum::Value(KeyValue::String(region.into())),
    ]
}

fn sorted(batch: &integrity_core::RowBatch) -> Vec<Vec<Datum>> {
    let mut rows = batch.rows().to_vec();
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

/// The parent of `parent()` plus position delete file pos1.parquet deleting row 1 of a.parquet
/// (`(2, "us")`), in delete manifest d1.avro: list parent-pos.avro.
fn parent_with_position_delete() -> Table {
    let mut t = parent();
    t.positions("pos1.parquet", &[("a.parquet", 1)]);
    t.manifest("d1.avro", &[(1, 1, "pos1.parquet", "PARQUET", 1)]);
    t.list("parent-pos.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    t
}

#[test]
fn position_deletes_remove_exactly_the_rows_they_name() {
    let mut t = parent();
    t.positions("pos.parquet", &[("a.parquet", 1)]);
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert!(changes.added.is_empty() && changes.removed.is_empty());
    let rows = t.rows(&changes).unwrap();
    assert_eq!(sorted(&rows.removed), [row(2, "us")]);
    assert!(rows.added.is_empty());
    check_operation(Operation::Delete, &rows).unwrap();
}

#[test]
fn rows_already_deleted_are_not_removed_again() {
    let mut t = parent_with_position_delete();
    // Deletes row 1 again (already gone) and row 0, with a duplicate entry.
    t.positions(
        "pos2.parquet",
        &[("a.parquet", 1), ("a.parquet", 0), ("a.parquet", 0)],
    );
    t.manifest("d2.avro", &[(1, 1, "pos2.parquet", "PARQUET", 3)]);
    t.list(
        "new.avro",
        &[("m1.avro", 0), ("d1.avro", 1), ("d2.avro", 1)],
    );
    let rows = t
        .rows(&t.diff(Some("parent-pos.avro"), "new.avro").unwrap())
        .unwrap();
    assert_eq!(sorted(&rows.removed), [row(1, "eu")]);
    assert!(rows.added.is_empty());
}

#[test]
fn a_merge_on_read_update_removes_old_rows_and_adds_new_ones() {
    let mut t = parent();
    t.data("c.parquet", &[(2, "eu")]);
    t.manifest("m2.avro", &[(1, 0, "c.parquet", "PARQUET", 1)]);
    t.positions("pos.parquet", &[("a.parquet", 1)]);
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list(
        "new.avro",
        &[("m1.avro", 0), ("m2.avro", 0), ("d1.avro", 1)],
    );
    let rows = t
        .rows(&t.diff(Some("parent.avro"), "new.avro").unwrap())
        .unwrap();
    assert_eq!(sorted(&rows.removed), [row(2, "us")]);
    assert_eq!(sorted(&rows.added), [row(2, "eu")]);
    check_operation(Operation::Overwrite, &rows).unwrap();
}

#[test]
fn compaction_of_a_merge_on_read_table_compares_live_rows() {
    // a.parquet has (1, eu) live and (2, us) deleted; b.parquet has (3, eu).
    let mut t = parent_with_position_delete();
    t.data("ab.parquet", &[(3, "eu"), (1, "eu")]);
    t.manifest("m2.avro", &[(1, 0, "ab.parquet", "PARQUET", 2)]);
    t.list("compacted.avro", &[("m2.avro", 0)]);
    let changes = t.diff(Some("parent-pos.avro"), "compacted.avro").unwrap();
    assert_eq!(paths(&changes.removed), ["a.parquet", "b.parquet"]);
    let rows = t.rows(&changes).unwrap();
    assert_eq!(sorted(&rows.removed), [row(1, "eu"), row(3, "eu")]);
    check_operation(Operation::Replace, &rows).unwrap();

    // A rewrite that brings the deleted row back is not a compaction.
    t.data("abc.parquet", &[(1, "eu"), (2, "us"), (3, "eu")]);
    t.manifest("m3.avro", &[(1, 0, "abc.parquet", "PARQUET", 3)]);
    t.list("resurrected.avro", &[("m3.avro", 0)]);
    let rows = t
        .rows(&t.diff(Some("parent-pos.avro"), "resurrected.avro").unwrap())
        .unwrap();
    assert!(check_operation(Operation::Replace, &rows).is_err());
}

#[test]
fn removing_a_position_delete_file_restores_its_rows() {
    let t = parent_with_position_delete();
    let rows = t
        .rows(&t.diff(Some("parent-pos.avro"), "parent.avro").unwrap())
        .unwrap();
    assert_eq!(sorted(&rows.added), [row(2, "us")]);
    assert!(rows.removed.is_empty());
}

#[test]
fn deletes_naming_files_outside_the_table_change_nothing() {
    let mut t = parent();
    t.positions("pos.parquet", &[("gone.parquet", 0)]);
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let rows = t
        .rows(&t.diff(Some("parent.avro"), "new.avro").unwrap())
        .unwrap();
    assert!(rows.added.is_empty() && rows.removed.is_empty());
}

#[test]
fn unprovable_position_deletes_are_unsupported() {
    // Deletion vectors (v3, Puffin).
    let mut t = parent();
    t.manifest("dv.avro", &[(1, 1, "dv.puffin", "PUFFIN", 1)]);
    t.list("dv-list.avro", &[("m1.avro", 0), ("dv.avro", 1)]);
    assert!(unsupported(t.diff(Some("parent.avro"), "dv-list.avro")).contains("deletion vector"));

    // A position beyond the end of the file.
    let mut t = parent();
    t.positions("pos.parquet", &[("b.parquet", 5)]);
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("beyond"));

    // A delete file whose record count lies.
    let mut t = parent();
    t.positions("pos.parquet", &[("a.parquet", 0)]);
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 7)]);
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("manifest says"));
}

#[test]
fn equality_deletes_are_read_as_delete_keys() {
    // Flink upsert: a data file and an equality delete file on `id` in the same snapshot.
    let mut t = parent();
    t.data("upsert.parquet", &[(1, "us"), (9, "eu")]);
    t.id_deletes("eq-1.parquet", &[1, 9]);
    t.manifest("m2.avro", &[(1, 0, "upsert.parquet", "PARQUET", 2)]);
    t.manifest("d1.avro", &[(1, 2, "eq-1.parquet", "PARQUET", 2)]);
    t.list(
        "new.avro",
        &[("m1.avro", 0), ("m2.avro", 0), ("d1.avro", 1)],
    );
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert_eq!(paths(&changes.added), ["upsert.parquet"]);
    assert_eq!(paths(&changes.equality_deletes), ["eq-1.parquet"]);
    let rows = t.rows(&changes).unwrap();
    let deletes = rows.equality_deletes.unwrap();
    assert_eq!(deletes.columns(), [FieldId(1)]);
    assert_eq!(
        deletes.rows(),
        &[
            vec![Datum::Value(KeyValue::Integer(1))],
            vec![Datum::Value(KeyValue::Integer(9))]
        ]
    );
    assert_eq!(rows.added.len(), 2);
}

#[test]
fn unsafe_equality_deletes_are_unsupported() {
    let mut t = parent();
    t.id_deletes("eq-1.parquet", &[1]);
    // An explicit (possibly older) sequence number would change which rows it deletes.
    t.manifest_seq(
        "d1.avro",
        &[((0, 2, "eq-1.parquet", "PARQUET", 1), Some(1))],
    );
    t.list("seq.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    assert!(unsupported(t.diff(Some("parent.avro"), "seq.avro")).contains("sequence number"));

    // Different field sets in one commit.
    t.manifest(
        "d2.avro",
        &[
            (1, 2, "eq-1.parquet", "PARQUET", 1),
            (1, 2, "eq-region.parquet", "PARQUET", 1),
        ],
    );
    t.list("mixed.avro", &[("m1.avro", 0), ("d2.avro", 1)]);
    assert!(unsupported(t.diff(Some("parent.avro"), "mixed.avro")).contains("different"));

    // Equality deletes together with removed data files.
    t.manifest("d3.avro", &[(1, 2, "eq-1.parquet", "PARQUET", 1)]);
    t.manifest("m2.avro", &[(1, 0, "a.parquet", "PARQUET", 2)]);
    t.list("cow.avro", &[("m2.avro", 0), ("d3.avro", 1)]);
    assert!(unsupported(t.diff(Some("parent.avro"), "cow.avro")).contains("delete files"));
}

// ---------- position and equality deletes in one table (ADR 0028) ----------

fn ids(batch: &integrity_core::RowBatch) -> Vec<Vec<Datum>> {
    batch.rows().to_vec()
}

fn id(i: i64) -> Vec<Datum> {
    vec![Datum::Value(KeyValue::Integer(i))]
}

/// Flink upserts `(4, eu)` then `(4, us)` in one checkpoint: data file f.parquet with both rows,
/// an equality delete of key 4 (for older rows) and a position delete of f.parquet row 0 (the row
/// it wrote a moment ago). List flink-1.avro.
fn flink_upsert_twice() -> Table {
    let mut t = parent();
    t.data("f.parquet", &[(4, "eu"), (4, "us")]);
    t.id_deletes("eq-f.parquet", &[4]);
    t.positions("pos-f.parquet", &[("f.parquet", 0)]);
    t.manifest("m2.avro", &[(1, 0, "f.parquet", "PARQUET", 2)]);
    t.manifest(
        "d1.avro",
        &[
            (1, 2, "eq-f.parquet", "PARQUET", 1),
            (1, 1, "pos-f.parquet", "PARQUET", 1),
        ],
    );
    t.list(
        "flink-1.avro",
        &[("m1.avro", 0), ("m2.avro", 0), ("d1.avro", 1)],
    );
    t
}

#[test]
fn a_key_upserted_twice_in_one_commit_adds_its_last_row() {
    let t = flink_upsert_twice();
    let changes = t.diff(Some("parent.avro"), "flink-1.avro").unwrap();
    assert_eq!(paths(&changes.added), ["f.parquet"]);
    assert_eq!(paths(&changes.equality_deletes), ["eq-f.parquet"]);
    let rows = t.rows(&changes).unwrap();
    assert_eq!(ids(&rows.added), [row(4, "us")]);
    assert!(rows.removed.is_empty());
    assert_eq!(ids(&rows.equality_deletes.unwrap()), [id(4)]);
}

#[test]
fn later_upserts_to_a_table_with_both_kinds_of_deletes() {
    // The next checkpoint upserts the existing key 1 twice.
    let mut t = flink_upsert_twice();
    t.data("g.parquet", &[(1, "x"), (1, "y")]);
    t.id_deletes("eq-g.parquet", &[1]);
    t.positions("pos-g.parquet", &[("g.parquet", 0)]);
    t.manifest("m3.avro", &[(1, 0, "g.parquet", "PARQUET", 2)]);
    t.manifest(
        "d2.avro",
        &[
            (1, 2, "eq-g.parquet", "PARQUET", 1),
            (1, 1, "pos-g.parquet", "PARQUET", 1),
        ],
    );
    t.list(
        "flink-2.avro",
        &[
            ("m1.avro", 0),
            ("m2.avro", 0),
            ("d1.avro", 1),
            ("m3.avro", 0),
            ("d2.avro", 1),
        ],
    );
    let changes = t.diff(Some("flink-1.avro"), "flink-2.avro").unwrap();
    let rows = t.rows(&changes).unwrap();
    assert_eq!(ids(&rows.added), [row(1, "y")]);
    assert!(rows.removed.is_empty());
    assert_eq!(ids(&rows.equality_deletes.unwrap()), [id(1)]);

    // A checkpoint with only new keys and no equality deletes reads the same way.
    t.data("h.parquet", &[(7, "eu")]);
    t.manifest("m4.avro", &[(1, 0, "h.parquet", "PARQUET", 1)]);
    t.list(
        "flink-3.avro",
        &[
            ("m1.avro", 0),
            ("m2.avro", 0),
            ("d1.avro", 1),
            ("m4.avro", 0),
        ],
    );
    let changes = t.diff(Some("flink-1.avro"), "flink-3.avro").unwrap();
    let columns = [
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::String),
    ];
    let mut added = Vec::new();
    for_each_commit_batch(&t.io, &changes, &columns, |side, batch| {
        assert_eq!(side, Side::Added);
        added.extend(batch.rows().to_vec());
        Ok::<_, InspectError>(())
    })
    .unwrap();
    assert_eq!(added, [row(7, "eu")]);
}

#[test]
fn equality_deletes_in_a_table_with_position_deletes() {
    // The position delete of a.parquet row 1 stays as it is; key 3 is deleted by equality.
    let mut t = parent_with_position_delete();
    t.id_deletes("eq.parquet", &[3]);
    t.manifest("d2.avro", &[(1, 2, "eq.parquet", "PARQUET", 1)]);
    t.list(
        "mixed.avro",
        &[("m1.avro", 0), ("d1.avro", 1), ("d2.avro", 1)],
    );
    let rows = t
        .rows(&t.diff(Some("parent-pos.avro"), "mixed.avro").unwrap())
        .unwrap();
    assert!(rows.added.is_empty() && rows.removed.is_empty());
    assert_eq!(ids(&rows.equality_deletes.unwrap()), [id(3)]);
}

#[test]
fn positions_of_existing_rows_cannot_change_next_to_equality_deletes() {
    // A row of a.parquet may already be deleted by an equality delete: deleting its position
    // could remove it twice.
    let mut t = flink_upsert_twice();
    t.positions("pos-a.parquet", &[("a.parquet", 0)]);
    t.manifest("d2.avro", &[(1, 1, "pos-a.parquet", "PARQUET", 1)]);
    t.list(
        "new.avro",
        &[
            ("m1.avro", 0),
            ("m2.avro", 0),
            ("d1.avro", 1),
            ("d2.avro", 1),
        ],
    );
    let changes = t.diff(Some("flink-1.avro"), "new.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("change rows of a.parquet"));

    // Undoing a position delete of f.parquet (an existing file now) is refused the same way.
    t.manifest("d3.avro", &[(1, 2, "eq-f.parquet", "PARQUET", 1)]);
    t.list(
        "undo.avro",
        &[("m1.avro", 0), ("m2.avro", 0), ("d3.avro", 1)],
    );
    let changes = t.diff(Some("flink-1.avro"), "undo.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("change rows of f.parquet"));
}

#[test]
fn an_older_delete_file_cannot_name_a_new_data_file() {
    // pos-n.parquet, live before the commit, names n.parquet, which the commit adds. Iceberg
    // does not apply it to the new file (lower sequence number); it must not be counted.
    for with_equality in [false, true] {
        let mut t = parent();
        t.positions("pos-n.parquet", &[("n.parquet", 0)]);
        t.id_deletes("eq.parquet", &[9]);
        let deletes: &[Entry] = if with_equality {
            &[
                (1, 1, "pos-n.parquet", "PARQUET", 1),
                (1, 2, "eq.parquet", "PARQUET", 1),
            ]
        } else {
            &[(1, 1, "pos-n.parquet", "PARQUET", 1)]
        };
        t.manifest("d1.avro", deletes);
        t.list("before.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
        t.data("n.parquet", &[(5, "eu")]);
        t.manifest("m2.avro", &[(1, 0, "n.parquet", "PARQUET", 1)]);
        t.list(
            "after.avro",
            &[("m1.avro", 0), ("d1.avro", 1), ("m2.avro", 0)],
        );
        let changes = t.diff(Some("before.avro"), "after.avro").unwrap();
        assert!(
            unsupported(t.rows(&changes)).contains("names n.parquet"),
            "with equality deletes: {with_equality}"
        );
    }
}

#[test]
fn new_data_files_inherit_their_sequence_number_next_to_equality_deletes() {
    // An older explicit sequence number would put the new rows under the equality deletes.
    let mut t = flink_upsert_twice();
    t.data("old.parquet", &[(8, "eu")]);
    t.manifest_seq("m3.avro", &[((1, 0, "old.parquet", "PARQUET", 1), Some(1))]);
    t.list(
        "new.avro",
        &[
            ("m1.avro", 0),
            ("m2.avro", 0),
            ("d1.avro", 1),
            ("m3.avro", 0),
        ],
    );
    assert!(
        unsupported(t.diff(Some("flink-1.avro"), "new.avro")).contains("explicit sequence number")
    );

    // Without equality deletes it does not matter (compaction sets one).
    let mut t = parent();
    t.manifest_seq("m3.avro", &[((1, 0, "old.parquet", "PARQUET", 1), Some(1))]);
    t.data("old.parquet", &[(8, "eu")]);
    t.list("new.avro", &[("m1.avro", 0), ("m3.avro", 0)]);
    assert_eq!(
        paths(&t.diff(Some("parent.avro"), "new.avro").unwrap().added),
        ["old.parquet"]
    );
}

#[test]
fn kept_files_keep_their_data_sequence_number() {
    // a.parquet and b.parquet inherit 1 from m1; pos1.parquet inherits 2.
    let mut t = parent_with_position_delete();
    t.list_seq("v2-parent.avro", &[("m1.avro", 0, 1), ("d1.avro", 1, 2)]);
    // A manifest rewrite that keeps the numbers is fine.
    t.manifest_seq(
        "m9.avro",
        &[
            ((0, 0, "a.parquet", "PARQUET", 2), Some(1)),
            ((0, 0, "b.parquet", "PARQUET", 1), Some(1)),
        ],
    );
    t.list_seq("rewritten.avro", &[("m9.avro", 0, 3), ("d1.avro", 1, 2)]);
    let changes = t.diff(Some("v2-parent.avro"), "rewritten.avro").unwrap();
    let rows = t.rows(&changes).unwrap();
    assert!(rows.added.is_empty() && rows.removed.is_empty());

    // Raising a.parquet's above the position delete's would bring row 1 back unseen.
    t.manifest_seq(
        "m10.avro",
        &[
            ((0, 0, "a.parquet", "PARQUET", 2), Some(3)),
            ((0, 0, "b.parquet", "PARQUET", 1), Some(1)),
        ],
    );
    t.list_seq("raised.avro", &[("m10.avro", 0, 3), ("d1.avro", 1, 2)]);
    assert!(
        unsupported(t.diff(Some("v2-parent.avro"), "raised.avro"))
            .contains("a.parquet changes its data sequence number")
    );

    // So would re-listing the delete manifest with another number.
    t.list_seq("relisted.avro", &[("m1.avro", 0, 1), ("d1.avro", 1, 0)]);
    assert!(
        unsupported(t.diff(Some("v2-parent.avro"), "relisted.avro"))
            .contains("pos1.parquet changes its data sequence number")
    );
}

// ---------- deletes apply as Iceberg readers apply them (ADR 0030) ----------

/// a.parquet `[(1, eu), (2, us)]` in partition eu and b.parquet `[(3, eu)]` in partition us, data
/// sequence number 5 (list pparent.avro).
fn partitioned_parent() -> Table {
    let mut t = parent();
    t.part_manifest(
        "m1p.avro",
        &[
            ((1, 0, "a.parquet", "PARQUET", 2), None, Some("eu"), None),
            ((1, 0, "b.parquet", "PARQUET", 1), None, Some("us"), None),
        ],
    );
    t.list_seq("pparent.avro", &[("m1p.avro", 0, 5)]);
    t
}

/// A position delete of a.parquet row 1 (`(2, us)`) in partition `region`, its manifest listed
/// in `list` at sequence number `seq` with partition spec `spec`.
fn with_position_delete(t: &mut Table, list: &str, region: &'static str, seq: i64, spec: i32) {
    t.positions("pos-a.parquet", &[("a.parquet", 1)]);
    t.part_manifest(
        "d1p.avro",
        &[(
            (1, 1, "pos-a.parquet", "PARQUET", 1),
            None,
            Some(region),
            None,
        )],
    );
    t.list_spec(list, &[("m1p.avro", 0, 5, 0), ("d1p.avro", 1, seq, spec)]);
}

#[test]
fn position_deletes_apply_within_their_partition_spec_and_sequence_number() {
    let removed = |t: &Table, list: &str| {
        sorted(
            &t.rows(&t.diff(Some("pparent.avro"), list).unwrap())
                .unwrap()
                .removed,
        )
    };
    let mut t = partitioned_parent();
    with_position_delete(&mut t, "right.avro", "eu", 6, 0);
    assert_eq!(removed(&t, "right.avro"), [row(2, "us")]);
    // The same sequence number still applies (rows written in the same commit).
    with_position_delete(&mut t, "equal.avro", "eu", 5, 0);
    assert_eq!(removed(&t, "equal.avro"), [row(2, "us")]);

    // Readers do not apply these; neither does the Plane.
    for (list, region, seq, spec) in [
        ("other-partition.avro", "us", 6, 0),
        ("other-spec.avro", "eu", 6, 1),
        ("older.avro", "eu", 4, 0),
    ] {
        let mut t = partitioned_parent();
        with_position_delete(&mut t, list, region, seq, spec);
        assert!(removed(&t, list).is_empty(), "{list}");
    }
}

#[test]
fn a_referenced_data_file_limits_a_position_delete_file() {
    let mut t = partitioned_parent();
    t.positions("pos-a.parquet", &[("a.parquet", 1)]);
    t.part_manifest(
        "d1p.avro",
        &[(
            (1, 1, "pos-a.parquet", "PARQUET", 1),
            None,
            Some("eu"),
            Some("b.parquet"),
        )],
    );
    t.list_seq("new.avro", &[("m1p.avro", 0, 5), ("d1p.avro", 1, 6)]);
    let rows = t
        .rows(&t.diff(Some("pparent.avro"), "new.avro").unwrap())
        .unwrap();
    assert!(rows.removed.is_empty() && rows.added.is_empty());
}

#[test]
fn a_deletion_vector_replaces_position_delete_files_of_its_data_file() {
    // pos1.parquet deletes a.parquet row 1; a vector for a.parquet that marks only row 0 is what
    // readers apply from now on: row 0 goes, row 1 comes back.
    let mut t = parent_with_position_delete();
    let (offset, size) = t.dv("dv.puffin", &[0]);
    t.dv_manifest("d2.avro", &[(1, "dv.puffin", "a.parquet", offset, size, 1)]);
    t.list(
        "new.avro",
        &[("m1.avro", 0), ("d1.avro", 1), ("d2.avro", 1)],
    );
    let rows = t
        .rows(&t.diff(Some("parent-pos.avro"), "new.avro").unwrap())
        .unwrap();
    assert_eq!(sorted(&rows.removed), [row(1, "eu")]);
    assert_eq!(sorted(&rows.added), [row(2, "us")]);

    // Two live vectors for one data file are invalid.
    let (o2, s2) = t.dv("dv2.puffin", &[1]);
    t.dv_manifest(
        "d3.avro",
        &[
            (1, "dv.puffin", "a.parquet", offset, size, 1),
            (1, "dv2.puffin", "a.parquet", o2, s2, 1),
        ],
    );
    t.list("two.avro", &[("m1.avro", 0), ("d3.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "two.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("more than one deletion vector"));
}

#[test]
fn partitioned_equality_deletes_are_unsupported() {
    // A partitioned equality delete reaches only its partition's files; the Plane cannot tell
    // which rows that leaves without reading them.
    let mut t = partitioned_parent();
    t.id_deletes("eq.parquet", &[1]);
    t.part_manifest(
        "d1p.avro",
        &[((1, 2, "eq.parquet", "PARQUET", 1), None, Some("eu"), None)],
    );
    t.list_seq("new.avro", &[("m1p.avro", 0, 5), ("d1p.avro", 1, 6)]);
    assert!(
        unsupported(t.diff(Some("pparent.avro"), "new.avro"))
            .contains("partitioned equality delete")
    );
}

#[test]
fn malformed_listings_are_rejected() {
    let mut t = parent();
    t.list("twice.avro", &[("m1.avro", 0), ("m1.avro", 0)]);
    assert!(unsupported(t.diff(None, "twice.avro")).contains("listed twice"));

    t.manifest(
        "dup.avro",
        &[
            (1, 0, "x.parquet", "PARQUET", 1),
            (1, 0, "x.parquet", "PARQUET", 1),
        ],
    );
    t.list("dup-list.avro", &[("dup.avro", 0)]);
    assert!(unsupported(t.diff(None, "dup-list.avro")).contains("listed twice"));

    // A data manifest that lists a delete file.
    t.manifest("mixed.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list("mixed-list.avro", &[("mixed.avro", 0)]);
    assert!(matches!(
        t.diff(None, "mixed-list.avro"),
        Err(InspectError::Manifest(_))
    ));

    t.manifest("orc.avro", &[(1, 0, "x.orc", "ORC", 1)]);
    t.list("orc-list.avro", &[("orc.avro", 0)]);
    assert!(unsupported(t.diff(None, "orc-list.avro")).contains("ORC"));

    t.io.insert("garbage.avro", Bytes::from_static(b"Obj\x01garbage"));
    t.list("garbage-list.avro", &[("garbage.avro", 0)]);
    assert!(matches!(
        t.diff(None, "garbage-list.avro"),
        Err(InspectError::Manifest(_))
    ));
    assert!(matches!(
        t.diff(None, "missing.avro"),
        Err(InspectError::Read(_))
    ));
}

#[test]
fn record_count_must_match_the_file() {
    let mut t = Table::new();
    t.data("a.parquet", &[(1, "eu"), (2, "us")]);
    t.manifest("m.avro", &[(1, 0, "a.parquet", "PARQUET", 3)]);
    t.list("l.avro", &[("m.avro", 0)]);
    let changes = t.diff(None, "l.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("3"));
}

#[test]
fn declared_operation_must_match_the_files() {
    let mut t = parent();
    t.data("c.parquet", &[(4, "eu")]);
    t.manifest("m2.avro", &[(1, 0, "c.parquet", "PARQUET", 1)]);
    t.list("swap.avro", &[("m2.avro", 0)]);
    let rows = t
        .rows(&t.diff(Some("parent.avro"), "swap.avro").unwrap())
        .unwrap();
    assert!(unsupported(check_operation(Operation::Append, &rows)).contains("append"));
    assert!(unsupported(check_operation(Operation::Delete, &rows)).contains("delete"));
    check_operation(Operation::Overwrite, &rows).unwrap();
}

// ---------- v3 deletion vectors ----------

#[test]
fn deletion_vectors_remove_the_rows_they_mark() {
    let mut t = parent();
    let (offset, size) = t.dv("dv1.puffin", &[1]);
    t.dv_manifest(
        "d1.avro",
        &[(1, "dv1.puffin", "a.parquet", offset, size, 1)],
    );
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let rows = t
        .rows(&t.diff(Some("parent.avro"), "new.avro").unwrap())
        .unwrap();
    assert_eq!(sorted(&rows.removed), [row(2, "us")]);
    assert!(rows.added.is_empty());
}

#[test]
fn a_replaced_deletion_vector_removes_only_the_newly_marked_rows() {
    // v3 keeps one vector per data file: a delete replaces it with a superset.
    let mut t = parent();
    let (o1, s1) = t.dv("dv1.puffin", &[1]);
    t.dv_manifest("d1.avro", &[(1, "dv1.puffin", "a.parquet", o1, s1, 1)]);
    t.list("parent-dv.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let (o2, s2) = t.dv("dv2.puffin", &[0, 1]);
    t.dv_manifest(
        "d2.avro",
        &[
            (2, "dv1.puffin", "a.parquet", o1, s1, 1),
            (1, "dv2.puffin", "a.parquet", o2, s2, 2),
        ],
    );
    t.list("new.avro", &[("m1.avro", 0), ("d2.avro", 1)]);
    let rows = t
        .rows(&t.diff(Some("parent-dv.avro"), "new.avro").unwrap())
        .unwrap();
    assert_eq!(sorted(&rows.removed), [row(1, "eu")]);
    assert!(rows.added.is_empty());
}

#[test]
fn invalid_deletion_vectors_are_unsupported() {
    // A corrupted blob.
    let mut t = parent();
    let (offset, size) = t.dv("dv1.puffin", &[1]);
    t.io.insert("bad.puffin", {
        let mut bytes = t.io.read("dv1.puffin").unwrap().to_vec();
        bytes[offset as usize + 9] ^= 0xFF;
        bytes
    });
    t.dv_manifest(
        "d1.avro",
        &[(1, "bad.puffin", "a.parquet", offset, size, 1)],
    );
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "new.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("deletion vector"));
    // A row count that disagrees with the vector.
    t.dv_manifest(
        "d2.avro",
        &[(1, "dv1.puffin", "a.parquet", offset, size, 5)],
    );
    t.list("new2.avro", &[("m1.avro", 0), ("d2.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "new2.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("manifest says"));
    // A position beyond the data file.
    let (o3, s3) = t.dv("dv3.puffin", &[9]);
    t.dv_manifest("d3.avro", &[(1, "dv3.puffin", "b.parquet", o3, s3, 1)]);
    t.list("new3.avro", &[("m1.avro", 0), ("d3.avro", 1)]);
    let changes = t.diff(Some("parent.avro"), "new3.avro").unwrap();
    assert!(unsupported(t.rows(&changes)).contains("beyond"));
}

// ---------- whole-table streaming (onboarding) ----------

type Rows = Result<Vec<Vec<Datum>>, String>;

/// Every live row of the snapshot at `list`, streamed; and what `commit_rows` reads for it.
fn streamed_and_collected(t: &Table, list: &str) -> (Rows, Rows) {
    let changes = t.diff(None, list).unwrap();
    let columns = [
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::String),
    ];
    let mut streamed = Vec::new();
    let s = for_each_live_batch(&t.io, &changes, &columns, |b| {
        streamed.extend(b.rows().iter().cloned());
        Ok::<_, InspectError>(())
    })
    .map(|()| streamed)
    .map_err(|e| e.to_string());
    let c = t
        .rows(&changes)
        .map(|r| r.added.rows().to_vec())
        .map_err(|e| e.to_string());
    (s, c)
}

#[test]
fn streaming_a_whole_table_reads_what_commit_rows_reads() {
    let t = parent();
    let (s, c) = streamed_and_collected(&t, "parent.avro");
    assert_eq!(s.clone().unwrap().len(), 3);
    assert_eq!(s, c);

    let t = parent_with_position_delete();
    let (s, c) = streamed_and_collected(&t, "parent-pos.avro");
    assert_eq!(s.clone().unwrap().len(), 2);
    assert_eq!(s, c);

    let mut t = parent();
    let (offset, size) = t.dv("dv1.puffin", &[0, 1]);
    t.dv_manifest(
        "d1.avro",
        &[(1, "dv1.puffin", "a.parquet", offset, size, 2)],
    );
    t.list("dv.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let (s, c) = streamed_and_collected(&t, "dv.avro");
    assert_eq!(s.clone().unwrap(), [row(3, "eu")]);
    assert_eq!(s, c);

    // Positions beyond the file and lying record counts fail the same way.
    let mut t = parent();
    t.positions("pos.parquet", &[("b.parquet", 5)]);
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list("new.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let (s, c) = streamed_and_collected(&t, "new.avro");
    assert!(s.clone().unwrap_err().contains("beyond"));
    assert_eq!(s, c);

    let mut t = Table::new();
    t.data("a.parquet", &[(1, "eu"), (2, "us")]);
    t.manifest("m1.avro", &[(1, 0, "a.parquet", "PARQUET", 3)]);
    t.list("l.avro", &[("m1.avro", 0)]);
    let (s, c) = streamed_and_collected(&t, "l.avro");
    assert!(s.clone().unwrap_err().contains("manifest says"));
    assert_eq!(s, c);
}

#[test]
fn streaming_refuses_a_diff_against_a_parent() {
    let t = parent_with_position_delete();
    let changes = t.diff(Some("parent.avro"), "parent-pos.avro").unwrap();
    let r = for_each_live_batch(&t.io, &changes, &[(FieldId(1), LogicalType::Long)], |_| {
        Ok::<_, InspectError>(())
    });
    assert!(unsupported(r).contains("whole-table"));
}

// ---------- commit streaming (validation, ADR 0019) ----------

/// Both sides of the commit `parent` → `new`, streamed; and what `commit_rows` reads.
fn streamed_sides(t: &Table, parent: &str, new: &str) -> (String, String) {
    let changes = t.diff(Some(parent), new).unwrap();
    let columns = [
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::String),
    ];
    let (mut added, mut removed) = (Vec::new(), Vec::new());
    let s = for_each_commit_batch(&t.io, &changes, &columns, |side, b| {
        assert!(!b.is_empty(), "empty batches are not handed over");
        match side {
            Side::Added => added.extend(b.rows().iter().cloned()),
            Side::Removed => removed.extend(b.rows().iter().cloned()),
        }
        Ok::<_, InspectError>(())
    });
    let streamed = s.is_ok();
    let s = match s {
        Ok(()) => format!("{added:?} / {removed:?}"),
        Err(e) => e.to_string(),
    };
    // Each side read on its own (ADR 0024) gives the same rows in the same order, or fails too.
    let per_side = CommitDeletes::read(&t.io, &changes).and_then(|deletes| {
        let mut sides = Vec::new();
        for side in [Side::Added, Side::Removed] {
            let mut rows = Vec::new();
            for_each_side_batch(&t.io, &changes, &deletes, &columns, side, |b| {
                assert!(!b.is_empty(), "empty batches are not handed over");
                rows.extend(b.rows().iter().cloned());
                Ok::<_, InspectError>(())
            })?;
            sides.push(rows);
        }
        Ok(format!("{:?} / {:?}", sides[0], sides[1]))
    });
    match &per_side {
        Ok(p) => assert_eq!(p, &s, "each side alone"),
        Err(e) => assert!(
            !streamed,
            "a side alone failed ({e}) where both sides streamed"
        ),
    }
    let c = match t.rows(&changes) {
        Ok(r) => format!("{:?} / {:?}", r.added.rows(), r.removed.rows()),
        Err(e) => e.to_string(),
    };
    (s, c)
}

#[test]
fn streaming_a_commit_reads_what_commit_rows_reads() {
    // Copy-on-write, merge-on-read update, compaction of a merge-on-read table, restored rows,
    // replaced deletion vectors, positions beyond the file.
    let mut t = parent_with_position_delete();
    t.data("c.parquet", &[(2, "eu"), (4, "us")]);
    t.manifest("m2.avro", &[(1, 0, "c.parquet", "PARQUET", 2)]);
    t.positions("pos2.parquet", &[("b.parquet", 0)]);
    t.manifest("d2.avro", &[(1, 1, "pos2.parquet", "PARQUET", 1)]);
    t.list(
        "update.avro",
        &[("m1.avro", 0), ("m2.avro", 0), ("d2.avro", 1)],
    );
    t.data("ab.parquet", &[(3, "eu"), (1, "eu")]);
    t.manifest("m3.avro", &[(1, 0, "ab.parquet", "PARQUET", 2)]);
    t.list("compacted.avro", &[("m3.avro", 0)]);
    t.positions("far.parquet", &[("a.parquet", 9)]);
    t.manifest("d3.avro", &[(1, 1, "far.parquet", "PARQUET", 1)]);
    t.list(
        "far.avro",
        &[("m1.avro", 0), ("d1.avro", 1), ("d3.avro", 1)],
    );
    for (parent, new) in [
        ("parent.avro", "parent-pos.avro"),
        ("parent-pos.avro", "parent.avro"),
        ("parent-pos.avro", "update.avro"),
        ("parent-pos.avro", "compacted.avro"),
        ("parent.avro", "compacted.avro"),
        ("parent-pos.avro", "far.avro"),
    ] {
        let (s, c) = streamed_sides(&t, parent, new);
        assert_eq!(s, c, "{parent} -> {new}");
    }

    let mut t = parent();
    let (o1, s1) = t.dv("dv1.puffin", &[1]);
    t.dv_manifest("d1.avro", &[(1, "dv1.puffin", "a.parquet", o1, s1, 1)]);
    t.list("parent-dv.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    let (o2, s2) = t.dv("dv2.puffin", &[0, 1]);
    t.dv_manifest(
        "d2.avro",
        &[
            (2, "dv1.puffin", "a.parquet", o1, s1, 1),
            (1, "dv2.puffin", "a.parquet", o2, s2, 2),
        ],
    );
    t.list("new.avro", &[("m1.avro", 0), ("d2.avro", 1)]);
    let (s, c) = streamed_sides(&t, "parent-dv.avro", "new.avro");
    assert_eq!(s, c);
    let (s, c) = streamed_sides(&t, "new.avro", "parent-dv.avro");
    assert_eq!(s, c);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config { cases: 128, ..Default::default() })]

    /// Random parent and new snapshots over four data files with position deletes on both sides.
    #[test]
    fn streaming_random_commits_reads_what_commit_rows_reads(
        rows in proptest::collection::vec(1usize..6, 4),
        parent_files in proptest::collection::vec(proptest::bool::ANY, 4),
        new_files in proptest::collection::vec(proptest::bool::ANY, 4),
        parent_deletes in proptest::collection::vec((0usize..4, 0i64..6), 0..4),
        new_deletes in proptest::collection::vec((0usize..4, 0i64..6), 0..4),
        keep_parent_deletes in proptest::bool::ANY,
        row_group in proptest::option::of(1usize..4),
    ) {
        let mut t = Table::new();
        t.row_group = row_group;
        let names = ["f0.parquet", "f1.parquet", "f2.parquet", "f3.parquet"];
        for (i, n) in rows.iter().enumerate() {
            let data: Vec<(i64, &str)> =
                (0..*n).map(|r| ((i * 10 + r) as i64, ["eu", "us"][r % 2])).collect();
            t.data(names[i], &data);
            t.manifest(&format!("m{i}.avro"), &[(1, 0, names[i], "PARQUET", *n as i64)]);
        }
        fn deletes(names: &[&'static str], d: &[(usize, i64)]) -> Vec<(&'static str, i64)> {
            d.iter().map(|&(f, p)| (names[f], p)).collect()
        }
        fn as_refs(l: &[(String, i32)]) -> Vec<(&str, i32)> {
            l.iter().map(|(m, c)| (m.as_str(), *c)).collect()
        }
        t.positions("pd-parent.parquet", &deletes(&names, &parent_deletes));
        t.manifest("dp.avro", &[(1, 1, "pd-parent.parquet", "PARQUET", parent_deletes.len() as i64)]);
        t.positions("pd-new.parquet", &deletes(&names, &new_deletes));
        t.manifest("dn.avro", &[(1, 1, "pd-new.parquet", "PARQUET", new_deletes.len() as i64)]);
        let list = |files: &[bool], with: &[&str]| -> Vec<(String, i32)> {
            let mut l: Vec<(String, i32)> = files
                .iter()
                .enumerate()
                .filter(|(_, on)| **on)
                .map(|(i, _)| (format!("m{i}.avro"), 0))
                .collect();
            l.extend(with.iter().map(|m| (m.to_string(), 1)));
            l
        };
        let p = list(&parent_files, if parent_deletes.is_empty() { &[] } else { &["dp.avro"] });
        let mut new_with: Vec<&str> = Vec::new();
        if keep_parent_deletes && !parent_deletes.is_empty() {
            new_with.push("dp.avro");
        }
        if !new_deletes.is_empty() {
            new_with.push("dn.avro");
        }
        let n = list(&new_files, &new_with);
        t.list("p.avro", &as_refs(&p));
        t.list("n.avro", &as_refs(&n));
        if t.diff(Some("p.avro"), "n.avro").is_ok() {
            let (s, c) = streamed_sides(&t, "p.avro", "n.avro");
            proptest::prop_assert_eq!(s, c);
        }
    }
}

/// Counts the requests made through `inner` per location, and the most made at once; each takes
/// at least `delay` (an object store's round trip).
struct Requests<'a> {
    inner: &'a MemoryIo,
    delay: std::time::Duration,
    reads: std::sync::Mutex<HashMap<String, usize>>,
    opens: std::sync::Mutex<HashMap<String, usize>>,
    in_flight: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
}

impl<'a> Requests<'a> {
    fn new(inner: &'a MemoryIo, delay: std::time::Duration) -> Self {
        Self {
            inner,
            delay,
            reads: Default::default(),
            opens: Default::default(),
            in_flight: Default::default(),
            most: Default::default(),
        }
    }

    fn request<T>(&self, f: impl FnOnce() -> T) -> T {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        self.most.fetch_max(now, SeqCst);
        std::thread::sleep(self.delay);
        let out = f();
        self.in_flight.fetch_sub(1, SeqCst);
        out
    }

    fn most_at_once(&self) -> usize {
        self.most.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl FileIo for Requests<'_> {
    fn read(&self, location: &str) -> Result<Bytes, integrity_iceberg::ReadError> {
        *self
            .reads
            .lock()
            .unwrap()
            .entry(location.to_owned())
            .or_default() += 1;
        self.request(|| self.inner.read(location))
    }

    fn size(&self, location: &str) -> Result<u64, integrity_iceberg::ReadError> {
        *self
            .opens
            .lock()
            .unwrap()
            .entry(location.to_owned())
            .or_default() += 1;
        self.request(|| self.inner.size(location))
    }

    fn read_range(
        &self,
        location: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, integrity_iceberg::ReadError> {
        self.request(|| self.inner.read_range(location, range))
    }
}

/// A merge-on-read table: `files` data files in a manifest each, and `deletes` position delete
/// files (one per manifest) deleting row 0 of the first data files. Lists `parent.avro` and
/// `new.avro`; the new snapshot adds one more delete file, for row 1 of `f0.parquet`.
fn merge_on_read_table(files: usize, deletes: usize) -> Table {
    let mut t = Table::new();
    let mut parent: Vec<(String, i32)> = Vec::new();
    for i in 0..files {
        let (data, manifest) = (format!("f{i}.parquet"), format!("m{i}.avro"));
        t.data(&data, &[(2 * i as i64, "eu"), (2 * i as i64 + 1, "us")]);
        let data: &'static str = Box::leak(data.into_boxed_str());
        t.manifest(&manifest, &[(1, 0, data, "PARQUET", 2)]);
        parent.push((manifest, 0));
    }
    for k in 0..deletes {
        let (pos, manifest) = (format!("pos{k}.parquet"), format!("d{k}.avro"));
        t.positions(&pos, &[(&format!("f{}.parquet", k % files), 0)]);
        let pos: &'static str = Box::leak(pos.into_boxed_str());
        t.manifest(&manifest, &[(1, 1, pos, "PARQUET", 1)]);
        parent.push((manifest, 1));
    }
    fn as_refs(v: &[(String, i32)]) -> Vec<(&str, i32)> {
        v.iter().map(|(m, c)| (m.as_str(), *c)).collect()
    }
    t.list("parent.avro", &as_refs(&parent));
    t.positions("pos-new.parquet", &[("f0.parquet", 1)]);
    t.manifest("d-new.avro", &[(1, 1, "pos-new.parquet", "PARQUET", 1)]);
    let mut new = parent;
    new.push(("d-new.avro".into(), 1));
    t.list("new.avro", &as_refs(&new));
    t
}

/// Rows of both sides of `parent.avro -> new.avro`, streamed through `io`.
fn streamed_through(io: &impl FileIo) -> (Vec<Vec<Datum>>, Vec<Vec<Datum>>) {
    let changes = diff_snapshots(io, Some("parent.avro"), "new.avro").unwrap();
    let columns = [
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::String),
    ];
    let (mut added, mut removed) = (Vec::new(), Vec::new());
    for_each_commit_batch(io, &changes, &columns, |side, b| {
        match side {
            Side::Added => added.extend(b.rows().iter().cloned()),
            Side::Removed => removed.extend(b.rows().iter().cloned()),
        }
        Ok::<_, InspectError>(())
    })
    .unwrap();
    (added, removed)
}

#[test]
fn a_merge_on_read_commit_reads_every_file_once() {
    let t = merge_on_read_table(12, 5);
    let io = Requests::new(&t.io, std::time::Duration::ZERO);
    let (added, removed) = streamed_through(&io);
    assert!(added.is_empty());
    assert_eq!(removed, [row(1, "us")]);
    let reads = io.reads.lock().unwrap();
    for (location, n) in reads.iter() {
        assert_eq!(*n, 1, "{location} read {n} times");
    }
    // Every manifest of both snapshots is needed once delete files exist (the complete picture
    // of deleted positions), and every delete file.
    assert_eq!(reads.len(), 2 + 12 + 5 + 1);
    let opens = io.opens.lock().unwrap();
    for k in 0..5 {
        assert_eq!(opens.get(&format!("pos{k}.parquet")), Some(&1), "pos{k}");
    }
    assert_eq!(opens.get("pos-new.parquet"), Some(&1));
}

#[test]
fn manifests_and_delete_files_are_read_concurrently() {
    let t = merge_on_read_table(12, 5);
    let slow = Requests::new(&t.io, std::time::Duration::from_millis(20));
    let started = std::time::Instant::now();
    let streamed = streamed_through(&slow);
    let took = started.elapsed();
    assert_eq!(streamed, streamed_through(&t.io));
    assert!(slow.most_at_once() > 1, "requests never overlapped");
    // One at a time this would be about 2 lists + 17 manifests + 6 delete files of 3 requests
    // + 1 data file of 3 requests, at 20 ms each: over 0.8 s.
    assert!(
        took < std::time::Duration::from_millis(600),
        "took {took:?}"
    );
}

#[test]
fn of_several_unreadable_manifests_the_first_listed_is_reported() {
    let mut t = merge_on_read_table(4, 1);
    t.list(
        "broken.avro",
        &[
            ("m0.avro", 0),
            ("missing-1.avro", 0),
            ("m1.avro", 0),
            ("missing-2.avro", 0),
            ("d0.avro", 1),
        ],
    );
    for _ in 0..20 {
        let err = diff_snapshots(&t.io, Some("parent.avro"), "broken.avro").unwrap_err();
        assert!(err.to_string().contains("missing-1.avro"), "{err}");
    }
}
