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
use integrity_core::{Datum, KeyValue, LogicalType};
use integrity_iceberg::{
    FileChanges, InspectError, MemoryIo, Operation, check_operation, commit_rows, diff_snapshots,
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

const ENTRY_SCHEMA: &str = r#"{"type": "record", "name": "manifest_entry", "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"]},
    {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
        {"name": "content", "type": "int"},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "record_count", "type": "long"},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}]}
    ]}}
]}"#;

/// `(status, content, path, format, record_count)` with status 0 existing, 1 added, 2 deleted;
/// content 0 data, 1 position deletes, 2 equality deletes.
type Entry = (i32, i32, &'static str, &'static str, i64);

struct Table {
    io: MemoryIo,
}

impl Table {
    fn new() -> Self {
        Self {
            io: MemoryIo::new(),
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

    /// A manifest; `content` 0 for data, 1 for deletes.
    fn manifest(&mut self, path: &str, entries: &[Entry]) {
        let records = entries
            .iter()
            .map(|&(status, content, file, format, count)| {
                vec![
                    ("status", Avro::Int(status)),
                    ("snapshot_id", Avro::Union(1, Box::new(Avro::Long(1)))),
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
                                    Avro::Union(1, Box::new(Avro::Array(vec![Avro::Int(1)])))
                                } else {
                                    Avro::Union(0, Box::new(Avro::Null))
                                },
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
        let mut w = ArrowWriter::try_new(&mut out, schema, None).unwrap();
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
fn delete_files_are_outside_this_step() {
    let mut t = parent();
    t.manifest("d1.avro", &[(1, 1, "pos.parquet", "PARQUET", 1)]);
    t.list("pos.avro", &[("m1.avro", 0), ("d1.avro", 1)]);
    assert!(unsupported(t.diff(Some("parent.avro"), "pos.avro")).contains("merge-on-read"));

    t.manifest("d2.avro", &[(1, 2, "eq.parquet", "PARQUET", 1)]);
    t.list("eq.avro", &[("m1.avro", 0), ("d2.avro", 1)]);
    assert!(unsupported(t.diff(Some("parent.avro"), "eq.avro")).contains("equality"));

    // A parent with delete files: removing data files is unsupported, appending is fine.
    t.list(
        "parent-with-deletes.avro",
        &[("m1.avro", 0), ("d2.avro", 1)],
    );
    t.manifest("m2.avro", &[(1, 0, "a.parquet", "PARQUET", 2)]);
    t.list("drop-b.avro", &[("m2.avro", 0), ("d2.avro", 1)]);
    assert!(
        unsupported(t.diff(Some("parent-with-deletes.avro"), "drop-b.avro"))
            .contains("delete files")
    );
    t.data("c.parquet", &[(4, "eu")]);
    t.manifest("m3.avro", &[(1, 0, "c.parquet", "PARQUET", 1)]);
    t.list(
        "append.avro",
        &[("m1.avro", 0), ("d2.avro", 1), ("m3.avro", 0)],
    );
    assert_eq!(
        paths(
            &t.diff(Some("parent-with-deletes.avro"), "append.avro")
                .unwrap()
                .added
        ),
        ["c.parquet"]
    );

    // Removing a delete file.
    t.list("no-deletes.avro", &[("m1.avro", 0)]);
    assert!(
        unsupported(t.diff(Some("parent-with-deletes.avro"), "no-deletes.avro"))
            .contains("removes delete files")
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
