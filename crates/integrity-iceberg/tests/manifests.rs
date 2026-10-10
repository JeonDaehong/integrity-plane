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
    FileChanges, FileIo, InspectError, MemoryIo, Operation, check_operation, commit_rows,
    diff_snapshots, for_each_commit_batch, for_each_live_batch,
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

    // Position and equality deletes in one table.
    let mut t = parent_with_position_delete();
    t.id_deletes("eq.parquet", &[3]);
    t.manifest("d2.avro", &[(1, 2, "eq.parquet", "PARQUET", 1)]);
    t.list(
        "mixed.avro",
        &[("m1.avro", 0), ("d1.avro", 1), ("d2.avro", 1)],
    );
    assert!(
        unsupported(t.diff(Some("parent-pos.avro"), "mixed.avro"))
            .contains("position and equality deletes")
    );

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
    let s = match s {
        Ok(()) => format!("{added:?} / {removed:?}"),
        Err(e) => e.to_string(),
    };
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
    ) {
        let mut t = Table::new();
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
