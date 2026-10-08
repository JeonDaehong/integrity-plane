//! Replays every commit of a real Iceberg table written by PyIceberg (`fixtures/generate_table.py`)
//! through classification, manifest diff and key extraction: append, copy-on-write delete,
//! whole-file delete, overwrite (two snapshots in one commit) and a commit to another branch.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeSet;

use bytes::Bytes;
use integrity_core::{Datum, KeyValue, LogicalType};
use integrity_iceberg::metadata::{FieldLookup, logical_type};
use integrity_iceberg::{
    Budgeted, Classification, CommitRequest, FileIo, Operation, ReadError, TableMetadata,
    check_operation, check_requirements, classify, commit_rows, diff_snapshots,
};
use integrity_types::{ErrorCode, FieldId, SnapshotId, TableId};
use serde_json::{Value, json};

/// Maps `…/warehouse/<rest>` to `tests/fixtures/table/warehouse/<rest>`.
struct FixtureIo;

impl FileIo for FixtureIo {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        let rest = location
            .split_once("/warehouse/")
            .map(|(_, r)| r)
            .ok_or_else(|| ReadError::Io(format!("unexpected location {location}")))?;
        let path = format!(
            "{}/tests/fixtures/table/warehouse/{rest}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(&path)
            .map(Bytes::from)
            .map_err(|e| ReadError::Io(format!("{path}: {e}")))
    }
}

fn metadata(file: &str) -> (TableMetadata, Value) {
    let json = FixtureIo
        .read(&format!("x/warehouse/db/orders/metadata/{file}"))
        .unwrap();
    let raw: Value = serde_json::from_slice(&json).unwrap();
    (serde_json::from_value(raw.clone()).unwrap(), raw)
}

fn commits() -> Vec<(String, String, String)> {
    let path = format!(
        "{}/tests/fixtures/table/commits.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let list: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    list.as_array()
        .unwrap()
        .iter()
        .map(|c| {
            let s = |k: &str| c[k].as_str().unwrap().to_owned();
            (s("name"), s("branch"), s("metadata"))
        })
        .collect()
}

/// The request a client sends to go from `before` to `after` on `branch`: every snapshot that is new
/// in `after`, in parent order, then the ref update.
fn request(before: &TableMetadata, after_raw: &Value, branch: &str) -> CommitRequest {
    let known: BTreeSet<i64> = before.snapshots.iter().map(|s| s.snapshot_id).collect();
    let mut updates: Vec<Value> = after_raw["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| !known.contains(&s["snapshot-id"].as_i64().unwrap()))
        .map(|s| json!({"action": "add-snapshot", "snapshot": s}))
        .collect();
    updates.sort_by_key(|u| u["snapshot"]["sequence-number"].as_i64());
    let head = after_raw["refs"][branch]["snapshot-id"].clone();
    updates.push(json!({"action": "set-snapshot-ref", "ref-name": branch, "snapshot-id": head, "type": "branch"}));
    let base = match branch {
        "main" => before.main_snapshot_id().map(|s| s.0),
        b => before.refs.get(b).map(|r| r.snapshot_id),
    };
    CommitRequest::from_json(json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": branch, "snapshot-id": base}],
        "updates": updates
    }))
    .unwrap()
}

fn columns(meta: &TableMetadata) -> Vec<(FieldId, LogicalType)> {
    let schema = meta.current_schema().unwrap();
    (1..=4)
        .map(|id| {
            let FieldLookup::TopLevel(f) = schema.lookup(FieldId(id)) else {
                panic!()
            };
            (FieldId(id), logical_type(&f.field_type))
        })
        .collect()
}

type Row = (i64, Option<i64>, Option<&'static str>, i128);

fn row(r: &Row) -> Vec<Datum> {
    let (id, cust, region, cents) = *r;
    vec![
        Datum::Value(KeyValue::Integer(id)),
        cust.map_or(Datum::Null, |c| Datum::Value(KeyValue::Integer(c))),
        region.map_or(Datum::Null, |s| Datum::Value(KeyValue::String(s.into()))),
        Datum::Value(KeyValue::Decimal {
            unscaled: cents,
            scale: 2,
        }),
    ]
}

fn sorted(rows: &[Vec<Datum>]) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
    v.sort();
    v
}

fn expect(rows: &[Row]) -> Vec<String> {
    sorted(&rows.iter().map(row).collect::<Vec<_>>())
}

const A: [Row; 3] = [
    (1, Some(7), Some("eu"), 100),
    (2, Some(8), Some("us"), 200),
    (3, Some(7), None, 300),
];
const B: [Row; 2] = [(4, Some(9), Some("eu"), 400), (5, Some(8), Some("us"), 500)];
const B2: [Row; 1] = [(4, Some(9), Some("eu"), 400)];
const C: [Row; 2] = [
    (10, Some(1), Some("eu"), 1000),
    (11, None, Some("us"), 1100),
];

/// (operation, added rows, removed rows) for every main step, per commit.
fn expected(name: &str) -> Vec<(Operation, Vec<String>, Vec<String>)> {
    match name {
        "append_a" => vec![(Operation::Append, expect(&A), vec![])],
        "append_b" => vec![(Operation::Append, expect(&B), vec![])],
        "delete_rows_cow" => vec![(Operation::Overwrite, expect(&B2), expect(&B))],
        "delete_whole_file" => vec![(Operation::Delete, vec![], expect(&A))],
        "overwrite_all" => vec![
            (Operation::Delete, vec![], expect(&B2)),
            (Operation::Append, expect(&C), vec![]),
        ],
        other => panic!("no expectation for {other}"),
    }
}

#[test]
fn every_fixture_commit_is_classified_diffed_and_extracted() {
    let commits = commits();
    let constrained: BTreeSet<FieldId> = (1..=4).map(FieldId).collect();
    let mut checked = 0;
    for pair in commits.windows(2) {
        let ((_, _, before_file), (name, branch, after_file)) = (&pair[0], &pair[1]);
        let (before, _) = metadata(before_file);
        let (after, after_raw) = metadata(after_file);
        let req = request(&before, &after_raw, branch);
        check_requirements(&before, &req).unwrap();
        let class = classify(&before, &req, &constrained).unwrap();

        if branch != "main" {
            assert_eq!(class, Classification::PassThrough, "{name}");
            checked += 1;
            continue;
        }
        let Classification::MainChange(change) = class else {
            panic!("{name}: {class:?}")
        };
        assert_eq!(change.parent, before.main_snapshot_id(), "{name}");
        let want = expected(name);
        assert_eq!(change.steps.len(), want.len(), "{name}");
        for (step, (op, added, removed)) in change.steps.iter().zip(want) {
            assert_eq!(step.operation, op, "{name}");
            let parent_list = step.parent.map(|p| {
                let snap = after.snapshot(p).unwrap();
                snap.manifest_list.clone().unwrap()
            });
            let changes =
                diff_snapshots(&FixtureIo, parent_list.as_deref(), &step.manifest_list).unwrap();
            let rows = commit_rows(
                &FixtureIo,
                TableId::new(after.table_uuid.clone()),
                step.snapshot,
                &changes,
                &columns(&after),
            )
            .unwrap();
            assert_eq!(sorted(rows.added.rows()), added, "{name} added");
            assert_eq!(sorted(rows.removed.rows()), removed, "{name} removed");
            check_operation(step.operation, &rows).unwrap();
        }
        checked += 1;
    }
    assert_eq!(checked, 6, "every commit after `create` was replayed");
}

#[test]
fn budget_is_enforced_on_real_files() {
    let commits = commits();
    let (_, _, file) = &commits[1];
    let (meta, _) = metadata(file);
    let snapshot = meta.snapshot(meta.main_snapshot_id().unwrap()).unwrap();
    let list = snapshot.manifest_list.clone().unwrap();

    let generous = Budgeted::new(&FixtureIo, 1 << 20);
    let changes = diff_snapshots(&generous, None, &list).unwrap();
    assert!(generous.used() > 0);

    let tight = Budgeted::new(&FixtureIo, 64);
    let err = diff_snapshots(&tight, None, &list).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValidationBudgetExceeded);

    // Data files count too.
    let small = Budgeted::new(&FixtureIo, generous.used() + 16);
    let _ = diff_snapshots(&small, None, &list).unwrap();
    let err = commit_rows(
        &small,
        TableId::new("t"),
        SnapshotId(1),
        &changes,
        &columns(&meta),
    )
    .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValidationBudgetExceeded);
}
