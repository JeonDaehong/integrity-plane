//! Streaming commit validation (ADR 0019) against reading the commit whole, on real Parquet and
//! Avro files: random copy-on-write deletes, appends, compactions (some changing rows) and
//! overwrites of a table with a PRIMARY KEY, a UNIQUE key and a NOT NULL column, with files of
//! several row groups and memory budgets small enough to spill many sorted runs. Both must reach
//! the same decision with the same details, or fail with the same code.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::AtomicI64;

use bytes::Bytes;
use integrity_iceberg::{
    FileIo, Operation, ReadError, TableMetadata, check_operation, commit_rows, diff_snapshots,
};
use integrity_index::{IndexEpoch, KeyIndex, MemoryIndex};
use integrity_server::config::ConstraintConfig;
use integrity_server::pipeline::Streamed;
use integrity_server::registry;
use integrity_types::{ConstraintId, SnapshotId};
use integrity_validator::{Decision, Validator};
use proptest::prelude::*;
use proptest::test_runner::Config;
use support::{Dir, Files, Row, empty_table};

struct Local;

impl FileIo for Local {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        std::fs::read(location)
            .map(Bytes::from)
            .map_err(|e| ReadError::Io(format!("{location}: {e}")))
    }
}

fn config(id: u64, kind: &str, columns: &[i32]) -> ConstraintConfig {
    ConstraintConfig {
        id,
        table: "db.t".into(),
        name: format!("c{id}"),
        kind: kind.into(),
        columns: columns.to_vec(),
        nulls: None,
        references: None,
        match_mode: None,
        column_names: None,
    }
}

/// PK(id), UNIQUE(ref), and (when `not_null`) NOT NULL(ref).
type Columns = Vec<(integrity_types::FieldId, integrity_core::LogicalType)>;

fn validator(not_null: bool) -> (Validator, Columns, integrity_types::TableId) {
    let mut configs = vec![config(1, "primary_key", &[1]), config(2, "unique", &[2])];
    if not_null {
        configs.push(config(3, "not_null", &[2]));
    }
    let meta =
        TableMetadata::from_json(&empty_table("00000000-0000-0000-0000-0000000000aa").to_string())
            .unwrap();
    let binding = registry::bind(&configs, "db.t", &meta).unwrap();
    let table = binding.table.clone();
    let bindings = BTreeMap::from([("db.t".to_string(), binding.clone())]);
    let resolved = registry::resolve(&configs, &bindings).unwrap();
    let v = Validator::new(resolved);
    let columns = v
        .projection(&table)
        .into_iter()
        .map(|f| (f, binding.columns[&f].clone()))
        .collect();
    (v, columns, table)
}

/// A step: the files of the new snapshot, by index into the files written so far, plus new files.
#[derive(Debug, Clone)]
struct Step {
    keep: Vec<bool>,
    new_files: Vec<Vec<Row>>,
    operation: Operation,
}

fn rows() -> impl Strategy<Value = Vec<Row>> {
    proptest::collection::vec(
        (0i64..30, proptest::option::weighted(0.85, 0i64..30)),
        0..12,
    )
}

fn case() -> impl Strategy<Value = (Vec<Vec<Row>>, Step, bool)> {
    let parent = proptest::collection::vec(rows(), 1..4);
    (parent, any::<bool>()).prop_flat_map(|(parent, not_null)| {
        let n = parent.len();
        let op = prop_oneof![
            Just(Operation::Append),
            Just(Operation::Delete),
            Just(Operation::Overwrite),
            Just(Operation::Replace),
        ];
        let compact = any::<bool>();
        (
            Just(parent),
            proptest::collection::vec(any::<bool>(), n),
            proptest::collection::vec(rows(), 0..3),
            op,
            compact,
            Just(not_null),
        )
            .prop_map(|(parent, keep, new_files, operation, compact, not_null)| {
                // A real compaction half the time: the dropped files' rows, rewritten in one.
                let new_files = if operation == Operation::Replace && compact {
                    let merged: Vec<Row> = parent
                        .iter()
                        .zip(&keep)
                        .filter(|(_, k)| !**k)
                        .flat_map(|(r, _)| r.iter().rev().cloned())
                        .collect();
                    vec![merged]
                } else {
                    new_files
                };
                (
                    parent,
                    Step {
                        keep,
                        new_files,
                        operation,
                    },
                    not_null,
                )
            })
    })
}

type Outcome = Result<String, String>;

fn describe(d: integrity_server::pipeline::Decided) -> String {
    match d {
        (Decision::Accepted(v), _) => format!("accepted {:?}", v.key_deltas()),
        (Decision::Rejected(v), details) => {
            let details: Vec<_> = details
                .iter()
                .map(|(v, d)| (v, d.count(), d.samples().to_vec()))
                .collect();
            format!("rejected {v:?} {details:?}")
        }
    }
}

fn compare(
    parent: &[Vec<Row>],
    step: &Step,
    not_null: bool,
    row_group: usize,
    memory: usize,
) -> Result<(), String> {
    let files = Files {
        dir: Dir::new(),
        next: AtomicI64::new(1),
        manifests_of: Mutex::new(HashMap::new()),
        rows_of: Mutex::new(HashMap::new()),
    };
    let (validator, columns, table) = validator(not_null);
    let parent_manifests: Vec<String> = parent
        .iter()
        .map(|r| files.manifest_with_row_groups(r, Some(row_group)))
        .collect();
    let (parent_id, parent_list) = files.list(&parent_manifests);
    let mut new_manifests: Vec<String> = parent_manifests
        .iter()
        .zip(&step.keep)
        .filter(|(_, k)| **k)
        .map(|(m, _)| m.clone())
        .collect();
    for r in &step.new_files {
        new_manifests.push(files.manifest_with_row_groups(r, Some(row_group)));
    }
    let (new_id, new_list) = files.list(&new_manifests);

    // Indexes holding the parent snapshot, if it is valid at all.
    let indexes: BTreeMap<ConstraintId, MemoryIndex> = [1u64, 2]
        .into_iter()
        .map(|id| {
            (
                ConstraintId(id),
                MemoryIndex::new(integrity_index::IndexKind::Unique),
            )
        })
        .collect();
    let initial = diff_snapshots(&Local, None, &parent_list).unwrap();
    let rows = commit_rows(
        &Local,
        table.clone(),
        SnapshotId(parent_id),
        &initial,
        &columns,
    )
    .unwrap();
    match validator.validate_with_details(&rows, &indexes).unwrap() {
        (Decision::Accepted(v), _) => {
            for (id, staged) in v.stage(&indexes).unwrap() {
                indexes[&id].apply(staged, IndexEpoch(1)).unwrap();
            }
        }
        (Decision::Rejected(_), _) => return Ok(()), // only valid states are committed to
    }

    let changes = diff_snapshots(&Local, Some(&parent_list), &new_list).unwrap();
    let whole: Outcome = (|| {
        let rows = commit_rows(
            &Local,
            table.clone(),
            SnapshotId(new_id),
            &changes,
            &columns,
        )
        .map_err(|e| format!("{:?}", e.code()))?;
        check_operation(step.operation, &rows).map_err(|e| format!("{:?}", e.code()))?;
        validator
            .validate_with_details(&rows, &indexes)
            .map(describe)
            .map_err(|e| format!("{:?}", e.code()))
    })();
    let scratch = Dir::new();
    let streamed = Streamed {
        validator: &validator,
        table: &table,
        snapshot: SnapshotId(new_id),
        operation: step.operation,
        columns: &columns,
        scratch: &scratch.0,
        memory,
    };
    let streamed: Outcome = match streamed.validate(&Local, &changes, &indexes) {
        Ok((d, _)) => Ok(describe(d)),
        Err(e) => Err(format!("{:?}", e.code)),
    };
    let leftover = std::fs::read_dir(&scratch.0)
        .map(|d| d.count())
        .unwrap_or(0);
    if leftover != 0 {
        return Err(format!("{leftover} scratch entries left"));
    }
    if streamed == whole {
        Ok(())
    } else {
        Err(format!("streamed {streamed:?}\n   whole {whole:?}"))
    }
}

proptest! {
    #![proptest_config(Config { cases: 96, ..Config::default() })]

    #[test]
    fn streaming_a_commit_decides_like_reading_it_whole(
        (parent, step, not_null) in case(),
        row_group in 1usize..6,
        memory in 1usize..3000,
    ) {
        if let Err(e) = compare(&parent, &step, not_null, row_group, memory) {
            prop_assert!(false, "{}", e);
        }
    }
}

/// Guards against a vacuous property: fixed cases with each outcome, with spills.
#[test]
fn every_outcome_with_spills() {
    let parent: Vec<Vec<Row>> = vec![
        (0..20).map(|i| (i, Some(i))).collect(),
        (20..40).map(|i| (i, Some(i))).collect(),
    ];
    // Compaction with the same rows; one that changes a row.
    let merged: Vec<Row> = parent.concat().into_iter().rev().collect();
    let same = Step {
        keep: vec![false, false],
        new_files: vec![merged.clone()],
        operation: Operation::Replace,
    };
    compare(&parent, &same, true, 3, 50).unwrap();
    let mut changed = merged;
    changed[0].1 = None;
    let changed = Step {
        keep: vec![false, false],
        new_files: vec![changed],
        operation: Operation::Replace,
    };
    compare(&parent, &changed, true, 3, 50).unwrap();
    // Copy-on-write delete of one row; an append of duplicates and NULLs.
    let cow = Step {
        keep: vec![false, true],
        new_files: vec![(1..20).map(|i| (i, Some(i))).collect()],
        operation: Operation::Overwrite,
    };
    compare(&parent, &cow, true, 4, 50).unwrap();
    let dups = Step {
        keep: vec![true, true],
        new_files: vec![vec![
            (5, Some(100)),
            (100, Some(5)),
            (101, None),
            (101, None),
        ]],
        operation: Operation::Append,
    };
    compare(&parent, &dups, true, 2, 50).unwrap();
}
