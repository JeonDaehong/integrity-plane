//! `TableScan` (onboarding a table batch by batch) against `validate_with_details` on one commit
//! adding every row: violations, violation details and the resulting indexes must be identical,
//! however the rows are split into batches.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use std::collections::BTreeMap;

use common::*;
use integrity_core::Violation;
use integrity_core::{
    CommitRows, ConstraintKind, Datum, EncodedKey, KeyValue, LogicalType, RowBatch,
};
use integrity_index::{IndexEpoch, IndexKind, IndexValue, KeyIndex, MemoryIndex};
use integrity_types::{ConstraintId, ErrorCode, SnapshotId, TableId};
use integrity_validator::{Decision, KeyCheck, Validator, ViolationDetail};
use proptest::prelude::*;
use proptest::test_runner::Config;

type Seeds = Vec<Vec<Option<u8>>>;

fn datum(ty: &LogicalType, seed: Option<u8>) -> Datum {
    let Some(v) = seed else { return Datum::Null };
    match ty {
        LogicalType::Long | LogicalType::Int => Datum::Value(KeyValue::Integer(i64::from(v))),
        LogicalType::String => Datum::Value(KeyValue::String(format!("s{v}"))),
        _ => Datum::Opaque,
    }
}

/// Rows of `table` projected the way a format adapter would.
fn batch(validator: &Validator, table: usize, seeds: &[Vec<Option<u8>>]) -> RowBatch {
    let id = TableId::new(TABLES[table]);
    let types = table_columns(table);
    let cols = validator.projection(&id);
    let mut out = RowBatch::new(cols.clone());
    for row in seeds {
        out.push(
            cols.iter()
                .map(|f| {
                    let i = (f.0 - 1) as usize;
                    datum(&types[i], row[i])
                })
                .collect(),
        )
        .unwrap();
    }
    out
}

fn split(rows: &RowBatch, size: usize) -> Vec<RowBatch> {
    rows.rows()
        .chunks(size.max(1))
        .map(|chunk| {
            let mut b = RowBatch::new(rows.columns().to_vec());
            for r in chunk {
                b.push(r.clone()).unwrap();
            }
            b
        })
        .collect()
}

type Indexes = BTreeMap<ConstraintId, MemoryIndex>;
type Built = BTreeMap<ConstraintId, BTreeMap<EncodedKey, IndexValue>>;
type Outcome = Result<BTreeMap<ConstraintId, Vec<(EncodedKey, IndexValue)>>, String>;

fn fresh() -> Indexes {
    constraints()
        .into_iter()
        .filter_map(|c| {
            let kind = match c.kind {
                ConstraintKind::NotNull(_) => return None,
                ConstraintKind::ForeignKey(_) => IndexKind::Reference,
                _ => IndexKind::Unique,
            };
            Some((c.id, MemoryIndex::new(kind)))
        })
        .collect()
}

fn contents(indexes: &Indexes) -> BTreeMap<ConstraintId, Vec<(EncodedKey, IndexValue)>> {
    indexes
        .iter()
        .map(|(id, i)| (*id, i.entries().unwrap()))
        .collect()
}

/// The reference: each table validated as one commit, parents first.
fn one_shot(validator: &Validator, tables: &[Seeds]) -> Outcome {
    let indexes = fresh();
    for (t, seeds) in tables.iter().enumerate() {
        if seeds.is_empty() {
            continue;
        }
        let commit = CommitRows {
            table: TableId::new(TABLES[t]),
            snapshot: SnapshotId(7),
            added: batch(validator, t, seeds),
            removed: RowBatch::new(validator.projection(&TableId::new(TABLES[t]))),
            equality_deletes: None,
        };
        match validator.validate_with_details(&commit, &indexes).unwrap() {
            (Decision::Rejected(_), details) => return Err(report(&details)),
            (Decision::Accepted(v), _) => {
                for (id, staged) in v.stage(&indexes).unwrap() {
                    indexes[&id]
                        .apply(staged, IndexEpoch(t as u64 + 1))
                        .unwrap();
                }
            }
        }
    }
    Ok(contents(&indexes))
}

/// The scan: batches of `size` rows, keys sorted per constraint, then checked in key order.
fn scanned(validator: &Validator, tables: &[Seeds], size: usize) -> Outcome {
    let mut built: Built = fresh()
        .into_keys()
        .map(|id| (id, BTreeMap::new()))
        .collect();
    for (t, seeds) in tables.iter().enumerate() {
        if seeds.is_empty() {
            continue;
        }
        let mut scan = validator.table_scan(&TableId::new(TABLES[t])).unwrap();
        let mut sorted: BTreeMap<ConstraintId, BTreeMap<EncodedKey, u64>> = BTreeMap::new();
        for b in split(&batch(validator, t, seeds), size) {
            scan.feed(&b, |id, key| {
                *sorted.entry(id).or_default().entry(key).or_default() += 1;
                Ok(())
            })
            .unwrap();
        }
        let mut writes = Vec::new();
        for (id, check) in scan.checks() {
            for (key, count) in sorted.remove(&id).unwrap_or_default() {
                match check {
                    KeyCheck::Unique(code) if count > 1 => scan.record_key(id, code, &key),
                    KeyCheck::Unique(_) => writes.push((
                        id,
                        key,
                        IndexValue::Unique {
                            last_snapshot: SnapshotId(7),
                        },
                    )),
                    KeyCheck::Parent(parent) => {
                        if !built[&parent].contains_key(&key) {
                            scan.record_key(id, ErrorCode::ForeignKeyViolation, &key);
                        } else {
                            writes.push((id, key, IndexValue::Reference { child_count: count }));
                        }
                    }
                }
            }
        }
        let (violations, details) = scan.finish();
        if !violations.is_empty() {
            assert_eq!(violations.len(), details.len());
            return Err(report(&details));
        }
        for (id, key, value) in writes {
            built.get_mut(&id).unwrap().insert(key, value);
        }
    }
    Ok(built
        .into_iter()
        .map(|(id, entries)| (id, entries.into_iter().collect()))
        .collect())
}

/// Violations with their counts and samples.
fn report(details: &BTreeMap<Violation, ViolationDetail>) -> String {
    let v: Vec<_> = details
        .iter()
        .map(|(v, d)| (v, d.count(), d.samples().to_vec()))
        .collect();
    format!("{v:?}")
}

fn rows() -> impl Strategy<Value = Seeds> {
    proptest::collection::vec(
        proptest::collection::vec(proptest::option::weighted(0.9, 0u8..12), 3),
        0..12,
    )
}

/// Three tables; each is either random or tidied into valid rows (unique keys, set scores,
/// orders referencing parents 0..3), so that clean domains and FK checks occur often.
fn tables() -> impl Strategy<Value = Vec<Seeds>> {
    proptest::collection::vec((any::<bool>(), rows()), 3).prop_map(|tables| {
        tables
            .into_iter()
            .enumerate()
            .map(|(t, (tidy, rows))| {
                if !tidy {
                    return rows;
                }
                rows.into_iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let i = i as u8;
                        match t {
                            CUSTOMER => vec![Some(i), Some(i), Some(1)],
                            ACCOUNT => vec![Some(i), Some(i), Some(i)],
                            _ => {
                                let parent = r[1].map(|p| p % 3);
                                vec![Some(i), parent, parent]
                            }
                        }
                    })
                    .collect()
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(Config { cases: 512, ..Config::default() })]

    #[test]
    fn batch_by_batch_equals_one_commit(
        tables in tables(),
        size in 1usize..8,
    ) {
        let (_, engine) = setup(Backend::Memory);
        let v = &engine.validator;
        prop_assert_eq!(scanned(v, &tables, size), one_shot(v, &tables));
    }
}

#[test]
fn both_outcomes_are_exercised() {
    let (_, engine) = setup(Backend::Memory);
    let v = &engine.validator;
    // Unique ids and emails, scores set: clean.
    let customers: Seeds = (0..4).map(|i| vec![Some(i), Some(i), Some(1)]).collect();
    let clean = vec![customers.clone(), vec![], vec![]];
    assert!(scanned(v, &clean, 2).unwrap()[&ConstraintId(1)].len() == 4);
    assert_eq!(scanned(v, &clean, 2), one_shot(v, &clean));
    // Duplicate ids and a NULL score.
    let mut dirty = customers;
    dirty.push(vec![Some(0), None, None]);
    let dirty = vec![dirty, vec![], vec![]];
    let err = scanned(v, &dirty, 3).unwrap_err();
    assert!(
        err.contains("DuplicatePrimaryKey") && err.contains("NotNullViolation"),
        "{err}"
    );
    assert_eq!(scanned(v, &dirty, 3), one_shot(v, &dirty));
}

/// Guards against a vacuous property: over a fixed-seed sample, clean domains and every kind of
/// violation occur.
#[test]
fn the_property_exercises_every_outcome() {
    use proptest::strategy::ValueTree;
    let (_, engine) = setup(Backend::Memory);
    let v = &engine.validator;
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = tables();
    let (mut clean, mut referenced, mut seen) = (0, 0, String::new());
    for _ in 0..400 {
        let tables = strategy.new_tree(&mut runner).unwrap().current();
        match one_shot(v, &tables) {
            Ok(indexes) => {
                clean += 1;
                referenced += usize::from(!indexes[&ConstraintId(8)].is_empty());
            }
            Err(e) => seen.push_str(&e),
        }
    }
    assert!(clean > 20, "only {clean} clean domains");
    assert!(
        referenced > 10,
        "only {referenced} clean domains with references"
    );
    for code in [
        "DuplicatePrimaryKey",
        "DuplicateUniqueKey",
        "ForeignKeyViolation",
        "NotNullViolation",
    ] {
        assert!(seen.contains(code), "{code} never occurs");
    }
}
