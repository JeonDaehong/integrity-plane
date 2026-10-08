//! Differential harness: the same commits go to the reference oracle and to the engine
//! (validator + in-memory indexes); verdicts must be identical.

#![allow(dead_code, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{
    CommitRows, Constraint, ConstraintKind, Datum, EnforcementMode, ForeignKeySpec, KeySpec,
    KeyValue, LogicalType, MatchMode, NullsMode, ReferentialAction, RowBatch, UniqueSpec,
};
use integrity_index::{IndexEpoch, IndexValue, KeyIndex, MemoryIndex};
use integrity_reference::{Commit, Oracle, Row, Verdict};
use integrity_types::{ConstraintId, ConstraintSetVersion, FieldId, SnapshotId, TableId};
use integrity_validator::{Decision, ResolvedConstraint, ValidationError, Validator};
use proptest::prelude::*;

// ---------- engine ----------

pub struct Engine {
    pub validator: Validator,
    pub indexes: BTreeMap<ConstraintId, MemoryIndex>,
    epoch: u64,
    snapshot: i64,
}

impl Engine {
    pub fn new(constraints: Vec<ResolvedConstraint>) -> Self {
        let indexes = constraints
            .iter()
            .filter(|rc| rc.constraint.mode == EnforcementMode::Enforced)
            .filter_map(|rc| Some((rc.constraint.id, MemoryIndex::new(rc.index_kind()?))))
            .collect();
        Self {
            validator: Validator::new(constraints),
            indexes,
            epoch: 0,
            snapshot: 0,
        }
    }

    /// Projects rows the way a format adapter would: only the validator's columns.
    pub fn rows(&self, table: &TableId, rows: &[Row]) -> RowBatch {
        let cols = self.validator.projection(table);
        let mut batch = RowBatch::new(cols.clone());
        for r in rows {
            batch
                .push(cols.iter().map(|c| r[c].clone()).collect())
                .unwrap();
        }
        batch
    }

    pub fn commit(&mut self, commit: &Commit) -> Result<Verdict, ValidationError> {
        self.snapshot += 1;
        let rows = CommitRows {
            table: commit.table.clone(),
            snapshot: SnapshotId(self.snapshot),
            added: self.rows(&commit.table, &commit.added),
            removed: self.rows(&commit.table, &commit.removed),
        };
        match self.validator.validate(&rows, &self.indexes)? {
            Decision::Rejected(v) => Ok(Verdict::Rejected(v)),
            Decision::Accepted(deltas) => {
                let staged = deltas.stage(&self.indexes)?;
                self.epoch += 1;
                for (id, s) in staged {
                    self.indexes[&id].apply(s, IndexEpoch(self.epoch)).unwrap();
                }
                Ok(Verdict::Accepted)
            }
        }
    }

    /// Index contents without `last_snapshot` (which depends on history).
    pub fn contents(&self) -> BTreeMap<ConstraintId, Vec<(Vec<u8>, u64)>> {
        self.indexes
            .iter()
            .map(|(id, index)| {
                let entries = index
                    .entries()
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| {
                        let n = match v {
                            IndexValue::Unique { .. } => 1,
                            IndexValue::Reference { child_count } => child_count,
                        };
                        (k.as_bytes().to_vec(), n)
                    })
                    .collect();
                (*id, entries)
            })
            .collect()
    }
}

// ---------- fixture ----------
//
// customer(1 id long, 2 email string, 3 score double)
//   PK(id) · UNIQUE NULLS DISTINCT(email) · NOT NULL(score)
// account(1 cust long, 2 region string, 3 aid long)
//   PK(aid) · UNIQUE NULLS NOT DISTINCT(cust, region)
// orders(1 oid long, 2 cust int, 3 region string)
//   PK(oid) · FK MATCH SIMPLE (cust) → customer PK · FK MATCH FULL (cust, region) → account UNIQUE

pub const CUSTOMER: usize = 0;
pub const ACCOUNT: usize = 1;
pub const ORDERS: usize = 2;
pub const TABLES: [&str; 3] = ["customer", "account", "orders"];

pub fn table_columns(table: usize) -> Vec<LogicalType> {
    match table {
        CUSTOMER => vec![LogicalType::Long, LogicalType::String, LogicalType::Double],
        ACCOUNT => vec![LogicalType::Long, LogicalType::String, LogicalType::Long],
        _ => vec![LogicalType::Long, LogicalType::Int, LogicalType::String],
    }
}

fn key(fields: &[i32]) -> KeySpec {
    KeySpec {
        columns: fields.iter().map(|&f| FieldId(f)).collect(),
    }
}

fn c(id: u64, table: &str, kind: ConstraintKind) -> Constraint {
    Constraint {
        id: ConstraintId(id),
        table: TableId::new(table),
        name: format!("c{id}"),
        kind,
        mode: EnforcementMode::Enforced,
        version: ConstraintSetVersion(1),
    }
}

fn fk(id: u64, cols: &[i32], parent: &str, pc: u64, m: MatchMode) -> Constraint {
    c(
        id,
        "orders",
        ConstraintKind::ForeignKey(ForeignKeySpec {
            child: key(cols),
            parent_table: TableId::new(parent),
            parent_constraint: ConstraintId(pc),
            match_mode: m,
            on_delete: ReferentialAction::Restrict,
        }),
    )
}

pub fn constraints() -> Vec<Constraint> {
    vec![
        c(1, "customer", ConstraintKind::PrimaryKey(key(&[1]))),
        c(
            2,
            "customer",
            ConstraintKind::Unique(UniqueSpec {
                key: key(&[2]),
                nulls: NullsMode::Distinct,
            }),
        ),
        c(3, "customer", ConstraintKind::NotNull(FieldId(3))),
        c(4, "account", ConstraintKind::PrimaryKey(key(&[3]))),
        c(
            5,
            "account",
            ConstraintKind::Unique(UniqueSpec {
                key: key(&[1, 2]),
                nulls: NullsMode::NotDistinct,
            }),
        ),
        c(6, "orders", ConstraintKind::PrimaryKey(key(&[1]))),
        fk(7, &[2], "customer", 1, MatchMode::Simple),
        fk(8, &[2, 3], "account", 5, MatchMode::Full),
    ]
}

/// A fresh oracle and engine over the fixture.
pub fn setup() -> (Oracle, Engine) {
    let mut oracle = Oracle::new();
    for (t, name) in TABLES.iter().enumerate() {
        let cols = table_columns(t)
            .into_iter()
            .enumerate()
            .map(|(i, ty)| (FieldId(i as i32 + 1), ty))
            .collect();
        oracle.create_table(TableId::new(*name), cols).unwrap();
    }
    let mut resolved = Vec::new();
    for constraint in constraints() {
        resolved.push(ResolvedConstraint::resolve(constraint.clone(), &oracle).unwrap());
        oracle.register(constraint).unwrap();
    }
    (oracle, Engine::new(resolved))
}

// ---------- random operations ----------

/// One cell seed: `None` is NULL; values come from a 3-value domain so that keys collide and
/// children often find parents (otherwise referenced-parent deletes almost never happen).
pub type CellSeed = Option<u8>;

#[derive(Debug, Clone)]
pub struct OpSeed {
    pub table: usize,
    pub adds: Vec<Vec<CellSeed>>,
    pub removes: Vec<prop::sample::Index>,
    /// Re-add every removed row unchanged (copy-on-write rewrite of untouched rows).
    pub rewrite: bool,
}

fn cell_seed() -> impl Strategy<Value = CellSeed> {
    proptest::option::weighted(0.8, 0u8..3)
}

pub fn op_seed() -> impl Strategy<Value = OpSeed> {
    (
        0..TABLES.len(),
        proptest::collection::vec(proptest::collection::vec(cell_seed(), 3), 1..3),
        proptest::collection::vec(any::<prop::sample::Index>(), 0..3),
        proptest::bool::weighted(0.2),
    )
        .prop_map(|(table, adds, removes, rewrite)| OpSeed {
            table,
            adds,
            removes,
            rewrite,
        })
}

fn datum(ty: &LogicalType, seed: CellSeed) -> Datum {
    let Some(v) = seed else { return Datum::Null };
    match ty {
        LogicalType::Long | LogicalType::Int => Datum::Value(KeyValue::Integer(i64::from(v))),
        LogicalType::String => Datum::Value(KeyValue::String(
            ["a", "b", "c", "d"][usize::from(v)].into(),
        )),
        _ => Datum::Opaque,
    }
}

/// Turns a seed into a concrete commit against the oracle's current rows.
pub fn op(oracle: &Oracle, seed: &OpSeed) -> Commit {
    let table = TableId::new(TABLES[seed.table]);
    let types = table_columns(seed.table);
    let added_new = seed.adds.iter().map(|cells| {
        cells
            .iter()
            .zip(&types)
            .enumerate()
            .map(|(i, (&s, ty))| (FieldId(i as i32 + 1), datum(ty, s)))
            .collect::<Row>()
    });
    let existing = oracle.rows(&table).unwrap();
    let mut positions = BTreeSet::new();
    if !existing.is_empty() {
        for idx in &seed.removes {
            positions.insert(idx.index(existing.len()));
        }
    }
    let removed: Vec<Row> = positions.iter().map(|&i| existing[i].clone()).collect();
    let mut added: Vec<Row> = added_new.collect();
    if seed.rewrite {
        added.extend(removed.iter().cloned());
    }
    Commit {
        table,
        added,
        removed,
    }
}

/// Runs a sequence through both sides; returns the verdicts, or a description of the first
/// disagreement.
pub fn run(seeds: &[OpSeed]) -> Result<Vec<Verdict>, String> {
    let (mut oracle, mut engine) = setup();
    let mut verdicts = Vec::new();
    for (step, seed) in seeds.iter().enumerate() {
        let commit = op(&oracle, seed);
        let expected = oracle.commit(&commit).unwrap();
        let actual = engine
            .commit(&commit)
            .map_err(|e| format!("step {step}: engine error {e} on {commit:?}"))?;
        if actual != expected {
            return Err(format!(
                "step {step}: oracle {expected:?} but engine {actual:?} on {commit:?}"
            ));
        }
        verdicts.push(actual);
    }

    // Final state: the live indexes equal indexes rebuilt from the oracle's rows by loading
    // them into a fresh engine, parents first.
    let (_, mut rebuilt) = setup();
    for name in TABLES {
        let table = TableId::new(name);
        let rows = oracle.rows(&table).unwrap().to_vec();
        let v = rebuilt
            .commit(&Commit {
                table,
                added: rows,
                removed: vec![],
            })
            .map_err(|e| format!("rebuild: engine error {e}"))?;
        if v != Verdict::Accepted {
            return Err(format!("rebuild of a valid state rejected: {v:?}"));
        }
    }
    if engine.contents() != rebuilt.contents() {
        return Err("live indexes differ from indexes rebuilt from the oracle state".into());
    }
    Ok(verdicts)
}
