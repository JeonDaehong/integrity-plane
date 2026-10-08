//! Equality deletes (ADR 0009) against the oracle: Flink-style upserts and pure equality deletes on
//! a PK, with a NOT NULL column and a foreign key referencing that PK, on both index backends.
//! Also the shapes the validator must refuse.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use std::collections::BTreeMap;

use common::{Backend, Engine};
use integrity_core::{
    Constraint, ConstraintKind, Datum, EnforcementMode, ForeignKeySpec, KeySpec, KeyValue,
    LogicalType, MatchMode, NullsMode, ReferentialAction, UniqueSpec,
};
use integrity_reference::{Commit, Oracle, Row, Verdict};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, TableId};
use integrity_validator::{ResolvedConstraint, ValidationError};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, TestRunner};

// customer(1 id long, 2 name string): PK(id) #1, NOT NULL(name) #2
// orders(1 oid long, 2 cust long):    PK(oid) #3, FK(cust) → customer PK #4

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

fn key(f: i32) -> KeySpec {
    KeySpec {
        columns: vec![FieldId(f)],
    }
}

fn fixture(extra: Vec<Constraint>) -> (Oracle, Vec<ResolvedConstraint>) {
    let mut oracle = Oracle::new();
    let cols = |types: [LogicalType; 2]| {
        types
            .into_iter()
            .enumerate()
            .map(|(i, t)| (FieldId(i as i32 + 1), t))
            .collect()
    };
    oracle
        .create_table(
            TableId::new("customer"),
            cols([LogicalType::Long, LogicalType::String]),
        )
        .unwrap();
    oracle
        .create_table(
            TableId::new("orders"),
            cols([LogicalType::Long, LogicalType::Long]),
        )
        .unwrap();
    let mut constraints = vec![
        c(1, "customer", ConstraintKind::PrimaryKey(key(1))),
        c(2, "customer", ConstraintKind::NotNull(FieldId(2))),
        c(3, "orders", ConstraintKind::PrimaryKey(key(1))),
        c(
            4,
            "orders",
            ConstraintKind::ForeignKey(ForeignKeySpec {
                child: key(2),
                parent_table: TableId::new("customer"),
                parent_constraint: ConstraintId(1),
                match_mode: MatchMode::Simple,
                on_delete: ReferentialAction::Restrict,
            }),
        ),
    ];
    constraints.extend(extra);
    let mut resolved = Vec::new();
    for constraint in constraints {
        resolved.push(ResolvedConstraint::resolve(constraint.clone(), &oracle).unwrap());
        oracle.register(constraint).unwrap();
    }
    (oracle, resolved)
}

fn int(v: Option<u8>) -> Datum {
    v.map_or(Datum::Null, |v| {
        Datum::Value(KeyValue::Integer(i64::from(v)))
    })
}

fn name(v: Option<u8>) -> Datum {
    v.map_or(Datum::Null, |v| {
        Datum::Value(KeyValue::String(format!("n{v}")))
    })
}

fn row(a: Datum, b: Datum) -> Row {
    [(FieldId(1), a), (FieldId(2), b)].into_iter().collect()
}

#[derive(Debug, Clone)]
enum Op {
    /// Flink upsert: delete every key being written (plus `extra`), then add the rows.
    Upsert {
        rows: Vec<(Option<u8>, Option<u8>)>,
        extra: Vec<Option<u8>>,
    },
    /// Equality delete on customer ids, no rows added.
    EqDelete(Vec<Option<u8>>),
    /// Plain customer insert.
    Insert(Vec<(Option<u8>, Option<u8>)>),
    /// Remove existing customer rows by position.
    DeleteRows(Vec<prop::sample::Index>),
    /// Order insert.
    Order(Vec<(Option<u8>, Option<u8>)>),
    /// Remove existing order rows by position.
    DeleteOrders(Vec<prop::sample::Index>),
}

fn small() -> impl Strategy<Value = Option<u8>> {
    proptest::option::weighted(0.85, 0u8..4)
}

fn op() -> impl Strategy<Value = Op> {
    let rows = || proptest::collection::vec((small(), small()), 1..3);
    let picks = || proptest::collection::vec(any::<prop::sample::Index>(), 1..3);
    prop_oneof![
        3 => (rows(), proptest::collection::vec(small(), 0..2)).prop_map(|(rows, extra)| Op::Upsert { rows, extra }),
        2 => proptest::collection::vec(small(), 1..3).prop_map(Op::EqDelete),
        2 => rows().prop_map(Op::Insert),
        1 => picks().prop_map(Op::DeleteRows),
        3 => rows().prop_map(Op::Order),
        2 => picks().prop_map(Op::DeleteOrders),
    ]
}

type Deletes = Option<(Vec<FieldId>, Vec<Vec<Datum>>)>;

fn concrete(oracle: &Oracle, op: &Op) -> (Commit, Deletes) {
    let pick = |table: &str, picks: &[prop::sample::Index]| -> Vec<Row> {
        let rows = oracle.rows(&TableId::new(table)).unwrap();
        if rows.is_empty() {
            return Vec::new();
        }
        let mut idx: Vec<usize> = picks.iter().map(|p| p.index(rows.len())).collect();
        idx.sort_unstable();
        idx.dedup();
        idx.into_iter().map(|i| rows[i].clone()).collect()
    };
    let commit = |table: &str, added: Vec<Row>, removed: Vec<Row>| Commit {
        table: TableId::new(table),
        added,
        removed,
    };
    match op {
        Op::Upsert { rows, extra } => {
            let keys = rows
                .iter()
                .map(|r| r.0)
                .chain(extra.iter().copied())
                .map(|k| vec![int(k)])
                .collect();
            let added = rows.iter().map(|&(k, n)| row(int(k), name(n))).collect();
            (
                commit("customer", added, vec![]),
                Some((vec![FieldId(1)], keys)),
            )
        }
        Op::EqDelete(ids) => (
            commit("customer", vec![], vec![]),
            Some((
                vec![FieldId(1)],
                ids.iter().map(|&k| vec![int(k)]).collect(),
            )),
        ),
        Op::Insert(rows) => (
            commit(
                "customer",
                rows.iter().map(|&(k, n)| row(int(k), name(n))).collect(),
                vec![],
            ),
            None,
        ),
        Op::DeleteRows(p) => (commit("customer", vec![], pick("customer", p)), None),
        Op::Order(rows) => (
            commit(
                "orders",
                rows.iter().map(|&(o, c)| row(int(o), int(c))).collect(),
                vec![],
            ),
            None,
        ),
        Op::DeleteOrders(p) => (commit("orders", vec![], pick("orders", p)), None),
    }
}

fn run(ops: &[Op], backend: Backend) -> Result<Vec<Verdict>, String> {
    let (mut oracle, resolved) = fixture(vec![]);
    let mut engine = Engine::new(resolved.clone(), backend);
    let mut verdicts = Vec::new();
    for (step, op) in ops.iter().enumerate() {
        let (commit, deletes) = concrete(&oracle, op);
        let d = deletes.as_ref().map(|(f, k)| (f.as_slice(), k.as_slice()));
        let expected = oracle.commit_with_deletes(&commit, d).unwrap();
        let actual = engine
            .commit_with_deletes(&commit, d)
            .map_err(|e| format!("step {step}: engine error {e} on {op:?}"))?;
        if actual != expected {
            return Err(format!(
                "step {step}: oracle {expected:?} but engine {actual:?} on {op:?}"
            ));
        }
        verdicts.push(actual);
    }
    // Live indexes equal indexes loaded from the oracle's final rows.
    let mut rebuilt = Engine::new(resolved, Backend::Memory);
    for table in ["customer", "orders"] {
        let rows = oracle.rows(&TableId::new(table)).unwrap().to_vec();
        let v = rebuilt
            .commit(&Commit {
                table: TableId::new(table),
                added: rows,
                removed: vec![],
            })
            .map_err(|e| format!("rebuild: {e}"))?;
        if v != Verdict::Accepted {
            return Err(format!("rebuild rejected: {v:?}"));
        }
    }
    if engine.contents() != rebuilt.contents() {
        return Err("live indexes differ from indexes rebuilt from the oracle".into());
    }
    Ok(verdicts)
}

proptest! {
    #![proptest_config(Config { cases: 384, ..Config::default() })]

    #[test]
    fn upserts_match_oracle(ops in proptest::collection::vec(op(), 1..30)) {
        if let Err(e) = run(&ops, Backend::Memory) {
            prop_assert!(false, "{}", e);
        }
    }
}

proptest! {
    #![proptest_config(Config { cases: 48, ..Config::default() })]

    #[test]
    fn upserts_match_oracle_persistent(ops in proptest::collection::vec(op(), 1..30)) {
        if let Err(e) = run(&ops, Backend::Persistent) {
            prop_assert!(false, "{}", e);
        }
    }
}

#[test]
fn upsert_runs_exercise_every_verdict() {
    let mut runner = TestRunner::deterministic();
    let strategy = proptest::collection::vec(op(), 1..30);
    let mut accepted = 0;
    let mut codes: BTreeMap<ErrorCode, usize> = BTreeMap::new();
    for _ in 0..300 {
        let ops = strategy.new_tree(&mut runner).unwrap().current();
        for v in run(&ops, Backend::Memory).unwrap() {
            match v {
                Verdict::Accepted => accepted += 1,
                Verdict::Rejected(vs) => {
                    for violation in vs {
                        *codes.entry(violation.code).or_default() += 1;
                    }
                }
            }
        }
    }
    eprintln!("accepted={accepted} rejections={codes:?}");
    assert!(accepted > 100);
    for code in [
        ErrorCode::DuplicatePrimaryKey,
        ErrorCode::NotNullViolation,
        ErrorCode::ForeignKeyViolation,
        ErrorCode::ReferencedRowDelete,
    ] {
        assert!(
            codes.get(&code).copied().unwrap_or(0) > 5,
            "{code} barely exercised: {codes:?}"
        );
    }
}

// ---------- unsupported shapes (fail closed) ----------

fn eq_delete(
    engine: &mut Engine,
    table: &str,
    field: i32,
    added: Vec<Row>,
    removed: Vec<Row>,
) -> Result<Verdict, ValidationError> {
    let fields = [FieldId(field)];
    let keys = [vec![int(Some(1))]];
    engine.commit_with_deletes(
        &Commit {
            table: TableId::new(table),
            added,
            removed,
        },
        Some((&fields, &keys)),
    )
}

#[test]
fn unsupported_equality_delete_shapes_are_refused() {
    let unsupported = |r: Result<Verdict, ValidationError>| match r {
        Err(e @ ValidationError::Unsupported(_)) => {
            assert_eq!(e.code(), ErrorCode::UnsupportedCommitOperation)
        }
        other => panic!("expected unsupported, got {other:?}"),
    };
    let (_, resolved) = fixture(vec![]);
    let mut engine = Engine::new(resolved, Backend::Memory);
    engine
        .commit(&Commit {
            table: TableId::new("customer"),
            added: vec![row(int(Some(1)), name(Some(1)))],
            removed: vec![],
        })
        .unwrap();
    // On a table with a foreign key (orders).
    unsupported(eq_delete(&mut engine, "orders", 1, vec![], vec![]));
    // On a non-key column.
    unsupported(eq_delete(&mut engine, "customer", 2, vec![], vec![]));
    // Together with removed data files.
    unsupported(eq_delete(
        &mut engine,
        "customer",
        1,
        vec![],
        vec![row(int(Some(1)), name(Some(1)))],
    ));

    // Another UNIQUE key on the table: the deleted rows' values for it are unknown.
    let uq = c(
        5,
        "customer",
        ConstraintKind::Unique(UniqueSpec {
            key: key(2),
            nulls: NullsMode::Distinct,
        }),
    );
    let (_, resolved) = fixture(vec![uq]);
    let mut engine = Engine::new(resolved, Backend::Memory);
    unsupported(eq_delete(&mut engine, "customer", 1, vec![], vec![]));
}

#[test]
fn deleting_a_missing_key_changes_nothing_and_an_upsert_keeps_references() {
    let (_, resolved) = fixture(vec![]);
    let mut engine = Engine::new(resolved, Backend::Memory);
    let customer = |rows| Commit {
        table: TableId::new("customer"),
        added: rows,
        removed: vec![],
    };
    engine
        .commit(&customer(vec![row(int(Some(1)), name(Some(1)))]))
        .unwrap();
    engine
        .commit(&Commit {
            table: TableId::new("orders"),
            added: vec![row(int(Some(9)), int(Some(1)))],
            removed: vec![],
        })
        .unwrap();
    let before = engine.contents();
    // Flink emits deletes for keys that never existed: no effect.
    let fields = [FieldId(1)];
    assert_eq!(
        engine
            .commit_with_deletes(&customer(vec![]), Some((&fields, &[vec![int(Some(3))]])))
            .unwrap(),
        Verdict::Accepted
    );
    assert_eq!(engine.contents(), before);
    // Upserting the referenced customer (delete + re-add) keeps the order valid.
    assert_eq!(
        engine
            .commit_with_deletes(
                &customer(vec![row(int(Some(1)), name(Some(2)))]),
                Some((&fields, &[vec![int(Some(1))]]))
            )
            .unwrap(),
        Verdict::Accepted
    );
    // A pure delete of it is a referenced-row delete.
    let Verdict::Rejected(v) = engine
        .commit_with_deletes(&customer(vec![]), Some((&fields, &[vec![int(Some(1))]])))
        .unwrap()
    else {
        panic!()
    };
    assert!(v.iter().all(|x| x.code == ErrorCode::ReferencedRowDelete));
}
