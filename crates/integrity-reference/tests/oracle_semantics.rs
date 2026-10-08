//! Oracle scenarios: every spec §7 row, plus the §8 commit-level rules and §20 onboarding.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{
    Constraint, ConstraintKind, EnforcementMode, ForeignKeySpec, InvalidConstraint, KeySpec,
    KeyValue, LogicalType, MatchMode, NullsMode, ReferentialAction, UniqueSpec,
};
use integrity_reference::{Commit, Datum, Oracle, OracleError, Row, Verdict, Violation};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, TableId};

// ---------- fixture ----------
//
// customer(1 id long, 2 name string, 3 score double)
// orders(1 order_id long, 2 customer_id int, 3 region string)
// account(1 cust long, 2 region string)                -- composite parent key
// shipment(1 id long, 2 cust int, 3 region string)     -- composite FK child

const PK_CUSTOMER: ConstraintId = ConstraintId(1);
const PK_ORDERS: ConstraintId = ConstraintId(2);
const FK_ORDERS_CUSTOMER: ConstraintId = ConstraintId(3);
const PK_ACCOUNT: ConstraintId = ConstraintId(4);
const FK_SHIPMENT: ConstraintId = ConstraintId(5);
const UQ: ConstraintId = ConstraintId(6);
const NN_SCORE: ConstraintId = ConstraintId(7);

fn t(name: &str) -> TableId {
    TableId::new(name)
}

fn cols(types: &[LogicalType]) -> BTreeMap<FieldId, LogicalType> {
    types
        .iter()
        .enumerate()
        .map(|(i, ty)| (FieldId(i as i32 + 1), ty.clone()))
        .collect()
}

fn key(fields: &[i32]) -> KeySpec {
    KeySpec {
        columns: fields.iter().map(|&f| FieldId(f)).collect(),
    }
}

fn constraint(id: ConstraintId, table: &str, kind: ConstraintKind) -> Constraint {
    Constraint {
        id,
        table: t(table),
        name: format!("c{}", id.0),
        kind,
        mode: EnforcementMode::Enforced,
        version: ConstraintSetVersion(1),
    }
}

fn fk(
    id: ConstraintId,
    child: &str,
    cols: &[i32],
    parent: &str,
    pc: ConstraintId,
    m: MatchMode,
) -> Constraint {
    constraint(
        id,
        child,
        ConstraintKind::ForeignKey(ForeignKeySpec {
            child: key(cols),
            parent_table: t(parent),
            parent_constraint: pc,
            match_mode: m,
            on_delete: ReferentialAction::Restrict,
        }),
    )
}

fn db() -> Oracle {
    let mut db = Oracle::new();
    db.create_table(
        t("customer"),
        cols(&[LogicalType::Long, LogicalType::String, LogicalType::Double]),
    )
    .unwrap();
    db.create_table(
        t("orders"),
        cols(&[LogicalType::Long, LogicalType::Int, LogicalType::String]),
    )
    .unwrap();
    db.create_table(
        t("account"),
        cols(&[LogicalType::Long, LogicalType::String]),
    )
    .unwrap();
    db.create_table(
        t("shipment"),
        cols(&[LogicalType::Long, LogicalType::Int, LogicalType::String]),
    )
    .unwrap();
    db.register(constraint(
        PK_CUSTOMER,
        "customer",
        ConstraintKind::PrimaryKey(key(&[1])),
    ))
    .unwrap();
    db.register(constraint(
        PK_ORDERS,
        "orders",
        ConstraintKind::PrimaryKey(key(&[1])),
    ))
    .unwrap();
    db.register(fk(
        FK_ORDERS_CUSTOMER,
        "orders",
        &[2],
        "customer",
        PK_CUSTOMER,
        MatchMode::Simple,
    ))
    .unwrap();
    db.register(constraint(
        PK_ACCOUNT,
        "account",
        ConstraintKind::PrimaryKey(key(&[1, 2])),
    ))
    .unwrap();
    db
}

/// A datum: integers, strings, `null`, or `opaque` for the double column.
#[derive(Clone, Copy)]
enum D {
    I(i64),
    S(&'static str),
    Null,
    Opaque,
}
use D::*;

fn row(values: &[D]) -> Row {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let datum = match *v {
                I(n) => Datum::Value(KeyValue::Integer(n)),
                S(s) => Datum::Value(KeyValue::String(s.into())),
                Null => Datum::Null,
                Opaque => Datum::Opaque,
            };
            (FieldId(i as i32 + 1), datum)
        })
        .collect()
}

fn insert(db: &mut Oracle, table: &str, rows: &[&[D]]) -> Verdict {
    db.commit(&Commit {
        table: t(table),
        added: rows.iter().map(|r| row(r)).collect(),
        removed: vec![],
    })
    .unwrap()
}

fn delete(db: &mut Oracle, table: &str, rows: &[&[D]]) -> Verdict {
    db.commit(&Commit {
        table: t(table),
        added: vec![],
        removed: rows.iter().map(|r| row(r)).collect(),
    })
    .unwrap()
}

fn rejected(pairs: &[(ConstraintId, ErrorCode)]) -> Verdict {
    Verdict::Rejected(
        pairs
            .iter()
            .map(|&(constraint, code)| Violation { constraint, code })
            .collect::<BTreeSet<_>>(),
    )
}

// ---------- spec §7, one test per row ----------

#[test]
fn s7_primary_key_null_is_not_null_violation() {
    let mut db = db();
    assert_eq!(
        insert(&mut db, "customer", &[&[Null, S("a"), Opaque]]),
        rejected(&[(PK_CUSTOMER, ErrorCode::NotNullViolation)])
    );
    assert_eq!(
        insert(&mut db, "account", &[&[I(1), Null]]),
        rejected(&[(PK_ACCOUNT, ErrorCode::NotNullViolation)])
    );
}

#[test]
fn s7_unique_nulls_distinct_allows_any_number_of_null_tuples() {
    let mut db = db();
    db.register(constraint(
        UQ,
        "orders",
        ConstraintKind::Unique(UniqueSpec {
            key: key(&[2, 3]),
            nulls: NullsMode::Distinct,
        }),
    ))
    .unwrap();
    insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]);
    let v = insert(
        &mut db,
        "orders",
        &[
            &[I(10), I(1), Null],
            &[I(11), I(1), Null],
            &[I(12), Null, Null],
            &[I(13), Null, Null],
        ],
    );
    assert_eq!(v, Verdict::Accepted);
    assert_eq!(
        insert(
            &mut db,
            "orders",
            &[&[I(20), I(1), S("eu")], &[I(21), I(1), S("eu")]]
        ),
        rejected(&[(UQ, ErrorCode::DuplicateUniqueKey)])
    );
}

#[test]
fn s7_unique_nulls_not_distinct_treats_null_as_equal() {
    let mut db = db();
    db.register(constraint(
        UQ,
        "orders",
        ConstraintKind::Unique(UniqueSpec {
            key: key(&[2, 3]),
            nulls: NullsMode::NotDistinct,
        }),
    ))
    .unwrap();
    insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]);
    assert_eq!(
        insert(&mut db, "orders", &[&[I(10), I(1), Null]]),
        Verdict::Accepted
    );
    assert_eq!(
        insert(&mut db, "orders", &[&[I(11), I(1), Null]]),
        rejected(&[(UQ, ErrorCode::DuplicateUniqueKey)])
    );
    assert_eq!(
        insert(&mut db, "orders", &[&[I(12), Null, Null]]),
        Verdict::Accepted
    );
    assert_eq!(
        insert(&mut db, "orders", &[&[I(13), Null, Null]]),
        rejected(&[(UQ, ErrorCode::DuplicateUniqueKey)])
    );
    // A different non-NULL part does not conflict.
    assert_eq!(
        insert(&mut db, "orders", &[&[I(14), I(1), S("eu")]]),
        Verdict::Accepted
    );
}

#[test]
fn s7_fk_match_simple_any_null_needs_no_parent() {
    let mut db = db();
    db.register(fk(
        FK_SHIPMENT,
        "shipment",
        &[2, 3],
        "account",
        PK_ACCOUNT,
        MatchMode::Simple,
    ))
    .unwrap();
    assert_eq!(
        insert(
            &mut db,
            "shipment",
            &[
                &[I(1), I(9), Null],
                &[I(2), Null, S("eu")],
                &[I(3), Null, Null]
            ]
        ),
        Verdict::Accepted
    );
    assert_eq!(
        insert(&mut db, "shipment", &[&[I(4), I(9), S("eu")]]),
        rejected(&[(FK_SHIPMENT, ErrorCode::ForeignKeyViolation)])
    );
}

#[test]
fn s7_fk_match_full_all_null_ok_partial_null_violation() {
    let mut db = db();
    db.register(fk(
        FK_SHIPMENT,
        "shipment",
        &[2, 3],
        "account",
        PK_ACCOUNT,
        MatchMode::Full,
    ))
    .unwrap();
    insert(&mut db, "account", &[&[I(9), S("eu")]]);
    assert_eq!(
        insert(&mut db, "shipment", &[&[I(1), Null, Null]]),
        Verdict::Accepted
    );
    for partial in [&[I(2), I(9), Null], &[I(3), Null, S("eu")]] {
        assert_eq!(
            insert(&mut db, "shipment", &[partial]),
            rejected(&[(FK_SHIPMENT, ErrorCode::ForeignKeyViolation)])
        );
    }
    // Partial NULL is a violation even when the non-NULL part matches a parent.
    assert_eq!(
        insert(&mut db, "shipment", &[&[I(4), I(9), S("eu")]]),
        Verdict::Accepted
    );
}

#[test]
fn s7_fk_match_full_partial_null_never_matches_a_null_holding_parent() {
    // A UNIQUE NULLS NOT DISTINCT parent can hold (9, NULL). A child (9, NULL) equals it
    // structurally, but MATCH FULL still rejects the partially NULL child; MATCH SIMPLE
    // exempts it without needing the parent.
    let mut db = db();
    db.create_table(t("region"), cols(&[LogicalType::Long, LogicalType::String]))
        .unwrap();
    db.register(constraint(
        UQ,
        "region",
        ConstraintKind::Unique(UniqueSpec {
            key: key(&[1, 2]),
            nulls: NullsMode::NotDistinct,
        }),
    ))
    .unwrap();
    db.register(fk(
        FK_SHIPMENT,
        "shipment",
        &[2, 3],
        "region",
        UQ,
        MatchMode::Full,
    ))
    .unwrap();
    assert_eq!(
        insert(&mut db, "region", &[&[I(9), Null]]),
        Verdict::Accepted
    );
    assert_eq!(
        insert(&mut db, "shipment", &[&[I(1), I(9), Null]]),
        rejected(&[(FK_SHIPMENT, ErrorCode::ForeignKeyViolation)])
    );
}

// ---------- spec §8 and related ----------

#[test]
fn duplicate_primary_key_within_commit_and_against_state() {
    let mut db = db();
    assert_eq!(
        insert(
            &mut db,
            "customer",
            &[&[I(1), S("a"), Opaque], &[I(1), S("b"), Opaque]]
        ),
        rejected(&[(PK_CUSTOMER, ErrorCode::DuplicatePrimaryKey)])
    );
    assert_eq!(
        insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]),
        Verdict::Accepted
    );
    assert_eq!(
        insert(&mut db, "customer", &[&[I(1), S("other"), Opaque]]),
        rejected(&[(PK_CUSTOMER, ErrorCode::DuplicatePrimaryKey)])
    );
}

#[test]
fn copy_on_write_rewrite_is_unchanged() {
    let mut db = db();
    insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]);
    let v = db
        .commit(&Commit {
            table: t("customer"),
            removed: vec![row(&[I(1), S("a"), Opaque])],
            added: vec![row(&[I(1), S("renamed"), Opaque])],
        })
        .unwrap();
    assert_eq!(v, Verdict::Accepted);
}

#[test]
fn readding_a_key_twice_while_removing_it_once_is_a_duplicate() {
    // Spec §8 warning: net delta +1, but the post-commit state holds the key twice.
    let mut db = db();
    insert(&mut db, "customer", &[&[I(7), S("a"), Opaque]]);
    let v = db
        .commit(&Commit {
            table: t("customer"),
            removed: vec![row(&[I(7), S("a"), Opaque])],
            added: vec![row(&[I(7), S("b"), Opaque]), row(&[I(7), S("c"), Opaque])],
        })
        .unwrap();
    assert_eq!(
        v,
        rejected(&[(PK_CUSTOMER, ErrorCode::DuplicatePrimaryKey)])
    );
}

#[test]
fn fk_int_child_matches_long_parent() {
    let mut db = db();
    insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]);
    assert_eq!(
        insert(&mut db, "orders", &[&[I(10), I(1), S("eu")]]),
        Verdict::Accepted
    );
    assert_eq!(
        insert(&mut db, "orders", &[&[I(11), I(999), S("eu")]]),
        rejected(&[(FK_ORDERS_CUSTOMER, ErrorCode::ForeignKeyViolation)])
    );
}

#[test]
fn deleting_a_referenced_parent_is_referenced_row_delete() {
    let mut db = db();
    insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]);
    insert(&mut db, "orders", &[&[I(10), I(1), S("eu")]]);
    assert_eq!(
        delete(&mut db, "customer", &[&[I(1), S("a"), Opaque]]),
        rejected(&[(FK_ORDERS_CUSTOMER, ErrorCode::ReferencedRowDelete)])
    );
    // Children first, then the parent (spec §15 ordering consequence).
    assert_eq!(
        delete(&mut db, "orders", &[&[I(10), I(1), S("eu")]]),
        Verdict::Accepted
    );
    assert_eq!(
        delete(&mut db, "customer", &[&[I(1), S("a"), Opaque]]),
        Verdict::Accepted
    );
}

#[test]
fn rejected_commit_leaves_state_unchanged() {
    let mut db = db();
    insert(&mut db, "customer", &[&[I(1), S("a"), Opaque]]);
    let before = db.rows(&t("customer")).unwrap().to_vec();
    insert(
        &mut db,
        "customer",
        &[&[I(2), S("b"), Opaque], &[I(1), S("dup"), Opaque]],
    );
    assert_eq!(db.rows(&t("customer")).unwrap(), before.as_slice());
}

#[test]
fn all_violations_are_reported() {
    let mut db = db();
    db.register(constraint(
        NN_SCORE,
        "customer",
        ConstraintKind::NotNull(FieldId(3)),
    ))
    .unwrap();
    assert_eq!(
        insert(
            &mut db,
            "customer",
            &[
                &[I(1), S("a"), Null],
                &[I(1), S("b"), Opaque],
                &[Null, S("c"), Opaque]
            ]
        ),
        rejected(&[
            (PK_CUSTOMER, ErrorCode::DuplicatePrimaryKey),
            (PK_CUSTOMER, ErrorCode::NotNullViolation),
            (NN_SCORE, ErrorCode::NotNullViolation),
        ])
    );
}

#[test]
fn disabled_constraints_are_not_evaluated() {
    let mut db = db();
    db.register(Constraint {
        mode: EnforcementMode::Disabled,
        ..constraint(NN_SCORE, "customer", ConstraintKind::NotNull(FieldId(3)))
    })
    .unwrap();
    assert_eq!(
        insert(&mut db, "customer", &[&[I(1), S("a"), Null]]),
        Verdict::Accepted
    );
}

// ---------- registration and misuse ----------

#[test]
fn onboarding_rejects_existing_violations() {
    let mut db = db();
    insert(
        &mut db,
        "orders",
        &[&[I(10), Null, S("eu")], &[I(11), Null, S("eu")]],
    );
    let uq = constraint(
        UQ,
        "orders",
        ConstraintKind::Unique(UniqueSpec {
            key: key(&[3]),
            nulls: NullsMode::Distinct,
        }),
    );
    assert_eq!(
        db.register(uq),
        Err(OracleError::OnboardingViolations(
            [Violation {
                constraint: UQ,
                code: ErrorCode::DuplicateUniqueKey
            }]
            .into()
        ))
    );
    // Nothing was registered: the duplicate region is still accepted.
    assert_eq!(
        insert(&mut db, "orders", &[&[I(12), Null, S("eu")]]),
        Verdict::Accepted
    );
}

#[test]
fn invalid_definitions_are_rejected() {
    let mut db = db();
    let float_pk = constraint(UQ, "customer", ConstraintKind::PrimaryKey(key(&[3])));
    assert!(matches!(
        db.register(float_pk),
        Err(OracleError::InvalidConstraint(
            InvalidConstraint::UnsupportedKeyType { .. }
        ))
    ));
    assert_eq!(
        db.register(constraint(
            PK_CUSTOMER,
            "customer",
            ConstraintKind::PrimaryKey(key(&[1]))
        )),
        Err(OracleError::DuplicateConstraint(PK_CUSTOMER))
    );
}

#[test]
fn malformed_commits_are_errors_not_verdicts() {
    let mut db = db();
    let bad_type = Commit {
        table: t("customer"),
        added: vec![row(&[S("not an int"), S("a"), Opaque])],
        removed: vec![],
    };
    assert_eq!(
        db.commit(&bad_type),
        Err(OracleError::MalformedRow {
            field: Some(FieldId(1))
        })
    );
    let opaque_in_key_column = Commit {
        table: t("customer"),
        added: vec![row(&[Opaque, S("a"), Opaque])],
        removed: vec![],
    };
    assert_eq!(
        db.commit(&opaque_in_key_column),
        Err(OracleError::MalformedRow {
            field: Some(FieldId(1))
        })
    );
    let short = Commit {
        table: t("customer"),
        added: vec![row(&[I(1)])],
        removed: vec![],
    };
    assert_eq!(
        db.commit(&short),
        Err(OracleError::MalformedRow { field: None })
    );
    assert_eq!(
        db.commit(&Commit {
            table: t("customer"),
            added: vec![],
            removed: vec![row(&[I(1), S("a"), Opaque])]
        }),
        Err(OracleError::RemovedRowMissing)
    );
    assert_eq!(
        db.commit(&Commit {
            table: t("nope"),
            added: vec![],
            removed: vec![]
        }),
        Err(OracleError::UnknownTable(t("nope")))
    );
}
