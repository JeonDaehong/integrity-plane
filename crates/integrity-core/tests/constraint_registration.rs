//! Spec §6 / §9: constraint definitions are validated at registration; failures are
//! `INVALID_CONSTRAINT`.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeMap;

use integrity_core::{
    Constraint, ConstraintKind, EnforcementMode, ForeignKeySpec, InvalidConstraint, KeySpec,
    LogicalType, MatchMode, NullsMode, ReferentialAction, RegistrationContext, TypeFamily,
    UniqueSpec,
};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, TableId};

struct Ctx {
    columns: BTreeMap<(TableId, FieldId), LogicalType>,
    constraints: BTreeMap<ConstraintId, Constraint>,
}

impl RegistrationContext for Ctx {
    fn column_type(&self, table: &TableId, field: FieldId) -> Option<LogicalType> {
        self.columns.get(&(table.clone(), field)).cloned()
    }
    fn constraint(&self, id: ConstraintId) -> Option<&Constraint> {
        self.constraints.get(&id)
    }
}

fn customer() -> TableId {
    TableId::new("customer")
}

fn orders() -> TableId {
    TableId::new("orders")
}

const PK_CUSTOMER: ConstraintId = ConstraintId(1);
const NN_CUSTOMER_NAME: ConstraintId = ConstraintId(2);
const UQ_CUSTOMER_AMOUNT: ConstraintId = ConstraintId(3);
const PK_CUSTOMER_DISABLED: ConstraintId = ConstraintId(4);

fn constraint(id: u64, table: TableId, kind: ConstraintKind) -> Constraint {
    Constraint {
        id: ConstraintId(id),
        table,
        name: format!("c{id}"),
        kind,
        mode: EnforcementMode::Enforced,
        version: ConstraintSetVersion(1),
    }
}

fn key(fields: &[i32]) -> KeySpec {
    KeySpec {
        columns: fields.iter().map(|&f| FieldId(f)).collect(),
    }
}

/// customer(1 id long, 2 name string, 3 score double, 4 amount decimal(10,2), 5 tags nested)
/// orders(1 order_id long, 2 customer_id int, 3 amount decimal(12,3), 4 amount2 decimal(12,2))
fn ctx() -> Ctx {
    let mut columns = BTreeMap::new();
    for (f, t) in [
        (1, LogicalType::Long),
        (2, LogicalType::String),
        (3, LogicalType::Double),
        (
            4,
            LogicalType::Decimal {
                precision: 10,
                scale: 2,
            },
        ),
        (5, LogicalType::Nested),
    ] {
        columns.insert((customer(), FieldId(f)), t);
    }
    for (f, t) in [
        (1, LogicalType::Long),
        (2, LogicalType::Int),
        (
            3,
            LogicalType::Decimal {
                precision: 12,
                scale: 3,
            },
        ),
        (
            4,
            LogicalType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
    ] {
        columns.insert((orders(), FieldId(f)), t);
    }
    let mut constraints = BTreeMap::new();
    for c in [
        constraint(1, customer(), ConstraintKind::PrimaryKey(key(&[1]))),
        constraint(2, customer(), ConstraintKind::NotNull(FieldId(2))),
        constraint(
            3,
            customer(),
            ConstraintKind::Unique(UniqueSpec {
                key: key(&[4]),
                nulls: NullsMode::Distinct,
            }),
        ),
        Constraint {
            mode: EnforcementMode::Disabled,
            ..constraint(4, customer(), ConstraintKind::PrimaryKey(key(&[1])))
        },
    ] {
        constraints.insert(c.id, c);
    }
    Ctx {
        columns,
        constraints,
    }
}

fn fk(child: &[i32], parent: ConstraintId) -> Constraint {
    constraint(
        10,
        orders(),
        ConstraintKind::ForeignKey(ForeignKeySpec {
            child: key(child),
            parent_table: customer(),
            parent_constraint: parent,
            match_mode: MatchMode::Simple,
            on_delete: ReferentialAction::Restrict,
        }),
    )
}

fn err(c: &Constraint) -> InvalidConstraint {
    let e = c.validate(&ctx()).unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidConstraint);
    e
}

#[test]
fn primary_key_resolves_key_schema() {
    let c = constraint(9, customer(), ConstraintKind::PrimaryKey(key(&[1, 2])));
    let schema = c.validate(&ctx()).unwrap().unwrap();
    assert_eq!(schema.families(), [TypeFamily::Integer, TypeFamily::String]);
}

#[test]
fn float_and_nested_columns_cannot_be_keys() {
    let c = constraint(9, customer(), ConstraintKind::PrimaryKey(key(&[3])));
    assert_eq!(
        err(&c),
        InvalidConstraint::UnsupportedKeyType {
            field: FieldId(3),
            logical_type: LogicalType::Double
        }
    );
    let c = constraint(
        9,
        customer(),
        ConstraintKind::Unique(UniqueSpec {
            key: key(&[1, 5]),
            nulls: NullsMode::NotDistinct,
        }),
    );
    assert_eq!(
        err(&c),
        InvalidConstraint::UnsupportedKeyType {
            field: FieldId(5),
            logical_type: LogicalType::Nested
        }
    );
}

#[test]
fn malformed_keys_are_rejected() {
    let pk = |cols: &[i32]| constraint(9, customer(), ConstraintKind::PrimaryKey(key(cols)));
    assert_eq!(err(&pk(&[])), InvalidConstraint::EmptyKey);
    assert_eq!(
        err(&pk(&[1, 1])),
        InvalidConstraint::DuplicateColumn(FieldId(1))
    );
    assert_eq!(
        err(&pk(&[99])),
        InvalidConstraint::UnknownColumn(FieldId(99))
    );
    let unnamed = Constraint {
        name: " ".into(),
        ..pk(&[1])
    };
    assert_eq!(err(&unnamed), InvalidConstraint::EmptyName);
}

#[test]
fn not_null_accepts_any_existing_column() {
    let c = constraint(9, customer(), ConstraintKind::NotNull(FieldId(3)));
    assert_eq!(c.validate(&ctx()), Ok(None));
    let c = constraint(9, customer(), ConstraintKind::NotNull(FieldId(42)));
    assert_eq!(err(&c), InvalidConstraint::UnknownColumn(FieldId(42)));
}

#[test]
fn fk_int_child_references_long_parent() {
    let schema = fk(&[2], PK_CUSTOMER).validate(&ctx()).unwrap().unwrap();
    assert_eq!(schema.families(), [TypeFamily::Integer]);
}

#[test]
fn fk_decimal_scales_must_match() {
    assert_eq!(
        err(&fk(&[3], UQ_CUSTOMER_AMOUNT)),
        InvalidConstraint::FamilyMismatch {
            column: 0,
            child: TypeFamily::Decimal { scale: 3 },
            parent: TypeFamily::Decimal { scale: 2 },
        }
    );
    // Same scale, different precision: allowed.
    assert!(fk(&[4], UQ_CUSTOMER_AMOUNT).validate(&ctx()).is_ok());
}

#[test]
fn fk_type_and_arity_must_match_parent() {
    assert_eq!(
        err(&fk(&[1, 2], PK_CUSTOMER)),
        InvalidConstraint::ArityMismatch {
            child: 2,
            parent: 1
        }
    );
    assert_eq!(
        err(&fk(&[4], PK_CUSTOMER)),
        InvalidConstraint::FamilyMismatch {
            column: 0,
            child: TypeFamily::Decimal { scale: 2 },
            parent: TypeFamily::Integer,
        }
    );
}

#[test]
fn fk_parent_must_be_an_enforced_key_on_the_parent_table() {
    assert_eq!(
        err(&fk(&[2], ConstraintId(77))),
        InvalidConstraint::ParentConstraintNotFound(ConstraintId(77))
    );
    assert_eq!(
        err(&fk(&[2], NN_CUSTOMER_NAME)),
        InvalidConstraint::ParentNotAKey(NN_CUSTOMER_NAME)
    );
    assert_eq!(
        err(&fk(&[2], PK_CUSTOMER_DISABLED)),
        InvalidConstraint::ParentNotEnforced(PK_CUSTOMER_DISABLED)
    );

    let mut wrong_table = fk(&[2], PK_CUSTOMER);
    if let ConstraintKind::ForeignKey(spec) = &mut wrong_table.kind {
        spec.parent_table = TableId::new("elsewhere");
    }
    assert_eq!(err(&wrong_table), InvalidConstraint::ParentTableMismatch);
}

#[test]
fn self_referencing_fk_is_rejected_in_0_1() {
    let mut c = fk(&[1], PK_CUSTOMER);
    c.table = customer();
    assert_eq!(err(&c), InvalidConstraint::SelfReference);
}

#[test]
fn key_role_follows_kind() {
    use integrity_core::KeyRole;
    let c = fk(&[2], PK_CUSTOMER);
    assert_eq!(
        c.kind.key_role(),
        Some(KeyRole::ForeignKeyChild(MatchMode::Simple))
    );
    assert_eq!(c.kind.key(), Some(&key(&[2])));
    assert_eq!(ConstraintKind::NotNull(FieldId(1)).key_role(), None);
}
