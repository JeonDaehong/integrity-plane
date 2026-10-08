//! Cross-check: the oracle (direct value comparison) and `integrity-core` (§7 `classify` plus
//! key encoding) must agree on every spec §7 row for random schemas and tuples. They share no
//! verdict code, so agreement is evidence that both are right.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{
    Constraint, ConstraintKind, EncodedKey, EnforcementMode, ForeignKeySpec, KeyDisposition,
    KeyRole, KeySchema, KeySpec, MatchMode, NullsMode, ReferentialAction, UniqueSpec, classify,
};
use integrity_reference::strategies::{Tuple, complete_tuple, logical_type, schema, tuple};
use integrity_reference::{Commit, Datum, Oracle, Row, Verdict, Violation};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, TableId};
use proptest::prelude::*;

const KEY: ConstraintId = ConstraintId(1);
const FK: ConstraintId = ConstraintId(2);

fn all_columns(s: &KeySchema) -> KeySpec {
    KeySpec {
        columns: (1..=s.len() as i32).map(FieldId).collect(),
    }
}

fn table(db: &mut Oracle, name: &str, s: &KeySchema) {
    let cols = s
        .families()
        .iter()
        .enumerate()
        .map(|(i, &f)| (FieldId(i as i32 + 1), logical_type(f)))
        .collect::<BTreeMap<_, _>>();
    db.create_table(TableId::new(name), cols).unwrap();
}

fn constraint(id: ConstraintId, table: &str, kind: ConstraintKind) -> Constraint {
    Constraint {
        id,
        table: TableId::new(table),
        name: "c".into(),
        kind,
        mode: EnforcementMode::Enforced,
        version: ConstraintSetVersion(1),
    }
}

fn row(t: &Tuple) -> Row {
    t.iter()
        .enumerate()
        .map(|(i, v)| {
            let d = v.clone().map_or(Datum::Null, Datum::Value);
            (FieldId(i as i32 + 1), d)
        })
        .collect()
}

fn insert(db: &mut Oracle, table: &str, rows: &[&Tuple]) -> Verdict {
    db.commit(&Commit {
        table: TableId::new(table),
        added: rows.iter().map(|t| row(t)).collect(),
        removed: vec![],
    })
    .unwrap()
}

fn verdict(violations: BTreeSet<(ConstraintId, ErrorCode)>) -> Verdict {
    if violations.is_empty() {
        Verdict::Accepted
    } else {
        Verdict::Rejected(
            violations
                .into_iter()
                .map(|(constraint, code)| Violation { constraint, code })
                .collect(),
        )
    }
}

/// What core predicts for inserting rows `a` and `b` together under a PK/UNIQUE role.
fn predicted_key_verdict(role: KeyRole, s: &KeySchema, a: &Tuple, b: &Tuple) -> Verdict {
    let duplicate = match role {
        KeyRole::PrimaryKey => ErrorCode::DuplicatePrimaryKey,
        _ => ErrorCode::DuplicateUniqueKey,
    };
    let da = classify(role, s, a).unwrap();
    let db = classify(role, s, b).unwrap();
    let mut out = BTreeSet::new();
    for d in [&da, &db] {
        if let KeyDisposition::Violation(code) = d {
            out.insert((KEY, *code));
        }
    }
    if let (KeyDisposition::Key(x), KeyDisposition::Key(y)) = (&da, &db) {
        if x == y {
            out.insert((KEY, duplicate));
        }
    }
    verdict(out)
}

fn key_role_case(role: KeyRole, s: KeySchema, a: Tuple, b: Tuple) -> Result<(), TestCaseError> {
    let mut db = Oracle::new();
    table(&mut db, "t", &s);
    let kind = match role {
        KeyRole::PrimaryKey => ConstraintKind::PrimaryKey(all_columns(&s)),
        KeyRole::Unique(nulls) => ConstraintKind::Unique(UniqueSpec {
            key: all_columns(&s),
            nulls,
        }),
        KeyRole::ForeignKeyChild(_) => unreachable!(),
    };
    db.register(constraint(KEY, "t", kind)).unwrap();
    let expected = predicted_key_verdict(role, &s, &a, &b);
    prop_assert_eq!(insert(&mut db, "t", &[&a, &b]), expected);
    Ok(())
}

fn schema_and_two(present: f64) -> impl Strategy<Value = (KeySchema, Tuple, Tuple)> {
    schema().prop_flat_map(move |s| {
        let a = tuple(&s, present);
        let b = tuple(&s, present);
        (Just(s), a, b)
    })
}

/// The parent constraint an FK references. UNIQUE parents may hold NULLs in their keys,
/// which a partially NULL child must still never match under MATCH FULL.
#[derive(Debug, Clone, Copy)]
enum Parent {
    PrimaryKey,
    Unique(NullsMode),
}

fn parent_kind() -> impl Strategy<Value = Parent> {
    prop_oneof![
        Just(Parent::PrimaryKey),
        Just(Parent::Unique(NullsMode::Distinct)),
        Just(Parent::Unique(NullsMode::NotDistinct)),
    ]
}

/// A parent key `p`, and a child tuple that is either random or `p` with some cells NULLed,
/// so that matches, misses and partial NULLs are all common.
fn fk_case() -> impl Strategy<Value = (Parent, KeySchema, Tuple, Tuple)> {
    (parent_kind(), schema()).prop_flat_map(|(kind, s)| {
        let n = s.len();
        let p = match kind {
            Parent::PrimaryKey => complete_tuple(&s),
            Parent::Unique(_) => tuple(&s, 0.7),
        };
        let random_child = tuple(&s, 0.7);
        let mask = proptest::collection::vec(proptest::bool::weighted(0.3), n);
        (Just(kind), Just(s), p, random_child, mask, any::<bool>()).prop_map(
            |(kind, s, p, random, mask, from_parent)| {
                let child = if from_parent {
                    p.iter()
                        .zip(&mask)
                        .map(|(v, &null)| if null { None } else { v.clone() })
                        .collect()
                } else {
                    random
                };
                (kind, s, p, child)
            },
        )
    })
}

fn fk_role_case(
    mode: MatchMode,
    kind: Parent,
    s: KeySchema,
    parent: Tuple,
    child: Tuple,
) -> Result<(), TestCaseError> {
    let mut db = Oracle::new();
    table(&mut db, "parent", &s);
    table(&mut db, "child", &s);
    let parent_constraint = match kind {
        Parent::PrimaryKey => ConstraintKind::PrimaryKey(all_columns(&s)),
        Parent::Unique(nulls) => ConstraintKind::Unique(UniqueSpec {
            key: all_columns(&s),
            nulls,
        }),
    };
    db.register(constraint(KEY, "parent", parent_constraint))
        .unwrap();
    db.register(constraint(
        FK,
        "child",
        ConstraintKind::ForeignKey(ForeignKeySpec {
            child: all_columns(&s),
            parent_table: TableId::new("parent"),
            parent_constraint: KEY,
            match_mode: mode,
            on_delete: ReferentialAction::Restrict,
        }),
    ))
    .unwrap();
    prop_assert_eq!(insert(&mut db, "parent", &[&parent]), Verdict::Accepted);

    let parent_key = EncodedKey::encode(&s, &parent).unwrap();
    let (expected, references_parent) =
        match classify(KeyRole::ForeignKeyChild(mode), &s, &child).unwrap() {
            KeyDisposition::Exempt => (Verdict::Accepted, false),
            KeyDisposition::Violation(code) => (verdict([(FK, code)].into()), false),
            KeyDisposition::Key(k) if k == parent_key => (Verdict::Accepted, true),
            KeyDisposition::Key(_) => (
                verdict([(FK, ErrorCode::ForeignKeyViolation)].into()),
                false,
            ),
        };
    prop_assert_eq!(insert(&mut db, "child", &[&child]), expected.clone());

    // Deleting the parent must fail exactly when an accepted child references it.
    let delete = db
        .commit(&Commit {
            table: TableId::new("parent"),
            added: vec![],
            removed: vec![row(&parent)],
        })
        .unwrap();
    let expected_delete = if expected == Verdict::Accepted && references_parent {
        verdict([(FK, ErrorCode::ReferencedRowDelete)].into())
    } else {
        Verdict::Accepted
    };
    prop_assert_eq!(delete, expected_delete);
    Ok(())
}

proptest! {
    #[test]
    fn s7_primary_key((s, a, b) in schema_and_two(0.8)) {
        key_role_case(KeyRole::PrimaryKey, s, a, b)?;
    }

    #[test]
    fn s7_unique_nulls_distinct((s, a, b) in schema_and_two(0.7)) {
        key_role_case(KeyRole::Unique(NullsMode::Distinct), s, a, b)?;
    }

    #[test]
    fn s7_unique_nulls_not_distinct((s, a, b) in schema_and_two(0.7)) {
        key_role_case(KeyRole::Unique(NullsMode::NotDistinct), s, a, b)?;
    }

    #[test]
    fn s7_fk_match_simple((kind, s, p, c) in fk_case()) {
        fk_role_case(MatchMode::Simple, kind, s, p, c)?;
    }

    #[test]
    fn s7_fk_match_full((kind, s, p, c) in fk_case()) {
        fk_role_case(MatchMode::Full, kind, s, p, c)?;
    }
}
