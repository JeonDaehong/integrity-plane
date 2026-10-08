//! Spec §7 (normative NULL semantics): one unit test and one property test per table row.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use common::*;
use integrity_core::{
    EncodedKey, KeyDisposition, KeyRole, KeySchema, KeyValue, MatchMode, NullsMode, TypeFamily,
    classify,
};
use integrity_types::ErrorCode;
use proptest::prelude::*;

const PK: KeyRole = KeyRole::PrimaryKey;
const UNIQUE_DISTINCT: KeyRole = KeyRole::Unique(NullsMode::Distinct);
const UNIQUE_NOT_DISTINCT: KeyRole = KeyRole::Unique(NullsMode::NotDistinct);
const FK_SIMPLE: KeyRole = KeyRole::ForeignKeyChild(MatchMode::Simple);
const FK_FULL: KeyRole = KeyRole::ForeignKeyChild(MatchMode::Full);

fn pair() -> KeySchema {
    KeySchema::new(vec![TypeFamily::Integer, TypeFamily::String]).unwrap()
}

fn int(v: i64) -> Option<KeyValue> {
    Some(KeyValue::Integer(v))
}

fn text(v: &str) -> Option<KeyValue> {
    Some(KeyValue::String(v.into()))
}

fn key(t: &[Option<KeyValue>]) -> KeyDisposition {
    KeyDisposition::Key(EncodedKey::encode(&pair(), t).unwrap())
}

fn run(role: KeyRole, t: &[Option<KeyValue>]) -> KeyDisposition {
    classify(role, &pair(), t).unwrap()
}

// ---------- unit tests, one per §7 row ----------

#[test]
fn primary_key_any_null_is_not_null_violation() {
    let violation = KeyDisposition::Violation(ErrorCode::NotNullViolation);
    assert_eq!(run(PK, &[int(1), None]), violation);
    assert_eq!(run(PK, &[None, text("a")]), violation);
    assert_eq!(run(PK, &[None, None]), violation);
    assert_eq!(run(PK, &[int(1), text("a")]), key(&[int(1), text("a")]));
}

#[test]
fn unique_nulls_distinct_null_tuples_are_exempt() {
    assert_eq!(
        run(UNIQUE_DISTINCT, &[int(1), None]),
        KeyDisposition::Exempt
    );
    assert_eq!(run(UNIQUE_DISTINCT, &[None, None]), KeyDisposition::Exempt);
    assert_eq!(
        run(UNIQUE_DISTINCT, &[int(1), text("a")]),
        key(&[int(1), text("a")])
    );
}

#[test]
fn unique_nulls_not_distinct_indexes_null_and_null_equals_null() {
    let a = run(UNIQUE_NOT_DISTINCT, &[int(1), None]);
    let b = run(UNIQUE_NOT_DISTINCT, &[int(1), None]);
    assert_eq!(a, key(&[int(1), None]));
    assert_eq!(a, b, "two (1, NULL) tuples must conflict");
    assert_ne!(a, run(UNIQUE_NOT_DISTINCT, &[None, text("")]));
    assert_eq!(run(UNIQUE_NOT_DISTINCT, &[None, None]), key(&[None, None]));
}

#[test]
fn fk_match_simple_any_null_needs_no_parent() {
    assert_eq!(run(FK_SIMPLE, &[int(1), None]), KeyDisposition::Exempt);
    assert_eq!(run(FK_SIMPLE, &[None, None]), KeyDisposition::Exempt);
    assert_eq!(
        run(FK_SIMPLE, &[int(1), text("a")]),
        key(&[int(1), text("a")])
    );
}

#[test]
fn fk_match_full_all_null_exempt_partial_null_violation() {
    assert_eq!(run(FK_FULL, &[None, None]), KeyDisposition::Exempt);
    assert_eq!(
        run(FK_FULL, &[int(1), None]),
        KeyDisposition::Violation(ErrorCode::ForeignKeyViolation)
    );
    assert_eq!(
        run(FK_FULL, &[None, text("a")]),
        KeyDisposition::Violation(ErrorCode::ForeignKeyViolation)
    );
    assert_eq!(
        run(FK_FULL, &[int(1), text("a")]),
        key(&[int(1), text("a")])
    );
}

#[test]
fn single_column_fk_full_and_simple_agree() {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    for role in [FK_SIMPLE, FK_FULL] {
        assert_eq!(classify(role, &s, &[None]).unwrap(), KeyDisposition::Exempt);
    }
}

#[test]
fn exempt_tuples_are_still_type_checked() {
    let bad = [Some(KeyValue::Boolean(true)), None];
    for role in [PK, UNIQUE_DISTINCT, UNIQUE_NOT_DISTINCT, FK_SIMPLE, FK_FULL] {
        assert!(classify(role, &pair(), &bad).is_err(), "{role:?}");
    }
}

// ---------- property tests, one per §7 row ----------

fn nulls(t: &[Option<KeyValue>]) -> (bool, bool) {
    let n = t.iter().filter(|v| v.is_none()).count();
    (n > 0, n == t.len())
}

proptest! {
    #[test]
    fn prop_primary_key((s, t) in schema_and_tuple(0.6)) {
        let (any_null, _) = nulls(&t);
        let d = classify(PK, &s, &t).unwrap();
        if any_null {
            prop_assert_eq!(d, KeyDisposition::Violation(ErrorCode::NotNullViolation));
        } else {
            prop_assert_eq!(d, KeyDisposition::Key(encode(&s, &t)));
        }
    }

    #[test]
    fn prop_unique_nulls_distinct((s, a, b) in schema_and_two_tuples(0.6)) {
        let (any_null, _) = nulls(&a);
        let da = classify(UNIQUE_DISTINCT, &s, &a).unwrap();
        if any_null {
            prop_assert_eq!(&da, &KeyDisposition::Exempt);
        } else {
            prop_assert_eq!(&da, &KeyDisposition::Key(encode(&s, &a)));
        }
        // Two tuples conflict only if both are indexed and equal, never through a NULL.
        let db = classify(UNIQUE_DISTINCT, &s, &b).unwrap();
        let conflict = matches!((&da, &db), (KeyDisposition::Key(x), KeyDisposition::Key(y)) if x == y);
        prop_assert_eq!(conflict, a == b && !any_null);
    }

    #[test]
    fn prop_unique_nulls_not_distinct((s, a, b) in schema_and_two_tuples(0.6)) {
        let da = classify(UNIQUE_NOT_DISTINCT, &s, &a).unwrap();
        let db = classify(UNIQUE_NOT_DISTINCT, &s, &b).unwrap();
        prop_assert_eq!(&da, &KeyDisposition::Key(encode(&s, &a)));
        // NULL equals NULL: tuples conflict exactly when they are equal, NULLs included.
        prop_assert_eq!(da == db, a == b);
    }

    #[test]
    fn prop_fk_match_simple((s, t) in schema_and_tuple(0.6)) {
        let (any_null, _) = nulls(&t);
        let d = classify(FK_SIMPLE, &s, &t).unwrap();
        if any_null {
            prop_assert_eq!(d, KeyDisposition::Exempt);
        } else {
            prop_assert_eq!(d, KeyDisposition::Key(encode(&s, &t)));
        }
    }

    #[test]
    fn prop_fk_match_full((s, t) in schema_and_tuple(0.5)) {
        let (any_null, all_null) = nulls(&t);
        let d = classify(FK_FULL, &s, &t).unwrap();
        if all_null {
            prop_assert_eq!(d, KeyDisposition::Exempt);
        } else if any_null {
            prop_assert_eq!(d, KeyDisposition::Violation(ErrorCode::ForeignKeyViolation));
        } else {
            prop_assert_eq!(d, KeyDisposition::Key(encode(&s, &t)));
        }
    }
}
