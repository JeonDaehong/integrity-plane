//! RFC 0002 certificate format v1. The golden vectors were produced by an independent
//! implementation written from the RFC text (Python + BLAKE3), so a match shows the RFC is
//! precise enough to reimplement.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeMap;

use integrity_core::{
    CertificateInput, Constraint, ConstraintKind, Digest, EncodedKey, EnforcementMode,
    ForeignKeySpec, KeyDelta, KeyMultiset, KeySchema, KeySpec, KeyValue, MatchMode, NetDelta,
    NullsMode, ReferentialAction, TypeFamily, UniqueSpec, certificate, constraint_set_digest,
    key_delta_digest, parse_table_uuid,
};
use integrity_types::{ConstraintId, ConstraintSetVersion, FieldId, SnapshotId, TableId};

const TABLE: &str = "6c1b2d42-0000-4000-8000-000000000001";
const GOLDEN_CONSTRAINT_SET: &str =
    "beefdb9fb8d82373d5d96d38044197d1317b589966b30184668eeed356c368cb";
const GOLDEN_KEY_DELTA: &str = "e27cb3961b604010c23c1159871d47a9198c98fa9a33196236b226e969b12ea1";
const GOLDEN_CERT_30: &str = "71332e1024dd5f0391672d18081bb5f2d6f71244e2c134b94af8b52e1d702991";
const GOLDEN_CERT_31: &str = "25a91bf4b865418097beb225e5d524bed77f23109e0643066f662d0529d2f40a";
const GOLDEN_CERT_ROOT: &str = "b9e4336facdb1a13838dc1707b48471703600c14a494240872eadc399bf9e22a";

fn key(fields: &[i32]) -> KeySpec {
    KeySpec {
        columns: fields.iter().map(|&f| FieldId(f)).collect(),
    }
}

fn c(id: u64, table: &str, name: &str, kind: ConstraintKind, mode: EnforcementMode) -> Constraint {
    Constraint {
        id: ConstraintId(id),
        table: TableId::new(table),
        name: name.into(),
        kind,
        mode,
        version: ConstraintSetVersion(1),
    }
}

fn constraints() -> Vec<Constraint> {
    use EnforcementMode::*;
    vec![
        c(
            4,
            TABLE,
            "nn",
            ConstraintKind::NotNull(FieldId(4)),
            Enforced,
        ),
        c(
            1,
            TABLE,
            "pk",
            ConstraintKind::PrimaryKey(key(&[1])),
            Enforced,
        ),
        c(
            5,
            TABLE,
            "off",
            ConstraintKind::PrimaryKey(key(&[9])),
            Disabled,
        ),
        c(
            3,
            "child-table",
            "fk",
            ConstraintKind::ForeignKey(ForeignKeySpec {
                child: key(&[2]),
                parent_table: TableId::new(TABLE),
                parent_constraint: ConstraintId(1),
                match_mode: MatchMode::Full,
                on_delete: ReferentialAction::Restrict,
            }),
            Enforced,
        ),
        c(
            2,
            TABLE,
            "uq",
            ConstraintKind::Unique(UniqueSpec {
                key: key(&[2, 3]),
                nulls: NullsMode::NotDistinct,
            }),
            Enforced,
        ),
    ]
}

fn set_digest(cs: &[Constraint], version: u64) -> Digest {
    let refs: Vec<&Constraint> = cs.iter().collect();
    constraint_set_digest(ConstraintSetVersion(version), &refs).unwrap()
}

fn int_key(v: i64) -> EncodedKey {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap()
}

fn net(added: &[i64], removed: &[i64]) -> NetDelta {
    KeyDelta {
        added: KeyMultiset::from_keys(added.iter().map(|&v| int_key(v))).unwrap(),
        removed: KeyMultiset::from_keys(removed.iter().map(|&v| int_key(v))).unwrap(),
    }
    .net()
}

fn deltas() -> BTreeMap<ConstraintId, NetDelta> {
    [
        (ConstraintId(2), net(&[], &[])),
        (ConstraintId(1), net(&[1, 3], &[2, 3])),
    ]
    .into()
}

fn input(snapshot: i64, parent: Option<i64>, previous: Digest) -> CertificateInput {
    CertificateInput {
        // Upper case on purpose: the UUID is parsed, not hashed as text.
        table_uuid: parse_table_uuid(&TABLE.to_uppercase()).unwrap(),
        snapshot: SnapshotId(snapshot),
        parent: parent.map(SnapshotId),
        constraint_set: set_digest(&constraints(), 7),
        key_delta: key_delta_digest(&deltas()).unwrap(),
        previous,
    }
}

#[test]
fn golden_vectors_match_an_independent_implementation() {
    assert_eq!(
        set_digest(&constraints(), 7).to_hex(),
        GOLDEN_CONSTRAINT_SET
    );
    assert_eq!(
        key_delta_digest(&deltas()).unwrap().to_hex(),
        GOLDEN_KEY_DELTA
    );
    let c30 = certificate(&input(30, Some(20), Digest::ZERO));
    assert_eq!(c30.to_hex(), GOLDEN_CERT_30);
    let c31 = certificate(&input(31, Some(30), c30));
    assert_eq!(c31.to_hex(), GOLDEN_CERT_31);
    assert_eq!(
        certificate(&input(1, None, Digest::ZERO)).to_hex(),
        GOLDEN_CERT_ROOT
    );
}

#[test]
fn constraint_set_ignores_order_names_and_disabled_constraints() {
    let base = set_digest(&constraints(), 7);
    let mut reversed = constraints();
    reversed.reverse();
    assert_eq!(set_digest(&reversed, 7), base);
    let renamed: Vec<_> = constraints()
        .into_iter()
        .map(|mut c| {
            c.name = "x".into();
            c
        })
        .collect();
    assert_eq!(set_digest(&renamed, 7), base);
    let without_disabled: Vec<_> = constraints()
        .into_iter()
        .filter(|c| c.id != ConstraintId(5))
        .collect();
    assert_eq!(set_digest(&without_disabled, 7), base);
}

#[test]
fn constraint_set_changes_with_semantics() {
    let base = set_digest(&constraints(), 7);
    assert_ne!(set_digest(&constraints(), 8), base, "version");
    let mut cs = constraints();
    cs[4].kind = ConstraintKind::Unique(UniqueSpec {
        key: key(&[2, 3]),
        nulls: NullsMode::Distinct,
    });
    assert_ne!(set_digest(&cs, 7), base, "nulls mode");
    let mut cs = constraints();
    cs[1].kind = ConstraintKind::PrimaryKey(key(&[1, 2]));
    assert_ne!(set_digest(&cs, 7), base, "key columns");
    let mut cs = constraints();
    cs[2].mode = EnforcementMode::Enforced;
    assert_ne!(set_digest(&cs, 7), base, "enabling a constraint");
    let mut cs = constraints();
    if let ConstraintKind::ForeignKey(fk) = &mut cs[3].kind {
        fk.match_mode = MatchMode::Simple;
    }
    assert_ne!(set_digest(&cs, 7), base, "match mode");
}

#[test]
fn key_delta_digest_covers_constraints_and_changes() {
    let base = key_delta_digest(&deltas()).unwrap();
    // A constraint with an empty delta still counts.
    let mut fewer = deltas();
    fewer.remove(&ConstraintId(2));
    assert_ne!(key_delta_digest(&fewer).unwrap(), base);
    // Same net change from different gross changes: same digest.
    let mut regrossed = deltas();
    regrossed.insert(ConstraintId(1), net(&[1, 5], &[2, 5]));
    assert_eq!(key_delta_digest(&regrossed).unwrap(), base);
    let mut other = deltas();
    other.insert(ConstraintId(1), net(&[1], &[]));
    assert_ne!(key_delta_digest(&other).unwrap(), base);
}

#[test]
fn every_certificate_input_matters() {
    let base = certificate(&input(30, Some(20), Digest::ZERO));
    let mut i = input(30, Some(20), Digest::ZERO);
    i.snapshot = SnapshotId(31);
    assert_ne!(certificate(&i), base);
    let mut i = input(30, Some(20), Digest::ZERO);
    i.parent = None;
    assert_ne!(certificate(&i), base);
    let mut i = input(30, Some(20), Digest::ZERO);
    i.parent = Some(SnapshotId(0));
    assert_ne!(certificate(&i), base, "parent 0 differs from no parent");
    let mut i = input(30, Some(20), Digest::ZERO);
    i.table_uuid[15] ^= 1;
    assert_ne!(certificate(&i), base);
    let mut i = input(30, Some(20), Digest::ZERO);
    i.previous = Digest([1; 32]);
    assert_ne!(certificate(&i), base);
}

#[test]
fn hex_and_uuid_parsing() {
    let d = Digest::from_hex(GOLDEN_CERT_30).unwrap();
    assert_eq!(d.to_hex(), GOLDEN_CERT_30);
    assert_eq!(Digest::from_hex(&GOLDEN_CERT_30.to_uppercase()).unwrap(), d);
    for bad in [
        "",
        "00",
        &GOLDEN_CERT_30[1..],
        &format!("{}g", &GOLDEN_CERT_30[1..]),
    ] {
        assert!(Digest::from_hex(bad).is_err(), "{bad}");
    }
    assert_eq!(
        parse_table_uuid(TABLE).unwrap(),
        parse_table_uuid(&TABLE.to_uppercase()).unwrap()
    );
    for bad in [
        "",
        "6c1b2d42000040008000000000000001",
        "6c1b2d42-0000-4000-8000-00000000000g",
        "6c1b2d4-20000-4000-8000-000000000001",
    ] {
        assert!(parse_table_uuid(bad).is_err(), "{bad}");
    }
}
