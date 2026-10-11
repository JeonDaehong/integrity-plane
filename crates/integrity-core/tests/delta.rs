//! Spec §8: key delta multisets. Oracle: plain per-key counts in a `BTreeMap`.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeMap;

use integrity_core::{
    DeltaError, EncodedKey, KeyDelta, KeyMultiset, KeySchema, KeyValue, NetDelta, TypeFamily,
};
use proptest::collection::vec;
use proptest::prelude::*;

fn k(v: i64) -> EncodedKey {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap()
}

fn set(keys: &[i64]) -> KeyMultiset {
    KeyMultiset::from_keys(keys.iter().map(|&v| k(v))).unwrap()
}

#[test]
fn copy_on_write_rewrite_nets_to_unchanged() {
    let d = KeyDelta {
        added: set(&[1, 2, 3]),
        removed: set(&[1, 2, 3]),
    };
    assert!(d.net().is_empty());
}

#[test]
fn intra_commit_duplicate_is_visible_even_when_net_change_is_one() {
    // Spec §8 warning: added {k, k}, removed {k} nets to +1, but the post-commit
    // state holds k twice. Multiplicities of `added` must be checked first.
    let d = KeyDelta {
        added: set(&[7, 7]),
        removed: set(&[7]),
    };
    assert_eq!(d.net().get(&k(7)), 1);
    let dups: Vec<_> = d.added.duplicates().collect();
    assert_eq!(dups, vec![(&k(7), 2)]);
}

#[test]
fn net_splits_into_increases_and_decreases() {
    let d = KeyDelta {
        added: set(&[1, 2, 2]),
        removed: set(&[2, 3]),
    };
    let net = d.net();
    assert_eq!(
        net.increases().collect::<Vec<_>>(),
        vec![(&k(1), 1), (&k(2), 1)]
    );
    assert_eq!(net.decreases().collect::<Vec<_>>(), vec![(&k(3), -1)]);
}

#[test]
fn apply_is_atomic_on_underflow() {
    let mut m = set(&[1]);
    let before = m.clone();
    // Adds 2 but removes 3, which is absent: nothing may change.
    let d = KeyDelta {
        added: set(&[2]),
        removed: set(&[3]),
    };
    assert_eq!(m.apply(&d.net()), Err(DeltaError::Underflow));
    assert_eq!(m, before);
}

#[test]
fn apply_reports_overflow() {
    let mut m = KeyMultiset::new();
    m.insert_n(k(1), u64::MAX).unwrap();
    assert_eq!(m.insert(k(1)), Err(DeltaError::Overflow));
    let d = KeyDelta {
        added: set(&[1]),
        removed: KeyMultiset::new(),
    };
    assert_eq!(m.apply(&d.net()), Err(DeltaError::Overflow));
    assert_eq!(m.count(&k(1)), u64::MAX);
}

#[test]
fn apply_drops_zero_counts() {
    let mut m = set(&[1]);
    m.apply(
        &KeyDelta {
            added: KeyMultiset::new(),
            removed: set(&[1]),
        }
        .net(),
    )
    .unwrap();
    assert!(m.is_empty());
    assert_eq!(m, KeyMultiset::new());
}

// ---------- properties ----------

/// Small key domain so that added, removed and the base state overlap.
fn keys() -> impl Strategy<Value = Vec<i64>> {
    vec(0i64..6, 0..12)
}

fn counts(keys: &[i64]) -> BTreeMap<i64, i128> {
    let mut m = BTreeMap::new();
    for &v in keys {
        *m.entry(v).or_insert(0) += 1;
    }
    m
}

proptest! {
    #[test]
    fn net_is_added_minus_removed(added in keys(), removed in keys()) {
        let d = KeyDelta { added: set(&added), removed: set(&removed) };
        let net = d.net();
        let (a, r) = (counts(&added), counts(&removed));
        for v in 0..6 {
            let expected = a.get(&v).copied().unwrap_or(0) - r.get(&v).copied().unwrap_or(0);
            prop_assert_eq!(net.get(&k(v)), expected);
        }
        prop_assert!(net.iter().all(|(_, c)| c != 0));
    }

    #[test]
    fn net_of_identical_sides_is_empty(x in keys()) {
        let net = KeyDelta { added: set(&x), removed: set(&x) }.net();
        prop_assert!(net.is_empty());
    }

    #[test]
    fn duplicates_are_exactly_multiplicities_above_one(x in keys()) {
        let m = set(&x);
        let expected: Vec<_> = counts(&x)
            .into_iter()
            .filter(|&(_, n)| n > 1)
            .map(|(v, n)| (k(v), n as u64))
            .collect();
        let actual: Vec<_> = m.duplicates().map(|(key, n)| (key.clone(), n)).collect();
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn apply_matches_oracle(base in keys(), added in keys(), removed in keys()) {
        let mut m = set(&base);
        let before = m.clone();
        let result = m.apply(&KeyDelta { added: set(&added), removed: set(&removed) }.net());

        let mut oracle = counts(&base);
        for (v, n) in counts(&added) { *oracle.entry(v).or_insert(0) += n; }
        for (v, n) in counts(&removed) { *oracle.entry(v).or_insert(0) -= n; }

        if oracle.values().any(|&n| n < 0) {
            prop_assert_eq!(result, Err(DeltaError::Underflow));
            prop_assert_eq!(m, before);
        } else {
            prop_assert_eq!(result, Ok(()));
            for v in 0..6 {
                prop_assert_eq!(i128::from(m.count(&k(v))), oracle.get(&v).copied().unwrap_or(0));
            }
            prop_assert_eq!(m.distinct_len(), oracle.values().filter(|&&n| n > 0).count());
        }
    }

    #[test]
    fn apply_then_negate_is_identity(base in keys(), added in keys(), removed in keys()) {
        let mut m = set(&base);
        let before = m.clone();
        let d = KeyDelta { added: set(&added), removed: set(&removed) }.net();
        if m.apply(&d).is_ok() {
            m.apply(&d.negate()).unwrap();
            prop_assert_eq!(m, before);
        }
    }
}

#[test]
fn from_sorted_takes_only_strictly_ordered_nonzero_changes() {
    let d = NetDelta::from_sorted(vec![(k(1), 2), (k(3), -1)]).unwrap();
    assert_eq!((d.get(&k(1)), d.get(&k(2)), d.get(&k(3))), (2, 0, -1));
    assert_eq!(d.len(), 2);
    assert!(NetDelta::from_sorted(Vec::new()).unwrap().is_empty());
    for bad in [
        vec![(k(3), 1), (k(1), 1)],
        vec![(k(1), 1), (k(1), 1)],
        vec![(k(1), 1), (k(2), 0)],
    ] {
        assert_eq!(NetDelta::from_sorted(bad), Err(DeltaError::NotSorted));
    }
}

fn changes() -> impl Strategy<Value = BTreeMap<i64, i128>> {
    proptest::collection::btree_map(0i64..8, -3i128..4, 0..8)
}

fn net_of(m: &BTreeMap<i64, i128>) -> NetDelta {
    NetDelta::from_sorted(
        m.iter()
            .filter(|&(_, &c)| c != 0)
            .map(|(&v, &c)| (k(v), c))
            .collect(),
    )
    .unwrap()
}

proptest! {
    #[test]
    fn from_sorted_equals_the_net_of_the_same_counts(added in keys(), removed in keys()) {
        let d = KeyDelta { added: set(&added), removed: set(&removed) }.net();
        let rebuilt = NetDelta::from_sorted(d.iter().map(|(key, c)| (key.clone(), c)).collect())
            .unwrap();
        prop_assert_eq!(rebuilt, d);
    }

    #[test]
    fn plus_adds_changes_key_by_key(a in changes(), b in changes()) {
        let sum = net_of(&a).plus(&net_of(&b)).unwrap();
        let mut oracle = a.clone();
        for (v, c) in &b {
            *oracle.entry(*v).or_insert(0) += c;
        }
        prop_assert_eq!(sum, net_of(&oracle));
    }
}
