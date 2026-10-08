//! `MemoryIndex` against a plain-map model (spec §13).

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeMap;

use integrity_core::{EncodedKey, KeyDelta, KeyMultiset, KeySchema, KeyValue, TypeFamily};
use integrity_index::{
    IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, MemoryIndex,
};
use integrity_types::SnapshotId;
use proptest::collection::vec;
use proptest::prelude::*;

fn k(v: i64) -> EncodedKey {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap()
}

/// A delta from `(key, signed change)` pairs.
fn delta(snapshot: i64, changes: &[(i64, i64)]) -> IndexDelta {
    let mut d = KeyDelta::default();
    for &(key, n) in changes {
        let side = if n > 0 { &mut d.added } else { &mut d.removed };
        side.insert_n(k(key), n.unsigned_abs()).unwrap();
    }
    IndexDelta {
        snapshot: SnapshotId(snapshot),
        changes: d.net(),
    }
}

fn commit(index: &MemoryIndex, d: &IndexDelta) -> Result<(), IndexError> {
    let staged = index.stage(d)?;
    let next = IndexEpoch(index.epoch()?.0 + 1);
    index.apply(staged, next)
}

fn unique(snapshot: i64) -> Option<IndexValue> {
    Some(IndexValue::Unique {
        last_snapshot: SnapshotId(snapshot),
    })
}

fn refs(child_count: u64) -> Option<IndexValue> {
    Some(IndexValue::Reference { child_count })
}

#[test]
fn unique_insert_lookup_remove() {
    let index = MemoryIndex::new(IndexKind::Unique);
    commit(&index, &delta(10, &[(1, 1), (2, 1)])).unwrap();
    assert_eq!(
        index.get_many(&[k(1), k(2), k(3)]).unwrap(),
        vec![unique(10), unique(10), None]
    );
    commit(&index, &delta(11, &[(1, -1)])).unwrap();
    assert_eq!(
        index.get_many(&[k(1), k(2)]).unwrap(),
        vec![None, unique(10)]
    );
    assert_eq!(index.epoch().unwrap(), IndexEpoch(2));
}

#[test]
fn unique_rejects_inconsistent_deltas() {
    let index = MemoryIndex::new(IndexKind::Unique);
    commit(&index, &delta(1, &[(1, 1)])).unwrap();
    assert_eq!(
        index.stage(&delta(2, &[(1, 1)])),
        Err(IndexError::KeyAlreadyPresent)
    );
    assert_eq!(
        index.stage(&delta(2, &[(5, -1)])),
        Err(IndexError::KeyAbsent)
    );
    assert_eq!(
        index.stage(&delta(2, &[(5, 2)])),
        Err(IndexError::InvalidMultiplicity(2))
    );
}

#[test]
fn reference_counts_and_drops_zero() {
    let index = MemoryIndex::new(IndexKind::Reference);
    commit(&index, &delta(1, &[(1, 3), (2, 1)])).unwrap();
    assert_eq!(
        index.get_many(&[k(1), k(2)]).unwrap(),
        vec![refs(3), refs(1)]
    );
    commit(&index, &delta(2, &[(1, -1), (2, -1)])).unwrap();
    assert_eq!(index.get_many(&[k(1), k(2)]).unwrap(), vec![refs(2), None]);
    assert_eq!(index.entries().unwrap().len(), 1);
    assert_eq!(
        index.stage(&delta(3, &[(1, -3)])),
        Err(IndexError::KeyAbsent)
    );
}

#[test]
fn stage_has_no_visible_effect() {
    let index = MemoryIndex::new(IndexKind::Unique);
    let staged = index.stage(&delta(1, &[(1, 1)])).unwrap();
    assert_eq!(staged.base_epoch(), IndexEpoch(0));
    assert_eq!(index.get_many(&[k(1)]).unwrap(), vec![None]);
    assert_eq!(index.epoch().unwrap(), IndexEpoch(0));
}

#[test]
fn apply_is_idempotent_for_the_same_staged_delta_and_epoch() {
    let index = MemoryIndex::new(IndexKind::Reference);
    let staged = index.stage(&delta(1, &[(1, 1)])).unwrap();
    index.apply(staged.clone(), IndexEpoch(1)).unwrap();
    index.apply(staged.clone(), IndexEpoch(1)).unwrap();
    assert_eq!(
        index.get_many(&[k(1)]).unwrap(),
        vec![refs(1)],
        "replay must not double-count"
    );
    assert_eq!(index.epoch().unwrap(), IndexEpoch(1));
}

#[test]
fn replay_with_different_content_or_old_epoch_is_rejected() {
    let index = MemoryIndex::new(IndexKind::Reference);
    let a = index.stage(&delta(1, &[(1, 1)])).unwrap();
    let b = index.stage(&delta(1, &[(2, 1)])).unwrap();
    index.apply(a.clone(), IndexEpoch(5)).unwrap();
    let conflict = Err(IndexError::EpochConflict {
        requested: IndexEpoch(5),
        current: IndexEpoch(5),
    });
    assert_eq!(index.apply(b, IndexEpoch(5)), conflict);
    assert_eq!(
        index.apply(a, IndexEpoch(4)),
        Err(IndexError::EpochConflict {
            requested: IndexEpoch(4),
            current: IndexEpoch(5)
        })
    );
}

#[test]
fn stale_stage_is_rejected() {
    let index = MemoryIndex::new(IndexKind::Unique);
    let first = index.stage(&delta(1, &[(1, 1)])).unwrap();
    let second = index.stage(&delta(1, &[(1, 1)])).unwrap();
    index.apply(first, IndexEpoch(1)).unwrap();
    // `second` was resolved before key 1 existed; applying it would break uniqueness.
    assert_eq!(
        index.apply(second, IndexEpoch(2)),
        Err(IndexError::StaleStage {
            staged_at: IndexEpoch(0),
            current: IndexEpoch(1)
        })
    );
    assert_eq!(index.get_many(&[k(1)]).unwrap(), vec![unique(1)]);
}

#[test]
fn errors_do_not_print_keys() {
    let e = IndexError::KeyAlreadyPresent.to_string();
    assert!(!e.contains("EncodedKey"));
}

// ---------- model-based property ----------

fn changes() -> impl Strategy<Value = Vec<(i64, i64)>> {
    vec((0i64..5, -2i64..=2), 0..6)
}

/// Sums duplicate keys the way `KeyDelta::net` does.
fn net(changes: &[(i64, i64)]) -> BTreeMap<i64, i64> {
    let mut m = BTreeMap::new();
    for &(key, n) in changes {
        *m.entry(key).or_insert(0) += n;
    }
    m.retain(|_, n| *n != 0);
    m
}

proptest! {
    #[test]
    fn matches_model(kind_is_unique in any::<bool>(), steps in vec(changes(), 1..20)) {
        let kind = if kind_is_unique { IndexKind::Unique } else { IndexKind::Reference };
        let index = MemoryIndex::new(kind);
        // Model: key -> (count, snapshot that last inserted it).
        let mut model: BTreeMap<i64, (u64, i64)> = BTreeMap::new();

        for (step, ch) in steps.iter().enumerate() {
            let snapshot = step as i64 + 100;
            let d = delta(snapshot, ch);
            let before = index.entries().unwrap();
            let epoch_before = index.epoch().unwrap();

            let mut next = model.clone();
            let mut valid = true;
            for (key, n) in net(ch) {
                let (count, snap) = next.get(&key).copied().unwrap_or((0, 0));
                let new = count as i64 + n;
                if new < 0 || (kind == IndexKind::Unique && n.abs() != 1) {
                    valid = false;
                    break;
                }
                let snap = if n > 0 { snapshot } else { snap };
                if new == 0 { next.remove(&key); } else { next.insert(key, (new as u64, snap)); }
            }
            if kind == IndexKind::Unique && next.values().any(|&(c, _)| c > 1) {
                valid = false;
            }

            let result = commit(&index, &d);
            prop_assert_eq!(result.is_ok(), valid, "step {} {:?}", step, ch);
            if valid {
                model = next;
                prop_assert_eq!(index.epoch().unwrap(), IndexEpoch(epoch_before.0 + 1));
            } else {
                prop_assert_eq!(index.entries().unwrap(), before);
                prop_assert_eq!(index.epoch().unwrap(), epoch_before);
            }

            let expected: Vec<_> = model
                .iter()
                .map(|(&key, &(count, snap))| {
                    let value = match kind {
                        IndexKind::Unique => IndexValue::Unique { last_snapshot: SnapshotId(snap) },
                        IndexKind::Reference => IndexValue::Reference { child_count: count },
                    };
                    (k(key), value)
                })
                .collect();
            prop_assert_eq!(index.entries().unwrap(), expected);
        }
    }
}

#[test]
fn multiset_helper_sanity() {
    // The test helper must produce the same net delta as building multisets directly.
    let mut added = KeyMultiset::new();
    added.insert_n(k(1), 2).unwrap();
    let expected = KeyDelta {
        added,
        removed: KeyMultiset::new(),
    }
    .net();
    assert_eq!(delta(0, &[(1, 2)]).changes, expected);
}
