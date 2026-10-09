//! `KeyIndex` conformance (spec §13, ADR 0003): every backend must behave identically.
//! Unit tests run against every backend; the model-based property runs against each.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use std::collections::BTreeMap;

use common::*;
use integrity_core::{KeyDelta, KeyMultiset};
use integrity_index::{IndexEpoch, IndexError, IndexKind, IndexValue};
use integrity_types::SnapshotId;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::Config;

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
    for b in BACKENDS {
        let index = make(b, IndexKind::Unique);
        commit(&*index, &delta(10, &[(1, 1), (2, 1)])).unwrap();
        assert_eq!(
            index.get_many(&[k(1), k(2), k(3)]).unwrap(),
            vec![unique(10), unique(10), None],
            "{b:?}"
        );
        commit(&*index, &delta(11, &[(1, -1)])).unwrap();
        assert_eq!(
            index.get_many(&[k(1), k(2)]).unwrap(),
            vec![None, unique(10)],
            "{b:?}"
        );
        assert_eq!(index.epoch().unwrap(), IndexEpoch(2), "{b:?}");
    }
}

#[test]
fn unique_rejects_inconsistent_deltas() {
    for b in BACKENDS {
        let index = make(b, IndexKind::Unique);
        commit(&*index, &delta(1, &[(1, 1)])).unwrap();
        assert_eq!(
            index.stage(&delta(2, &[(1, 1)])),
            Err(IndexError::KeyAlreadyPresent),
            "{b:?}"
        );
        assert_eq!(
            index.stage(&delta(2, &[(5, -1)])),
            Err(IndexError::KeyAbsent),
            "{b:?}"
        );
        assert_eq!(
            index.stage(&delta(2, &[(5, 2)])),
            Err(IndexError::InvalidMultiplicity(2)),
            "{b:?}"
        );
    }
}

#[test]
fn reference_counts_and_drops_zero() {
    for b in BACKENDS {
        let index = make(b, IndexKind::Reference);
        commit(&*index, &delta(1, &[(1, 3), (2, 1)])).unwrap();
        assert_eq!(
            index.get_many(&[k(1), k(2)]).unwrap(),
            vec![refs(3), refs(1)],
            "{b:?}"
        );
        commit(&*index, &delta(2, &[(1, -1), (2, -1)])).unwrap();
        assert_eq!(
            index.get_many(&[k(1), k(2)]).unwrap(),
            vec![refs(2), None],
            "{b:?}"
        );
        assert_eq!(index.entries().unwrap().len(), 1, "{b:?}");
        assert_eq!(
            index.stage(&delta(3, &[(1, -3)])),
            Err(IndexError::KeyAbsent),
            "{b:?}"
        );
    }
}

#[test]
fn stage_has_no_visible_effect() {
    for b in BACKENDS {
        let index = make(b, IndexKind::Unique);
        let staged = index.stage(&delta(1, &[(1, 1)])).unwrap();
        assert_eq!(staged.base_epoch(), IndexEpoch(0), "{b:?}");
        assert_eq!(index.get_many(&[k(1)]).unwrap(), vec![None], "{b:?}");
        assert_eq!(index.epoch().unwrap(), IndexEpoch(0), "{b:?}");
    }
}

#[test]
fn apply_is_idempotent_for_the_same_staged_delta_and_epoch() {
    for b in BACKENDS {
        let index = make(b, IndexKind::Reference);
        let staged = index.stage(&delta(1, &[(1, 1)])).unwrap();
        index.apply(staged.clone(), IndexEpoch(1)).unwrap();
        index.apply(staged.clone(), IndexEpoch(1)).unwrap();
        assert_eq!(
            index.get_many(&[k(1)]).unwrap(),
            vec![refs(1)],
            "{b:?}: replay double-counted"
        );
        assert_eq!(index.epoch().unwrap(), IndexEpoch(1), "{b:?}");
    }
}

#[test]
fn replay_with_different_content_or_old_epoch_is_rejected() {
    for b in BACKENDS {
        let index = make(b, IndexKind::Reference);
        let a = index.stage(&delta(1, &[(1, 1)])).unwrap();
        let other = index.stage(&delta(1, &[(2, 1)])).unwrap();
        index.apply(a.clone(), IndexEpoch(5)).unwrap();
        assert_eq!(
            index.apply(other, IndexEpoch(5)),
            Err(IndexError::EpochConflict {
                requested: IndexEpoch(5),
                current: IndexEpoch(5)
            }),
            "{b:?}"
        );
        assert_eq!(
            index.apply(a, IndexEpoch(4)),
            Err(IndexError::EpochConflict {
                requested: IndexEpoch(4),
                current: IndexEpoch(5)
            }),
            "{b:?}"
        );
    }
}

#[test]
fn stale_stage_is_rejected() {
    for b in BACKENDS {
        let index = make(b, IndexKind::Unique);
        let first = index.stage(&delta(1, &[(1, 1)])).unwrap();
        let second = index.stage(&delta(1, &[(1, 1)])).unwrap();
        index.apply(first, IndexEpoch(1)).unwrap();
        // `second` was resolved before key 1 existed; applying it would break uniqueness.
        assert_eq!(
            index.apply(second, IndexEpoch(2)),
            Err(IndexError::StaleStage {
                staged_at: IndexEpoch(0),
                current: IndexEpoch(1)
            }),
            "{b:?}"
        );
        assert_eq!(index.get_many(&[k(1)]).unwrap(), vec![unique(1)], "{b:?}");
    }
}

#[test]
fn digest_identifies_content() {
    let index = make(Backend::Memory, IndexKind::Reference);
    let a = index.stage(&delta(1, &[(1, 1)])).unwrap();
    assert_eq!(
        a.digest(),
        index.stage(&delta(9, &[(1, 1)])).unwrap().digest()
    );
    assert_ne!(
        a.digest(),
        index.stage(&delta(1, &[(1, 2)])).unwrap().digest()
    );
    assert_ne!(
        a.digest(),
        index.stage(&delta(1, &[(2, 1)])).unwrap().digest()
    );
}

#[test]
fn errors_do_not_print_keys() {
    let e = IndexError::KeyAlreadyPresent.to_string();
    assert!(!e.contains("EncodedKey"));
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

fn check_model(
    backend: Backend,
    kind: IndexKind,
    steps: &[Vec<(i64, i64)>],
) -> Result<(), TestCaseError> {
    let index = make(backend, kind);
    // Model: key -> (count, snapshot that last inserted it).
    let mut model: BTreeMap<i64, (u64, i64)> = BTreeMap::new();

    for (step, ch) in steps.iter().enumerate() {
        let snapshot = step as i64 + 100;
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
            if new == 0 {
                next.remove(&key);
            } else {
                next.insert(key, (new as u64, snap));
            }
        }
        if kind == IndexKind::Unique && next.values().any(|&(c, _)| c > 1) {
            valid = false;
        }

        let result = commit(&*index, &delta(snapshot, ch));
        prop_assert_eq!(
            result.is_ok(),
            valid,
            "{:?} step {} {:?}",
            backend,
            step,
            ch
        );
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
                    IndexKind::Unique => IndexValue::Unique {
                        last_snapshot: SnapshotId(snap),
                    },
                    IndexKind::Reference => IndexValue::Reference { child_count: count },
                };
                (k(key), value)
            })
            .collect();
        prop_assert_eq!(index.entries().unwrap(), expected);
    }
    Ok(())
}

fn kind() -> impl Strategy<Value = IndexKind> {
    prop_oneof![Just(IndexKind::Unique), Just(IndexKind::Reference)]
}

proptest! {
    #[test]
    fn memory_matches_model(kind in kind(), steps in vec(changes(), 1..20)) {
        check_model(Backend::Memory, kind, &steps)?;
    }
}

proptest! {
    // Every persistent apply is a durable two-phase commit; keep the case count modest.
    #![proptest_config(Config { cases: 48, ..Config::default() })]

    #[test]
    fn persistent_matches_model(kind in kind(), steps in vec(changes(), 1..20)) {
        check_model(Backend::Persistent, kind, &steps)?;
    }
}

#[test]
fn staged_deltas_round_trip_and_reject_corruption() {
    for kind in [IndexKind::Unique, IndexKind::Reference] {
        let index = make(Backend::Memory, kind);
        commit(&*index, &delta(1, &[(1, 1), (2, 1)])).unwrap();
        let staged = index.stage(&delta(2, &[(1, -1), (3, 1), (4, 1)])).unwrap();
        let bytes = staged.encode();
        let decoded = integrity_index::StagedDelta::decode(&bytes).unwrap();
        assert_eq!(decoded, staged);
        assert_eq!(decoded.digest(), staged.digest());
        // The decoded delta replays as the same apply.
        index.apply(staged, IndexEpoch(2)).unwrap();
        index.apply(decoded, IndexEpoch(2)).unwrap();

        assert!(integrity_index::StagedDelta::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(integrity_index::StagedDelta::decode(&longer).is_err());
        for i in 0..bytes.len() {
            let mut flipped = bytes.clone();
            flipped[i] ^= 0x80;
            // Either rejected, or (for bits inside values) a different delta: never the same.
            if let Ok(d) = integrity_index::StagedDelta::decode(&flipped) {
                assert_ne!(d.encode(), bytes, "byte {i}");
            }
        }
    }
}
