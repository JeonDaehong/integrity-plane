//! Properties only a persistent backend has: state survives reopen, replay works across
//! restarts, and corruption is never silently served.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use common::*;
use integrity_index::{IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, PersistentStore};
use integrity_types::{ConstraintId, SnapshotId};

#[test]
fn reopen_preserves_entries_epoch_and_replay_identity() {
    let dir = TempDir::new("reopen");
    let path = dir.path().join("index.redb");
    let staged = {
        let store = PersistentStore::open(&path).unwrap();
        let index = store.index(ConstraintId(7), IndexKind::Reference).unwrap();
        commit(&index, &delta(1, &[(1, 2), (2, 1)])).unwrap();
        let staged = index.stage(&delta(2, &[(3, 1)])).unwrap();
        index.apply(staged.clone(), IndexEpoch(9)).unwrap();
        staged
    };

    let store = PersistentStore::open(&path).unwrap();
    let index = store.index(ConstraintId(7), IndexKind::Reference).unwrap();
    assert_eq!(index.epoch().unwrap(), IndexEpoch(9));
    assert_eq!(
        index.get_many(&[k(1), k(2), k(3)]).unwrap(),
        vec![
            Some(IndexValue::Reference { child_count: 2 }),
            Some(IndexValue::Reference { child_count: 1 }),
            Some(IndexValue::Reference { child_count: 1 }),
        ]
    );
    // Recovery replays the last apply after a restart: it must be a no-op success.
    index.apply(staged, IndexEpoch(9)).unwrap();
    assert_eq!(
        index.get_many(&[k(3)]).unwrap(),
        vec![Some(IndexValue::Reference { child_count: 1 })]
    );
    // A different delta claiming the same epoch is still a conflict.
    let other = index.stage(&delta(3, &[(4, 1)])).unwrap();
    assert_eq!(
        index.apply(other, IndexEpoch(9)),
        Err(IndexError::EpochConflict {
            requested: IndexEpoch(9),
            current: IndexEpoch(9)
        })
    );
}

#[test]
fn indexes_in_one_store_are_independent() {
    let dir = TempDir::new("multi");
    let store = PersistentStore::open(dir.path().join("index.redb")).unwrap();
    let a = store.index(ConstraintId(1), IndexKind::Unique).unwrap();
    let b = store.index(ConstraintId(2), IndexKind::Unique).unwrap();
    commit(&a, &delta(5, &[(1, 1)])).unwrap();
    assert_eq!(a.epoch().unwrap(), IndexEpoch(1));
    assert_eq!(b.epoch().unwrap(), IndexEpoch(0));
    assert_eq!(b.get_many(&[k(1)]).unwrap(), vec![None]);
    commit(&b, &delta(6, &[(1, 1)])).unwrap();
    assert_eq!(
        a.get_many(&[k(1)]).unwrap(),
        vec![Some(IndexValue::Unique {
            last_snapshot: SnapshotId(5)
        })]
    );
}

#[test]
fn reopening_an_index_with_another_kind_is_corrupt() {
    let dir = TempDir::new("kind");
    let store = PersistentStore::open(dir.path().join("index.redb")).unwrap();
    store.index(ConstraintId(1), IndexKind::Unique).unwrap();
    assert_eq!(
        store.index(ConstraintId(1), IndexKind::Reference).err(),
        Some(IndexError::Corrupt)
    );
}

#[test]
fn corrupted_file_is_never_served_silently() {
    let dir = TempDir::new("corrupt");
    let path = dir.path().join("index.redb");
    let expected = {
        let store = PersistentStore::open(&path).unwrap();
        let index = store.index(ConstraintId(1), IndexKind::Reference).unwrap();
        for i in 0..50 {
            commit(&index, &delta(i, &[(i, 1), (i + 1000, 2)])).unwrap();
        }
        index.entries().unwrap()
    };

    // Overwrite everything after the first page with garbage, several patterns.
    let original = std::fs::read(&path).unwrap();
    for pattern in [0x00u8, 0xFF, 0x5A] {
        let mut bytes = original.clone();
        for b in bytes.iter_mut().skip(4096) {
            *b ^= pattern | 0x01;
        }
        std::fs::write(&path, &bytes).unwrap();
        // Either the store refuses to open / read, or it serves exactly the committed data.
        let outcome = PersistentStore::open(&path)
            .and_then(|s| s.index(ConstraintId(1), IndexKind::Reference))
            .and_then(|i| i.entries());
        if let Ok(entries) = outcome {
            assert_eq!(entries, expected, "pattern {pattern:#x} served wrong data");
        }
    }
}

#[test]
fn replace_all_swaps_contents_atomically_and_survives_reopen() {
    let dir = TempDir::new("replace");
    let path = dir.path().join("index.redb");
    {
        let store = PersistentStore::open(&path).unwrap();
        let index = store.index(ConstraintId(3), IndexKind::Reference).unwrap();
        commit(&index, &delta(1, &[(1, 2), (2, 1)])).unwrap();
        let fresh = vec![(k(7), IndexValue::Reference { child_count: 4 })];
        // Older or equal epochs are refused.
        assert!(index.replace_all(&fresh, IndexEpoch(1)).is_err());
        // Values of the wrong kind are refused, and nothing changes.
        assert_eq!(
            index.replace_all(
                &[(
                    k(7),
                    IndexValue::Unique {
                        last_snapshot: SnapshotId(1)
                    }
                )],
                IndexEpoch(5)
            ),
            Err(IndexError::Corrupt)
        );
        assert_eq!(index.entries().unwrap().len(), 2);
        index.replace_all(&fresh, IndexEpoch(5)).unwrap();
    }
    let store = PersistentStore::open(&path).unwrap();
    let index = store.index(ConstraintId(3), IndexKind::Reference).unwrap();
    assert_eq!(index.epoch().unwrap(), IndexEpoch(5));
    assert_eq!(
        index.entries().unwrap(),
        vec![(k(7), IndexValue::Reference { child_count: 4 })]
    );
    // Normal applies continue after the swap.
    commit(&index, &delta(6, &[(7, -1), (8, 1)])).unwrap();
    assert_eq!(
        index.get_many(&[k(7), k(8)]).unwrap(),
        vec![
            Some(IndexValue::Reference { child_count: 3 }),
            Some(IndexValue::Reference { child_count: 1 })
        ]
    );
}

#[test]
fn apply_all_is_atomic_across_indexes_and_replayable() {
    let dir = TempDir::new("apply-all");
    let path = dir.path().join("index.redb");
    let store = PersistentStore::open(&path).unwrap();
    let a = store.index(ConstraintId(1), IndexKind::Unique).unwrap();
    let b = store.index(ConstraintId(2), IndexKind::Reference).unwrap();
    let sa = a.stage(&delta(1, &[(1, 1), (2, 1)])).unwrap();
    let sb = b.stage(&delta(1, &[(1, 3)])).unwrap();
    store
        .apply_all(&[(&a, &sa), (&b, &sb)], IndexEpoch(5))
        .unwrap();
    assert_eq!(a.epoch().unwrap(), IndexEpoch(5));
    assert_eq!(b.epoch().unwrap(), IndexEpoch(5));
    // Replaying the same batch (recovery) changes nothing and succeeds.
    store
        .apply_all(&[(&a, &sa), (&b, &sb)], IndexEpoch(5))
        .unwrap();
    assert_eq!(
        b.get_many(&[k(1)]).unwrap(),
        vec![Some(IndexValue::Reference { child_count: 3 })]
    );

    // One conflicting index aborts the whole batch: the other one is not written either.
    let sa2 = a.stage(&delta(2, &[(9, 1)])).unwrap();
    let stale = b.stage(&delta(2, &[(8, 1)])).unwrap();
    b.apply(stale.clone(), IndexEpoch(7)).unwrap();
    let sb_old = stale;
    assert!(
        store
            .apply_all(&[(&a, &sa2), (&b, &sb_old)], IndexEpoch(6))
            .is_err()
    );
    assert_eq!(a.epoch().unwrap(), IndexEpoch(5), "nothing applied");
    assert_eq!(a.get_many(&[k(9)]).unwrap(), vec![None]);

    // Indexes of another store are refused.
    let other_dir = TempDir::new("apply-all-other");
    let other = PersistentStore::open(other_dir.path().join("o.redb")).unwrap();
    let foreign = other.index(ConstraintId(1), IndexKind::Unique).unwrap();
    let sf = foreign.stage(&delta(1, &[(1, 1)])).unwrap();
    assert!(store.apply_all(&[(&foreign, &sf)], IndexEpoch(9)).is_err());
}
