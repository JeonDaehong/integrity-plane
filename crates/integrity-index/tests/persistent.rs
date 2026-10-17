//! Properties only a persistent backend has: state survives reopen, replay works across
//! restarts, and corruption is never silently served.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use common::*;
use integrity_index::{
    IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, PersistentStore, TableHead,
};
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
fn builds_are_installed_atomically_beside_the_live_contents() {
    let dir = TempDir::new("build");
    let path = dir.path().join("index.redb");
    let n = 120_000; // more than one build transaction
    {
        let store = PersistentStore::open(&path).unwrap();
        let refs = store.index(ConstraintId(3), IndexKind::Reference).unwrap();
        commit(&refs, &delta(1, &[(1, 2), (2, 1)])).unwrap();
        let mut a = store.build(ConstraintId(3), IndexKind::Reference).unwrap();
        let mut b = store.build(ConstraintId(4), IndexKind::Unique).unwrap();
        // Values of the wrong kind are refused.
        assert_eq!(
            a.push(
                k(7),
                IndexValue::Unique {
                    last_snapshot: SnapshotId(1)
                }
            ),
            Err(IndexError::Corrupt)
        );
        a.push(k(7), IndexValue::Reference { child_count: 4 })
            .unwrap();
        for i in 0..n {
            b.push(
                k(i),
                IndexValue::Unique {
                    last_snapshot: SnapshotId(9),
                },
            )
            .unwrap();
        }
        assert_eq!(
            b.get_many(&[k(5), k(n)]).unwrap(),
            vec![
                Some(IndexValue::Unique {
                    last_snapshot: SnapshotId(9)
                }),
                None
            ]
        );
        // Until installed, readers see the live contents.
        assert_eq!(refs.entries().unwrap().len(), 2);
        let unique = store.index(ConstraintId(4), IndexKind::Unique).unwrap();
        assert!(unique.entries().unwrap().is_empty());
        // One epoch above every index's current one.
        assert_eq!(store.install(vec![a, b], &[]).unwrap(), IndexEpoch(2));
    }
    let store = PersistentStore::open(&path).unwrap();
    let refs = store.index(ConstraintId(3), IndexKind::Reference).unwrap();
    let unique = store.index(ConstraintId(4), IndexKind::Unique).unwrap();
    assert_eq!(refs.epoch().unwrap(), IndexEpoch(2));
    assert_eq!(unique.epoch().unwrap(), IndexEpoch(2));
    assert_eq!(
        refs.entries().unwrap(),
        vec![(k(7), IndexValue::Reference { child_count: 4 })]
    );
    assert_eq!(unique.entries().unwrap().len(), n as usize);
    // Normal applies continue after the swap.
    commit(&refs, &delta(6, &[(7, -1), (8, 1)])).unwrap();
    assert_eq!(
        refs.get_many(&[k(7), k(8)]).unwrap(),
        vec![
            Some(IndexValue::Reference { child_count: 3 }),
            Some(IndexValue::Reference { child_count: 1 })
        ]
    );

    // A build that is never installed (the process died) changes nothing; the next build of the
    // same index starts empty.
    let mut stale = store.build(ConstraintId(3), IndexKind::Reference).unwrap();
    stale
        .push(k(9), IndexValue::Reference { child_count: 1 })
        .unwrap();
    stale.flush().unwrap();
    drop(stale);
    assert_eq!(refs.entries().unwrap().len(), 2);
    let empty = store.build(ConstraintId(3), IndexKind::Reference).unwrap();
    assert_eq!(store.install(vec![empty], &[]).unwrap(), IndexEpoch(4));
    assert!(refs.entries().unwrap().is_empty());
    // A discarded build leaves the index alone.
    let mut gone = store.build(ConstraintId(4), IndexKind::Unique).unwrap();
    gone.push(
        k(-1),
        IndexValue::Unique {
            last_snapshot: SnapshotId(1),
        },
    )
    .unwrap();
    gone.discard().unwrap();
    assert_eq!(unique.entries().unwrap().len(), n as usize);
    // A build of an existing index with another kind is refused.
    assert!(store.build(ConstraintId(4), IndexKind::Reference).is_err());
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

/// The writes of `staged`, as `apply_streamed` takes them.
fn writes_of(
    staged: &integrity_index::StagedDelta,
) -> Vec<Result<(integrity_core::EncodedKey, Option<IndexValue>), IndexError>> {
    staged
        .writes()
        .map(|(k, v)| Ok((k.clone(), v.copied())))
        .collect()
}

#[test]
fn a_streamed_apply_equals_apply_all_and_refuses_writes_that_do_not_match() {
    let dir = TempDir::new("apply-streamed");
    let store = PersistentStore::open(dir.path().join("index.redb")).unwrap();
    let a = store.index(ConstraintId(1), IndexKind::Unique).unwrap();
    let b = store.index(ConstraintId(2), IndexKind::Reference).unwrap();
    commit(&a, &delta(1, &[(1, 1), (5, 1)])).unwrap();
    let sa = a.stage(&delta(2, &[(1, -1), (2, 1), (3, 1)])).unwrap();
    let sb = b.stage(&delta(2, &[(1, 3), (4, 1)])).unwrap();

    // Whatever is wrong with one stream, nothing of the batch is applied.
    let mut short = writes_of(&sa);
    short.pop();
    let mut swapped = writes_of(&sa);
    swapped.swap(0, 1);
    let mut changed = writes_of(&sa);
    changed[1] = Ok((k(2), None));
    let mut extra = writes_of(&sa);
    extra.push(Ok((k(9), None)));
    let failing = vec![
        Err(IndexError::Storage("read failed".into())),
        Ok((k(1), None)),
    ];
    for bad in [short, swapped, changed, extra, failing] {
        let result = store.apply_streamed(
            vec![
                (&b, sb.header(), Box::new(writes_of(&sb).into_iter())),
                (&a, sa.header(), Box::new(bad.into_iter())),
            ],
            IndexEpoch(3),
            &[("db.t", head(Some(3)))],
        );
        assert!(result.is_err());
        assert_eq!(
            store.head("db.t").unwrap(),
            None,
            "no head without the indexes"
        );
        assert_eq!(a.epoch().unwrap(), IndexEpoch(1));
        assert_eq!(b.epoch().unwrap(), IndexEpoch(0));
        assert_eq!(b.get_many(&[k(1)]).unwrap(), vec![None]);
    }

    // The right writes apply exactly as apply_all would, and replay as a no-op.
    let twin_dir = TempDir::new("apply-streamed-twin");
    let twin = PersistentStore::open(twin_dir.path().join("index.redb")).unwrap();
    let ta = twin.index(ConstraintId(1), IndexKind::Unique).unwrap();
    let tb = twin.index(ConstraintId(2), IndexKind::Reference).unwrap();
    commit(&ta, &delta(1, &[(1, 1), (5, 1)])).unwrap();
    twin.apply_all(&[(&ta, &sa), (&tb, &sb)], IndexEpoch(3))
        .unwrap();
    for _ in 0..2 {
        store
            .apply_streamed(
                vec![
                    (&a, sa.header(), Box::new(writes_of(&sa).into_iter())),
                    (&b, sb.header(), Box::new(writes_of(&sb).into_iter())),
                ],
                IndexEpoch(3),
                &[],
            )
            .unwrap();
    }
    assert_eq!(a.entries().unwrap(), ta.entries().unwrap());
    assert_eq!(b.entries().unwrap(), tb.entries().unwrap());
    assert_eq!(a.epoch().unwrap(), IndexEpoch(3));
    assert_eq!(b.epoch().unwrap(), IndexEpoch(3));
}

fn head(snapshot: Option<i64>) -> TableHead {
    TableHead {
        table_uuid: "11111111-1111-1111-1111-111111111111".into(),
        snapshot,
    }
}

#[test]
fn heads_are_written_with_the_indexes_and_survive_reopen() {
    let dir = TempDir::new("heads");
    let path = dir.path().join("index.redb");
    {
        let store = PersistentStore::open(&path).unwrap();
        assert_eq!(store.head("db.t").unwrap(), None);
        store.set_head("db.t", head(None)).unwrap();
        assert_eq!(store.head("db.t").unwrap(), Some(head(None)));

        // An install records the scanned heads with the builds.
        let mut build = store.build(ConstraintId(1), IndexKind::Unique).unwrap();
        build
            .push(
                k(1),
                IndexValue::Unique {
                    last_snapshot: SnapshotId(1),
                },
            )
            .unwrap();
        let other = TableHead {
            table_uuid: "22222222-2222-2222-2222-222222222222".into(),
            snapshot: Some(-4),
        };
        store
            .install(
                vec![build],
                &[("db.t", head(Some(10))), ("db.u", other.clone())],
            )
            .unwrap();
        assert_eq!(store.head("db.t").unwrap(), Some(head(Some(10))));
        assert_eq!(store.head("db.u").unwrap(), Some(other));

        // An apply records the committed table's head, also when it writes no entry, and again
        // when replayed.
        let a = store.index(ConstraintId(1), IndexKind::Unique).unwrap();
        let sa = a.stage(&delta(2, &[(2, 1)])).unwrap();
        for _ in 0..2 {
            store
                .apply_streamed(
                    vec![(&a, sa.header(), Box::new(writes_of(&sa).into_iter()))],
                    IndexEpoch(2),
                    &[("db.t", head(Some(11)))],
                )
                .unwrap();
            assert_eq!(store.head("db.t").unwrap(), Some(head(Some(11))));
        }
        store
            .apply_streamed(Vec::new(), IndexEpoch(3), &[("db.t", head(Some(12)))])
            .unwrap();
        // A conflicting apply records nothing.
        let stale = a.stage(&delta(3, &[(3, 1)])).unwrap();
        assert!(
            store
                .apply_streamed(
                    vec![(&a, stale.header(), Box::new(writes_of(&stale).into_iter()))],
                    IndexEpoch(1),
                    &[("db.t", head(Some(13)))],
                )
                .is_err()
        );
    }
    let store = PersistentStore::open(&path).unwrap();
    assert_eq!(store.head("db.t").unwrap(), Some(head(Some(12))));
    assert_eq!(store.head("db.v").unwrap(), None);
}
