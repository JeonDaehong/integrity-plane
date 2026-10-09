//! `Overlay`: pending writes on top of a base index, used to validate several snapshots of one
//! commit in order without touching the base.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use common::*;
use integrity_index::{IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, Overlay};
use integrity_types::SnapshotId;

fn unique(s: i64) -> Option<IndexValue> {
    Some(IndexValue::Unique {
        last_snapshot: SnapshotId(s),
    })
}

#[test]
fn overlay_reads_through_and_never_writes_the_base() {
    for b in BACKENDS {
        let base = make(b, IndexKind::Unique);
        commit(&*base, &delta(1, &[(1, 1), (2, 1)])).unwrap();
        let overlay = Overlay::new(&*base);

        // Step 1: remove 1, add 3.
        let staged = overlay.stage(&delta(2, &[(1, -1), (3, 1)])).unwrap();
        overlay.apply(staged, IndexEpoch(99)).unwrap();
        assert_eq!(
            overlay.get_many(&[k(1), k(2), k(3)]).unwrap(),
            vec![None, unique(1), unique(2)],
            "{b:?}"
        );
        // Step 2 sees step 1: re-adding 1 is fine, adding 3 again is a duplicate.
        assert!(overlay.stage(&delta(3, &[(1, 1)])).is_ok(), "{b:?}");
        assert_eq!(
            overlay.stage(&delta(3, &[(3, 1)])),
            Err(IndexError::KeyAlreadyPresent),
            "{b:?}"
        );
        assert_eq!(overlay.entries().unwrap().len(), 2, "{b:?}");

        // The base is unchanged, epoch included.
        assert_eq!(
            base.get_many(&[k(1), k(3)]).unwrap(),
            vec![unique(1), None],
            "{b:?}"
        );
        assert_eq!(base.epoch().unwrap(), IndexEpoch(1), "{b:?}");
        assert_eq!(overlay.epoch().unwrap(), IndexEpoch(1), "{b:?}");
    }
}

#[test]
fn overlay_writes_become_one_replayable_staged_delta() {
    for b in BACKENDS {
        let base = make(b, IndexKind::Unique);
        commit(&*base, &delta(1, &[(1, 1)])).unwrap();
        let expected = {
            let overlay = Overlay::new(&*base);
            for (snapshot, changes) in [(2, vec![(1, -1), (2, 1)]), (3, vec![(3, 1)])] {
                let staged = overlay.stage(&delta(snapshot, &changes)).unwrap();
                overlay.apply(staged, IndexEpoch(0)).unwrap();
            }
            let entries = overlay.entries().unwrap();
            let staged = overlay.into_staged().unwrap();
            base.apply(staged.clone(), IndexEpoch(2)).unwrap();
            base.apply(staged, IndexEpoch(2)).unwrap(); // replay is a no-op
            entries
        };
        assert_eq!(base.entries().unwrap(), expected, "{b:?}");
        assert_eq!(
            base.get_many(&[k(2), k(3)]).unwrap(),
            vec![unique(2), unique(3)],
            "{b:?}"
        );
    }
}
