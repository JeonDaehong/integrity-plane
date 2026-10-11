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

/// One step: signed changes per key (0 skipped), then a lookup of `probe` (any order, repeats).
type Step = (Vec<(i64, i64)>, Vec<i64>);

fn steps() -> impl proptest::strategy::Strategy<Value = Vec<Step>> {
    use proptest::prelude::*;
    proptest::collection::vec(
        (
            proptest::collection::btree_map(0i64..10, -2i64..3, 0..6)
                .prop_map(|m| m.into_iter().filter(|&(_, c)| c != 0).collect()),
            proptest::collection::vec(0i64..12, 0..8),
        ),
        1..6,
    )
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config { cases: 64, ..Default::default() })]

    /// Steps applied to an overlay behave exactly like the same steps applied to a copy of the
    /// base, and the overlay's staged writes turn the base into that copy.
    #[test]
    fn an_overlay_behaves_like_a_copy_of_its_base(
        kind in proptest::sample::select(vec![IndexKind::Unique, IndexKind::Reference]),
        initial in proptest::collection::btree_set(0i64..10, 0..6),
        steps in steps(),
    ) {
        for b in BACKENDS {
            let base = make(b, kind);
            let copy = make(Backend::Memory, kind);
            let first: Vec<(i64, i64)> = initial.iter().map(|&v| (v, 1)).collect();
            for index in [&base, &copy] {
                commit(&**index, &delta(1, &first)).unwrap();
            }
            let overlay = Overlay::new(&*base);
            for (i, (changes, probe)) in steps.iter().enumerate() {
                let d = delta(2 + i as i64, changes);
                let via_overlay = overlay.stage(&d);
                let via_copy = copy.stage(&d);
                // Staged at different epochs (the overlay's base does not move): compare writes.
                let writes = |r: &Result<integrity_index::StagedDelta, IndexError>| {
                    r.as_ref()
                        .map(|s| s.writes().map(|(k, v)| (k.clone(), v.copied())).collect::<Vec<_>>())
                        .map_err(Clone::clone)
                };
                proptest::prop_assert_eq!(writes(&via_overlay), writes(&via_copy));
                if let (Ok(o), Ok(c)) = (via_overlay, via_copy) {
                    overlay.apply(o, IndexEpoch(0)).unwrap();
                    let next = IndexEpoch(copy.epoch().unwrap().0 + 1);
                    copy.apply(c, next).unwrap();
                }
                let keys: Vec<_> = probe.iter().map(|&v| k(v)).collect();
                proptest::prop_assert_eq!(overlay.get_many(&keys).unwrap(), copy.get_many(&keys).unwrap());
                proptest::prop_assert_eq!(overlay.entries().unwrap(), copy.entries().unwrap());
            }
            let expected = copy.entries().unwrap();
            base.apply(overlay.into_staged().unwrap(), IndexEpoch(2)).unwrap();
            proptest::prop_assert_eq!(base.entries().unwrap(), expected, "{:?}", b);
        }
    }
}
