//! Staged index deltas stored in the transaction log: never panic; canonical when accepted.
#![no_main]

use integrity_index::StagedDelta;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(delta) = StagedDelta::decode(data) {
        assert_eq!(delta.encode(), data);
    }
});
