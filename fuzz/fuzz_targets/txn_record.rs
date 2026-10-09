//! Transaction log records: never panic; whatever decodes re-encodes to the same bytes.
#![no_main]

use integrity_txn::log::{decode_record, encode_record};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok((state, txn, payload)) = decode_record(data) {
        assert_eq!(encode_record(state, txn, &payload), data);
    }
});
