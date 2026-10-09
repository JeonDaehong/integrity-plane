//! Key bytes read back from an index: never panic; whatever decodes re-encodes to the same bytes.
#![no_main]

use integrity_core::EncodedKey;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(key) = EncodedKey::from_bytes(data) {
        let (schema, values) = key.decode().expect("from_bytes accepted it");
        let again = EncodedKey::encode(&schema, &values).expect("decoded values encode");
        assert_eq!(again.as_bytes(), data, "non-canonical key encoding");
    }
});
