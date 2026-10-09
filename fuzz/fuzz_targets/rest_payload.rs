//! REST payloads: commit requests and table metadata parse to errors, never panics.
#![no_main]

use integrity_iceberg::{CommitRequest, TableMetadata};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = CommitRequest::from_json(value);
    }
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(meta) = TableMetadata::from_json(text)
    {
        let _ = meta.current_schema();
        let _ = meta.main_snapshot_id();
    }
});
