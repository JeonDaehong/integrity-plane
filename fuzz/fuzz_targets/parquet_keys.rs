//! Client-written Parquet data files: key extraction returns errors, never panics.
#![no_main]

use bytes::Bytes;
use integrity_core::LogicalType;
use integrity_iceberg::extract_rows;
use integrity_types::FieldId;
use libfuzzer_sys::fuzz_target;

/// The decoders under test sit behind `catch_unwind` (a panic in the third-party decoder becomes
/// an error, which is what production relies on). libfuzzer-sys installs a panic hook that aborts
/// even on caught panics, so this target restores a silent hook: a panic that escapes our
/// boundary still unwinds into `fuzz_target!`, which reports it as a crash.
fn quiet_contained_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| std::panic::set_hook(Box::new(|_| {})));
}

fuzz_target!(|data: &[u8]| {
    quiet_contained_panics();
    let columns = [
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::Long),
        (FieldId(3), LogicalType::String),
        (FieldId(4), LogicalType::Decimal { precision: 12, scale: 2 }),
    ];
    let _ = extract_rows(Bytes::copy_from_slice(data), &columns);
});
