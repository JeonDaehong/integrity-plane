//! Client-written Parquet data files: key extraction returns errors, never panics.
#![no_main]

use bytes::Bytes;
use integrity_core::LogicalType;
use integrity_iceberg::extract_rows;
use integrity_types::FieldId;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let columns = [
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::Long),
        (FieldId(3), LogicalType::String),
        (FieldId(4), LogicalType::Decimal { precision: 12, scale: 2 }),
    ];
    let _ = extract_rows(Bytes::copy_from_slice(data), &columns);
});
