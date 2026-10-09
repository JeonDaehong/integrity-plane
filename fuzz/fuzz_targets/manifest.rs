//! Client-written manifests and manifest lists (Avro): errors, never panics.
#![no_main]

use bytes::Bytes;
use integrity_iceberg::manifest::{read_manifest, read_manifest_list};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let bytes = Bytes::copy_from_slice(data);
    let _ = read_manifest(&bytes);
    let _ = read_manifest_list(&bytes);
});
