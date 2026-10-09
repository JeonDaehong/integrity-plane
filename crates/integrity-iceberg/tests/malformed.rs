//! Spec §28 fuzzing, on stable: malformed manifests, manifest lists, Parquet files, commit requests
//! and table metadata must yield errors, never panics. Inputs are real fixtures with random byte
//! flips, truncations and insertions, plus arbitrary bytes. (`fuzz/` runs the same targets under
//! libFuzzer.)

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use bytes::Bytes;
use integrity_core::LogicalType;
use integrity_iceberg::manifest::{read_manifest, read_manifest_list};
use integrity_iceberg::{CommitRequest, TableMetadata, extract_rows};
use integrity_types::FieldId;
use proptest::prelude::*;

fn fixture(rel: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/{rel}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

const TABLE: &str = "table/warehouse/db/orders/metadata";

#[derive(Debug, Clone)]
enum Edit {
    Flip(usize, u8),
    Truncate(usize),
    Insert(usize, Vec<u8>),
}

fn edits() -> impl Strategy<Value = Vec<Edit>> {
    let edit = prop_oneof![
        4 => (any::<usize>(), 1u8..=255).prop_map(|(i, x)| Edit::Flip(i, x)),
        1 => any::<usize>().prop_map(Edit::Truncate),
        1 => (any::<usize>(), proptest::collection::vec(any::<u8>(), 1..16))
            .prop_map(|(i, b)| Edit::Insert(i, b)),
    ];
    proptest::collection::vec(edit, 1..6)
}

fn mutate(mut data: Vec<u8>, edits: &[Edit]) -> Vec<u8> {
    for e in edits {
        if data.is_empty() {
            break;
        }
        match e {
            Edit::Flip(i, x) => {
                let i = i % data.len();
                data[i] ^= x;
            }
            Edit::Truncate(i) => data.truncate(i % data.len()),
            Edit::Insert(i, bytes) => {
                let i = i % data.len();
                data.splice(i..i, bytes.iter().copied());
            }
        }
    }
    data
}

fn basic_columns() -> Vec<(FieldId, LogicalType)> {
    vec![
        (FieldId(1), LogicalType::Long),
        (FieldId(2), LogicalType::Long),
        (FieldId(3), LogicalType::String),
        (
            FieldId(4),
            LogicalType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
    ]
}

fn manifests() -> Vec<Vec<u8>> {
    let dir = format!("{}/tests/fixtures/{TABLE}", env!("CARGO_MANIFEST_DIR"));
    let mut out: Vec<Vec<u8>> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().is_some_and(|x| x == "avro")).then(|| std::fs::read(p).unwrap())
        })
        .collect();
    out.sort();
    out
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn mutated_manifests_never_panic(which in any::<prop::sample::Index>(), e in edits()) {
        let all = manifests();
        let data = Bytes::from(mutate(all[which.index(all.len())].clone(), &e));
        let _ = read_manifest(&data);
        let _ = read_manifest_list(&data);
    }

    #[test]
    fn mutated_parquet_never_panics(e in edits()) {
        let data = mutate(fixture("keys_basic.parquet"), &e);
        let _ = extract_rows(Bytes::from(data), &basic_columns());
    }

    #[test]
    fn arbitrary_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        let b = Bytes::from(data.clone());
        let _ = read_manifest(&b);
        let _ = read_manifest_list(&b);
        let _ = extract_rows(b, &basic_columns());
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&data) {
            let _ = CommitRequest::from_json(v);
        }
    }

    #[test]
    fn mutated_metadata_and_requests_never_panic(which in 0usize..8, e in edits()) {
        let name = std::fs::read_dir(format!("{}/tests/fixtures/{TABLE}", env!("CARGO_MANIFEST_DIR")))
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .filter(|n| n.ends_with(".metadata.json"))
            .nth(which)
            .unwrap();
        let text = mutate(fixture(&format!("{TABLE}/{name}")), &e);
        if let Ok(s) = std::str::from_utf8(&text)
            && let Ok(meta) = TableMetadata::from_json(s)
        {
            let _ = meta.current_schema();
            let _ = meta.main_snapshot_id();
        }
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&text) {
            let _ = CommitRequest::from_json(v);
        }
    }
}
