//! Phase 5 exit criterion: fixture-based key extraction tests, including composite keys, NULLs
//! and widened types. Fixtures are written by pyarrow (`tests/fixtures/generate.py`); expected
//! values below mirror that script.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use bytes::Bytes;
use integrity_core::{
    Datum, EncodedKey, KeyDisposition, KeyRole, KeySchema, KeyValue, LogicalType, MatchMode,
    TypeFamily, classify,
};
use integrity_iceberg::{ExtractError, extract_rows};
use integrity_types::FieldId;

fn fixture(name: &str) -> Bytes {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    Bytes::from(std::fs::read(path).unwrap())
}

fn f(id: i32, ty: LogicalType) -> (FieldId, LogicalType) {
    (FieldId(id), ty)
}

fn v(kv: KeyValue) -> Datum {
    Datum::Value(kv)
}

const NULL: Datum = Datum::Null;

fn int(n: i64) -> Datum {
    v(KeyValue::Integer(n))
}

fn s(x: &str) -> Datum {
    v(KeyValue::String(x.into()))
}

fn dec(unscaled: i128, scale: u8) -> Datum {
    v(KeyValue::Decimal { unscaled, scale })
}

fn uuid(last: u8) -> Datum {
    v(KeyValue::Uuid(if last == 1 {
        let mut u = [0u8; 16];
        u[15] = 1;
        u
    } else {
        [0xFF; 16]
    }))
}

/// Table types for `keys_basic.parquet`. Field 2 is `int` in the file but `long` in the table.
fn basic_columns() -> Vec<(FieldId, LogicalType)> {
    vec![
        f(1, LogicalType::Long),
        f(2, LogicalType::Long),
        f(3, LogicalType::String),
        f(
            4,
            LogicalType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
        f(5, LogicalType::Date),
        f(6, LogicalType::Timestamp),
        f(7, LogicalType::TimestampTz),
        f(8, LogicalType::Uuid),
        f(9, LogicalType::Binary),
        f(10, LogicalType::Boolean),
        f(11, LogicalType::Double),
        f(12, LogicalType::TimestampNs),
    ]
}

fn basic_expected() -> Vec<Vec<Datum>> {
    let us = |n: i64| v(KeyValue::timestamp_micros(n));
    let us_tz = |n: i64| v(KeyValue::timestamptz_micros(n));
    let ns = |n: i64| v(KeyValue::timestamp_nanos(n));
    let bin = |b: &[u8]| v(KeyValue::Binary(b.to_vec()));
    let b = |x: bool| v(KeyValue::Boolean(x));
    vec![
        vec![
            int(1),
            int(7),
            s("eu"),
            dec(100, 2),
            v(KeyValue::Date(0)),
            us(1),
            us_tz(1_791_504_000_000_000),
            uuid(1),
            bin(b""),
            b(true),
            Datum::Opaque,
            ns(1000),
        ],
        vec![
            int(-1),
            NULL,
            NULL,
            dec(-1, 2),
            v(KeyValue::Date(-1)),
            NULL,
            NULL,
            uuid(0xFF),
            bin(b"\x00"),
            b(false),
            NULL,
            ns(1),
        ],
        vec![
            int(0),
            int(i64::from(i32::MIN)),
            s(""),
            NULL,
            NULL,
            us(-1),
            NULL,
            NULL,
            NULL,
            NULL,
            Datum::Opaque,
            NULL,
        ],
        vec![
            int(i64::MAX),
            int(i64::from(i32::MAX)),
            s("a\0b"),
            dec(9_999_999_999, 2),
            v(KeyValue::Date(20735)),
            us(1_791_547_200_000_000),
            us_tz(0),
            uuid(1),
            bin(b"\xff\x00\xff"),
            b(true),
            Datum::Opaque,
            ns(-1),
        ],
        vec![
            int(i64::MIN),
            int(7),
            s("한글"),
            dec(0, 2),
            v(KeyValue::Date(-719_162)),
            us(0),
            us_tz(1),
            uuid(0xFF),
            bin(b"abc"),
            b(false),
            Datum::Opaque,
            ns(0),
        ],
    ]
}

#[test]
fn reads_every_key_family_with_nulls_across_row_groups() {
    let batch = extract_rows(fixture("keys_basic.parquet"), &basic_columns()).unwrap();
    let ids: Vec<FieldId> = basic_columns().into_iter().map(|(id, _)| id).collect();
    assert_eq!(batch.columns(), ids.as_slice());
    let expected = basic_expected();
    assert_eq!(batch.len(), expected.len());
    for (i, (row, want)) in batch.rows().iter().zip(&expected).enumerate() {
        assert_eq!(row, want, "row {i}");
    }
}

#[test]
fn projection_follows_the_requested_order_and_subset() {
    let cols = [
        f(
            4,
            LogicalType::Decimal {
                precision: 10,
                scale: 2,
            },
        ),
        f(1, LogicalType::Long),
    ];
    let batch = extract_rows(fixture("keys_basic.parquet"), &cols).unwrap();
    assert_eq!(batch.columns(), [FieldId(4), FieldId(1)]);
    let firsts: Vec<_> = batch
        .rows()
        .iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect();
    assert_eq!(firsts[0], (dec(100, 2), int(1)));
    assert_eq!(firsts[2], (NULL, int(0)));
}

#[test]
fn int_file_column_under_long_table_column_matches_long_keys() {
    // Spec §9: `int → long` widening needs no re-encode.
    let batch = extract_rows(fixture("keys_basic.parquet"), &[f(2, LogicalType::Long)]).unwrap();
    let schema = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    let Datum::Value(kv) = &batch.rows()[0][0] else {
        panic!()
    };
    assert_eq!(
        EncodedKey::encode(&schema, &[Some(kv.clone())]).unwrap(),
        EncodedKey::encode(&schema, &[Some(KeyValue::Integer(7))]).unwrap()
    );
}

#[test]
fn composite_key_tuples_follow_null_semantics() {
    // (customer_id, region) as a MATCH FULL foreign key read straight from the file.
    let cols = [f(2, LogicalType::Long), f(3, LogicalType::String)];
    let batch = extract_rows(fixture("keys_basic.parquet"), &cols).unwrap();
    let schema = KeySchema::new(vec![TypeFamily::Integer, TypeFamily::String]).unwrap();
    let role = KeyRole::ForeignKeyChild(MatchMode::Full);
    let dispositions: Vec<_> = batch
        .rows()
        .iter()
        .map(|row| {
            let tuple: Vec<Option<KeyValue>> = row
                .iter()
                .map(|d| match d {
                    Datum::Value(kv) => Some(kv.clone()),
                    _ => None,
                })
                .collect();
            classify(role, &schema, &tuple).unwrap()
        })
        .collect();
    let key = |c: i64, r: &str| {
        KeyDisposition::Key(
            EncodedKey::encode(
                &schema,
                &[Some(KeyValue::Integer(c)), Some(KeyValue::String(r.into()))],
            )
            .unwrap(),
        )
    };
    assert_eq!(dispositions[0], key(7, "eu"));
    assert_eq!(dispositions[1], KeyDisposition::Exempt); // (NULL, NULL)
    assert_eq!(dispositions[2], key(i64::from(i32::MIN), ""));
    assert_eq!(dispositions[3], key(i64::from(i32::MAX), "a\0b"));
    assert_eq!(dispositions[4], key(7, "한글"));
}

#[test]
fn decimals_in_every_physical_encoding() {
    let cols = [
        f(
            1,
            LogicalType::Decimal {
                precision: 9,
                scale: 2,
            },
        ),
        f(
            2,
            LogicalType::Decimal {
                precision: 18,
                scale: 4,
            },
        ),
        f(
            3,
            LogicalType::Decimal {
                precision: 38,
                scale: 6,
            },
        ),
    ];
    let batch = extract_rows(fixture("keys_decimals.parquet"), &cols).unwrap();
    let max38 = -99_999_999_999_999_999_999_999_999_999_999_999_999i128;
    assert_eq!(
        batch.rows(),
        &[
            vec![dec(123_456_789, 2), dec(999_999_999_999_999_999, 4), NULL],
            vec![dec(-1, 2), NULL, dec(max38, 6)],
            vec![NULL, dec(-10_000, 4), dec(1, 6)],
        ]
    );
}

#[test]
fn absent_field_reads_as_null() {
    // A column added to the table after this file was written.
    let batch = extract_rows(
        fixture("keys_basic.parquet"),
        &[f(1, LogicalType::Long), f(99, LogicalType::String)],
    )
    .unwrap();
    assert!(batch.rows().iter().all(|r| r[1] == NULL));
    assert_eq!(batch.len(), 5);
}

#[test]
fn incompatible_types_are_rejected() {
    let file = fixture("keys_basic.parquet");
    let cases = [
        f(3, LogicalType::Long),        // string as integer
        f(6, LogicalType::TimestampTz), // no-tz timestamp as tz
        f(7, LogicalType::Timestamp),   // tz timestamp as no-tz
        f(2, LogicalType::Uuid),        // int as uuid
        f(9, LogicalType::Uuid),        // variable binary as uuid
        f(10, LogicalType::String),     // boolean as string
    ];
    for col in cases {
        let err = extract_rows(file.clone(), std::slice::from_ref(&col)).unwrap_err();
        assert!(
            matches!(err, ExtractError::TypeMismatch { .. }),
            "{col:?}: {err:?}"
        );
        assert_eq!(
            err.code(),
            integrity_types::ErrorCode::UnsupportedCommitOperation
        );
    }
    // Same family, different decimal scale.
    let scale_3 = LogicalType::Decimal {
        precision: 10,
        scale: 3,
    };
    let err = extract_rows(file, &[f(4, scale_3)]);
    assert!(
        matches!(err, Err(ExtractError::TypeMismatch { .. })),
        "{err:?}"
    );
}

#[test]
fn files_without_field_ids_fail_closed() {
    // Reading them as "column absent" would silently produce NULL keys.
    assert_eq!(
        extract_rows(fixture("no_field_ids.parquet"), &[f(1, LogicalType::Long)]).err(),
        Some(ExtractError::MissingFieldIds)
    );
}

#[test]
fn nested_key_fields_are_unsupported_but_top_level_columns_work() {
    let file = fixture("nested.parquet");
    assert_eq!(
        extract_rows(file.clone(), &[f(3, LogicalType::Long)]).err(),
        Some(ExtractError::NestedField(FieldId(3)))
    );
    let batch = extract_rows(file.clone(), &[f(2, LogicalType::Long)]).unwrap();
    assert_eq!(batch.rows(), &[vec![int(10)], vec![int(20)]]);
    // A struct column as a NOT NULL target: only presence matters.
    let batch = extract_rows(file, &[f(1, LogicalType::Nested)]).unwrap();
    assert_eq!(batch.rows(), &[vec![Datum::Opaque], vec![Datum::Opaque]]);
}

#[test]
fn garbage_is_a_parquet_error_not_a_panic() {
    for bytes in [&b""[..], b"PAR1", b"PAR1garbagePAR1", &[0u8; 64]] {
        let err = extract_rows(Bytes::copy_from_slice(bytes), &[f(1, LogicalType::Long)]);
        assert!(
            matches!(err, Err(ExtractError::Parquet(_))),
            "{bytes:?}: {err:?}"
        );
    }
}

/// Only the footer and the key column chunks are read: a wide file costs about its key columns.
#[test]
fn wide_files_are_read_for_their_key_columns_only() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use integrity_iceberg::{Budgeted, MemoryIo};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::properties::WriterProperties;

    let meta = |id: &str| {
        std::collections::HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(meta("1")),
        Field::new("payload", DataType::Utf8, false).with_metadata(meta("2")),
    ]));
    let n = 4000i64;
    // Incompressible payloads so the file is genuinely wide.
    let payload: Vec<String> = (0..n)
        .map(|i| {
            (0..64)
                .map(|j| {
                    format!(
                        "{:016x}",
                        ((i * 7919 + j) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    )
                })
                .collect()
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap();
    let mut file = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1000)) // several row groups
        .build();
    let mut w = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let size = file.len();

    let mut io = MemoryIo::new();
    io.insert("wide.parquet", file);
    let budgeted = Budgeted::new(&io, u64::MAX);
    let rows = integrity_iceberg::extract_rows_from(
        &budgeted,
        "wide.parquet",
        &[(FieldId(1), LogicalType::Long)],
    )
    .unwrap();
    assert_eq!(rows.len(), n as usize);
    assert_eq!(
        rows.rows()[1234],
        vec![integrity_core::Datum::Value(KeyValue::Integer(1234))]
    );
    let read = budgeted.used() as usize;
    assert!(size > 4_000_000, "file is {size} bytes");
    assert!(
        read * 20 < size,
        "read {read} of {size} bytes: more than the key column"
    );
}
