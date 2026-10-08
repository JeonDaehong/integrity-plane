//! Property: random composite key tuples (every family, with NULLs) written to Parquet with field
//! IDs come back from `extract_rows` exactly, across small row groups.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use integrity_core::{Datum, KeySchema, KeyValue, TypeFamily};
use integrity_iceberg::extract_rows;
use integrity_reference::strategies::{Tuple, logical_type, schema, tuple};
use integrity_types::FieldId;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;
use proptest::prelude::*;

const MAX_38_DIGITS: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// One Arrow column for `family`, or `None` if the values are outside what Iceberg can store.
fn column(family: TypeFamily, values: &[Option<KeyValue>]) -> Option<ArrayRef> {
    macro_rules! pick {
        ($variant:ident) => {
            values
                .iter()
                .map(|v| match v {
                    Some(KeyValue::$variant(x)) => Some(x.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
    }
    Some(match family {
        TypeFamily::Boolean => Arc::new(BooleanArray::from(pick!(Boolean))),
        TypeFamily::Integer => Arc::new(Int64Array::from(pick!(Integer))),
        TypeFamily::Date => Arc::new(Date32Array::from(pick!(Date))),
        TypeFamily::Decimal { scale } => {
            let vals: Vec<Option<i128>> = values
                .iter()
                .map(|v| match v {
                    Some(KeyValue::Decimal { unscaled, .. }) => Some(*unscaled),
                    _ => None,
                })
                .collect();
            if vals
                .iter()
                .flatten()
                .any(|u| u.unsigned_abs() > MAX_38_DIGITS as u128)
            {
                return None;
            }
            Arc::new(
                Decimal128Array::from(vals)
                    .with_precision_and_scale(38, scale as i8)
                    .ok()?,
            )
        }
        TypeFamily::Timestamp | TypeFamily::TimestampTz => {
            let nanos: Vec<Option<i128>> = values
                .iter()
                .map(|v| match v {
                    Some(KeyValue::Timestamp(n) | KeyValue::TimestampTz(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            let tz = (family == TypeFamily::TimestampTz).then_some("UTC");
            let as_micros: Option<Vec<Option<i64>>> = nanos
                .iter()
                .map(|n| match n {
                    None => Some(None),
                    Some(n) if n % 1000 == 0 => i64::try_from(n / 1000).ok().map(Some),
                    Some(_) => None,
                })
                .collect();
            if let Some(us) = as_micros {
                let a = TimestampMicrosecondArray::from(us);
                Arc::new(match tz {
                    Some(tz) => a.with_timezone(tz),
                    None => a,
                })
            } else {
                let ns: Vec<Option<i64>> = nanos
                    .iter()
                    .map(|n| n.map(i64::try_from).transpose())
                    .collect::<Result<_, _>>()
                    .ok()?;
                let a = TimestampNanosecondArray::from(ns);
                Arc::new(match tz {
                    Some(tz) => a.with_timezone(tz),
                    None => a,
                })
            }
        }
        TypeFamily::String => {
            let vals = pick!(String);
            Arc::new(StringArray::from(
                vals.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            ))
        }
        TypeFamily::Binary => {
            let vals = pick!(Binary);
            Arc::new(BinaryArray::from_opt_vec(
                vals.iter().map(|v| v.as_deref()).collect(),
            ))
        }
        TypeFamily::Uuid => {
            let vals = pick!(Uuid);
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(vals.into_iter(), 16).ok()?,
            )
        }
    })
}

/// Writes rows as a Parquet file whose columns carry field IDs 1..=n, 3 rows per row group.
fn write(schema: &KeySchema, rows: &[Tuple]) -> Option<Bytes> {
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (i, &family) in schema.families().iter().enumerate() {
        let values: Vec<Option<KeyValue>> = rows.iter().map(|r| r[i].clone()).collect();
        let array = column(family, &values)?;
        let meta = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), (i + 1).to_string())]);
        fields
            .push(Field::new(format!("c{i}"), array.data_type().clone(), true).with_metadata(meta));
        arrays.push(array);
    }
    let arrow_schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, arrow_schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Some(Bytes::from(out))
}

fn schema_and_rows() -> impl Strategy<Value = (KeySchema, Vec<Tuple>)> {
    schema().prop_flat_map(|s| {
        let rows = proptest::collection::vec(tuple(&s, 0.7), 1..12);
        (Just(s), rows)
    })
}

proptest! {
    #[test]
    fn extracted_rows_equal_written_rows((s, rows) in schema_and_rows()) {
        let Some(file) = write(&s, &rows) else {
            return Err(TestCaseError::reject("values not representable in Iceberg"));
        };
        let columns: Vec<_> = s
            .families()
            .iter()
            .enumerate()
            .map(|(i, &fam)| (FieldId(i as i32 + 1), logical_type(fam)))
            .collect();
        let batch = extract_rows(file, &columns).unwrap();
        let expected: Vec<Vec<Datum>> = rows
            .iter()
            .map(|r| r.iter().map(|v| v.clone().map_or(Datum::Null, Datum::Value)).collect())
            .collect();
        prop_assert_eq!(batch.rows(), expected.as_slice());
    }
}
