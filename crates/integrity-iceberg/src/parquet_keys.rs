//! Key-column extraction from Parquet data files (spec §14 step 5, §21 `extract`).
//!
//! Reads only the requested top-level columns (projection by Iceberg field ID) and converts them
//! into [`RowBatch`] cells typed by the table schema:
//!
//! - key-capable columns become [`Datum::Value`] in their key family; physical representations
//!   within a family are accepted (`int` data under a `long` column, decimals stored as INT32,
//!   INT64 or FIXED_LEN_BYTE_ARRAY, µs or ns timestamps), anything else is a type mismatch;
//! - other columns (NOT NULL targets of non-key types) become [`Datum::Opaque`] or [`Datum::Null`];
//! - a field ID absent from the file reads as NULL in every row (Iceberg schema evolution, a column
//!   added after the file was written). Iceberg v3 `initial-default` values are not supported here;
//!   callers must reject such schemas.
//!
//! Anything that cannot be mapped unambiguously fails closed: files without field IDs (a
//! name-mapped migrated table), duplicate field IDs, and key fields nested inside structs.

use std::collections::BTreeMap;
use std::fmt;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal32Type, Decimal64Type, Decimal128Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, TimeUnit};
use bytes::Bytes;
use integrity_core::{Datum, KeyValue, LogicalType, RowBatch, TypeFamily};
use integrity_types::{ErrorCode, FieldId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::schema::types::Type;

const BATCH_SIZE: usize = 8192;

/// Why key columns could not be extracted. Every variant rejects the commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractError {
    /// The file is not readable Parquet (or decoding failed).
    Parquet(String),
    /// A top-level column has no field ID, so columns cannot be matched to the table schema.
    MissingFieldIds,
    /// Two columns in the file carry the same field ID.
    DuplicateFieldId(FieldId),
    /// The requested field is nested inside a struct, list or map (not supported in 0.1).
    NestedField(FieldId),
    /// The file's column type cannot represent the table's column type.
    TypeMismatch {
        /// The column.
        field: FieldId,
        /// The table schema's type.
        expected: LogicalType,
        /// The Arrow type found in the file.
        found: String,
    },
    /// A value cannot be represented in its key family (e.g. a decimal with another scale).
    InvalidValue(FieldId),
}

impl ExtractError {
    /// The integrity code reported for the commit.
    pub fn code(&self) -> ErrorCode {
        ErrorCode::UnsupportedCommitOperation
    }
}

impl fmt::Display for ExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtractError::Parquet(m) => write!(f, "cannot read Parquet file: {m}"),
            ExtractError::MissingFieldIds => f.write_str("Parquet columns lack Iceberg field IDs"),
            ExtractError::DuplicateFieldId(id) => write!(f, "{id} appears twice in the file"),
            ExtractError::NestedField(id) => {
                write!(f, "{id} is nested; nested keys are unsupported")
            }
            ExtractError::TypeMismatch {
                field,
                expected,
                found,
            } => write!(f, "{field}: table type {expected:?} but file type {found}"),
            ExtractError::InvalidValue(id) => write!(f, "{id}: value outside its key family"),
        }
    }
}

impl std::error::Error for ExtractError {}

fn parquet_err(e: impl fmt::Display) -> ExtractError {
    ExtractError::Parquet(e.to_string())
}

/// Where each field ID lives in the file's schema.
enum Location {
    Root(usize),
    Nested,
}

/// Maps field IDs to root column positions; requires every root column to carry an ID.
fn locate(root: &Type) -> Result<BTreeMap<i32, Location>, ExtractError> {
    fn nested(t: &Type, out: &mut BTreeMap<i32, Location>) -> Result<(), ExtractError> {
        for child in t.get_fields() {
            let info = child.get_basic_info();
            if info.has_id() && out.insert(info.id(), Location::Nested).is_some() {
                return Err(ExtractError::DuplicateFieldId(FieldId(info.id())));
            }
            if child.is_group() {
                nested(child, out)?;
            }
        }
        Ok(())
    }

    let mut out = BTreeMap::new();
    for (i, field) in root.get_fields().iter().enumerate() {
        let info = field.get_basic_info();
        if !info.has_id() {
            return Err(ExtractError::MissingFieldIds);
        }
        if out.insert(info.id(), Location::Root(i)).is_some() {
            return Err(ExtractError::DuplicateFieldId(FieldId(info.id())));
        }
        if field.is_group() {
            nested(field, &mut out)?;
        }
    }
    Ok(out)
}

/// Reads `columns` (field ID and table type) from every row of a Parquet file.
///
/// The result has exactly `columns` as its projection, in that order.
pub fn extract_rows(
    data: Bytes,
    columns: &[(FieldId, LogicalType)],
) -> Result<RowBatch, ExtractError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(parquet_err)?;
    let locations = locate(builder.parquet_schema().root_schema())?;

    let mut roots = Vec::new();
    for (field, _) in columns {
        match locations.get(&field.0) {
            Some(Location::Root(i)) => roots.push(*i),
            Some(Location::Nested) => return Err(ExtractError::NestedField(*field)),
            None => {}
        }
    }
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()
        .map_err(parquet_err)?;

    let mut batch = RowBatch::new(columns.iter().map(|(f, _)| *f).collect());
    for record_batch in reader {
        let record_batch = record_batch.map_err(parquet_err)?;
        // Output columns of this record batch, by field ID.
        let mut by_id: BTreeMap<i32, &ArrayRef> = BTreeMap::new();
        for (field, array) in record_batch
            .schema()
            .fields()
            .iter()
            .zip(record_batch.columns())
        {
            let id = field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|v| v.parse::<i32>().ok())
                .ok_or(ExtractError::MissingFieldIds)?;
            by_id.insert(id, array);
        }
        let cells: Vec<Vec<Datum>> = columns
            .iter()
            .map(|(field, ty)| match by_id.get(&field.0) {
                Some(array) => convert(*field, ty, array),
                None => Ok(vec![Datum::Null; record_batch.num_rows()]),
            })
            .collect::<Result<_, _>>()?;
        for row in 0..record_batch.num_rows() {
            let row: Vec<Datum> = cells.iter().map(|col| col[row].clone()).collect();
            batch.push(row).map_err(parquet_err)?;
        }
    }
    Ok(batch)
}

fn mismatch(field: FieldId, expected: &LogicalType, array: &ArrayRef) -> ExtractError {
    ExtractError::TypeMismatch {
        field,
        expected: expected.clone(),
        found: array.data_type().to_string(),
    }
}

/// Converts one column to cells of the table type.
fn convert(field: FieldId, ty: &LogicalType, array: &ArrayRef) -> Result<Vec<Datum>, ExtractError> {
    let Ok(family) = ty.key_family() else {
        // Not a key type: only NULL-ness matters.
        return Ok((0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    Datum::Null
                } else {
                    Datum::Opaque
                }
            })
            .collect());
    };

    let values: Vec<Option<KeyValue>> = match (family, array.data_type()) {
        (TypeFamily::Boolean, DataType::Boolean) => {
            let a = array.as_boolean();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| KeyValue::Boolean(a.value(i))))
                .collect()
        }
        (TypeFamily::Integer, DataType::Int32) => {
            let a = array.as_primitive::<Int32Type>();
            a.iter()
                .map(|v| v.map(|v| KeyValue::Integer(i64::from(v))))
                .collect()
        }
        (TypeFamily::Integer, DataType::Int64) => {
            let a = array.as_primitive::<Int64Type>();
            a.iter().map(|v| v.map(KeyValue::Integer)).collect()
        }
        (TypeFamily::Decimal { scale }, DataType::Decimal32(_, s)) => {
            check_scale(field, ty, array, scale, *s)?;
            let a = array.as_primitive::<Decimal32Type>();
            a.iter()
                .map(|v| v.map(|v| decimal(i128::from(v), scale)))
                .collect()
        }
        (TypeFamily::Decimal { scale }, DataType::Decimal64(_, s)) => {
            check_scale(field, ty, array, scale, *s)?;
            let a = array.as_primitive::<Decimal64Type>();
            a.iter()
                .map(|v| v.map(|v| decimal(i128::from(v), scale)))
                .collect()
        }
        (TypeFamily::Decimal { scale }, DataType::Decimal128(_, s)) => {
            check_scale(field, ty, array, scale, *s)?;
            let a = array.as_primitive::<Decimal128Type>();
            a.iter().map(|v| v.map(|v| decimal(v, scale))).collect()
        }
        (TypeFamily::Date, DataType::Date32) => {
            let a = array.as_primitive::<Date32Type>();
            a.iter().map(|v| v.map(KeyValue::Date)).collect()
        }
        (TypeFamily::Timestamp, DataType::Timestamp(unit, None)) => timestamps(array, *unit)
            .ok_or_else(|| mismatch(field, ty, array))?
            .into_iter()
            .map(|v| v.map(KeyValue::Timestamp))
            .collect(),
        (TypeFamily::TimestampTz, DataType::Timestamp(unit, Some(_))) => timestamps(array, *unit)
            .ok_or_else(|| mismatch(field, ty, array))?
            .into_iter()
            .map(|v| v.map(KeyValue::TimestampTz))
            .collect(),
        (TypeFamily::String, DataType::Utf8) => {
            let a = array.as_string::<i32>();
            a.iter()
                .map(|v| v.map(|s| KeyValue::String(s.to_owned())))
                .collect()
        }
        (TypeFamily::String, DataType::LargeUtf8) => {
            let a = array.as_string::<i64>();
            a.iter()
                .map(|v| v.map(|s| KeyValue::String(s.to_owned())))
                .collect()
        }
        (TypeFamily::String, DataType::Utf8View) => {
            let a = array.as_string_view();
            a.iter()
                .map(|v| v.map(|s| KeyValue::String(s.to_owned())))
                .collect()
        }
        (TypeFamily::Binary, DataType::Binary) => {
            let a = array.as_binary::<i32>();
            a.iter()
                .map(|v| v.map(|b| KeyValue::Binary(b.to_vec())))
                .collect()
        }
        (TypeFamily::Binary, DataType::LargeBinary) => {
            let a = array.as_binary::<i64>();
            a.iter()
                .map(|v| v.map(|b| KeyValue::Binary(b.to_vec())))
                .collect()
        }
        (TypeFamily::Binary, DataType::BinaryView) => {
            let a = array.as_binary_view();
            a.iter()
                .map(|v| v.map(|b| KeyValue::Binary(b.to_vec())))
                .collect()
        }
        (TypeFamily::Binary, DataType::FixedSizeBinary(_)) => {
            let a = array.as_fixed_size_binary();
            a.iter()
                .map(|v| v.map(|b| KeyValue::Binary(b.to_vec())))
                .collect()
        }
        (TypeFamily::Uuid, DataType::FixedSizeBinary(16)) => {
            let a = array.as_fixed_size_binary();
            a.iter()
                .map(|v| {
                    v.map(|b| <[u8; 16]>::try_from(b).map(KeyValue::Uuid))
                        .transpose()
                        .map_err(|_| ExtractError::InvalidValue(field))
                })
                .collect::<Result<_, _>>()?
        }
        _ => return Err(mismatch(field, ty, array)),
    };
    Ok(values
        .into_iter()
        .map(|v| v.map_or(Datum::Null, Datum::Value))
        .collect())
}

fn check_scale(
    field: FieldId,
    ty: &LogicalType,
    array: &ArrayRef,
    expected: u8,
    found: i8,
) -> Result<(), ExtractError> {
    if i16::from(found) == i16::from(expected) {
        Ok(())
    } else {
        Err(mismatch(field, ty, array))
    }
}

fn decimal(unscaled: i128, scale: u8) -> KeyValue {
    KeyValue::Decimal { unscaled, scale }
}

/// Timestamps as nanoseconds; only the units Iceberg writes (µs, ns).
fn timestamps(array: &ArrayRef, unit: TimeUnit) -> Option<Vec<Option<i128>>> {
    match unit {
        TimeUnit::Microsecond => Some(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .map(|v| v.map(|v| i128::from(v) * 1_000))
                .collect(),
        ),
        TimeUnit::Nanosecond => Some(
            array
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .map(|v| v.map(i128::from))
                .collect(),
        ),
        TimeUnit::Second | TimeUnit::Millisecond => None,
    }
}
