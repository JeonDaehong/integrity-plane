//! proptest strategies for key schemas, key values and tuples.
//!
//! Values mix small domains (so that random tuples collide, share prefixes and hit parents)
//! with full ranges and extremes.

use integrity_core::{KeySchema, KeyValue, LogicalType, TypeFamily};
use proptest::collection::vec;
use proptest::prelude::*;

/// A key tuple: `None` is NULL.
pub type Tuple = Vec<Option<KeyValue>>;

/// Any supported key family (decimal scales 0..=3).
pub fn family() -> impl Strategy<Value = TypeFamily> {
    prop_oneof![
        Just(TypeFamily::Boolean),
        Just(TypeFamily::Integer),
        (0u8..=3).prop_map(|scale| TypeFamily::Decimal { scale }),
        Just(TypeFamily::Date),
        Just(TypeFamily::Timestamp),
        Just(TypeFamily::TimestampTz),
        Just(TypeFamily::String),
        Just(TypeFamily::Binary),
        Just(TypeFamily::Uuid),
    ]
}

/// A logical column type whose key family is `f`.
pub fn logical_type(f: TypeFamily) -> LogicalType {
    match f {
        TypeFamily::Boolean => LogicalType::Boolean,
        TypeFamily::Integer => LogicalType::Long,
        TypeFamily::Decimal { scale } => LogicalType::Decimal {
            precision: 38,
            scale,
        },
        TypeFamily::Date => LogicalType::Date,
        TypeFamily::Timestamp => LogicalType::Timestamp,
        TypeFamily::TimestampTz => LogicalType::TimestampTz,
        TypeFamily::String => LogicalType::String,
        TypeFamily::Binary => LogicalType::Binary,
        TypeFamily::Uuid => LogicalType::Uuid,
    }
}

/// Values of one family.
pub fn value(f: TypeFamily) -> BoxedStrategy<KeyValue> {
    match f {
        TypeFamily::Boolean => any::<bool>().prop_map(KeyValue::Boolean).boxed(),
        TypeFamily::Integer => {
            prop_oneof![-2i64..=2, any::<i64>(), Just(i64::MIN), Just(i64::MAX),]
                .prop_map(KeyValue::Integer)
                .boxed()
        }
        TypeFamily::Decimal { scale } => {
            prop_oneof![-2i128..=2, any::<i128>(), Just(i128::MIN), Just(i128::MAX)]
                .prop_map(move |unscaled| KeyValue::Decimal { unscaled, scale })
                .boxed()
        }
        TypeFamily::Date => prop_oneof![-2i32..=2, any::<i32>()]
            .prop_map(KeyValue::Date)
            .boxed(),
        TypeFamily::Timestamp => prop_oneof![
            (-2i128..=2).prop_map(KeyValue::Timestamp),
            any::<i64>().prop_map(KeyValue::timestamp_micros),
            any::<i64>().prop_map(KeyValue::timestamp_nanos),
        ]
        .boxed(),
        TypeFamily::TimestampTz => prop_oneof![
            (-2i128..=2).prop_map(KeyValue::TimestampTz),
            any::<i64>().prop_map(KeyValue::timestamptz_micros),
        ]
        .boxed(),
        TypeFamily::String => prop_oneof!["[a\\x00b]{0,3}", any::<String>()]
            .prop_map(KeyValue::String)
            .boxed(),
        TypeFamily::Binary => prop_oneof![
            vec(prop_oneof![Just(0x00u8), Just(0xFF), Just(b'a')], 0..4),
            vec(any::<u8>(), 0..16),
        ]
        .prop_map(KeyValue::Binary)
        .boxed(),
        TypeFamily::Uuid => prop_oneof![(0u8..3).prop_map(|b| [b; 16]), any::<[u8; 16]>()]
            .prop_map(KeyValue::Uuid)
            .boxed(),
    }
}

/// A key schema of 1..=3 columns.
pub fn schema() -> impl Strategy<Value = KeySchema> {
    vec(family(), 1..=3).prop_map(|f| KeySchema::new(f).unwrap_or_else(|_| unreachable!()))
}

/// A tuple for `schema`; each cell is non-NULL with probability `present` (strictly between 0 and 1).
pub fn tuple(schema: &KeySchema, present: f64) -> BoxedStrategy<Tuple> {
    schema
        .families()
        .iter()
        .map(|&f| proptest::option::weighted(present, value(f)).boxed())
        .collect::<Vec<_>>()
        .boxed()
}

/// A tuple for `schema` with no NULLs.
pub fn complete_tuple(schema: &KeySchema) -> BoxedStrategy<Tuple> {
    schema
        .families()
        .iter()
        .map(|&f| value(f).prop_map(Some).boxed())
        .collect::<Vec<_>>()
        .boxed()
}
