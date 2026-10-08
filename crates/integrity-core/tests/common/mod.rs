//! Shared proptest strategies and a reference ordering independent of the encoder.

#![allow(dead_code)]

use std::cmp::Ordering;

use integrity_core::{EncodedKey, KeySchema, KeyValue, TypeFamily};
use proptest::collection::vec;
use proptest::prelude::*;

pub type Tuple = Vec<Option<KeyValue>>;

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

/// Values of one family. Mixes small domains (so that random pairs collide and share
/// prefixes) with the full range and the extremes.
pub fn value(f: TypeFamily) -> BoxedStrategy<KeyValue> {
    match f {
        TypeFamily::Boolean => any::<bool>().prop_map(KeyValue::Boolean).boxed(),
        TypeFamily::Integer => prop_oneof![
            -2i64..=2,
            any::<i64>(),
            Just(i64::MIN),
            Just(i64::MAX),
            any::<i32>().prop_map(i64::from),
        ]
        .prop_map(KeyValue::Integer)
        .boxed(),
        TypeFamily::Decimal { scale } => {
            prop_oneof![-2i128..=2, any::<i128>(), Just(i128::MIN), Just(i128::MAX)]
                .prop_map(move |unscaled| KeyValue::Decimal { unscaled, scale })
                .boxed()
        }
        TypeFamily::Date => prop_oneof![-2i32..=2, any::<i32>(), Just(i32::MIN), Just(i32::MAX)]
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
            any::<i64>().prop_map(KeyValue::timestamptz_nanos),
        ]
        .boxed(),
        TypeFamily::String => prop_oneof!["[a\\x00b]{0,3}", any::<String>(),]
            .prop_map(KeyValue::String)
            .boxed(),
        TypeFamily::Binary => prop_oneof![
            vec(
                prop_oneof![Just(0x00u8), Just(0x01), Just(0xFF), Just(b'a')],
                0..4
            ),
            vec(any::<u8>(), 0..16),
        ]
        .prop_map(KeyValue::Binary)
        .boxed(),
        TypeFamily::Uuid => prop_oneof![(0u8..3).prop_map(|b| [b; 16]), any::<[u8; 16]>(),]
            .prop_map(KeyValue::Uuid)
            .boxed(),
    }
}

/// A key cell: NULL with probability `1 - present`.
pub fn cell(f: TypeFamily, present: f64) -> BoxedStrategy<Option<KeyValue>> {
    proptest::option::weighted(present, value(f)).boxed()
}

pub fn schema() -> impl Strategy<Value = KeySchema> {
    vec(family(), 1..=4).prop_map(|f| KeySchema::new(f).expect("non-empty"))
}

pub fn tuple(schema: &KeySchema, present: f64) -> BoxedStrategy<Tuple> {
    schema
        .families()
        .iter()
        .map(|&f| cell(f, present))
        .collect::<Vec<_>>()
        .boxed()
}

pub fn schema_and_tuple(present: f64) -> impl Strategy<Value = (KeySchema, Tuple)> {
    schema().prop_flat_map(move |s| {
        let t = tuple(&s, present);
        (Just(s), t)
    })
}

pub fn schema_and_two_tuples(present: f64) -> impl Strategy<Value = (KeySchema, Tuple, Tuple)> {
    schema().prop_flat_map(move |s| {
        let a = tuple(&s, present);
        let b = tuple(&s, present);
        (Just(s), a, b)
    })
}

pub fn encode(schema: &KeySchema, t: &[Option<KeyValue>]) -> EncodedKey {
    EncodedKey::encode(schema, t).expect("tuple matches schema")
}

/// The order RFC 0001 promises, written without reference to the encoding.
pub fn cmp_value(a: &KeyValue, b: &KeyValue) -> Ordering {
    use KeyValue::*;
    match (a, b) {
        (Boolean(x), Boolean(y)) => x.cmp(y),
        (Integer(x), Integer(y)) => x.cmp(y),
        (
            Decimal {
                unscaled: x,
                scale: sx,
            },
            Decimal {
                unscaled: y,
                scale: sy,
            },
        ) => {
            assert_eq!(sx, sy, "compared decimals of different scales");
            x.cmp(y)
        }
        (Date(x), Date(y)) => x.cmp(y),
        (Timestamp(x), Timestamp(y)) | (TimestampTz(x), TimestampTz(y)) => x.cmp(y),
        (String(x), String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Binary(x), Binary(y)) => x.cmp(y),
        (Uuid(x), Uuid(y)) => x.cmp(y),
        _ => panic!("compared values of different families"),
    }
}

/// Lexicographic tuple order with NULL before every value.
pub fn cmp_tuple(a: &[Option<KeyValue>], b: &[Option<KeyValue>]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        let o = match (x, y) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(x), Some(y)) => cmp_value(x, y),
        };
        if o != Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}
