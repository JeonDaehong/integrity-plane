//! Spec §9 / RFC 0001: key encoding unit and property tests.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use common::*;
use integrity_core::{
    DecodeError, EncodedKey, KEY_FORMAT_VERSION, KeyError, KeySchema, KeyValue, TypeFamily,
};
use proptest::prelude::*;

fn schema(f: &[TypeFamily]) -> KeySchema {
    KeySchema::new(f.to_vec()).unwrap()
}

fn one(family: TypeFamily, v: Option<KeyValue>) -> EncodedKey {
    EncodedKey::encode(&schema(&[family]), &[v]).unwrap()
}

// ---------- golden bytes: pin the RFC 0001 layout ----------

#[test]
fn golden_integer() {
    let k = one(TypeFamily::Integer, Some(KeyValue::Integer(1)));
    assert_eq!(
        k.as_bytes(),
        [0x01, 0x02, 0x01, 0x80, 0, 0, 0, 0, 0, 0, 0x01]
    );
    let k = one(TypeFamily::Integer, Some(KeyValue::Integer(-1)));
    assert_eq!(
        k.as_bytes(),
        [
            0x01, 0x02, 0x01, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
        ]
    );
}

#[test]
fn golden_null() {
    let k = one(TypeFamily::Integer, None);
    assert_eq!(k.as_bytes(), [0x01, 0x02, 0x00]);
}

#[test]
fn golden_string_escaping() {
    let k = one(TypeFamily::String, Some(KeyValue::String("a\0".into())));
    assert_eq!(
        k.as_bytes(),
        [0x01, 0x07, 0x01, b'a', 0x00, 0xFF, 0x00, 0x00]
    );
}

#[test]
fn golden_decimal_carries_scale() {
    let k = one(
        TypeFamily::Decimal { scale: 2 },
        Some(KeyValue::Decimal {
            unscaled: 0,
            scale: 2,
        }),
    );
    let mut expected = vec![0x01, 0x03, 0x02, 0x01, 0x80];
    expected.extend([0u8; 15]);
    assert_eq!(k.as_bytes(), expected.as_slice());
}

#[test]
fn golden_composite() {
    let s = schema(&[TypeFamily::Boolean, TypeFamily::Uuid]);
    let k = EncodedKey::encode(&s, &[Some(KeyValue::Boolean(true)), None]).unwrap();
    assert_eq!(
        k.as_bytes(),
        [KEY_FORMAT_VERSION, 0x01, 0x01, 0x01, 0x09, 0x00]
    );
}

// ---------- family rules ----------

#[test]
fn int_widened_to_long_encodes_identically() {
    let from_int = KeyValue::Integer(i64::from(42i32));
    let from_long = KeyValue::Integer(42i64);
    assert_eq!(
        one(TypeFamily::Integer, Some(from_int)),
        one(TypeFamily::Integer, Some(from_long))
    );
}

#[test]
fn timestamp_micros_equal_nanos() {
    assert_eq!(
        one(TypeFamily::Timestamp, Some(KeyValue::timestamp_micros(7))),
        one(
            TypeFamily::Timestamp,
            Some(KeyValue::timestamp_nanos(7_000))
        )
    );
    assert!(
        one(TypeFamily::Timestamp, Some(KeyValue::timestamp_micros(7)))
            < one(
                TypeFamily::Timestamp,
                Some(KeyValue::timestamp_nanos(7_001))
            )
    );
}

#[test]
fn timestamp_tz_and_ntz_never_equal() {
    assert_ne!(
        one(TypeFamily::Timestamp, Some(KeyValue::timestamp_micros(7))),
        one(
            TypeFamily::TimestampTz,
            Some(KeyValue::timestamptz_micros(7))
        )
    );
}

#[test]
fn decimal_same_unscaled_different_scale_never_equal() {
    // 1.00 (scale 2) vs 100 (scale 0): same unscaled value, different numbers.
    assert_ne!(
        one(
            TypeFamily::Decimal { scale: 2 },
            Some(KeyValue::Decimal {
                unscaled: 100,
                scale: 2
            })
        ),
        one(
            TypeFamily::Decimal { scale: 0 },
            Some(KeyValue::Decimal {
                unscaled: 100,
                scale: 0
            })
        )
    );
}

#[test]
fn string_and_binary_with_same_bytes_never_equal() {
    assert_ne!(
        one(TypeFamily::String, Some(KeyValue::String("ab".into()))),
        one(TypeFamily::Binary, Some(KeyValue::Binary(b"ab".to_vec())))
    );
}

// ---------- ordering edge cases ----------

#[test]
fn integer_order_across_sign() {
    let keys: Vec<_> = [i64::MIN, -1, 0, 1, i64::MAX]
        .into_iter()
        .map(|v| one(TypeFamily::Integer, Some(KeyValue::Integer(v))))
        .collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn string_order_with_embedded_zero_and_prefixes() {
    let keys: Vec<_> = ["", "\0", "\0\0", "\u{1}", "a", "a\0", "ab", "b"]
        .into_iter()
        .map(|s| one(TypeFamily::String, Some(KeyValue::String(s.into()))))
        .collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn composite_keys_are_unambiguous() {
    // ("a", "bc") vs ("ab", "c"): naive concatenation would collide.
    let s = schema(&[TypeFamily::String, TypeFamily::String]);
    let k = |a: &str, b: &str| {
        EncodedKey::encode(
            &s,
            &[
                Some(KeyValue::String(a.into())),
                Some(KeyValue::String(b.into())),
            ],
        )
        .unwrap()
    };
    assert_ne!(k("a", "bc"), k("ab", "c"));
    assert!(k("a", "bc") < k("ab", "c"));
    assert!(k("a", "\u{10FFFF}") < k("a\0", ""));
}

#[test]
fn null_sorts_first() {
    assert!(
        one(TypeFamily::Integer, None)
            < one(TypeFamily::Integer, Some(KeyValue::Integer(i64::MIN)))
    );
}

// ---------- input validation ----------

#[test]
fn encode_rejects_mismatched_tuples() {
    let s = schema(&[TypeFamily::Integer, TypeFamily::String]);
    assert_eq!(
        EncodedKey::encode(&s, &[Some(KeyValue::Integer(1))]),
        Err(KeyError::ArityMismatch {
            expected: 2,
            actual: 1
        })
    );
    assert_eq!(
        EncodedKey::encode(
            &s,
            &[Some(KeyValue::Integer(1)), Some(KeyValue::Integer(2))]
        ),
        Err(KeyError::FamilyMismatch {
            column: 1,
            expected: TypeFamily::String,
            actual: TypeFamily::Integer
        })
    );
    assert_eq!(KeySchema::new(vec![]), Err(KeyError::EmptySchema));
}

#[test]
fn decode_rejects_non_canonical_input() {
    let cases: &[(&[u8], DecodeError)] = &[
        (&[], DecodeError::Empty),
        (&[0x01], DecodeError::Empty),
        (&[0x02, 0x02, 0x00], DecodeError::UnsupportedVersion(2)),
        (&[0x01, 0x0A, 0x00], DecodeError::UnknownTag(0x0A)),
        (&[0x01, 0x00, 0x00], DecodeError::UnknownTag(0x00)),
        (&[0x01, 0x02, 0x02], DecodeError::InvalidNullMarker(0x02)),
        (&[0x01, 0x01, 0x01, 0x02], DecodeError::InvalidBoolean(0x02)),
        (&[0x01, 0x02, 0x01, 0x80, 0x00], DecodeError::Truncated),
        (&[0x01, 0x07, 0x01, b'a', 0x00], DecodeError::Truncated),
        (&[0x01, 0x07, 0x01, b'a'], DecodeError::Truncated),
        (&[0x01, 0x07, 0x01, 0x00, 0x01], DecodeError::InvalidEscape),
        (
            &[0x01, 0x07, 0x01, 0xC3, 0x00, 0x00],
            DecodeError::InvalidUtf8,
        ),
        (&[0x01, 0x03], DecodeError::Truncated),
        (&[0x01, 0x02], DecodeError::Truncated),
    ];
    for (bytes, err) in cases {
        assert_eq!(
            EncodedKey::from_bytes(bytes).err().as_ref(),
            Some(err),
            "{bytes:02x?}"
        );
    }
}

#[test]
fn debug_does_not_print_key_bytes() {
    let k = one(
        TypeFamily::String,
        Some(KeyValue::String("secret@example.com".into())),
    );
    let shown = format!("{k:?}");
    assert!(!shown.contains("secret"), "{shown}");
    assert_eq!(shown, format!("EncodedKey(<{} bytes>)", k.as_bytes().len()));
}

// ---------- properties ----------

proptest! {
    #[test]
    fn decode_inverts_encode((s, t) in schema_and_tuple(0.8)) {
        let k = encode(&s, &t);
        prop_assert_eq!(k.decode().unwrap(), (s, t));
        prop_assert_eq!(EncodedKey::from_bytes(k.as_bytes()).unwrap(), k);
    }

    #[test]
    fn equality_iff_same_tuple((s, a, b) in schema_and_two_tuples(0.7)) {
        prop_assert_eq!(encode(&s, &a) == encode(&s, &b), a == b);
    }

    #[test]
    fn order_preserved((s, a, b) in schema_and_two_tuples(0.7)) {
        prop_assert_eq!(encode(&s, &a).cmp(&encode(&s, &b)), cmp_tuple(&a, &b));
    }

    #[test]
    fn different_schemas_never_collide(
        (s1, t1) in schema_and_tuple(0.7),
        (s2, t2) in schema_and_tuple(0.7),
    ) {
        let equal = encode(&s1, &t1) == encode(&s2, &t2);
        prop_assert_eq!(equal, s1 == s2 && t1 == t2);
    }

    #[test]
    fn decode_arbitrary_bytes_is_canonical(bytes in proptest::collection::vec(any::<u8>(), 0..48)) {
        if let Ok(k) = EncodedKey::from_bytes(&bytes) {
            let (s, t) = k.decode().unwrap();
            let reencoded = encode(&s, &t);
            prop_assert_eq!(reencoded.as_bytes(), bytes.as_slice());
        }
    }

    #[test]
    fn decode_mutated_keys_is_canonical(
        (s, t) in schema_and_tuple(0.8),
        pos in any::<prop::sample::Index>(),
        byte in any::<u8>(),
        cut in any::<prop::sample::Index>(),
    ) {
        let mut bytes = encode(&s, &t).as_bytes().to_vec();
        let i = pos.index(bytes.len());
        bytes[i] = byte;
        bytes.truncate(cut.index(bytes.len() + 1).max(1));
        if let Ok(k) = EncodedKey::from_bytes(&bytes) {
            let (s2, t2) = k.decode().unwrap();
            let reencoded = encode(&s2, &t2);
            prop_assert_eq!(reencoded.as_bytes(), bytes.as_slice());
        }
    }
}
