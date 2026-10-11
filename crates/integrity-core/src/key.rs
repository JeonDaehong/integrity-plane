//! Typed, versioned, order-preserving key encoding (spec §9, RFC 0001).

use std::fmt;

/// Format version byte of every [`EncodedKey`] produced by this module.
pub const KEY_FORMAT_VERSION: u8 = 1;

const NULL: u8 = 0x00;
const PRESENT: u8 = 0x01;
const ESCAPE: u8 = 0x00;
const ESCAPED_ZERO: u8 = 0xFF;
const TERMINATOR: u8 = 0x00;

/// A key type family (spec §9). Values of the same family are comparable and can match
/// across FK sides; physical widening within a family needs no re-encode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeFamily {
    /// `boolean`.
    Boolean,
    /// `int` and `long`.
    Integer,
    /// `decimal(p, scale)` for any precision; different scales are different families.
    Decimal {
        /// Number of digits after the decimal point.
        scale: u8,
    },
    /// `date`.
    Date,
    /// `timestamp` and `timestamp_ns` (without time zone).
    Timestamp,
    /// `timestamptz` and `timestamptz_ns`.
    TimestampTz,
    /// `string`, compared bytewise without collation.
    String,
    /// `binary` and `fixed(L)`.
    Binary,
    /// `uuid`.
    Uuid,
}

impl TypeFamily {
    const fn tag(self) -> u8 {
        match self {
            TypeFamily::Boolean => 0x01,
            TypeFamily::Integer => 0x02,
            TypeFamily::Decimal { .. } => 0x03,
            TypeFamily::Date => 0x04,
            TypeFamily::Timestamp => 0x05,
            TypeFamily::TimestampTz => 0x06,
            TypeFamily::String => 0x07,
            TypeFamily::Binary => 0x08,
            TypeFamily::Uuid => 0x09,
        }
    }
}

/// A non-NULL value of a key column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    /// Boolean family.
    Boolean(bool),
    /// Integer family (`int` values are widened to `i64`).
    Integer(i64),
    /// Decimal family: `unscaled · 10^-scale`.
    Decimal {
        /// The unscaled integer value.
        unscaled: i128,
        /// Number of digits after the decimal point.
        scale: u8,
    },
    /// Days since 1970-01-01.
    Date(i32),
    /// Nanoseconds since the epoch, without time zone.
    Timestamp(i128),
    /// Nanoseconds since the epoch, UTC-adjusted.
    TimestampTz(i128),
    /// UTF-8 string.
    String(String),
    /// Binary or fixed.
    Binary(Vec<u8>),
    /// UUID bytes in canonical (big-endian) order.
    Uuid([u8; 16]),
}

impl KeyValue {
    /// A `timestamp` with microsecond precision.
    pub fn timestamp_micros(micros: i64) -> Self {
        KeyValue::Timestamp(i128::from(micros) * 1_000)
    }

    /// A `timestamp_ns` value.
    pub fn timestamp_nanos(nanos: i64) -> Self {
        KeyValue::Timestamp(i128::from(nanos))
    }

    /// A `timestamptz` with microsecond precision.
    pub fn timestamptz_micros(micros: i64) -> Self {
        KeyValue::TimestampTz(i128::from(micros) * 1_000)
    }

    /// A `timestamptz_ns` value.
    pub fn timestamptz_nanos(nanos: i64) -> Self {
        KeyValue::TimestampTz(i128::from(nanos))
    }

    /// The family this value belongs to.
    pub fn family(&self) -> TypeFamily {
        match self {
            KeyValue::Boolean(_) => TypeFamily::Boolean,
            KeyValue::Integer(_) => TypeFamily::Integer,
            KeyValue::Decimal { scale, .. } => TypeFamily::Decimal { scale: *scale },
            KeyValue::Date(_) => TypeFamily::Date,
            KeyValue::Timestamp(_) => TypeFamily::Timestamp,
            KeyValue::TimestampTz(_) => TypeFamily::TimestampTz,
            KeyValue::String(_) => TypeFamily::String,
            KeyValue::Binary(_) => TypeFamily::Binary,
            KeyValue::Uuid(_) => TypeFamily::Uuid,
        }
    }
}

/// The ordered column families of a key. Composite keys have more than one column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeySchema(Vec<TypeFamily>);

impl KeySchema {
    /// Creates a schema; a key has at least one column.
    pub fn new(families: Vec<TypeFamily>) -> Result<Self, KeyError> {
        if families.is_empty() {
            return Err(KeyError::EmptySchema);
        }
        Ok(Self(families))
    }

    /// The column families in key order.
    pub fn families(&self) -> &[TypeFamily] {
        &self.0
    }

    /// Number of key columns.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always `false`: a schema has at least one column.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Checks that `values` has this schema's arity and that every non-NULL value
    /// belongs to its column's family.
    pub fn check(&self, values: &[Option<KeyValue>]) -> Result<(), KeyError> {
        if values.len() != self.0.len() {
            return Err(KeyError::ArityMismatch {
                expected: self.0.len(),
                actual: values.len(),
            });
        }
        for (column, (family, value)) in self.0.iter().zip(values).enumerate() {
            if let Some(v) = value
                && v.family() != *family
            {
                return Err(KeyError::FamilyMismatch {
                    column,
                    expected: *family,
                    actual: v.family(),
                });
            }
        }
        Ok(())
    }
}

/// Error building a key from typed values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyError {
    /// A key schema needs at least one column.
    EmptySchema,
    /// The number of values differs from the number of key columns.
    ArityMismatch {
        /// Columns in the schema.
        expected: usize,
        /// Values supplied.
        actual: usize,
    },
    /// A value's family differs from its column's family.
    FamilyMismatch {
        /// Zero-based key column index.
        column: usize,
        /// The column's family.
        expected: TypeFamily,
        /// The value's family.
        actual: TypeFamily,
    },
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyError::EmptySchema => f.write_str("key schema has no columns"),
            KeyError::ArityMismatch { expected, actual } => {
                write!(
                    f,
                    "key has {expected} columns but {actual} values were given"
                )
            }
            KeyError::FamilyMismatch {
                column,
                expected,
                actual,
            } => write!(
                f,
                "key column {column} expects {expected:?} but got a {actual:?} value"
            ),
        }
    }
}

impl std::error::Error for KeyError {}

/// An encoded key: memcomparable bytes in the RFC 0001 v1 format.
///
/// Byte equality is key equality and byte order is key order within one schema.
/// `Debug` deliberately does not print the bytes, because keys can contain PII (spec §24).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedKey(Box<[u8]>);

impl EncodedKey {
    /// Encodes a key tuple. NULL is `None`.
    pub fn encode(schema: &KeySchema, values: &[Option<KeyValue>]) -> Result<Self, KeyError> {
        schema.check(values)?;
        let mut out = Vec::with_capacity(1 + values.len() * 10);
        out.push(KEY_FORMAT_VERSION);
        for (family, value) in schema.families().iter().zip(values) {
            out.push(family.tag());
            if let TypeFamily::Decimal { scale } = family {
                out.push(*scale);
            }
            match value {
                None => out.push(NULL),
                Some(v) => {
                    out.push(PRESENT);
                    encode_value(v, &mut out);
                }
            }
        }
        Ok(Self(out.into_boxed_slice()))
    }

    /// Validates bytes (e.g. read back from an index backend) as a canonical v1 key.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        check(bytes)?;
        Ok(Self(bytes.into()))
    }

    /// [`EncodedKey::from_bytes`] taking ownership: no copy when `bytes` has no spare capacity.
    pub fn from_vec(bytes: Vec<u8>) -> Result<Self, DecodeError> {
        check(&bytes)?;
        Ok(Self(bytes.into_boxed_slice()))
    }

    /// Decodes into the schema and values it was encoded from.
    pub fn decode(&self) -> Result<(KeySchema, Vec<Option<KeyValue>>), DecodeError> {
        decode(&self.0)
    }

    /// The encoded bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EncodedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EncodedKey(<{} bytes>)", self.0.len())
    }
}

/// Appends to `out` exactly the bytes of `EncodedKey::encode` for a one-column key of `value`'s
/// own family holding `value`, without the allocations of building a schema and a key.
pub(crate) fn encode_single(value: &KeyValue, out: &mut Vec<u8>) {
    let family = value.family();
    out.push(KEY_FORMAT_VERSION);
    out.push(family.tag());
    if let TypeFamily::Decimal { scale } = family {
        out.push(scale);
    }
    out.push(PRESENT);
    encode_value(value, out);
}

fn encode_value(value: &KeyValue, out: &mut Vec<u8>) {
    match value {
        KeyValue::Boolean(b) => out.push(u8::from(*b)),
        KeyValue::Integer(v) => out.extend_from_slice(&((*v as u64) ^ (1 << 63)).to_be_bytes()),
        KeyValue::Decimal { unscaled: v, .. }
        | KeyValue::Timestamp(v)
        | KeyValue::TimestampTz(v) => {
            out.extend_from_slice(&((*v as u128) ^ (1 << 127)).to_be_bytes())
        }
        KeyValue::Date(v) => out.extend_from_slice(&((*v as u32) ^ (1 << 31)).to_be_bytes()),
        KeyValue::String(s) => encode_escaped(s.as_bytes(), out),
        KeyValue::Binary(b) => encode_escaped(b, out),
        KeyValue::Uuid(u) => out.extend_from_slice(u),
    }
}

fn encode_escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == ESCAPE {
            out.push(ESCAPED_ZERO);
        }
    }
    out.push(ESCAPE);
    out.push(TERMINATOR);
}

/// Error decoding bytes that are not a canonical v1 key. Decoding never panics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// No bytes, or a version byte followed by no columns.
    Empty,
    /// The version byte is not [`KEY_FORMAT_VERSION`].
    UnsupportedVersion(u8),
    /// Unknown type family tag.
    UnknownTag(u8),
    /// Null marker other than `0x00`/`0x01`.
    InvalidNullMarker(u8),
    /// Boolean byte other than `0x00`/`0x01`.
    InvalidBoolean(u8),
    /// `0x00` inside an escaped value followed by something other than `0x00`/`0xFF`.
    InvalidEscape,
    /// A String column is not valid UTF-8.
    InvalidUtf8,
    /// The input ended in the middle of a column.
    Truncated,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Empty => f.write_str("empty key"),
            DecodeError::UnsupportedVersion(v) => write!(f, "unsupported key format version {v}"),
            DecodeError::UnknownTag(t) => write!(f, "unknown key type tag {t:#04x}"),
            DecodeError::InvalidNullMarker(m) => write!(f, "invalid null marker {m:#04x}"),
            DecodeError::InvalidBoolean(b) => write!(f, "invalid boolean byte {b:#04x}"),
            DecodeError::InvalidEscape => f.write_str("invalid escape sequence"),
            DecodeError::InvalidUtf8 => f.write_str("string key column is not valid UTF-8"),
            DecodeError::Truncated => f.write_str("truncated key"),
        }
    }
}

impl std::error::Error for DecodeError {}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn byte(&mut self) -> Result<u8, DecodeError> {
        let (&b, rest) = self.bytes.split_first().ok_or(DecodeError::Truncated)?;
        self.bytes = rest;
        Ok(b)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let (head, rest) = self
            .bytes
            .split_first_chunk::<N>()
            .ok_or(DecodeError::Truncated)?;
        self.bytes = rest;
        Ok(*head)
    }

    /// Reads past an escaped value like [`Reader::escaped`], failing the same way; with `utf8`,
    /// also as `String::from_utf8` of the unescaped bytes would (after the terminator). The
    /// unescaped bytes are the stretches between escapes joined by single zero bytes, and a zero
    /// byte is a whole UTF-8 character, so they are valid iff every stretch is.
    fn skip_escaped(&mut self, utf8: bool) -> Result<(), DecodeError> {
        let mut valid = true;
        loop {
            let at = self
                .bytes
                .iter()
                .position(|&b| b == ESCAPE)
                .ok_or(DecodeError::Truncated)?;
            valid &= !utf8 || std::str::from_utf8(&self.bytes[..at]).is_ok();
            self.bytes = &self.bytes[at + 1..];
            match self.byte()? {
                TERMINATOR if valid => return Ok(()),
                TERMINATOR => return Err(DecodeError::InvalidUtf8),
                ESCAPED_ZERO => {}
                _ => return Err(DecodeError::InvalidEscape),
            }
        }
    }

    fn escaped(&mut self) -> Result<Vec<u8>, DecodeError> {
        let mut out = Vec::new();
        loop {
            let b = self.byte()?;
            if b != ESCAPE {
                out.push(b);
                continue;
            }
            match self.byte()? {
                TERMINATOR => return Ok(out),
                ESCAPED_ZERO => out.push(0x00),
                _ => return Err(DecodeError::InvalidEscape),
            }
        }
    }
}

/// Whether `bytes` decode ([`decode`]), with the same error, without building the values: keys
/// read back from sort runs and index files are checked one by one.
fn check(bytes: &[u8]) -> Result<(), DecodeError> {
    let mut r = Reader { bytes };
    let version = r.byte().map_err(|_| DecodeError::Empty)?;
    if version != KEY_FORMAT_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    if r.bytes.is_empty() {
        return Err(DecodeError::Empty);
    }
    while !r.bytes.is_empty() {
        let tag = r.byte()?;
        if tag == 0x03 {
            r.byte()?; // scale
        } else if !(0x01..=0x09).contains(&tag) {
            return Err(DecodeError::UnknownTag(tag));
        }
        match r.byte()? {
            NULL => {}
            PRESENT => match tag {
                0x01 => {
                    let b = r.byte()?;
                    if b > 0x01 {
                        return Err(DecodeError::InvalidBoolean(b));
                    }
                }
                0x02 => {
                    r.array::<8>()?;
                }
                0x03 | 0x05 | 0x06 => {
                    r.array::<16>()?;
                }
                0x04 => {
                    r.array::<4>()?;
                }
                0x07 => r.skip_escaped(true)?,
                0x08 => r.skip_escaped(false)?,
                _ => {
                    r.array::<16>()?;
                }
            },
            m => return Err(DecodeError::InvalidNullMarker(m)),
        }
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<(KeySchema, Vec<Option<KeyValue>>), DecodeError> {
    let mut r = Reader { bytes };
    let version = r.byte().map_err(|_| DecodeError::Empty)?;
    if version != KEY_FORMAT_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    if r.bytes.is_empty() {
        return Err(DecodeError::Empty);
    }
    let mut families = Vec::new();
    let mut values = Vec::new();
    while !r.bytes.is_empty() {
        let family = match r.byte()? {
            0x01 => TypeFamily::Boolean,
            0x02 => TypeFamily::Integer,
            0x03 => TypeFamily::Decimal { scale: r.byte()? },
            0x04 => TypeFamily::Date,
            0x05 => TypeFamily::Timestamp,
            0x06 => TypeFamily::TimestampTz,
            0x07 => TypeFamily::String,
            0x08 => TypeFamily::Binary,
            0x09 => TypeFamily::Uuid,
            t => return Err(DecodeError::UnknownTag(t)),
        };
        let value = match r.byte()? {
            NULL => None,
            PRESENT => Some(decode_value(family, &mut r)?),
            m => return Err(DecodeError::InvalidNullMarker(m)),
        };
        families.push(family);
        values.push(value);
    }
    Ok((KeySchema(families), values))
}

fn decode_value(family: TypeFamily, r: &mut Reader<'_>) -> Result<KeyValue, DecodeError> {
    let i128_value = |r: &mut Reader<'_>| -> Result<i128, DecodeError> {
        Ok((u128::from_be_bytes(r.array()?) ^ (1 << 127)) as i128)
    };
    Ok(match family {
        TypeFamily::Boolean => match r.byte()? {
            0x00 => KeyValue::Boolean(false),
            0x01 => KeyValue::Boolean(true),
            b => return Err(DecodeError::InvalidBoolean(b)),
        },
        TypeFamily::Integer => {
            KeyValue::Integer((u64::from_be_bytes(r.array()?) ^ (1 << 63)) as i64)
        }
        TypeFamily::Decimal { scale } => KeyValue::Decimal {
            unscaled: i128_value(r)?,
            scale,
        },
        TypeFamily::Date => KeyValue::Date((u32::from_be_bytes(r.array()?) ^ (1 << 31)) as i32),
        TypeFamily::Timestamp => KeyValue::Timestamp(i128_value(r)?),
        TypeFamily::TimestampTz => KeyValue::TimestampTz(i128_value(r)?),
        TypeFamily::String => {
            KeyValue::String(String::from_utf8(r.escaped()?).map_err(|_| DecodeError::InvalidUtf8)?)
        }
        TypeFamily::Binary => KeyValue::Binary(r.escaped()?),
        TypeFamily::Uuid => KeyValue::Uuid(r.array()?),
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Payload bytes: escapes, UTF-8 lead and continuation bytes, booleans.
    fn payload() -> impl Strategy<Value = Vec<u8>> {
        let byte = prop_oneof![
            Just(0x00),
            Just(0x01),
            Just(0x02),
            Just(0xff),
            Just(0x80),
            Just(0xc3),
            Just(0xe2),
            Just(b'a'),
            any::<u8>(),
        ];
        prop_oneof![
            // Fixed-size values, sometimes a byte short or long.
            (
                prop_oneof![
                    Just(1usize),
                    Just(3),
                    Just(4),
                    Just(5),
                    Just(7),
                    Just(8),
                    Just(9),
                    Just(15),
                    Just(16),
                    Just(17)
                ],
                any::<u8>()
            )
                .prop_map(|(n, b)| vec![b; n]),
            // Escaped values: content, then usually a terminator.
            (proptest::collection::vec(byte, 0..8), any::<bool>()).prop_map(|(mut v, end)| {
                if end {
                    v.extend_from_slice(&[0x00, 0x00]);
                }
                v
            }),
        ]
    }

    /// Byte strings built column by column like keys (tag, scale, NULL marker, value), with
    /// wrong tags, markers and lengths mixed in, and sometimes cut short.
    fn near_keys() -> impl Strategy<Value = Vec<u8>> {
        let tag = prop_oneof![1u8..=9, Just(0u8), Just(0x0a)];
        let marker = prop_oneof![Just(0u8), Just(1u8), Just(1u8), Just(1u8), Just(2u8)];
        let column = (tag, any::<u8>(), marker, payload());
        (
            proptest::collection::vec(column, 0..4),
            any::<prop::sample::Index>(),
            any::<bool>(),
        )
            .prop_map(|(columns, cut, truncate)| {
                let mut v = vec![KEY_FORMAT_VERSION];
                for (tag, scale, marker, payload) in columns {
                    v.push(tag);
                    if tag == 0x03 {
                        v.push(scale);
                    }
                    v.push(marker);
                    if marker == 1 {
                        v.extend_from_slice(&payload);
                    }
                }
                if truncate {
                    v.truncate(cut.index(v.len() + 1));
                }
                v
            })
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config { cases: 4096, ..Default::default() })]

        #[test]
        fn check_agrees_with_decode(bytes in near_keys()) {
            prop_assert_eq!(check(&bytes), decode(&bytes).map(|_| ()));
        }

        #[test]
        fn check_agrees_with_decode_on_any_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..24)) {
            prop_assert_eq!(check(&bytes), decode(&bytes).map(|_| ()));
        }
    }
}
