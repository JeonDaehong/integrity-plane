//! Partition specs and transforms (Iceberg spec, "Partitioning", Appendix A "Partition Transforms"
//! and Appendix B "32-bit Hash Requirements"), for the partition invariant of ADR 0032: every row
//! of a data file lies in the partition its manifest entry names.
//!
//! Only what the Plane can compute exactly is computed; anything else is an error, and the caller
//! refuses what depends on it (fail closed).

use std::collections::BTreeMap;
use std::fmt;

use apache_avro::types::Value;
use integrity_core::{Datum, KeyValue, LogicalType};
use integrity_types::FieldId;

/// A partition transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transform {
    /// The source value.
    Identity,
    /// `bucket[N]`: Murmur3 hash modulo `N`.
    Bucket(u32),
    /// `truncate[W]`.
    Truncate(u32),
    /// Years since 1970.
    Year,
    /// Months since 1970-01.
    Month,
    /// Days since 1970-01-01.
    Day,
    /// Hours since 1970-01-01 00:00.
    Hour,
    /// Always NULL.
    Void,
}

impl Transform {
    /// Parses a transform name; `None` for one the Plane does not compute.
    pub fn parse(s: &str) -> Option<Self> {
        let arg = |prefix: &str| {
            s.strip_prefix(prefix)
                .and_then(|r| r.strip_suffix(']'))
                .and_then(|n| n.trim().parse::<u32>().ok())
                .filter(|n| *n > 0)
        };
        Some(match s.trim() {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            _ => match arg("bucket[") {
                Some(n) => Transform::Bucket(n),
                None => Transform::Truncate(arg("truncate[")?),
            },
        })
    }
}

/// A partition value, as manifests store it, normalized for comparison: integers of any width,
/// dates, times and timestamps (in their source unit) are `Int`; `fixed`, `binary` and `uuid` are
/// `Bytes`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartValue {
    /// NULL.
    Null,
    /// Boolean.
    Bool(bool),
    /// Any integer-like value.
    Int(i64),
    /// Decimal, unscaled (the scale is the source type's).
    Decimal(i128),
    /// String.
    Str(String),
    /// Bytes.
    Bytes(Vec<u8>),
    /// A value the Plane does not compare; equal to nothing a transform produces.
    Other(String),
}

impl PartValue {
    /// Normalizes an Avro partition field value.
    pub fn from_avro(v: &Value) -> Self {
        match v {
            Value::Null => PartValue::Null,
            Value::Union(_, inner) => PartValue::from_avro(inner),
            Value::Boolean(b) => PartValue::Bool(*b),
            Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => PartValue::Int(i64::from(*i)),
            Value::Long(i)
            | Value::TimeMicros(i)
            | Value::TimestampMicros(i)
            | Value::TimestampNanos(i)
            | Value::LocalTimestampMicros(i)
            | Value::LocalTimestampNanos(i) => PartValue::Int(*i),
            Value::String(s) => PartValue::Str(s.clone()),
            Value::Bytes(b) | Value::Fixed(_, b) => PartValue::Bytes(b.clone()),
            Value::Uuid(u) => PartValue::Bytes(u.as_bytes().to_vec()),
            Value::Decimal(d) => match <Vec<u8>>::try_from(d) {
                Ok(bytes) => match unscaled(&bytes) {
                    Some(u) => PartValue::Decimal(u),
                    None => PartValue::Other(format!("{v:?}")),
                },
                Err(_) => PartValue::Other(format!("{v:?}")),
            },
            other => PartValue::Other(format!("{other:?}")),
        }
    }
}

/// A big-endian two's complement integer of at most 16 bytes.
fn unscaled(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let fill = if bytes[0] & 0x80 != 0 { 0xff } else { 0 };
    let mut full = [fill; 16];
    full[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(full))
}

/// Why a transform cannot be computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uncomputable(pub String);

impl fmt::Display for Uncomputable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn cannot(t: Transform, ty: &LogicalType) -> Uncomputable {
    Uncomputable(format!("transform {t:?} of a {ty:?} value"))
}

/// Murmur3 x86 32-bit, seed 0 (Appendix B).
pub fn murmur3(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h: u32 = 0;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k: u32 = 0;
        for (i, b) in tail.iter().enumerate() {
            k |= u32::from(*b) << (8 * i);
        }
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

/// The minimum number of bytes of two's complement big-endian `v` (Appendix B, decimals).
fn min_bytes(v: i128) -> Vec<u8> {
    let bytes = v.to_be_bytes();
    let mut start = 0;
    while start < 15 {
        let (b, next) = (bytes[start], bytes[start + 1]);
        if (b == 0 && next & 0x80 == 0) || (b == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

const NANOS_PER_MICRO: i128 = 1_000;
const NANOS_PER_HOUR: i128 = 3_600 * 1_000_000_000;
const NANOS_PER_DAY: i128 = 24 * NANOS_PER_HOUR;

/// Year and month (1–12) of a day count since 1970-01-01 (proleptic Gregorian).
fn civil(days: i64) -> (i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

fn is_micros(ty: &LogicalType) -> bool {
    matches!(ty, LogicalType::Timestamp | LogicalType::TimestampTz)
}

fn is_nanos(ty: &LogicalType) -> bool {
    matches!(ty, LogicalType::TimestampNs | LogicalType::TimestampTzNs)
}

/// Whether the Plane computes `t` for values of type `ty`; [`apply`] then fails only on values
/// that cannot occur in a valid file.
pub fn supports(t: Transform, ty: &LogicalType) -> bool {
    use LogicalType as L;
    let integer = matches!(ty, L::Int | L::Long);
    let micros = is_micros(ty);
    match t {
        Transform::Void => true,
        Transform::Identity => {
            integer
                || micros
                || is_nanos(ty)
                || matches!(
                    ty,
                    L::Boolean
                        | L::Date
                        | L::Decimal { .. }
                        | L::String
                        | L::Binary
                        | L::Fixed(_)
                        | L::Uuid
                )
        }
        Transform::Bucket(_) => {
            integer
                || micros
                || matches!(
                    ty,
                    L::Date | L::Decimal { .. } | L::String | L::Binary | L::Fixed(_) | L::Uuid
                )
        }
        Transform::Truncate(_) => {
            integer || matches!(ty, L::Decimal { .. } | L::String | L::Binary)
        }
        Transform::Year | Transform::Month | Transform::Day => {
            micros || is_nanos(ty) || matches!(ty, L::Date)
        }
        Transform::Hour => micros || is_nanos(ty),
    }
}

/// Applies `t` to `v`, a value of a column of type `ty` as the Plane reads keys (ADR 0006).
/// Temporal transforms count whole units since 1970 rounding down, also before 1970.
pub fn apply(t: Transform, ty: &LogicalType, v: &Datum) -> Result<PartValue, Uncomputable> {
    let v = match v {
        Datum::Null => return Ok(PartValue::Null),
        Datum::Opaque => return Err(cannot(t, ty)),
        Datum::Value(v) => v,
    };
    let int = |i: i64| Ok(PartValue::Int(i));
    match t {
        Transform::Void => Ok(PartValue::Null),
        Transform::Identity => match (v, ty) {
            (KeyValue::Boolean(b), _) => Ok(PartValue::Bool(*b)),
            (KeyValue::Integer(i), LogicalType::Int | LogicalType::Long) => int(*i),
            (KeyValue::Date(d), LogicalType::Date) => int(i64::from(*d)),
            (KeyValue::Timestamp(n) | KeyValue::TimestampTz(n), ty) if is_micros(ty) => {
                micros(*n, t, ty).map(PartValue::Int)
            }
            (KeyValue::Timestamp(n) | KeyValue::TimestampTz(n), ty) if is_nanos(ty) => {
                i64::try_from(*n)
                    .map(PartValue::Int)
                    .map_err(|_| cannot(t, ty))
            }
            (KeyValue::Decimal { unscaled, .. }, LogicalType::Decimal { .. }) => {
                Ok(PartValue::Decimal(*unscaled))
            }
            (KeyValue::String(s), LogicalType::String) => Ok(PartValue::Str(s.clone())),
            (KeyValue::Binary(b), LogicalType::Binary | LogicalType::Fixed(_)) => {
                Ok(PartValue::Bytes(b.clone()))
            }
            (KeyValue::Uuid(u), LogicalType::Uuid) => Ok(PartValue::Bytes(u.to_vec())),
            _ => Err(cannot(t, ty)),
        },
        Transform::Bucket(n) => {
            let hash = match (v, ty) {
                // `int` hashes as `long` (Appendix B), so widening keeps buckets.
                (KeyValue::Integer(i), LogicalType::Int | LogicalType::Long) => {
                    murmur3(&i.to_le_bytes())
                }
                (KeyValue::Date(d), LogicalType::Date) => murmur3(&i64::from(*d).to_le_bytes()),
                (KeyValue::Timestamp(n) | KeyValue::TimestampTz(n), ty) if is_micros(ty) => {
                    murmur3(&micros(*n, t, ty)?.to_le_bytes())
                }
                (KeyValue::Decimal { unscaled, .. }, LogicalType::Decimal { .. }) => {
                    murmur3(&min_bytes(*unscaled))
                }
                (KeyValue::String(s), LogicalType::String) => murmur3(s.as_bytes()),
                (KeyValue::Binary(b), LogicalType::Binary | LogicalType::Fixed(_)) => murmur3(b),
                (KeyValue::Uuid(u), LogicalType::Uuid) => murmur3(u),
                _ => return Err(cannot(t, ty)),
            };
            int(i64::from((hash & i32::MAX) % n as i32))
        }
        Transform::Truncate(w) => match (v, ty) {
            (KeyValue::Integer(i), LogicalType::Int | LogicalType::Long) => {
                let w = i64::from(w);
                int(i - i.rem_euclid(w))
            }
            (KeyValue::Decimal { unscaled, .. }, LogicalType::Decimal { .. }) => {
                let w = i128::from(w);
                Ok(PartValue::Decimal(unscaled - unscaled.rem_euclid(w)))
            }
            (KeyValue::String(s), LogicalType::String) => {
                Ok(PartValue::Str(s.chars().take(w as usize).collect()))
            }
            (KeyValue::Binary(b), LogicalType::Binary) => Ok(PartValue::Bytes(
                b.iter().take(w as usize).copied().collect(),
            )),
            _ => Err(cannot(t, ty)),
        },
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
            let nanos = match (v, ty) {
                (KeyValue::Date(d), LogicalType::Date) if t != Transform::Hour => {
                    i128::from(*d) * NANOS_PER_DAY
                }
                (KeyValue::Timestamp(n) | KeyValue::TimestampTz(n), ty)
                    if is_micros(ty) || is_nanos(ty) =>
                {
                    *n
                }
                _ => return Err(cannot(t, ty)),
            };
            let days = i64::try_from(nanos.div_euclid(NANOS_PER_DAY)).map_err(|_| cannot(t, ty))?;
            match t {
                Transform::Hour => i64::try_from(nanos.div_euclid(NANOS_PER_HOUR))
                    .map(PartValue::Int)
                    .map_err(|_| cannot(t, ty)),
                Transform::Day => int(days),
                Transform::Year => int(civil(days).0 - 1970),
                _ => {
                    let (y, m) = civil(days);
                    int((y - 1970) * 12 + m - 1)
                }
            }
        }
    }
}

/// Microseconds of a timestamp the Plane holds in nanoseconds; exact for microsecond types.
fn micros(nanos: i128, t: Transform, ty: &LogicalType) -> Result<i64, Uncomputable> {
    if nanos % NANOS_PER_MICRO != 0 {
        return Err(cannot(t, ty));
    }
    i64::try_from(nanos / NANOS_PER_MICRO).map_err(|_| cannot(t, ty))
}

/// One partition field of a spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionField {
    /// The source column.
    pub source: FieldId,
    /// Its transform; `None` when the Plane does not compute it.
    pub transform: Option<Transform>,
}

/// The table's partition specs, by spec id, and whether every live data file is known to lie in
/// the partition its manifest entry names (the anchor's `partitions_checked`, ADR 0032).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Partitioning {
    /// Specs by id.
    pub specs: BTreeMap<i64, Vec<PartitionField>>,
    /// Every live data file passed the partition check.
    pub checked: bool,
}

impl Partitioning {
    /// No specs known: partitioned equality deletes are refused and no row is checked (ADR 0030).
    pub fn unknown() -> Self {
        Self::default()
    }

    /// The fields of spec `id` when every transform is computed and every source is among
    /// `columns`; otherwise `None` (rows of such files are not checked, ADR 0032).
    pub fn computable(
        &self,
        id: Option<i64>,
        columns: &[(FieldId, LogicalType)],
    ) -> Option<Vec<(usize, Transform, LogicalType)>> {
        let fields = self.specs.get(&id?)?;
        fields
            .iter()
            .map(|f| {
                let t = f.transform?;
                let at = columns.iter().position(|(c, _)| *c == f.source)?;
                let ty = &columns[at].1;
                supports(t, ty).then(|| (at, t, ty.clone()))
            })
            .collect()
    }

    /// Whether spec `id` is key-determined for the typed fields `key`: every partition field is a
    /// transform the Plane computes of one of them, and not all are `void` (ADR 0032).
    pub fn key_determined(&self, id: Option<i64>, key: &[(FieldId, LogicalType)]) -> bool {
        let Some(fields) = id.and_then(|id| self.specs.get(&id)) else {
            return false;
        };
        self.computable(id, key).is_some()
            && fields.iter().any(|f| f.transform != Some(Transform::Void))
    }

    /// Whether spec `id` is known and has a field that is not `void`: its deletes are partitioned
    /// for readers (a spec of `void` fields only is refused, ADR 0032).
    pub fn partitioned(&self, id: Option<i64>) -> bool {
        id.and_then(|id| self.specs.get(&id))
            .is_some_and(|fields| fields.iter().any(|f| f.transform != Some(Transform::Void)))
    }
}

/// Checks that `row` (projected on the columns `fields` index into) lies in `partition`.
pub fn check_row(
    fields: &[(usize, Transform, LogicalType)],
    row: &[Datum],
    partition: &[PartValue],
) -> Result<bool, Uncomputable> {
    if fields.len() != partition.len() {
        return Ok(false);
    }
    for ((at, t, ty), expected) in fields.iter().zip(partition) {
        let cell = row
            .get(*at)
            .ok_or_else(|| Uncomputable("row shorter than its projection".into()))?;
        let value = apply(*t, ty, cell)?;
        if value != *expected {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(k: KeyValue) -> Datum {
        Datum::Value(k)
    }

    fn bucket_hash(ty: LogicalType, k: KeyValue) -> i32 {
        // bucket[2^31 - 1] of a non-negative hash is the hash itself; the sign is checked apart.
        let PartValue::Int(b) = apply(Transform::Bucket(i32::MAX as u32), &ty, &v(k)).unwrap()
        else {
            panic!()
        };
        b as i32
    }

    /// The hash values of Appendix B, masked to non-negative as `bucket` does.
    #[test]
    fn appendix_b_hashes() {
        let masked = |h: i32| (h & i32::MAX) % i32::MAX;
        assert_eq!(murmur3(&34i64.to_le_bytes()), 2_017_239_379);
        assert_eq!(
            bucket_hash(LogicalType::Int, KeyValue::Integer(34)),
            masked(2_017_239_379)
        );
        assert_eq!(
            bucket_hash(LogicalType::Long, KeyValue::Integer(34)),
            masked(2_017_239_379)
        );
        let dec = LogicalType::Decimal {
            precision: 9,
            scale: 2,
        };
        assert_eq!(
            bucket_hash(
                dec,
                KeyValue::Decimal {
                    unscaled: 1420,
                    scale: 2
                }
            ),
            masked(-500_754_589)
        );
        // 2017-11-16
        assert_eq!(
            bucket_hash(LogicalType::Date, KeyValue::Date(17_486)),
            masked(-653_330_422)
        );
        // 2017-11-16T22:31:08
        let ts = 1_510_871_468_000_000i64;
        assert_eq!(
            bucket_hash(LogicalType::Timestamp, KeyValue::timestamp_micros(ts)),
            masked(-2_047_944_441)
        );
        assert_eq!(
            bucket_hash(LogicalType::TimestampTz, KeyValue::timestamptz_micros(ts)),
            masked(-2_047_944_441)
        );
        assert_eq!(murmur3(b"iceberg"), 1_210_000_089);
        assert_eq!(
            bucket_hash(LogicalType::String, KeyValue::String("iceberg".into())),
            masked(1_210_000_089)
        );
        let uuid = [
            0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7,
            0x85, 0xe7,
        ];
        assert_eq!(murmur3(&uuid), 1_488_055_340);
        assert_eq!(murmur3(&[0, 1, 2, 3]), -188_683_207);
        assert_eq!(
            bucket_hash(LogicalType::Fixed(4), KeyValue::Binary(vec![0, 1, 2, 3])),
            masked(-188_683_207)
        );
        assert_eq!(
            bucket_hash(LogicalType::Binary, KeyValue::Binary(vec![0, 1, 2, 3])),
            masked(-188_683_207)
        );
    }

    #[test]
    fn bucket_is_the_masked_hash_modulo_n() {
        // 34 hashes to 2017239379; mod 16 is 3.
        assert_eq!(
            apply(
                Transform::Bucket(16),
                &LogicalType::Long,
                &v(KeyValue::Integer(34))
            ),
            Ok(PartValue::Int(2_017_239_379 % 16))
        );
        // A negative hash is masked, not negated: -188683207 & MAX.
        assert_eq!(
            apply(
                Transform::Bucket(100),
                &LogicalType::Binary,
                &v(KeyValue::Binary(vec![0, 1, 2, 3]))
            ),
            Ok(PartValue::Int(i64::from(
                (-188_683_207i32 & i32::MAX) % 100
            )))
        );
        assert_eq!(
            apply(Transform::Bucket(4), &LogicalType::Long, &Datum::Null),
            Ok(PartValue::Null)
        );
        assert!(
            apply(
                Transform::Bucket(4),
                &LogicalType::TimestampNs,
                &v(KeyValue::timestamp_nanos(1))
            )
            .is_err()
        );
        assert!(
            apply(
                Transform::Bucket(4),
                &LogicalType::Boolean,
                &v(KeyValue::Boolean(true))
            )
            .is_err()
        );
    }

    /// Appendix A truncate examples.
    #[test]
    fn truncate_examples() {
        let t = Transform::Truncate(10);
        let int = |i| apply(t, &LogicalType::Int, &v(KeyValue::Integer(i)));
        assert_eq!(int(1), Ok(PartValue::Int(0)));
        assert_eq!(int(-1), Ok(PartValue::Int(-10)));
        assert_eq!(int(10), Ok(PartValue::Int(10)));
        assert_eq!(
            apply(t, &LogicalType::Long, &v(KeyValue::Integer(-11))),
            Ok(PartValue::Int(-20))
        );
        let dec = LogicalType::Decimal {
            precision: 9,
            scale: 2,
        };
        // truncate[50] of 10.65 is 10.50.
        assert_eq!(
            apply(
                Transform::Truncate(50),
                &dec,
                &v(KeyValue::Decimal {
                    unscaled: 1065,
                    scale: 2
                })
            ),
            Ok(PartValue::Decimal(1050))
        );
        assert_eq!(
            apply(
                Transform::Truncate(3),
                &LogicalType::String,
                &v(KeyValue::String("iceberg".into()))
            ),
            Ok(PartValue::Str("ice".into()))
        );
        // Code points, not bytes.
        assert_eq!(
            apply(
                Transform::Truncate(2),
                &LogicalType::String,
                &v(KeyValue::String("한글이".into()))
            ),
            Ok(PartValue::Str("한글".into()))
        );
        assert_eq!(
            apply(
                Transform::Truncate(2),
                &LogicalType::Binary,
                &v(KeyValue::Binary(vec![1, 2, 3]))
            ),
            Ok(PartValue::Bytes(vec![1, 2]))
        );
    }

    #[test]
    fn temporal_transforms() {
        // 2017-11-16T22:31:08
        let ts = v(KeyValue::timestamp_micros(1_510_871_468_000_000));
        let at = |t| apply(t, &LogicalType::Timestamp, &ts);
        assert_eq!(at(Transform::Year), Ok(PartValue::Int(47)));
        assert_eq!(at(Transform::Month), Ok(PartValue::Int(47 * 12 + 10)));
        assert_eq!(at(Transform::Day), Ok(PartValue::Int(17_486)));
        assert_eq!(at(Transform::Hour), Ok(PartValue::Int(17_486 * 24 + 22)));
        let date = v(KeyValue::Date(17_486));
        assert_eq!(
            apply(Transform::Month, &LogicalType::Date, &date),
            Ok(PartValue::Int(574))
        );
        assert_eq!(
            apply(Transform::Day, &LogicalType::Date, &date),
            Ok(PartValue::Int(17_486))
        );
        assert!(apply(Transform::Hour, &LogicalType::Date, &date).is_err());
        // 1970-01-01 and the last day of a leap February.
        assert_eq!(
            apply(Transform::Month, &LogicalType::Date, &v(KeyValue::Date(0))),
            Ok(PartValue::Int(0))
        );
        // 2000-02-29 is day 11016.
        assert_eq!(civil(11_016), (2000, 2));
        assert_eq!(civil(11_017), (2000, 3));
        // Before 1970, rounded down: 1969-12-31 is year -1, month -1, day -1.
        let eve = v(KeyValue::Date(-1));
        assert_eq!(
            apply(Transform::Year, &LogicalType::Date, &eve),
            Ok(PartValue::Int(-1))
        );
        assert_eq!(
            apply(Transform::Month, &LogicalType::Date, &eve),
            Ok(PartValue::Int(-1))
        );
        let before = v(KeyValue::timestamp_micros(-1));
        assert_eq!(
            apply(Transform::Hour, &LogicalType::Timestamp, &before),
            Ok(PartValue::Int(-1))
        );
        assert_eq!(
            apply(Transform::Day, &LogicalType::Timestamp, &before),
            Ok(PartValue::Int(-1))
        );
        let ns = v(KeyValue::timestamp_nanos(1_510_871_468_000_000_123));
        assert_eq!(
            apply(Transform::Day, &LogicalType::TimestampNs, &ns),
            Ok(PartValue::Int(17_486))
        );
    }

    #[test]
    fn identity_matches_manifest_values() {
        let ts = 1_510_871_468_000_000i64;
        assert_eq!(
            apply(
                Transform::Identity,
                &LogicalType::Timestamp,
                &v(KeyValue::timestamp_micros(ts))
            ),
            Ok(PartValue::from_avro(&Value::TimestampMicros(ts)))
        );
        assert_eq!(
            apply(
                Transform::Identity,
                &LogicalType::Int,
                &v(KeyValue::Integer(7))
            ),
            Ok(PartValue::from_avro(&Value::Union(
                1,
                Box::new(Value::Int(7))
            )))
        );
        assert_eq!(
            apply(
                Transform::Identity,
                &LogicalType::Date,
                &v(KeyValue::Date(3))
            ),
            Ok(PartValue::from_avro(&Value::Date(3)))
        );
        assert_eq!(
            PartValue::from_avro(&Value::Union(0, Box::new(Value::Null))),
            PartValue::Null
        );
    }

    #[test]
    fn decimals_hash_their_minimal_bytes() {
        assert_eq!(min_bytes(1420), vec![0x05, 0x8c]);
        assert_eq!(min_bytes(0), vec![0]);
        assert_eq!(min_bytes(-1), vec![0xff]);
        assert_eq!(min_bytes(128), vec![0x00, 0x80]);
        assert_eq!(min_bytes(-129), vec![0xff, 0x7f]);
        assert_eq!(unscaled(&[0xff, 0x7f]), Some(-129));
        assert_eq!(unscaled(&[0x05, 0x8c]), Some(1420));
    }

    #[test]
    fn transform_names() {
        assert_eq!(Transform::parse("bucket[16]"), Some(Transform::Bucket(16)));
        assert_eq!(
            Transform::parse("truncate[4]"),
            Some(Transform::Truncate(4))
        );
        assert_eq!(Transform::parse("bucket[0]"), None);
        assert_eq!(Transform::parse("zorder"), None);
        assert_eq!(Transform::parse("day"), Some(Transform::Day));
    }

    #[test]
    fn key_determined_specs() {
        let mut p = Partitioning::default();
        let f = |source, t| PartitionField {
            source: FieldId(source),
            transform: t,
        };
        p.specs.insert(0, vec![]);
        p.specs.insert(1, vec![f(1, Some(Transform::Bucket(8)))]);
        p.specs.insert(2, vec![f(2, Some(Transform::Identity))]);
        p.specs.insert(3, vec![f(1, None)]);
        p.specs.insert(4, vec![f(1, Some(Transform::Void))]);
        let key = [(FieldId(1), LogicalType::Long)];
        assert!(p.key_determined(Some(1), &key));
        assert!(!p.key_determined(Some(1), &[(FieldId(1), LogicalType::Boolean)]));
        assert!(!p.key_determined(Some(2), &key));
        assert!(!p.key_determined(Some(3), &key));
        assert!(!p.key_determined(Some(4), &key));
        assert!(!p.key_determined(Some(9), &key));
        assert!(!p.key_determined(None, &key));
        let columns = [(FieldId(1), LogicalType::Long)];
        assert_eq!(
            p.computable(Some(1), &columns),
            Some(vec![(0, Transform::Bucket(8), LogicalType::Long)])
        );
        assert_eq!(p.computable(Some(2), &columns), None);
        assert_eq!(p.computable(Some(3), &columns), None);
    }
}
