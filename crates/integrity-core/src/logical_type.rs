//! Column logical types as seen at constraint registration, and their key families (spec §9).

use crate::key::TypeFamily;

/// The logical type of a table column, independent of any table format's representation.
///
/// Adapters map their schema types onto this enum; anything they cannot map is [`LogicalType::Other`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalType {
    /// `boolean`.
    Boolean,
    /// 32-bit integer.
    Int,
    /// 64-bit integer.
    Long,
    /// 32-bit float.
    Float,
    /// 64-bit float.
    Double,
    /// Fixed-point decimal.
    Decimal {
        /// Total number of digits.
        precision: u8,
        /// Digits after the decimal point.
        scale: u8,
    },
    /// Calendar date.
    Date,
    /// Time of day.
    Time,
    /// Timestamp without time zone, microseconds.
    Timestamp,
    /// Timestamp with time zone, microseconds.
    TimestampTz,
    /// Timestamp without time zone, nanoseconds.
    TimestampNs,
    /// Timestamp with time zone, nanoseconds.
    TimestampTzNs,
    /// UTF-8 string.
    String,
    /// UUID.
    Uuid,
    /// Fixed-length binary.
    Fixed(u32),
    /// Variable-length binary.
    Binary,
    /// Struct, list or map.
    Nested,
    /// Semi-structured variant.
    Variant,
    /// Geometry or geography.
    Geospatial,
    /// Any other type, named for error messages.
    Other(String),
}

/// Why a column cannot be part of a key in 0.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedKeyType(pub LogicalType);

impl LogicalType {
    /// The key family of this type, or an error for types that cannot be keys in 0.1:
    /// float and double (NaN, −0.0), time, nested, variant, geospatial and unknown types.
    pub fn key_family(&self) -> Result<TypeFamily, UnsupportedKeyType> {
        match self {
            LogicalType::Boolean => Ok(TypeFamily::Boolean),
            LogicalType::Int | LogicalType::Long => Ok(TypeFamily::Integer),
            LogicalType::Decimal { scale, .. } => Ok(TypeFamily::Decimal { scale: *scale }),
            LogicalType::Date => Ok(TypeFamily::Date),
            LogicalType::Timestamp | LogicalType::TimestampNs => Ok(TypeFamily::Timestamp),
            LogicalType::TimestampTz | LogicalType::TimestampTzNs => Ok(TypeFamily::TimestampTz),
            LogicalType::String => Ok(TypeFamily::String),
            LogicalType::Uuid => Ok(TypeFamily::Uuid),
            LogicalType::Fixed(_) | LogicalType::Binary => Ok(TypeFamily::Binary),
            LogicalType::Float
            | LogicalType::Double
            | LogicalType::Time
            | LogicalType::Nested
            | LogicalType::Variant
            | LogicalType::Geospatial
            | LogicalType::Other(_) => Err(UnsupportedKeyType(self.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widening_stays_in_family() {
        assert_eq!(
            LogicalType::Int.key_family(),
            LogicalType::Long.key_family()
        );
        assert_eq!(
            LogicalType::Decimal {
                precision: 10,
                scale: 2
            }
            .key_family(),
            LogicalType::Decimal {
                precision: 38,
                scale: 2
            }
            .key_family()
        );
    }

    #[test]
    fn decimal_scales_are_distinct_families() {
        assert_ne!(
            LogicalType::Decimal {
                precision: 10,
                scale: 2
            }
            .key_family(),
            LogicalType::Decimal {
                precision: 10,
                scale: 3
            }
            .key_family()
        );
    }

    #[test]
    fn timestamp_precisions_share_family_but_tz_does_not() {
        assert_eq!(
            LogicalType::Timestamp.key_family(),
            LogicalType::TimestampNs.key_family()
        );
        assert_eq!(
            LogicalType::TimestampTz.key_family(),
            LogicalType::TimestampTzNs.key_family()
        );
        assert_ne!(
            LogicalType::Timestamp.key_family(),
            LogicalType::TimestampTz.key_family()
        );
    }

    #[test]
    fn rejected_key_types() {
        for t in [
            LogicalType::Float,
            LogicalType::Double,
            LogicalType::Time,
            LogicalType::Nested,
            LogicalType::Variant,
            LogicalType::Geospatial,
            LogicalType::Other("unknown".into()),
        ] {
            assert_eq!(t.key_family(), Err(UnsupportedKeyType(t.clone())));
        }
    }
}
