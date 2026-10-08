//! NULL semantics per constraint kind (spec §7, normative).

use integrity_types::ErrorCode;

use crate::constraint::{MatchMode, NullsMode};
use crate::key::{EncodedKey, KeyError, KeySchema, KeyValue};

/// The role a key tuple plays, which decides how NULLs in it are treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// A primary key tuple.
    PrimaryKey,
    /// A UNIQUE key tuple.
    Unique(NullsMode),
    /// The child side of a foreign key.
    ForeignKeyChild(MatchMode),
}

/// What a key tuple means for its constraint once NULL semantics are applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyDisposition {
    /// The tuple participates: it is indexed (PK/UNIQUE) or must match a parent key (FK).
    Key(EncodedKey),
    /// NULLs exempt the tuple: it never conflicts (UNIQUE) or needs no parent (FK).
    Exempt,
    /// NULLs alone make the tuple a violation.
    Violation(ErrorCode),
}

/// Applies spec §7 to one key tuple.
///
/// | Role | Rule |
/// |---|---|
/// | PRIMARY KEY | any NULL ⇒ `NOT_NULL_VIOLATION` |
/// | UNIQUE NULLS DISTINCT | any NULL ⇒ exempt (never conflicts, not indexed) |
/// | UNIQUE NULLS NOT DISTINCT | always indexed; NULL equals NULL |
/// | FK MATCH SIMPLE | any NULL ⇒ exempt |
/// | FK MATCH FULL | all NULL ⇒ exempt; some NULL ⇒ `FOREIGN_KEY_VIOLATION` |
///
/// The tuple is checked against `schema` first, including the families of exempt tuples.
pub fn classify(
    role: KeyRole,
    schema: &KeySchema,
    values: &[Option<KeyValue>],
) -> Result<KeyDisposition, KeyError> {
    schema.check(values)?;
    let nulls = values.iter().filter(|v| v.is_none()).count();
    let any_null = nulls > 0;
    let all_null = nulls == values.len();

    let disposition = match role {
        KeyRole::PrimaryKey if any_null => KeyDisposition::Violation(ErrorCode::NotNullViolation),
        KeyRole::Unique(NullsMode::Distinct) if any_null => KeyDisposition::Exempt,
        KeyRole::ForeignKeyChild(MatchMode::Simple) if any_null => KeyDisposition::Exempt,
        KeyRole::ForeignKeyChild(MatchMode::Full) if all_null => KeyDisposition::Exempt,
        KeyRole::ForeignKeyChild(MatchMode::Full) if any_null => {
            KeyDisposition::Violation(ErrorCode::ForeignKeyViolation)
        }
        KeyRole::PrimaryKey
        | KeyRole::Unique(NullsMode::Distinct | NullsMode::NotDistinct)
        | KeyRole::ForeignKeyChild(MatchMode::Simple | MatchMode::Full) => {
            KeyDisposition::Key(EncodedKey::encode(schema, values)?)
        }
    };
    Ok(disposition)
}
