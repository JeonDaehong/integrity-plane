use std::fmt;

/// Stable identifier of a table, assigned by the format adapter.
///
/// For Iceberg this is the table UUID, so it survives renames. The core treats it as opaque.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(String);

impl TableId {
    /// Wraps an adapter-assigned identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identifier as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifier of a registered constraint. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConstraintId(pub u64);

impl fmt::Display for ConstraintId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "constraint-{}", self.0)
    }
}

/// A column identified by its table-format field ID (Iceberg field ID), never by name,
/// so that renames do not break constraints (spec §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(pub i32);

impl fmt::Display for FieldId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "field-{}", self.0)
    }
}

/// Identifier of a table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotId(pub i64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "snapshot-{}", self.0)
    }
}

/// Monotonically increasing version of a table's constraint set (spec §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConstraintSetVersion(pub u64);

impl ConstraintSetVersion {
    /// The version after this one, or `None` on overflow.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for ConstraintSetVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_set_version_next_is_checked() {
        assert_eq!(
            ConstraintSetVersion(1).next(),
            Some(ConstraintSetVersion(2))
        );
        assert_eq!(ConstraintSetVersion(u64::MAX).next(), None);
    }

    #[test]
    fn table_id_is_opaque_string() {
        let id = TableId::new("9f1c0b1e-uuid");
        assert_eq!(id.as_str(), "9f1c0b1e-uuid");
        assert_eq!(id.to_string(), "9f1c0b1e-uuid");
    }
}
