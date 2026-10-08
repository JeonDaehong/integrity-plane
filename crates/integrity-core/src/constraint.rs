//! Constraint model (spec §6) and registration-time validation.

use std::collections::BTreeSet;
use std::fmt;

use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, TableId};

use crate::key::{KeySchema, TypeFamily};
use crate::logical_type::LogicalType;
use crate::nulls::KeyRole;

/// A column reference: a field ID, never a name (spec §6).
pub type ColumnRef = FieldId;

/// A declared integrity constraint on one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    /// Identifier, never reused.
    pub id: ConstraintId,
    /// The constrained table (the child table for a foreign key).
    pub table: TableId,
    /// User-facing name, e.g. `fk_orders_customer`.
    pub name: String,
    /// What the constraint requires.
    pub kind: ConstraintKind,
    /// Whether it is enforced.
    pub mode: EnforcementMode,
    /// The constraint set version that introduced it.
    pub version: ConstraintSetVersion,
}

/// Whether a constraint is enforced. There is deliberately no advisory mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementMode {
    /// Commits violating the constraint are rejected.
    Enforced,
    /// Explicitly switched off by an operator; snapshots are uncertified.
    Disabled,
}

/// The kinds of constraint supported in 0.1. CHECK arrives in 0.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintKind {
    /// PRIMARY KEY: unique, and every key column implicitly NOT NULL.
    PrimaryKey(KeySpec),
    /// UNIQUE.
    Unique(UniqueSpec),
    /// FOREIGN KEY referencing a PRIMARY KEY or UNIQUE constraint.
    ForeignKey(ForeignKeySpec),
    /// NOT NULL on a single column.
    NotNull(ColumnRef),
}

/// An ordered list of key columns; a composite key is one tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySpec {
    /// Key columns in key order.
    pub columns: Vec<ColumnRef>,
}

/// A UNIQUE constraint's key and NULL treatment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueSpec {
    /// Key columns.
    pub key: KeySpec,
    /// How NULLs compare.
    pub nulls: NullsMode,
}

/// NULL comparison for UNIQUE (spec §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullsMode {
    /// SQL standard: a tuple containing NULL never conflicts.
    #[default]
    Distinct,
    /// NULL equals NULL.
    NotDistinct,
}

/// A foreign key from this table to a parent PK/UNIQUE constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeySpec {
    /// Child key columns, matched positionally against the parent key.
    pub child: KeySpec,
    /// The parent table.
    pub parent_table: TableId,
    /// The parent's PRIMARY KEY or UNIQUE constraint.
    pub parent_constraint: ConstraintId,
    /// Partial-NULL handling (spec §7).
    pub match_mode: MatchMode,
    /// What happens when a referenced parent key is deleted.
    pub on_delete: ReferentialAction,
}

/// FK match mode (spec §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MatchMode {
    /// Any NULL child column ⇒ no parent match required.
    #[default]
    Simple,
    /// All NULL ⇒ no match required; partially NULL ⇒ violation.
    Full,
}

/// Action on deleting a referenced parent key. Only RESTRICT exists in 0.1 (spec §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReferentialAction {
    /// Reject the delete (equivalent to NO ACTION in 0.1).
    #[default]
    Restrict,
}

impl ConstraintKind {
    /// The key columns of a PK, UNIQUE or FK (child side) constraint.
    pub fn key(&self) -> Option<&KeySpec> {
        match self {
            ConstraintKind::PrimaryKey(key) => Some(key),
            ConstraintKind::Unique(spec) => Some(&spec.key),
            ConstraintKind::ForeignKey(spec) => Some(&spec.child),
            ConstraintKind::NotNull(_) => None,
        }
    }

    /// How NULLs in this constraint's key tuples are treated.
    pub fn key_role(&self) -> Option<KeyRole> {
        match self {
            ConstraintKind::PrimaryKey(_) => Some(KeyRole::PrimaryKey),
            ConstraintKind::Unique(spec) => Some(KeyRole::Unique(spec.nulls)),
            ConstraintKind::ForeignKey(spec) => Some(KeyRole::ForeignKeyChild(spec.match_mode)),
            ConstraintKind::NotNull(_) => None,
        }
    }
}

/// What a validator needs to know about the tables and constraints already registered.
pub trait RegistrationContext {
    /// The logical type of a column, or `None` if the table or field does not exist.
    fn column_type(&self, table: &TableId, field: FieldId) -> Option<LogicalType>;
    /// A registered constraint.
    fn constraint(&self, id: ConstraintId) -> Option<&Constraint>;
}

/// Why a constraint definition was rejected. Always reported as `INVALID_CONSTRAINT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidConstraint {
    /// The name is empty.
    EmptyName,
    /// A key has no columns.
    EmptyKey,
    /// A column appears twice in one key.
    DuplicateColumn(FieldId),
    /// The column does not exist in the table.
    UnknownColumn(FieldId),
    /// The column's type cannot be a key in 0.1.
    UnsupportedKeyType {
        /// The column.
        field: FieldId,
        /// Its type.
        logical_type: LogicalType,
    },
    /// The referenced parent constraint is not registered.
    ParentConstraintNotFound(ConstraintId),
    /// The parent constraint belongs to a different table than `parent_table`.
    ParentTableMismatch,
    /// The parent constraint is not a PRIMARY KEY or UNIQUE constraint.
    ParentNotAKey(ConstraintId),
    /// The parent constraint is not enforced, so parent keys cannot be proven.
    ParentNotEnforced(ConstraintId),
    /// Child and parent keys have different numbers of columns.
    ArityMismatch {
        /// Child key columns.
        child: usize,
        /// Parent key columns.
        parent: usize,
    },
    /// A child column's family differs from the parent column's (e.g. decimal scale).
    FamilyMismatch {
        /// Zero-based key column index.
        column: usize,
        /// Child family.
        child: TypeFamily,
        /// Parent family.
        parent: TypeFamily,
    },
    /// Self-referencing foreign keys are not supported in 0.1.
    SelfReference,
}

impl InvalidConstraint {
    /// The error code reported for every registration failure.
    pub const fn code(&self) -> ErrorCode {
        ErrorCode::InvalidConstraint
    }
}

impl fmt::Display for InvalidConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidConstraint::EmptyName => f.write_str("constraint name is empty"),
            InvalidConstraint::EmptyKey => f.write_str("key has no columns"),
            InvalidConstraint::DuplicateColumn(c) => write!(f, "{c} appears twice in the key"),
            InvalidConstraint::UnknownColumn(c) => write!(f, "{c} does not exist"),
            InvalidConstraint::UnsupportedKeyType {
                field,
                logical_type,
            } => write!(
                f,
                "{field} has type {logical_type:?}, which cannot be a key"
            ),
            InvalidConstraint::ParentConstraintNotFound(c) => write!(f, "{c} does not exist"),
            InvalidConstraint::ParentTableMismatch => {
                f.write_str("parent constraint is not on the referenced table")
            }
            InvalidConstraint::ParentNotAKey(c) => {
                write!(f, "{c} is not a PRIMARY KEY or UNIQUE constraint")
            }
            InvalidConstraint::ParentNotEnforced(c) => write!(f, "{c} is not enforced"),
            InvalidConstraint::ArityMismatch { child, parent } => write!(
                f,
                "foreign key has {child} columns but the parent key has {parent}"
            ),
            InvalidConstraint::FamilyMismatch {
                column,
                child,
                parent,
            } => write!(
                f,
                "foreign key column {column} is {child:?} but the parent column is {parent:?}"
            ),
            InvalidConstraint::SelfReference => {
                f.write_str("self-referencing foreign keys are not supported in 0.1")
            }
        }
    }
}

impl std::error::Error for InvalidConstraint {}

impl Constraint {
    /// Validates the definition against the table schema and registered constraints.
    ///
    /// Returns the key schema for PK, UNIQUE and FK constraints (for an FK, the child
    /// schema, which equals the parent schema), and `None` for NOT NULL.
    pub fn validate(
        &self,
        ctx: &impl RegistrationContext,
    ) -> Result<Option<KeySchema>, InvalidConstraint> {
        if self.name.trim().is_empty() {
            return Err(InvalidConstraint::EmptyName);
        }
        match &self.kind {
            ConstraintKind::NotNull(field) => {
                ctx.column_type(&self.table, *field)
                    .ok_or(InvalidConstraint::UnknownColumn(*field))?;
                Ok(None)
            }
            ConstraintKind::PrimaryKey(key) => key_schema(&self.table, key, ctx).map(Some),
            ConstraintKind::Unique(spec) => key_schema(&self.table, &spec.key, ctx).map(Some),
            ConstraintKind::ForeignKey(spec) => {
                let child = key_schema(&self.table, &spec.child, ctx)?;
                if spec.parent_table == self.table {
                    return Err(InvalidConstraint::SelfReference);
                }
                let parent = ctx.constraint(spec.parent_constraint).ok_or(
                    InvalidConstraint::ParentConstraintNotFound(spec.parent_constraint),
                )?;
                if parent.table != spec.parent_table {
                    return Err(InvalidConstraint::ParentTableMismatch);
                }
                let parent_key = match &parent.kind {
                    ConstraintKind::PrimaryKey(key) => key,
                    ConstraintKind::Unique(u) => &u.key,
                    ConstraintKind::ForeignKey(_) | ConstraintKind::NotNull(_) => {
                        return Err(InvalidConstraint::ParentNotAKey(parent.id));
                    }
                };
                if parent.mode != EnforcementMode::Enforced {
                    return Err(InvalidConstraint::ParentNotEnforced(parent.id));
                }
                let parent_schema = key_schema(&parent.table, parent_key, ctx)?;
                if child.len() != parent_schema.len() {
                    return Err(InvalidConstraint::ArityMismatch {
                        child: child.len(),
                        parent: parent_schema.len(),
                    });
                }
                for (column, (c, p)) in child
                    .families()
                    .iter()
                    .zip(parent_schema.families())
                    .enumerate()
                {
                    if c != p {
                        return Err(InvalidConstraint::FamilyMismatch {
                            column,
                            child: *c,
                            parent: *p,
                        });
                    }
                }
                Ok(Some(child))
            }
        }
    }
}

fn key_schema(
    table: &TableId,
    key: &KeySpec,
    ctx: &impl RegistrationContext,
) -> Result<KeySchema, InvalidConstraint> {
    let mut seen = BTreeSet::new();
    let mut families = Vec::with_capacity(key.columns.len());
    for &field in &key.columns {
        if !seen.insert(field) {
            return Err(InvalidConstraint::DuplicateColumn(field));
        }
        let logical_type = ctx
            .column_type(table, field)
            .ok_or(InvalidConstraint::UnknownColumn(field))?;
        let family =
            logical_type
                .key_family()
                .map_err(|e| InvalidConstraint::UnsupportedKeyType {
                    field,
                    logical_type: e.0,
                })?;
        families.push(family);
    }
    KeySchema::new(families).map_err(|_| InvalidConstraint::EmptyKey)
}
