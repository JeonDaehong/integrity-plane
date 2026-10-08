//! In-memory relational oracle for differential tests (spec §28).
//!
//! The oracle holds every table in full and evaluates every enforced constraint over the whole
//! post-commit state, by direct value comparison. It is deliberately naive: it does not use key
//! encoding, key deltas, `integrity_core::classify` or any index, so that it shares no
//! verdict logic with the engine it checks. It reuses only the constraint *model* and its
//! registration checks from `integrity-core`.
//!
//! A commit is accepted iff the post-commit state satisfies every enforced constraint; a
//! rejected commit leaves the state unchanged.

pub mod strategies;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use integrity_core::{
    Constraint, ConstraintKind, EnforcementMode, InvalidConstraint, KeySpec, KeyValue, LogicalType,
    MatchMode, NullsMode, RegistrationContext,
};
use integrity_types::{ConstraintId, ErrorCode, FieldId, TableId};

/// One cell of a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    /// SQL NULL.
    Null,
    /// A value of a key-capable column type.
    Value(KeyValue),
    /// A non-NULL value of a type that cannot be a key (float, nested, …). Its content is
    /// irrelevant to every constraint, so it is not represented.
    Opaque,
}

/// A row: one datum per column of its table.
pub type Row = BTreeMap<FieldId, Datum>;

/// A single-table commit: rows to add and rows to remove (each removes one equal row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The table written.
    pub table: TableId,
    /// Rows added.
    pub added: Vec<Row>,
    /// Rows removed; each must match an existing row exactly.
    pub removed: Vec<Row>,
}

/// One violated constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Violation {
    /// The violated constraint.
    pub constraint: ConstraintId,
    /// Why.
    pub code: ErrorCode,
}

/// The oracle's decision on a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Applied.
    Accepted,
    /// Not applied; every violated `(constraint, code)` pair.
    Rejected(BTreeSet<Violation>),
}

/// Misuse of the oracle (as opposed to a constraint violation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    /// The table does not exist.
    UnknownTable(TableId),
    /// The table already exists.
    DuplicateTable(TableId),
    /// A constraint with this id already exists.
    DuplicateConstraint(ConstraintId),
    /// The constraint definition is invalid.
    InvalidConstraint(InvalidConstraint),
    /// Existing rows violate the constraint being registered (`ONBOARDING_VIOLATIONS`).
    OnboardingViolations(BTreeSet<Violation>),
    /// A row's columns or value types do not match the table.
    MalformedRow {
        /// The offending column, if a single one is to blame.
        field: Option<FieldId>,
    },
    /// A removed row does not exist (or not as many times as it is removed).
    RemovedRowMissing,
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for OracleError {}

#[derive(Debug, Clone)]
struct Table {
    columns: BTreeMap<FieldId, LogicalType>,
    rows: Vec<Row>,
}

/// The oracle database.
#[derive(Debug, Clone, Default)]
pub struct Oracle {
    tables: BTreeMap<TableId, Table>,
    constraints: BTreeMap<ConstraintId, Constraint>,
}

impl RegistrationContext for Oracle {
    fn column_type(&self, table: &TableId, field: FieldId) -> Option<LogicalType> {
        self.tables.get(table)?.columns.get(&field).cloned()
    }

    fn constraint(&self, id: ConstraintId) -> Option<&Constraint> {
        self.constraints.get(&id)
    }
}

impl Oracle {
    /// An empty database.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty table.
    pub fn create_table(
        &mut self,
        id: TableId,
        columns: BTreeMap<FieldId, LogicalType>,
    ) -> Result<(), OracleError> {
        if self.tables.contains_key(&id) {
            return Err(OracleError::DuplicateTable(id));
        }
        self.tables.insert(
            id,
            Table {
                columns,
                rows: Vec::new(),
            },
        );
        Ok(())
    }

    /// Registers a constraint. Existing rows must already satisfy it (spec §20).
    pub fn register(&mut self, constraint: Constraint) -> Result<(), OracleError> {
        if self.constraints.contains_key(&constraint.id) {
            return Err(OracleError::DuplicateConstraint(constraint.id));
        }
        constraint
            .validate(self)
            .map_err(OracleError::InvalidConstraint)?;
        if constraint.mode == EnforcementMode::Enforced {
            let violations = violations_of(&constraint, &self.tables, &self.constraints, None);
            if !violations.is_empty() {
                return Err(OracleError::OnboardingViolations(violations));
            }
        }
        self.constraints.insert(constraint.id, constraint);
        Ok(())
    }

    /// The rows of a table, in insertion order.
    pub fn rows(&self, table: &TableId) -> Option<&[Row]> {
        self.tables.get(table).map(|t| t.rows.as_slice())
    }

    /// Evaluates a commit and applies it if every enforced constraint holds afterwards.
    pub fn commit(&mut self, commit: &Commit) -> Result<Verdict, OracleError> {
        let table = self
            .tables
            .get(&commit.table)
            .ok_or_else(|| OracleError::UnknownTable(commit.table.clone()))?;
        for row in commit.added.iter().chain(&commit.removed) {
            check_row(&table.columns, row)?;
        }

        let mut rows = table.rows.clone();
        for removed in &commit.removed {
            let at = rows
                .iter()
                .position(|r| r == removed)
                .ok_or(OracleError::RemovedRowMissing)?;
            rows.remove(at);
        }
        rows.extend(commit.added.iter().cloned());

        let mut after = self.tables.clone();
        if let Some(t) = after.get_mut(&commit.table) {
            t.rows = rows;
        }

        let violations: BTreeSet<Violation> = self
            .constraints
            .values()
            .filter(|c| c.mode == EnforcementMode::Enforced)
            .flat_map(|c| violations_of(c, &after, &self.constraints, Some(&commit.table)))
            .collect();

        if violations.is_empty() {
            self.tables = after;
            Ok(Verdict::Accepted)
        } else {
            Ok(Verdict::Rejected(violations))
        }
    }
}

fn check_row(columns: &BTreeMap<FieldId, LogicalType>, row: &Row) -> Result<(), OracleError> {
    if row.len() != columns.len() {
        return Err(OracleError::MalformedRow { field: None });
    }
    for (field, ty) in columns {
        let ok = match (row.get(field), ty.key_family()) {
            (None, _) => false,
            (Some(Datum::Null), _) => true,
            (Some(Datum::Value(v)), Ok(family)) => v.family() == family,
            (Some(Datum::Opaque), Err(_)) => true,
            (Some(Datum::Value(_)), Err(_)) | (Some(Datum::Opaque), Ok(_)) => false,
        };
        if !ok {
            return Err(OracleError::MalformedRow {
                field: Some(*field),
            });
        }
    }
    Ok(())
}

/// A key tuple: `None` is NULL.
type Tuple = Vec<Option<KeyValue>>;

fn tuple(row: &Row, key: &KeySpec) -> Tuple {
    key.columns
        .iter()
        .map(|f| match row.get(f) {
            Some(Datum::Value(v)) => Some(v.clone()),
            Some(Datum::Null | Datum::Opaque) | None => None,
        })
        .collect()
}

/// Every violation of `c` in `tables`.
///
/// `committed` names the table the commit wrote. A child row without a parent is
/// `FOREIGN_KEY_VIOLATION` when the commit wrote the child table and `REFERENCED_ROW_DELETE`
/// when it wrote the parent table: since the pre-commit state was valid and commits touch one
/// table, the dangling row was respectively added, or orphaned by a parent delete. During
/// registration (`committed = None`) it is `FOREIGN_KEY_VIOLATION`.
fn violations_of(
    c: &Constraint,
    tables: &BTreeMap<TableId, Table>,
    constraints: &BTreeMap<ConstraintId, Constraint>,
    committed: Option<&TableId>,
) -> BTreeSet<Violation> {
    let Some(table) = tables.get(&c.table) else {
        return BTreeSet::new();
    };
    let rows = &table.rows;
    let mut codes = BTreeSet::new();

    match &c.kind {
        ConstraintKind::NotNull(field) => {
            if rows
                .iter()
                .any(|r| matches!(r.get(field), Some(Datum::Null)))
            {
                codes.insert(ErrorCode::NotNullViolation);
            }
        }
        ConstraintKind::PrimaryKey(key) => {
            let tuples: Vec<Tuple> = rows.iter().map(|r| tuple(r, key)).collect();
            if tuples.iter().any(|t| t.iter().any(Option::is_none)) {
                codes.insert(ErrorCode::NotNullViolation);
            }
            let complete: Vec<&Tuple> = tuples
                .iter()
                .filter(|t| t.iter().all(Option::is_some))
                .collect();
            if has_duplicate(&complete) {
                codes.insert(ErrorCode::DuplicatePrimaryKey);
            }
        }
        ConstraintKind::Unique(spec) => {
            let tuples: Vec<Tuple> = rows.iter().map(|r| tuple(r, &spec.key)).collect();
            let compared: Vec<&Tuple> = match spec.nulls {
                // A tuple with any NULL never equals another.
                NullsMode::Distinct => tuples
                    .iter()
                    .filter(|t| t.iter().all(Option::is_some))
                    .collect(),
                // NULL equals NULL: plain structural equality.
                NullsMode::NotDistinct => tuples.iter().collect(),
            };
            if has_duplicate(&compared) {
                codes.insert(ErrorCode::DuplicateUniqueKey);
            }
        }
        ConstraintKind::ForeignKey(spec) => {
            let parent_key = match constraints.get(&spec.parent_constraint).map(|p| &p.kind) {
                Some(ConstraintKind::PrimaryKey(key)) => key,
                Some(ConstraintKind::Unique(u)) => &u.key,
                _ => return BTreeSet::new(),
            };
            let parents: Vec<Tuple> = tables
                .get(&spec.parent_table)
                .map(|t| t.rows.iter().map(|r| tuple(r, parent_key)).collect())
                .unwrap_or_default();
            let missing_parent = if committed == Some(&spec.parent_table) {
                ErrorCode::ReferencedRowDelete
            } else {
                ErrorCode::ForeignKeyViolation
            };
            for row in rows {
                let child = tuple(row, &spec.child);
                let nulls = child.iter().filter(|v| v.is_none()).count();
                match spec.match_mode {
                    MatchMode::Simple if nulls > 0 => continue,
                    MatchMode::Full if nulls == child.len() => continue,
                    MatchMode::Full if nulls > 0 => {
                        codes.insert(ErrorCode::ForeignKeyViolation);
                        continue;
                    }
                    MatchMode::Simple | MatchMode::Full => {}
                }
                if !parents.contains(&child) {
                    codes.insert(missing_parent);
                }
            }
        }
    }
    codes
        .into_iter()
        .map(|code| Violation {
            constraint: c.id,
            code,
        })
        .collect()
}

fn has_duplicate(tuples: &[&Tuple]) -> bool {
    for (i, a) in tuples.iter().enumerate() {
        if tuples[i + 1..].iter().any(|b| a == b) {
            return true;
        }
    }
    false
}
