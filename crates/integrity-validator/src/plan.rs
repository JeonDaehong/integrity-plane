//! Validation planner (spec §13.3): turns projected rows into per-constraint key deltas and
//! the violations that need no index (NULLs, intra-commit duplicates).

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{
    CommitRows, ConstraintKind, Datum, KeyDelta, KeyDisposition, KeyError, KeyMultiset, KeySpec,
    KeyValue, RowBatch, Violation, classify,
};
use integrity_types::{ConstraintId, ErrorCode, FieldId};

use crate::{Inconsistency, ResolvedConstraint, ValidationError};

/// Key deltas and index-free violations for one commit.
#[derive(Debug, Default)]
pub(crate) struct Plan {
    /// Per PK/UNIQUE/FK-child constraint on the committed table.
    pub deltas: BTreeMap<ConstraintId, KeyDelta>,
    /// Violations decided without any index.
    pub violations: BTreeSet<Violation>,
}

impl Plan {
    /// Builds the plan from the constraints on the committed table.
    pub fn build<'a>(
        commit: &CommitRows,
        constraints: impl IntoIterator<Item = &'a ResolvedConstraint>,
    ) -> Result<Self, ValidationError> {
        let mut plan = Plan::default();
        for rc in constraints {
            let c = &rc.constraint;
            match &c.kind {
                ConstraintKind::NotNull(field) => {
                    let col = column(&commit.added, *field)?;
                    if col.is_some_and(|i| commit.added.rows().iter().any(|r| r[i] == Datum::Null))
                    {
                        plan.flag(c.id, ErrorCode::NotNullViolation);
                    }
                }
                ConstraintKind::PrimaryKey(key)
                | ConstraintKind::Unique(integrity_core::UniqueSpec { key, .. })
                | ConstraintKind::ForeignKey(integrity_core::ForeignKeySpec {
                    child: key, ..
                }) => {
                    let delta = plan.key_delta(rc, key, commit)?;
                    let duplicate = match c.kind {
                        ConstraintKind::PrimaryKey(_) => Some(ErrorCode::DuplicatePrimaryKey),
                        ConstraintKind::Unique(_) => Some(ErrorCode::DuplicateUniqueKey),
                        _ => None,
                    };
                    // Spec §8: multiplicities of added keys, before any index probe.
                    if let Some(code) = duplicate {
                        if delta.added.duplicates().next().is_some() {
                            plan.flag(c.id, code);
                        }
                    }
                    plan.deltas.insert(c.id, delta);
                }
            }
        }
        Ok(plan)
    }

    fn flag(&mut self, constraint: ConstraintId, code: ErrorCode) {
        self.violations.insert(Violation { constraint, code });
    }

    fn key_delta(
        &mut self,
        rc: &ResolvedConstraint,
        key: &KeySpec,
        commit: &CommitRows,
    ) -> Result<KeyDelta, ValidationError> {
        let c = &rc.constraint;
        let (Some(schema), Some(role)) = (&rc.schema, c.kind.key_role()) else {
            return Err(ValidationError::Unprovable(c.id));
        };

        let mut added = KeyMultiset::new();
        for tuple in tuples(&commit.added, key)? {
            let tuple = tuple?;
            match classify(role, schema, &tuple).map_err(|e| malformed(e, key))? {
                KeyDisposition::Key(k) => added.insert(k).map_err(ValidationError::Delta)?,
                KeyDisposition::Exempt => {}
                KeyDisposition::Violation(code) => self.flag(c.id, code),
            }
        }

        let mut removed = KeyMultiset::new();
        for tuple in tuples(&commit.removed, key)? {
            let tuple = tuple?;
            match classify(role, schema, &tuple).map_err(|e| malformed(e, key))? {
                KeyDisposition::Key(k) => removed.insert(k).map_err(ValidationError::Delta)?,
                KeyDisposition::Exempt => {}
                // A committed row cannot violate an enforced constraint by its NULLs alone.
                KeyDisposition::Violation(_) => {
                    return Err(ValidationError::Inconsistent(
                        Inconsistency::RemovedRowViolates(c.id),
                    ));
                }
            }
        }
        Ok(KeyDelta { added, removed })
    }
}

/// Position of `field` in a batch. A batch without rows needs no columns.
fn column(batch: &RowBatch, field: FieldId) -> Result<Option<usize>, ValidationError> {
    if batch.is_empty() {
        return Ok(None);
    }
    batch
        .column_index(field)
        .map(Some)
        .ok_or(ValidationError::MissingColumn(field))
}

type Tuple = Vec<Option<KeyValue>>;

/// The key tuple of every row in `batch`.
fn tuples<'b>(
    batch: &'b RowBatch,
    key: &'b KeySpec,
) -> Result<impl Iterator<Item = Result<Tuple, ValidationError>> + 'b, ValidationError> {
    let cols = key
        .columns
        .iter()
        .map(|&f| Ok((f, column(batch, f)?)))
        .collect::<Result<Vec<_>, ValidationError>>()?;
    Ok(batch.rows().iter().map(move |row| {
        cols.iter()
            .map(|&(field, i)| match i.map(|i| &row[i]) {
                Some(Datum::Value(v)) => Ok(Some(v.clone())),
                Some(Datum::Null) => Ok(None),
                Some(Datum::Opaque) | None => {
                    Err(ValidationError::MalformedValue { field: Some(field) })
                }
            })
            .collect()
    }))
}

fn malformed(e: KeyError, key: &KeySpec) -> ValidationError {
    match e {
        KeyError::FamilyMismatch { column, .. } => ValidationError::MalformedValue {
            field: key.columns.get(column).copied(),
        },
        KeyError::EmptySchema | KeyError::ArityMismatch { .. } => {
            ValidationError::MalformedValue { field: None }
        }
    }
}
