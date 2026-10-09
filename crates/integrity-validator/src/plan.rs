//! Validation planner (spec §13.3): turns projected rows into per-constraint key deltas and
//! the violations that need no index (NULLs, intra-commit duplicates).

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{
    CommitRows, ConstraintKind, Datum, EncodedKey, KeyDelta, KeyDisposition, KeyError, KeyMultiset,
    KeySpec, KeyValue, RowBatch, Violation, classify,
};
use integrity_types::{ConstraintId, ErrorCode, FieldId};

use crate::{Inconsistency, ResolvedConstraint, ValidationError, ViolationDetail};

/// Key deltas and violations for one commit (index-free ones from [`Plan::build`], the others
/// added by the validator).
#[derive(Debug, Default)]
pub(crate) struct Plan {
    /// Per PK/UNIQUE/FK-child constraint on the committed table.
    pub deltas: BTreeMap<ConstraintId, KeyDelta>,
    /// Every violated `(constraint, code)`.
    pub violations: BTreeSet<Violation>,
    /// What violates each of them.
    pub details: BTreeMap<Violation, ViolationDetail>,
}

impl Plan {
    /// Records that `key` violates `(constraint, code)`.
    pub fn record_key(&mut self, constraint: ConstraintId, code: ErrorCode, key: &EncodedKey) {
        let v = Violation { constraint, code };
        self.violations.insert(v);
        self.details.entry(v).or_default().add_key(key);
    }

    /// Records a row that violates `(constraint, code)` without forming a key (NULLs), with the
    /// key tuple if there is one.
    pub fn record_row(&mut self, constraint: ConstraintId, code: ErrorCode, tuple: Option<Tuple>) {
        let v = Violation { constraint, code };
        self.violations.insert(v);
        self.details.entry(v).or_default().add_row(tuple);
    }

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
                    if let Some(i) = column(&commit.added, *field)? {
                        for row in commit.added.rows() {
                            if row[i] == Datum::Null {
                                plan.record_row(c.id, ErrorCode::NotNullViolation, None);
                            }
                        }
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
                        let dups: Vec<EncodedKey> =
                            delta.added.duplicates().map(|(k, _)| k.clone()).collect();
                        for k in &dups {
                            plan.record_key(c.id, code, k);
                        }
                    }
                    plan.deltas.insert(c.id, delta);
                }
            }
        }
        Ok(plan)
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
                KeyDisposition::Violation(code) => self.record_row(c.id, code, Some(tuple)),
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

pub(crate) type Tuple = Vec<Option<KeyValue>>;

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
