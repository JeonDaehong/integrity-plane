//! PK / UNIQUE / NOT NULL / FK validation of single-table commits (spec §8, §13.3, §14 steps 6–8).
//!
//! [`Validator::validate`] takes the projected rows a commit adds and removes, and decides against
//! the key indexes whether the post-commit state satisfies every enforced constraint:
//!
//! 1. classify every key tuple by spec §7 and build per-constraint key deltas; flag NULL
//!    violations and intra-commit PK/UNIQUE duplicates **before** any probe (spec §8);
//! 2. probe, batched and deduplicated per index: PK/UNIQUE keys whose count rises (must be
//!    absent) or falls (must be present); FK child keys whose count rises (parent must exist);
//!    parent keys that disappear (no child may reference them);
//! 3. return every violated `(constraint, code)`, or the index deltas to stage.
//!
//! Correctness relies on the pre-commit state being valid and indexed (the invariant every
//! accepted commit preserves). Anything that contradicts that invariant, or that the validator
//! cannot prove, is an error rather than a verdict, so callers fail closed.

mod plan;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use integrity_core::{
    CommitRows, Constraint, ConstraintKind, DeltaError, Digest, EncodedKey, EnforcementMode,
    InvalidCertificate, InvalidConstraint, KeySchema, NetDelta, RegistrationContext, Violation,
    key_delta_digest,
};
use integrity_index::{
    IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, StagedDelta,
};
use integrity_types::{ConstraintId, ErrorCode, FieldId, TableId};

use plan::Plan;

/// A registered constraint with its resolved key schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConstraint {
    /// The definition.
    pub constraint: Constraint,
    /// Key schema for PK/UNIQUE/FK; `None` for NOT NULL.
    pub schema: Option<KeySchema>,
}

impl ResolvedConstraint {
    /// Validates a definition (spec §6) and resolves its key schema.
    pub fn resolve(
        constraint: Constraint,
        ctx: &impl RegistrationContext,
    ) -> Result<Self, InvalidConstraint> {
        let schema = constraint.validate(ctx)?;
        Ok(Self { constraint, schema })
    }

    /// The index this constraint needs, if any.
    pub fn index_kind(&self) -> Option<IndexKind> {
        match self.constraint.kind {
            ConstraintKind::PrimaryKey(_) | ConstraintKind::Unique(_) => Some(IndexKind::Unique),
            ConstraintKind::ForeignKey(_) => Some(IndexKind::Reference),
            ConstraintKind::NotNull(_) => None,
        }
    }

    fn enforced(&self) -> bool {
        self.constraint.mode == EnforcementMode::Enforced
    }
}

/// Access to the index of each constraint.
pub trait IndexSet {
    /// The index of constraint `id`.
    fn index(&self, id: ConstraintId) -> Option<&dyn KeyIndex>;
}

impl<I: KeyIndex> IndexSet for BTreeMap<ConstraintId, I> {
    fn index(&self, id: ConstraintId) -> Option<&dyn KeyIndex> {
        self.get(&id).map(|i| i as &dyn KeyIndex)
    }
}

/// The outcome of validating a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Every enforced constraint holds after the commit.
    Accepted(ValidatedDeltas),
    /// Every violated `(constraint, code)`.
    Rejected(BTreeSet<Violation>),
}

/// The index changes of an accepted commit, and the index epochs its decision was based on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedDeltas {
    deltas: BTreeMap<ConstraintId, IndexDelta>,
    key_deltas: BTreeMap<ConstraintId, NetDelta>,
    observed: BTreeMap<ConstraintId, IndexEpoch>,
}

impl ValidatedDeltas {
    /// The net key change of every PK/UNIQUE/FK constraint on the table, empty ones included:
    /// the input of the certificate's key delta digest (RFC 0002).
    pub fn key_deltas(&self) -> &BTreeMap<ConstraintId, NetDelta> {
        &self.key_deltas
    }

    /// The RFC 0002 key delta digest of this commit.
    pub fn key_delta_digest(&self) -> Result<Digest, InvalidCertificate> {
        key_delta_digest(&self.key_deltas)
    }

    /// Non-empty index deltas, per constraint.
    pub fn deltas(&self) -> impl Iterator<Item = (ConstraintId, &IndexDelta)> {
        self.deltas.iter().map(|(id, d)| (*id, d))
    }

    /// Stages every delta. Fails with [`ValidationError::StaleValidation`] if any index the
    /// decision read or writes has moved since validation.
    pub fn stage(
        &self,
        indexes: &impl IndexSet,
    ) -> Result<Vec<(ConstraintId, StagedDelta)>, ValidationError> {
        for (&id, &epoch) in &self.observed {
            if index(indexes, id)?
                .epoch()
                .map_err(ValidationError::Index)?
                != epoch
            {
                return Err(ValidationError::StaleValidation);
            }
        }
        self.deltas
            .iter()
            .map(|(&id, d)| {
                let staged = index(indexes, id)?
                    .stage(d)
                    .map_err(ValidationError::Index)?;
                Ok((id, staged))
            })
            .collect()
    }
}

/// Something other than a constraint verdict prevented a decision. Callers MUST reject the
/// commit (fail closed) with [`ValidationError::code`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// The projected rows lack a column a constraint needs.
    MissingColumn(FieldId),
    /// A value does not fit its column (e.g. an opaque value in a key column).
    MalformedValue {
        /// The column, when known.
        field: Option<FieldId>,
    },
    /// An enforced constraint depends on one that is missing, unresolved or not enforced.
    Unprovable(ConstraintId),
    /// A constraint has no index.
    MissingIndex(ConstraintId),
    /// Committed data and indexes disagree.
    Inconsistent(Inconsistency),
    /// An index operation failed.
    Index(IndexError),
    /// A key count overflowed.
    Delta(DeltaError),
    /// An index changed between validation and staging.
    StaleValidation,
}

/// Evidence that the pre-commit state or an index is not what the invariants require.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inconsistency {
    /// A removed row violates the constraint by its NULLs alone.
    RemovedRowViolates(ConstraintId),
    /// A removed key is not in the constraint's unique index, or is removed more than once.
    RemovedKeyNotIndexed(ConstraintId),
    /// An index holds a value of the wrong kind.
    WrongIndexKind(ConstraintId),
}

impl ValidationError {
    /// The integrity code to report: input the Plane cannot prove is an unsupported operation;
    /// index trouble degrades the domain.
    pub fn code(&self) -> ErrorCode {
        match self {
            ValidationError::MissingColumn(_)
            | ValidationError::MalformedValue { .. }
            | ValidationError::Unprovable(_) => ErrorCode::UnsupportedCommitOperation,
            ValidationError::MissingIndex(_)
            | ValidationError::Inconsistent(_)
            | ValidationError::Index(_)
            | ValidationError::Delta(_)
            | ValidationError::StaleValidation => ErrorCode::IndexDegraded,
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {self:?}", self.code())
    }
}

impl std::error::Error for ValidationError {}

/// Validates commits against a fixed set of registered constraints.
#[derive(Debug, Clone, Default)]
pub struct Validator {
    constraints: BTreeMap<ConstraintId, ResolvedConstraint>,
}

impl Validator {
    /// A validator for `constraints` (enforced and disabled).
    pub fn new(constraints: impl IntoIterator<Item = ResolvedConstraint>) -> Self {
        Self {
            constraints: constraints
                .into_iter()
                .map(|rc| (rc.constraint.id, rc))
                .collect(),
        }
    }

    /// Every constraint governing commits to `table`: those declared on it and foreign keys that
    /// reference it, enforced or not (the certificate's constraint set digest skips disabled ones).
    pub fn governing(&self, table: &TableId) -> Vec<&Constraint> {
        self.constraints
            .values()
            .map(|rc| &rc.constraint)
            .filter(|c| {
                c.table == *table
                    || matches!(&c.kind, ConstraintKind::ForeignKey(fk) if fk.parent_table == *table)
            })
            .collect()
    }

    fn on_table<'a>(&'a self, table: &'a TableId) -> impl Iterator<Item = &'a ResolvedConstraint> {
        self.constraints
            .values()
            .filter(move |rc| rc.enforced() && rc.constraint.table == *table)
    }

    /// The columns a format adapter must read from added and removed rows of `table`.
    pub fn projection(&self, table: &TableId) -> Vec<FieldId> {
        let mut cols = BTreeSet::new();
        for rc in self.on_table(table) {
            match &rc.constraint.kind {
                ConstraintKind::NotNull(f) => {
                    cols.insert(*f);
                }
                kind => cols.extend(
                    kind.key()
                        .into_iter()
                        .flat_map(|k| k.columns.iter().copied()),
                ),
            }
        }
        cols.into_iter().collect()
    }

    /// Decides a single-table commit. Reads indexes; never writes them.
    pub fn validate(
        &self,
        commit: &CommitRows,
        indexes: &impl IndexSet,
    ) -> Result<Decision, ValidationError> {
        let table = &commit.table;
        let own: Vec<&ResolvedConstraint> = self.on_table(table).collect();
        // Enforced FKs whose parent key lives on this table.
        let referencing: Vec<(&ResolvedConstraint, ConstraintId)> = self
            .constraints
            .values()
            .filter(|rc| rc.enforced())
            .filter_map(|rc| match &rc.constraint.kind {
                ConstraintKind::ForeignKey(fk) if fk.parent_table == *table => {
                    Some((rc, fk.parent_constraint))
                }
                _ => None,
            })
            .collect();

        // Fail closed on anything we cannot prove, before reading data.
        for rc in &own {
            if let ConstraintKind::ForeignKey(fk) = &rc.constraint.kind {
                self.require_enforced(fk.parent_constraint, rc.constraint.id)?;
            }
        }
        for (rc, parent) in &referencing {
            self.require_enforced(*parent, rc.constraint.id)?;
        }

        // Record every index epoch before reading any index.
        let mut observed = BTreeMap::new();
        let mut observe = |id: ConstraintId| -> Result<(), ValidationError> {
            let epoch = index(indexes, id)?
                .epoch()
                .map_err(ValidationError::Index)?;
            observed.insert(id, epoch);
            Ok(())
        };
        for rc in &own {
            if let ConstraintKind::ForeignKey(fk) = &rc.constraint.kind {
                observe(fk.parent_constraint)?;
            }
            if rc.index_kind().is_some() {
                observe(rc.constraint.id)?;
            }
        }
        for (rc, _) in &referencing {
            observe(rc.constraint.id)?;
        }

        let mut plan = Plan::build(commit, own.iter().copied())?;

        for rc in &own {
            let id = rc.constraint.id;
            let Some(delta) = plan.deltas.get(&id) else {
                continue;
            };
            let net = delta.net();
            match &rc.constraint.kind {
                ConstraintKind::PrimaryKey(_) | ConstraintKind::Unique(_) => {
                    let duplicate = match rc.constraint.kind {
                        ConstraintKind::PrimaryKey(_) => ErrorCode::DuplicatePrimaryKey,
                        _ => ErrorCode::DuplicateUniqueKey,
                    };
                    if net.decreases().any(|(_, c)| c < -1) {
                        return Err(ValidationError::Inconsistent(
                            Inconsistency::RemovedKeyNotIndexed(id),
                        ));
                    }
                    let keys: Vec<EncodedKey> = net.iter().map(|(k, _)| k.clone()).collect();
                    let found = probe(indexes, id, &keys)?;
                    for ((_, change), value) in net.iter().zip(found) {
                        match (change > 0, value) {
                            // Count rises and the key is already present: duplicate.
                            (true, Some(_)) => {
                                plan.violations.insert(Violation {
                                    constraint: id,
                                    code: duplicate,
                                });
                            }
                            // Count falls but the key was never indexed.
                            (false, None) => {
                                return Err(ValidationError::Inconsistent(
                                    Inconsistency::RemovedKeyNotIndexed(id),
                                ));
                            }
                            (true, None) | (false, Some(_)) => {}
                        }
                    }
                }
                ConstraintKind::ForeignKey(fk) => {
                    // Only keys whose count rises need a parent: a key that was already
                    // referenced had a parent before, and the parent table is not written.
                    let keys: Vec<EncodedKey> = net.increases().map(|(k, _)| k.clone()).collect();
                    let found = probe(indexes, fk.parent_constraint, &keys)?;
                    if found.iter().any(Option::is_none) {
                        plan.violations.insert(Violation {
                            constraint: id,
                            code: ErrorCode::ForeignKeyViolation,
                        });
                    }
                }
                ConstraintKind::NotNull(_) => {}
            }
        }

        for (rc, parent) in &referencing {
            let Some(parent_delta) = plan.deltas.get(parent) else {
                return Err(ValidationError::Unprovable(rc.constraint.id));
            };
            // Parent keys that disappear (removed and not re-added).
            let gone: Vec<EncodedKey> = parent_delta
                .net()
                .decreases()
                .map(|(k, _)| k.clone())
                .collect();
            for value in probe(indexes, rc.constraint.id, &gone)? {
                match value {
                    None => {}
                    Some(IndexValue::Reference { .. }) => {
                        plan.violations.insert(Violation {
                            constraint: rc.constraint.id,
                            code: ErrorCode::ReferencedRowDelete,
                        });
                    }
                    Some(IndexValue::Unique { .. }) => {
                        return Err(ValidationError::Inconsistent(
                            Inconsistency::WrongIndexKind(rc.constraint.id),
                        ));
                    }
                }
            }
        }

        if !plan.violations.is_empty() {
            return Ok(Decision::Rejected(plan.violations));
        }
        let key_deltas: BTreeMap<ConstraintId, NetDelta> = plan
            .deltas
            .into_iter()
            .map(|(id, d)| (id, d.net()))
            .collect();
        let deltas = key_deltas
            .iter()
            .filter(|(_, net)| !net.is_empty())
            .map(|(id, net)| {
                (
                    *id,
                    IndexDelta {
                        snapshot: commit.snapshot,
                        changes: net.clone(),
                    },
                )
            })
            .collect();
        Ok(Decision::Accepted(ValidatedDeltas {
            deltas,
            key_deltas,
            observed,
        }))
    }

    fn require_enforced(&self, id: ConstraintId, by: ConstraintId) -> Result<(), ValidationError> {
        match self.constraints.get(&id) {
            Some(rc) if rc.enforced() && rc.schema.is_some() => Ok(()),
            _ => Err(ValidationError::Unprovable(by)),
        }
    }
}

fn index(indexes: &impl IndexSet, id: ConstraintId) -> Result<&dyn KeyIndex, ValidationError> {
    indexes.index(id).ok_or(ValidationError::MissingIndex(id))
}

/// One batched lookup per index (spec §13.3). Callers pass distinct keys.
fn probe(
    indexes: &impl IndexSet,
    id: ConstraintId,
    keys: &[EncodedKey],
) -> Result<Vec<Option<IndexValue>>, ValidationError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let found = index(indexes, id)?
        .get_many(keys)
        .map_err(ValidationError::Index)?;
    if found.len() != keys.len() {
        return Err(ValidationError::Index(IndexError::Corrupt));
    }
    Ok(found)
}
