//! Validation of a `main` change (spec §14 steps 5–8b), run on a blocking thread.
//!
//! Each new snapshot is diffed against its parent, its rows extracted and validated against an
//! overlay of the indexes that already contains the previous steps. The overlays' writes become one
//! staged delta per index for the whole commit: recorded in the transaction log, applied after the
//! upstream commit succeeds, and replayed identically by recovery (RFC 0004).

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{LogicalType, Violation};
use integrity_iceberg::{
    Budgeted, FileIo, MainChange, NewSnapshot, TableMetadata, check_operation, commit_rows,
    diff_snapshots,
};
use integrity_index::{IndexEpoch, KeyIndex, Overlay, PersistentIndex, StagedDelta};
use integrity_txn::{FaultPoint, fault};
use integrity_types::{ConstraintId, ErrorCode, FieldId, TableId};
use integrity_validator::{Decision, ValidatedDeltas, Validator};

use crate::error::ApiError;

/// A validated step, ready to be certified.
#[derive(Debug, Clone)]
pub struct StepPlan {
    /// The snapshot.
    pub step: NewSnapshot,
    /// Its validated index changes.
    pub validated: ValidatedDeltas,
}

/// The decision on a main change.
#[derive(Debug)]
pub enum Outcome {
    /// Every step is valid.
    Accepted {
        /// One plan per step, oldest first.
        plans: Vec<StepPlan>,
        /// The whole commit's writes, one staged delta per index that changes.
        staged: Vec<(ConstraintId, StagedDelta)>,
    },
    /// A step violates constraints.
    Rejected(BTreeSet<Violation>),
}

/// Everything the blocking validation needs, owned.
pub struct Job {
    /// File access.
    pub io: std::sync::Arc<dyn FileIo + Send + Sync>,
    /// Read budget in bytes.
    pub budget: u64,
    /// Current table metadata.
    pub meta: TableMetadata,
    /// The table.
    pub table: TableId,
    /// The change to validate.
    pub change: MainChange,
    /// Projected columns with table types.
    pub columns: Vec<(FieldId, LogicalType)>,
    /// The domain's validator.
    pub validator: Validator,
    /// The domain's indexes.
    pub indexes: BTreeMap<ConstraintId, PersistentIndex>,
}

fn api(e: impl std::fmt::Display, code: ErrorCode) -> ApiError {
    ApiError::new(code, e.to_string())
}

/// Runs the job.
pub fn run(job: &Job) -> Result<Outcome, ApiError> {
    let io = Budgeted::new(job.io.as_ref(), job.budget);
    let overlays: BTreeMap<ConstraintId, Overlay<'_>> = job
        .indexes
        .iter()
        .map(|(id, index)| (*id, Overlay::new(index as &dyn KeyIndex)))
        .collect();

    let mut plans = Vec::new();
    let mut previous_list: Option<String> = None;
    let mut touched = BTreeSet::new();
    for (n, step) in job.change.steps.iter().enumerate() {
        let parent_list = if n == 0 {
            match job.change.parent {
                Some(p) => Some(
                    job.meta
                        .snapshot(p)
                        .and_then(|s| s.manifest_list.clone())
                        .ok_or_else(|| {
                            ApiError::new(
                                ErrorCode::UnsupportedCommitOperation,
                                "parent snapshot without manifest list",
                            )
                        })?,
                ),
                None => None,
            }
        } else {
            previous_list.clone()
        };
        let changes = diff_snapshots(&io, parent_list.as_deref(), &step.manifest_list)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
        let rows = commit_rows(
            &io,
            job.table.clone(),
            step.snapshot,
            &changes,
            &job.columns,
        )
        .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
        check_operation(step.operation, &rows)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;

        match job
            .validator
            .validate(&rows, &overlays)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?
        {
            Decision::Rejected(violations) => return Ok(Outcome::Rejected(violations)),
            Decision::Accepted(validated) => {
                for (id, staged) in validated
                    .stage(&overlays)
                    .map_err(|e| ApiError::new(e.code(), e.to_string()))?
                {
                    overlays[&id]
                        .apply(staged, IndexEpoch(0))
                        .map_err(|e| api(e, ErrorCode::IndexDegraded))?;
                    touched.insert(id);
                }
                plans.push(StepPlan {
                    step: step.clone(),
                    validated,
                });
            }
        }
        previous_list = Some(step.manifest_list.clone());
    }
    let mut staged = Vec::new();
    for (id, overlay) in overlays {
        if touched.contains(&id) {
            staged.push((
                id,
                overlay
                    .into_staged()
                    .map_err(|e| api(e, ErrorCode::IndexDegraded))?,
            ));
        }
    }
    Ok(Outcome::Accepted { plans, staged })
}

/// Applies a commit's staged deltas at `epoch`. Idempotent: re-applying after a crash is a no-op
/// for indexes that were already updated (ADR 0003).
pub fn apply(
    staged: &[(ConstraintId, StagedDelta)],
    indexes: &BTreeMap<ConstraintId, PersistentIndex>,
    epoch: u64,
) -> Result<(), ApiError> {
    for (n, (id, delta)) in staged.iter().enumerate() {
        if n > 0 {
            fault::hit(FaultPoint::DuringIndexApply);
        }
        let index = indexes
            .get(id)
            .ok_or_else(|| ApiError::new(ErrorCode::IndexDegraded, format!("no index for {id}")))?;
        index
            .apply(delta.clone(), IndexEpoch(epoch))
            .map_err(|e| api(e, ErrorCode::IndexDegraded))?;
    }
    Ok(())
}
