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
use integrity_validator::{Decision, ValidatedDeltas, Validator, ViolationDetail};

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
    /// A step violates constraints: what violates each of them.
    Rejected(BTreeMap<Violation, ViolationDetail>),
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
    /// Where the job reports what it read.
    pub stats: std::sync::Arc<Stats>,
}

/// What a validation read (metrics `integrity_bytes_read_total`, `integrity_keys_validated_total`).
#[derive(Debug, Default)]
pub struct Stats {
    /// Bytes read from storage (manifest lists, manifests, data files).
    pub bytes: std::sync::atomic::AtomicU64,
    /// Rows whose keys were extracted (added, removed and equality-deleted).
    pub rows: std::sync::atomic::AtomicU64,
    /// Nanoseconds spent in persistent index lookups.
    pub probe_nanos: std::sync::atomic::AtomicU64,
}

/// A persistent index whose lookups are timed into [`Stats::probe_nanos`].
struct Timed<'a> {
    inner: &'a PersistentIndex,
    nanos: &'a std::sync::atomic::AtomicU64,
}

impl KeyIndex for Timed<'_> {
    fn kind(&self) -> integrity_index::IndexKind {
        self.inner.kind()
    }
    fn get_many(
        &self,
        keys: &[integrity_core::EncodedKey],
    ) -> integrity_index::Result<Vec<Option<integrity_index::IndexValue>>> {
        let start = std::time::Instant::now();
        let out = self.inner.get_many(keys);
        self.nanos.fetch_add(
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        out
    }
    fn stage(&self, delta: &integrity_index::IndexDelta) -> integrity_index::Result<StagedDelta> {
        self.inner.stage(delta)
    }
    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> integrity_index::Result<()> {
        self.inner.apply(staged, epoch)
    }
    fn epoch(&self) -> integrity_index::Result<IndexEpoch> {
        self.inner.epoch()
    }
    fn entries(
        &self,
    ) -> integrity_index::Result<Vec<(integrity_core::EncodedKey, integrity_index::IndexValue)>>
    {
        self.inner.entries()
    }
}

fn api(e: impl std::fmt::Display, code: ErrorCode) -> ApiError {
    ApiError::new(code, e.to_string())
}

/// Runs the job.
pub fn run(job: &Job) -> Result<Outcome, ApiError> {
    let io = Budgeted::new(job.io.as_ref(), job.budget);
    let outcome = run_with(job, &io);
    job.stats
        .bytes
        .fetch_add(io.used(), std::sync::atomic::Ordering::Relaxed);
    outcome
}

fn run_with(job: &Job, io: &Budgeted<'_, dyn FileIo + Send + Sync>) -> Result<Outcome, ApiError> {
    let timed: BTreeMap<ConstraintId, Timed<'_>> = job
        .indexes
        .iter()
        .map(|(id, index)| {
            (
                *id,
                Timed {
                    inner: index,
                    nanos: &job.stats.probe_nanos,
                },
            )
        })
        .collect();
    let overlays: BTreeMap<ConstraintId, Overlay<'_>> = timed
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
        let changes = diff_snapshots(io, parent_list.as_deref(), &step.manifest_list)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
        let rows = commit_rows(io, job.table.clone(), step.snapshot, &changes, &job.columns)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;
        let extracted = rows.added.len()
            + rows.removed.len()
            + rows.equality_deletes.as_ref().map_or(0, |d| d.len());
        job.stats
            .rows
            .fetch_add(extracted as u64, std::sync::atomic::Ordering::Relaxed);
        check_operation(step.operation, &rows)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?;

        match job
            .validator
            .validate_with_details(&rows, &overlays)
            .map_err(|e| ApiError::new(e.code(), e.to_string()))?
        {
            (Decision::Rejected(_), details) => return Ok(Outcome::Rejected(details)),
            (Decision::Accepted(validated), _) => {
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
