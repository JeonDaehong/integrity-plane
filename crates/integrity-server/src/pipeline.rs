//! Validation of a `main` change (spec §14 steps 5–8b), run on a blocking thread.
//!
//! Each new snapshot is diffed against its parent, its rows extracted and validated against an
//! overlay of the indexes that already contains the previous steps. The overlays' writes become one
//! staged delta per index for the whole commit: recorded in the transaction log, applied after the
//! upstream commit succeeds, and replayed identically by recovery (RFC 0004).
//!
//! Rows are streamed, never held (ADR 0019): the keys of each side are sorted externally within
//! the scan memory budget and only the net change of each key is probed. Commits with equality
//! deletes are read whole (`commit_rows`), as before.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use integrity_core::{EncodedKey, LogicalType, Side, Violation, encode_row};
use integrity_iceberg::{
    Budgeted, FileChanges, FileIo, InspectError, MainChange, NewSnapshot, Operation, TableMetadata,
    check_operation, check_sides, commit_rows, diff_snapshots, for_each_commit_batch,
};
use integrity_index::{
    IndexEpoch, IndexError, KeyIndex, KeySorter, Overlay, PersistentIndex, PersistentStore,
    StagedDelta,
};
use integrity_txn::{FaultPoint, fault};
use integrity_types::{ConstraintId, ErrorCode, FieldId, SnapshotId, TableId};
use integrity_validator::{
    Decision, IndexSet, ValidatedDeltas, ValidationError, Validator, ViolationDetail,
};

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
    /// Directory for sort runs of large commits (the index store's scratch directory).
    pub scratch: PathBuf,
    /// Bytes of keys a validation keeps in memory before spilling sorted runs.
    pub memory: usize,
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
        let decided = if changes.equality_deletes.is_empty() {
            let streamed = Streamed {
                validator: &job.validator,
                table: &job.table,
                snapshot: step.snapshot,
                operation: step.operation,
                columns: &job.columns,
                scratch: &job.scratch,
                memory: job.memory,
            };
            let (decided, rows) = streamed.validate(io, &changes, &overlays)?;
            job.stats
                .rows
                .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
            decided
        } else {
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
            job.validator
                .validate_with_details(&rows, &overlays)
                .map_err(|e| ApiError::new(e.code(), e.to_string()))?
        };
        match decided {
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

/// A decision and what violates each violated constraint.
pub type Decided = (Decision, BTreeMap<Violation, ViolationDetail>);

/// One step validated by streaming its rows (ADR 0019).
pub struct Streamed<'a> {
    /// The domain's validator.
    pub validator: &'a Validator,
    /// The table.
    pub table: &'a TableId,
    /// The snapshot the step publishes.
    pub snapshot: SnapshotId,
    /// Its declared operation.
    pub operation: Operation,
    /// Projected columns with table types.
    pub columns: &'a [(FieldId, LogicalType)],
    /// Directory for sort runs.
    pub scratch: &'a Path,
    /// Bytes of keys kept in memory before spilling.
    pub memory: usize,
}

/// Reading rows or sorting their keys failed.
enum Failed {
    Inspect(InspectError),
    Api(ApiError),
}

impl From<InspectError> for Failed {
    fn from(e: InspectError) -> Self {
        Failed::Inspect(e)
    }
}

impl From<Failed> for ApiError {
    fn from(f: Failed) -> Self {
        match f {
            Failed::Inspect(e) => ApiError::new(e.code(), e.to_string()),
            Failed::Api(e) => e,
        }
    }
}

fn spill(e: IndexError) -> ApiError {
    ApiError::new(ErrorCode::IndexDegraded, format!("validation sort: {e}"))
}

fn invalid(e: ValidationError) -> ApiError {
    ApiError::new(e.code(), e.to_string())
}

/// A pair of sorters, one per side.
struct Sides([KeySorter; 2]);

impl Sides {
    fn new(scratch: &Path, budget: usize) -> Self {
        Sides([
            KeySorter::new(scratch, budget),
            KeySorter::new(scratch, budget),
        ])
    }

    fn side(&mut self, side: Side) -> &mut KeySorter {
        match side {
            Side::Added => &mut self.0[0],
            Side::Removed => &mut self.0[1],
        }
    }
}

impl Streamed<'_> {
    /// Validates the step: what `check_operation` and `validate_with_details` decide for the
    /// rows `commit_rows` reads, without holding them. Also returns the number of rows read.
    pub fn validate(
        &self,
        io: &impl FileIo,
        changes: &FileChanges,
        indexes: &impl IndexSet,
    ) -> Result<(Decided, u64), ApiError> {
        let mut scan = self
            .validator
            .commit_scan(self.table, self.snapshot)
            .map_err(invalid)?;
        let keyed = scan.keyed();
        let replace = self.operation == Operation::Replace;
        let sorters_needed = 2 * keyed.len() + if replace { 2 } else { 0 };
        let budget = self.memory / sorters_needed.max(1);
        let mut keys: BTreeMap<ConstraintId, Sides> = keyed
            .iter()
            .map(|id| (*id, Sides::new(self.scratch, budget)))
            .collect();
        let mut rows_of = replace.then(|| Sides::new(self.scratch, budget));
        let (mut adds, mut removes, mut read) = (false, false, 0u64);

        for_each_commit_batch(io, changes, self.columns, |side, batch| {
            read += batch.len() as u64;
            match side {
                Side::Added => adds = true,
                Side::Removed => removes = true,
            }
            if let Some(rows) = &mut rows_of {
                let sorter = rows.side(side);
                for row in batch.rows() {
                    let bytes = encode_row(row).map_err(|e| {
                        Failed::Api(ApiError::new(
                            ErrorCode::UnsupportedCommitOperation,
                            format!("row encoding: {e}"),
                        ))
                    })?;
                    sorter
                        .push_bytes(&bytes)
                        .map_err(|e| Failed::Api(spill(e)))?;
                }
            }
            scan.feed(side, &batch, |id, key| -> Result<(), ApiError> {
                keys.get_mut(&id)
                    .ok_or_else(|| invalid(ValidationError::MissingIndex(id)))?
                    .side(side)
                    .push(&key)
                    .map_err(spill)
            })
            .map_err(Failed::Api)
        })?;

        check_sides(self.operation, adds, removes, || {
            let Some(Sides([added, removed])) = rows_of.take() else {
                return Ok(true);
            };
            same(
                added.finish_bytes().map_err(|e| Failed::Api(spill(e)))?,
                removed.finish_bytes().map_err(|e| Failed::Api(spill(e)))?,
            )
            .map_err(|e| Failed::Api(spill(e)))
        })?;

        for id in keyed {
            let Some(Sides([added, removed])) = keys.remove(&id) else {
                continue;
            };
            let mut added = added.finish().map_err(spill)?.peekable();
            let mut removed = removed.finish().map_err(spill)?.peekable();
            // Merge the two sorted sides: each distinct key once, with its count on each side.
            loop {
                let next = match (added.peek(), removed.peek()) {
                    (None, None) => break,
                    (Some(Err(_)), _) | (_, Some(Err(_))) => {
                        let e = match (added.next(), removed.next()) {
                            (Some(Err(e)), _) | (_, Some(Err(e))) => e,
                            _ => IndexError::Corrupt,
                        };
                        return Err(spill(e));
                    }
                    (Some(Ok((a, _))), Some(Ok((r, _)))) => a.cmp(r),
                    (Some(Ok(_)), None) => std::cmp::Ordering::Less,
                    (None, Some(Ok(_))) => std::cmp::Ordering::Greater,
                };
                let (key, a, r): (EncodedKey, u64, u64) = match next {
                    std::cmp::Ordering::Less => {
                        let (k, n) = take(&mut added)?;
                        (k, n, 0)
                    }
                    std::cmp::Ordering::Greater => {
                        let (k, n) = take(&mut removed)?;
                        (k, 0, n)
                    }
                    std::cmp::Ordering::Equal => {
                        let (k, n) = take(&mut added)?;
                        let (_, m) = take(&mut removed)?;
                        (k, n, m)
                    }
                };
                scan.counts(id, key, a, r).map_err(invalid)?;
            }
        }
        let decided = scan.decide(indexes).map_err(invalid)?;
        Ok((decided, read))
    }
}

fn take(
    it: &mut std::iter::Peekable<integrity_index::SortedKeys>,
) -> Result<(EncodedKey, u64), ApiError> {
    match it.next() {
        Some(Ok(item)) => Ok(item),
        Some(Err(e)) => Err(spill(e)),
        None => Err(spill(IndexError::Corrupt)),
    }
}

/// Whether two sorted streams of distinct byte strings with counts are equal.
fn same(
    mut a: integrity_index::SortedBytes,
    mut b: integrity_index::SortedBytes,
) -> Result<bool, IndexError> {
    loop {
        match (a.next().transpose()?, b.next().transpose()?) {
            (None, None) => return Ok(true),
            (Some(x), Some(y)) if x == y => {}
            _ => return Ok(false),
        }
    }
}

/// Applies a commit's staged deltas at `epoch`, atomically across its indexes. Idempotent:
/// re-applying after a crash is a no-op (ADR 0003).
pub fn apply(
    store: &PersistentStore,
    staged: &[(ConstraintId, StagedDelta)],
    indexes: &BTreeMap<ConstraintId, PersistentIndex>,
    epoch: u64,
) -> Result<(), ApiError> {
    let batch = staged
        .iter()
        .map(|(id, delta)| {
            indexes.get(id).map(|index| (index, delta)).ok_or_else(|| {
                ApiError::new(ErrorCode::IndexDegraded, format!("no index for {id}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    fault::hit(FaultPoint::DuringIndexApply);
    // One atomic transaction for every index of the commit.
    store
        .apply_all(&batch, IndexEpoch(epoch))
        .map_err(|e| api(e, ErrorCode::IndexDegraded))
}
