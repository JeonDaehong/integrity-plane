//! Validation of a `main` change (spec §14 steps 5–8b), run on a blocking thread.
//!
//! Each new snapshot is diffed against its parent, its rows extracted and validated against an
//! overlay of the indexes that already contains the previous steps. The overlays' writes become one
//! staged delta per index for the whole commit: recorded in the transaction log, applied after the
//! upstream commit succeeds, and replayed identically by recovery (RFC 0004).
//!
//! Rows are streamed, never held (ADR 0019): the keys of each side are sorted externally within
//! the scan memory budget and only the net change of each key is probed. Commits with equality
//! deletes are read whole (`commit_rows`), as before. A commit of one step without equality
//! deletes is also decided, staged and logged in chunks (ADR 0025): its memory does not grow with
//! the keys it changes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::ScopedJoinHandle;

use integrity_core::{
    Digest, EncodedKey, InvalidCertificate, LogicalType, RowBatch, Side, Violation, encode_row_into,
};
use integrity_iceberg::{
    Budgeted, CommitDeletes, FileChanges, FileIo, InspectError, MainChange, NewSnapshot, Operation,
    TableMetadata, check_operation, check_sides, commit_rows, diff_snapshots, for_each_side_batch,
};
use integrity_index::{
    IndexEpoch, IndexError, KeyIndex, KeySorter, Overlay, PersistentIndex, PersistentStore, Run,
    StagedDelta, TableHead,
};
use integrity_txn::{FaultPoint, StagedEntry, StagedSource, TxnId, TxnLog, fault};
use integrity_types::{ConstraintId, ErrorCode, FieldId, SnapshotId, TableId};
use integrity_validator::{
    CommitScan, Decision, IndexSet, MergedKeys, MergedKeysWriter, StagedRun, StreamOptions,
    StreamedDecision, ValidationError, Validator, ViolationDetail,
};

use crate::error::ApiError;

/// A validated step, ready to be certified.
#[derive(Debug, Clone)]
pub struct StepPlan {
    /// The snapshot.
    pub step: NewSnapshot,
    /// The RFC 0002 key delta digest of its changes.
    pub key_delta: Result<Digest, InvalidCertificate>,
}

/// A commit's writes, one staged delta per index that changes.
#[derive(Debug)]
pub enum Staged {
    /// Held in memory (commits of several steps, or with equality deletes).
    Whole(Vec<(ConstraintId, StagedDelta)>),
    /// In runs that spill to scratch (ADR 0025).
    Runs(Vec<StagedRun>),
}

impl Staged {
    /// The deltas as the transaction log takes them.
    pub fn sources(&self) -> Result<Vec<StagedSource<'_>>, ApiError> {
        match self {
            Staged::Whole(deltas) => Ok(integrity_txn::log::sources(deltas)),
            Staged::Runs(runs) => runs
                .iter()
                .map(|run| {
                    Ok(StagedSource {
                        constraint: run.constraint,
                        header: run.header,
                        writes: run.writes().map_err(spill)?,
                    })
                })
                .collect(),
        }
    }
}

/// Keys per probe and per staged batch of a commit decided in chunks (ADR 0025).
const DECIDE_CHUNK: usize = 100_000;

/// The decision on a main change.
#[derive(Debug)]
pub enum Outcome {
    /// Every step is valid.
    Accepted {
        /// One plan per step, oldest first.
        plans: Vec<StepPlan>,
        /// The whole commit's writes.
        staged: Staged,
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
    /// Locations readable besides the table's own (RFC 0013).
    pub allowed: Vec<String>,
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
    // Only the table's own files (RFC 0013): the writer names every location.
    let confined = integrity_iceberg::Confined::for_table(job.io.as_ref(), &job.meta, &job.allowed);
    let io = Budgeted::new(&confined as &(dyn FileIo + Send + Sync), job.budget);
    let outcome = run_with(job, &io);
    job.stats
        .bytes
        .fetch_add(io.used(), std::sync::atomic::Ordering::Relaxed);
    outcome
}

fn run_with<'a>(
    job: &'a Job,
    io: &Budgeted<'a, dyn FileIo + Send + Sync + 'a>,
) -> Result<Outcome, ApiError> {
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
        let streamed = Streamed {
            validator: &job.validator,
            table: &job.table,
            snapshot: step.snapshot,
            operation: step.operation,
            columns: &job.columns,
            scratch: &job.scratch,
            memory: job.memory,
        };
        if job.change.steps.len() == 1 && changes.equality_deletes.is_empty() {
            // One step: nothing earlier to see, so no overlay; decided and staged in chunks.
            let (decided, rows) = streamed.validate_bounded(io, &changes, &timed, DECIDE_CHUNK)?;
            job.stats
                .rows
                .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
            return match decided {
                (StreamedDecision::Rejected(_), details) => Ok(Outcome::Rejected(details)),
                (StreamedDecision::Accepted(deltas), _) => Ok(Outcome::Accepted {
                    plans: vec![StepPlan {
                        step: step.clone(),
                        key_delta: deltas.key_delta_digest(),
                    }],
                    staged: Staged::Runs(deltas.staged),
                }),
            };
        }
        let decided = if changes.equality_deletes.is_empty() {
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
                    key_delta: validated.key_delta_digest(),
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
    Ok(Outcome::Accepted {
        plans,
        staged: Staged::Whole(staged),
    })
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
    /// The consumer of the rows stopped (and reports why).
    Stopped,
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
            Failed::Stopped => {
                ApiError::new(ErrorCode::IndexDegraded, "validation stopped reading")
            }
        }
    }
}

/// Batches of rows buffered between the reading thread and each consumer.
const QUEUED_BATCHES: usize = 4;

fn spill(e: IndexError) -> ApiError {
    ApiError::new(ErrorCode::IndexDegraded, format!("validation sort: {e}"))
}

fn invalid(e: ValidationError) -> ApiError {
    ApiError::new(e.code(), e.to_string())
}

/// Bytes of keys a sorter takes on the feeding thread before it moves to a thread of its own, and
/// bytes sent to that thread at once; less when the sorter's memory budget is smaller (it spills
/// anyway).
const CHUNK: usize = 256 * 1024;

/// A sorter thread's result.
type Sorted<'s, T> = ScopedJoinHandle<'s, Result<T, ApiError>>;

/// Moves a sorter to a thread of its own: the sender of its chunks, and its result.
type Spawn<'s, T> = Box<dyn FnOnce(KeySorter) -> (SyncSender<Vec<u8>>, Sorted<'s, T>) + Send + 's>;

/// The feeding end of a [`KeySorter`] (ADR 0024). A sorter starts on the feeding thread; once it
/// has taken `limit` bytes it moves to a thread of its own and is sent byte strings packed in
/// chunks, so that sorting and spilling run beside the thread that encodes the keys. Small commits
/// start no thread.
struct Feeder<'s, T> {
    state: State<'s, T>,
    finish: fn(KeySorter) -> Result<T, IndexError>,
    /// [`CHUNK`], or the sorter's budget when smaller.
    limit: usize,
}

enum State<'s, T> {
    Here {
        sorter: KeySorter,
        bytes: usize,
        spawn: Spawn<'s, T>,
    },
    There {
        chunk: Vec<u8>,
        send: SyncSender<Vec<u8>>,
        sorted: Sorted<'s, T>,
    },
    /// Only while moving.
    Moving,
}

/// What a closed [`Feeder`] gives: its sorter's result, or the thread computing it.
enum Finished<'s, T> {
    Here(Result<T, ApiError>),
    There(Sorted<'s, T>),
}

impl<T> Finished<'_, T> {
    fn threaded(&self) -> bool {
        matches!(self, Finished::There(_))
    }

    fn result(self) -> Result<T, ApiError> {
        match self {
            Finished::Here(r) => r,
            Finished::There(h) => joined(h),
        }
    }
}

impl<'s, T: Send + 's> Feeder<'s, T> {
    /// A feeder whose sorter keeps about `budget` bytes in memory and returns `finish(sorter)`.
    fn new(
        s: &'s std::thread::Scope<'s, '_>,
        scratch: &'s Path,
        budget: usize,
        finish: fn(KeySorter) -> Result<T, IndexError>,
    ) -> Self {
        let spawn: Spawn<'s, T> = Box::new(move |sorter| {
            let (send, chunks) = sync_channel::<Vec<u8>>(QUEUED_BATCHES);
            let sorted = s.spawn(move || sort_chunks(sorter, &chunks, finish));
            (send, sorted)
        });
        Self {
            state: State::Here {
                sorter: KeySorter::new(scratch, budget),
                bytes: 0,
                spawn,
            },
            finish,
            limit: CHUNK.min(budget).max(1),
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), ApiError> {
        let grown = match &mut self.state {
            State::Here {
                sorter,
                bytes: taken,
                ..
            } => {
                sorter.push_bytes(bytes).map_err(spill)?;
                *taken += bytes.len();
                *taken >= self.limit
            }
            State::There { chunk, send, .. } => {
                let len = u32::try_from(bytes.len()).map_err(|_| spill(IndexError::Corrupt))?;
                chunk.extend_from_slice(&len.to_le_bytes());
                chunk.extend_from_slice(bytes);
                if chunk.len() >= self.limit {
                    let full = std::mem::replace(chunk, Vec::with_capacity(self.limit + 64));
                    send.send(full).map_err(|_| stopped_sorting())?;
                }
                false
            }
            State::Moving => return Err(spill(IndexError::Corrupt)),
        };
        if grown
            && let State::Here { sorter, spawn, .. } =
                std::mem::replace(&mut self.state, State::Moving)
        {
            let (send, sorted) = spawn(sorter);
            self.state = State::There {
                chunk: Vec::with_capacity(self.limit + 64),
                send,
                sorted,
            };
        }
        Ok(())
    }

    /// Sends what is left and closes the feeder: whether that worked, and the sorter's result.
    fn close(self) -> (Result<(), ApiError>, Finished<'s, T>) {
        match self.state {
            State::Here { sorter, .. } => {
                (Ok(()), Finished::Here((self.finish)(sorter).map_err(spill)))
            }
            State::There {
                chunk,
                send,
                sorted,
            } => {
                let sent = if chunk.is_empty() {
                    Ok(())
                } else {
                    send.send(chunk).map_err(|_| stopped_sorting())
                };
                (sent, Finished::There(sorted))
            }
            State::Moving => (
                Err(spill(IndexError::Corrupt)),
                Finished::Here(Err(spill(IndexError::Corrupt))),
            ),
        }
    }
}

/// A sorter's thread: pushes the byte strings of every chunk, then returns `finish(sorter)`.
fn sort_chunks<T>(
    mut sorter: KeySorter,
    chunks: &std::sync::mpsc::Receiver<Vec<u8>>,
    finish: fn(KeySorter) -> Result<T, IndexError>,
) -> Result<T, ApiError> {
    for chunk in chunks {
        let mut at = 0;
        while at < chunk.len() {
            let len = chunk
                .get(at..at + 4)
                .and_then(|b| <[u8; 4]>::try_from(b).ok())
                .map(u32::from_le_bytes)
                .ok_or_else(|| spill(IndexError::Corrupt))? as usize;
            let bytes = chunk
                .get(at + 4..at + 4 + len)
                .ok_or_else(|| spill(IndexError::Corrupt))?;
            sorter.push_bytes(bytes).map_err(spill)?;
            at += 4 + len;
        }
    }
    finish(sorter).map_err(spill)
}

/// Closes `feeders`: the first failure to send, and every sorter's result in order.
fn close_all<'s, T: Send + 's>(
    feeders: impl IntoIterator<Item = Feeder<'s, T>>,
) -> (Result<(), ApiError>, Vec<Finished<'s, T>>) {
    let mut sent = Ok(());
    let mut finished = Vec::new();
    for f in feeders {
        let (s, done) = f.close();
        if sent.is_ok() {
            sent = s;
        }
        finished.push(done);
    }
    (sent, finished)
}

/// What the threads of a streamed validation return: whether both sides hold the same rows (a
/// `replace`), the merged keys of each constraint, and the number of rows added and removed.
type Scanned = (Option<Result<bool, ApiError>>, Vec<Merged>, (u64, u64));

impl<'a> Streamed<'a> {
    /// Validates the step: what `check_operation` and `validate_with_details` decide for the
    /// rows `commit_rows` reads, without holding them. Also returns the number of rows read.
    pub fn validate(
        &self,
        io: &impl FileIo,
        changes: &FileChanges,
        indexes: &impl IndexSet,
    ) -> Result<(Decided, u64), ApiError> {
        let (mut scan, runs, rows) = self.merged(io, changes)?;
        for (id, merged) in &runs {
            for counted in merged.counts().map_err(invalid)? {
                let (key, a, r) = counted.map_err(invalid)?;
                scan.counts(*id, key, a, r).map_err(invalid)?;
            }
        }
        Ok((scan.decide(indexes).map_err(invalid)?, rows))
    }

    /// [`Streamed::validate`], then decided, staged and digested `chunk` keys at a time
    /// (ADR 0025): for a commit of one step, whose memory must not grow with its keys.
    pub fn validate_bounded(
        &self,
        io: &impl FileIo,
        changes: &FileChanges,
        indexes: &impl IndexSet,
        chunk: usize,
    ) -> Result<(BoundedDecided, u64), ApiError> {
        let (scan, runs, rows) = self.merged(io, changes)?;
        let runs: BTreeMap<ConstraintId, MergedKeys> = runs.into_iter().collect();
        let options = StreamOptions {
            scratch: self.scratch,
            memory: self.memory,
            chunk,
        };
        let decided = scan
            .decide_streamed(indexes, &runs, options)
            .map_err(invalid)?;
        Ok((decided, rows))
    }

    /// The RFC 0002 key delta digest of the step's rows, read and merged like
    /// [`Streamed::validate_bounded`] reads them, without probing anything: what `verify`
    /// recomputes. Unlike validating, it does not check the rows against the declared operation
    /// (pass [`Operation::Overwrite`]: a certificate covers the rows, whatever the summary says).
    pub fn key_delta_digest(
        &self,
        io: &impl FileIo,
        changes: &FileChanges,
    ) -> Result<Result<Digest, InvalidCertificate>, ApiError> {
        let (scan, runs, _) = self.merged(io, changes)?;
        let runs: BTreeMap<ConstraintId, MergedKeys> = runs.into_iter().collect();
        scan.key_delta_digest(&runs).map_err(invalid)
    }

    /// Reads, checks and sorts both sides of the step, and merges each keyed constraint's sides
    /// into a run of the keys that matter ([`counted_value`] records), in [`CommitScan::keyed`]
    /// order. Also returns the number of rows read.
    #[allow(clippy::type_complexity)]
    fn merged(
        &self,
        io: &impl FileIo,
        changes: &FileChanges,
    ) -> Result<(CommitScan<'a>, Vec<(ConstraintId, MergedKeys)>, u64), ApiError> {
        let mut scan = self
            .validator
            .commit_scan(self.table, self.snapshot)
            .map_err(invalid)?;
        let deletes = CommitDeletes::read(io, changes).map_err(Failed::Inspect)?;
        let keyed = scan.keyed();
        let replace = self.operation == Operation::Replace;
        let sorters_needed = 2 * keyed.len() + if replace { 2 } else { 0 };
        let budget = self.memory / sorters_needed.max(1);
        let run_budget = self.memory / keyed.len().max(1);
        let scratch = self.scratch;
        let new_run = move || MergedKeysWriter::new(Run::new(scratch, run_budget));
        let mut removed_scan = scan.removed();
        // A side without data files to read, and no kept files, has no rows.
        let has = |side| {
            changes.positions.is_some()
                || match side {
                    Side::Added => !changes.added.is_empty(),
                    Side::Removed => !changes.removed.is_empty(),
                }
        };
        // Threads (ADR 0024): each side is read on its own thread and fed on its own: the added
        // rows here (NULL rules and keys, in the order the files are read), the removed rows on
        // another (keys only); for a `replace`, one encodes the rows of both sides. Every side of
        // every constraint (and of the rows) is sorted on a thread of its own once it is large,
        // and then each constraint's two sides are merged on one, each side read ahead on its
        // own. Each channel holds a few batches or chunks, so memory stays bounded. The results
        // are used in the order validating at once would use them: the rows first, then the keys
        // by constraint.
        let (same_rows, merged, counts) = std::thread::scope(|s| -> Result<Scanned, ApiError> {
            let deletes = &deletes;
            let read_side = |side: Side| {
                let (send, batches) = sync_channel::<Arc<RowBatch>>(QUEUED_BATCHES);
                let reader = has(side).then(|| {
                    s.spawn(move || {
                        for_each_side_batch(io, changes, deletes, self.columns, side, |batch| {
                            send.send(Arc::new(batch)).map_err(|_| Failed::Stopped)
                        })
                    })
                });
                (reader, batches)
            };
            let (added_reader, added_batches) = read_side(Side::Added);
            let (removed_reader, removed_batches) = read_side(Side::Removed);

            let new_keys = || Feeder::new(s, self.scratch, budget, KeySorter::finish);
            let mut added_feeders: BTreeMap<ConstraintId, _> =
                keyed.iter().map(|id| (*id, new_keys())).collect();
            let mut removed_feeders: BTreeMap<ConstraintId, _> =
                keyed.iter().map(|id| (*id, new_keys())).collect();
            let (send_rows, rows) = sync_channel::<(Side, Arc<RowBatch>)>(QUEUED_BATCHES);
            let row_encoder = replace.then(|| {
                let new_rows = || Feeder::new(s, self.scratch, budget, KeySorter::finish_bytes);
                let mut sides = [new_rows(), new_rows()];
                s.spawn(move || {
                    let mut bytes = Vec::new();
                    for (side, batch) in rows {
                        let feeder = &mut sides[usize::from(side == Side::Removed)];
                        for row in batch.rows() {
                            encode_row_into(row, &mut bytes);
                            feeder.push(&bytes)?;
                        }
                    }
                    Ok::<_, ApiError>(sides)
                })
            });
            let send_rows = row_encoder.is_some().then_some(send_rows);

            // The removed side: keys only.
            let removed_rows = send_rows.clone();
            let feed_removed = move || {
                let mut count = 0u64;
                let mut fed = Ok(());
                // Leaving the loop early drops the receiver, which stops the reader.
                for batch in removed_batches {
                    count += batch.len() as u64;
                    if let Some(rows) = &removed_rows
                        && rows.send((Side::Removed, Arc::clone(&batch))).is_err()
                    {
                        break;
                    }
                    fed = removed_scan.feed(&batch, |id, key| feed(&mut removed_feeders, id, &key));
                    if fed.is_err() {
                        break;
                    }
                }
                let (sent, finished) = close_all(removed_feeders.into_values());
                (removed_scan, count, fed.and(sent), finished)
            };
            let removed_feed = if removed_reader.is_some() {
                Ok(s.spawn(feed_removed))
            } else {
                Err(feed_removed())
            };

            // The added side, here.
            let mut added_count = 0u64;
            let mut fed = Ok(());
            for batch in added_batches {
                added_count += batch.len() as u64;
                // A row encoder that stopped reports its error below.
                if let Some(rows) = &send_rows
                    && rows.send((Side::Added, Arc::clone(&batch))).is_err()
                {
                    break;
                }
                fed = scan.feed(Side::Added, &batch, |id, key| {
                    feed(&mut added_feeders, id, &key)
                });
                if fed.is_err() {
                    break;
                }
            }
            drop(send_rows);
            let (sent, added_sorted) = close_all(added_feeders.into_values());
            let (removed_scan, removed_count, removed_fed, removed_sorted) =
                removed_feed.map_or_else(|done| done, joined);
            scan.absorb(removed_scan);

            // Every thread is joined before anything is reported: a sorter that stopped explains
            // why the feeding stopped.
            let rows_sorted = row_encoder.map(|encoder| {
                let (sent, finished) = close_all(joined(encoder)?);
                sent?;
                Ok::<_, ApiError>(finished)
            });
            let large = added_sorted
                .iter()
                .chain(&removed_sorted)
                .any(Finished::threaded)
                || rows_sorted
                    .iter()
                    .flatten()
                    .flatten()
                    .any(Finished::threaded);
            let rows_sorted = rows_sorted.map(|finished| {
                finished.and_then(|f| {
                    f.into_iter()
                        .map(Finished::result)
                        .collect::<Result<Vec<_>, _>>()
                })
            });
            let sorted: Vec<(Result<_, ApiError>, Result<_, ApiError>)> = added_sorted
                .into_iter()
                .map(Finished::result)
                .zip(removed_sorted.into_iter().map(Finished::result))
                .collect();
            let reads = [added_reader, removed_reader].map(|r| r.map(joined));
            if let Some(e) = sorted
                .iter()
                .flat_map(|(a, r)| [a.as_ref().err(), r.as_ref().err()])
                .flatten()
                .next()
            {
                return Err(e.clone());
            }
            fed?;
            removed_fed?;
            sent?;
            for read in reads.into_iter().flatten() {
                match read {
                    Ok(()) => {}
                    Err(Failed::Stopped) => {
                        return Err(ApiError::new(
                            ErrorCode::IndexDegraded,
                            "validation stopped reading",
                        ));
                    }
                    Err(f) => return Err(f.into()),
                }
            }
            let rows_sorted = rows_sorted.transpose()?;
            if !large {
                // A small commit: everything sorted in memory, merged here.
                let same_rows = rows_sorted.map(|sides| match <[_; 2]>::try_from(sides) {
                    Ok([a, r]) => same(a, r).map_err(spill),
                    Err(_) => Err(spill(IndexError::Corrupt)),
                });
                let merged = sorted
                    .into_iter()
                    .map(|(a, r)| merge_sides(a?, r?, new_run()))
                    .collect();
                return Ok((same_rows, merged, (added_count, removed_count)));
            }
            // The rows are compared while each constraint's sides are merged.
            let comparing = match rows_sorted.map(<[_; 2]>::try_from) {
                Some(Ok([a, r])) => {
                    let (a, r) = (ahead(s, a), ahead(s, r));
                    Some(s.spawn(move || same(a, r).map_err(spill)))
                }
                Some(Err(_)) => return Err(spill(IndexError::Corrupt)),
                None => None,
            };
            let merging = sorted
                .into_iter()
                .map(|(a, r)| -> Result<_, ApiError> {
                    let (a, r) = (ahead(s, a?), ahead(s, r?));
                    let run = new_run();
                    Ok(s.spawn(move || merge_sides(a, r, run)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let same_rows = comparing.map(joined);
            Ok((
                same_rows,
                merging.into_iter().map(joined).collect(),
                (added_count, removed_count),
            ))
        })?;
        let (added_count, removed_count) = counts;
        check_sides(self.operation, added_count > 0, removed_count > 0, || {
            same_rows.unwrap_or(Ok(true)).map_err(Failed::Api)
        })?;
        let runs = keyed
            .iter()
            .zip(merged)
            .map(|(id, run)| Ok((*id, run?)))
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok((scan, runs, added_count + removed_count))
    }
}

/// A decision taken in chunks and what violates each violated constraint.
pub type BoundedDecided = (StreamedDecision, BTreeMap<Violation, ViolationDetail>);

/// Hands `key` of constraint `id` to its sorter.
fn feed<'s, T: Send + 's>(
    feeders: &mut BTreeMap<ConstraintId, Feeder<'s, T>>,
    id: ConstraintId,
    key: &EncodedKey,
) -> Result<(), ApiError> {
    feeders
        .get_mut(&id)
        .ok_or_else(|| invalid(ValidationError::MissingIndex(id)))?
        .push(key.as_bytes())
}

fn stopped_sorting() -> ApiError {
    ApiError::new(ErrorCode::IndexDegraded, "validation sort stopped")
}

/// Records sent from a reading thread at once.
const AHEAD: usize = 4096;

/// A sorted side read ahead on a thread of its own (ADR 0024): the two sides of a merge are read
/// at once, and the merging thread only compares.
struct Ahead<T> {
    batches: std::sync::mpsc::Receiver<Vec<Result<T, IndexError>>>,
    batch: std::vec::IntoIter<Result<T, IndexError>>,
}

impl<T> Iterator for Ahead<T> {
    type Item = Result<T, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.batch.next() {
                return Some(item);
            }
            // The sender is dropped once the side is read (or failed, its error sent last).
            self.batch = self.batches.recv().ok()?.into_iter();
        }
    }
}

/// Reads `sorted` ahead on a thread of the scope `s`; dropping the result stops the thread.
fn ahead<'s, T: Send + 's>(
    s: &'s std::thread::Scope<'s, '_>,
    sorted: impl Iterator<Item = Result<T, IndexError>> + Send + 's,
) -> Ahead<T> {
    let (send, batches) = sync_channel(2);
    s.spawn(move || {
        let mut batch = Vec::with_capacity(AHEAD);
        for item in sorted {
            let failed = item.is_err();
            batch.push(item);
            if batch.len() == AHEAD || failed {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(AHEAD));
                if send.send(full).is_err() || failed {
                    return;
                }
            }
        }
        let _ = send.send(batch);
    });
    Ahead {
        batches,
        batch: Vec::new().into_iter(),
    }
}

/// The result of a scoped thread; its panic, if any, continues on the caller.
fn joined<T>(h: ScopedJoinHandle<'_, T>) -> T {
    h.join().unwrap_or_else(|p| std::panic::resume_unwind(p))
}

/// A distinct key with its count among the added and the removed rows.
type Counted = (EncodedKey, u64, u64);

/// The keys of one constraint that matter, merged from both sides.
type Merged = Result<MergedKeys, ApiError>;

/// Sorted distinct keys with their counts.
trait Keys: Iterator<Item = Result<(EncodedKey, u64), IndexError>> {}

impl<I: Iterator<Item = Result<(EncodedKey, u64), IndexError>>> Keys for I {}

/// Merges the two sorted sides of one constraint: each distinct key once, with its count on each
/// side, keeping only the keys that matter to the validator ([`CommitScan::counts_matter`]).
fn merge_sides(added: impl Keys, removed: impl Keys, mut out: MergedKeysWriter) -> Merged {
    let mut added = added.peekable();
    let mut removed = removed.peekable();
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
        let (key, a, r): Counted = match next {
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
        if CommitScan::counts_matter(a, r) {
            out.push(key.as_bytes(), a, r).map_err(spill)?;
        }
    }
    out.finish().map_err(spill)
}

fn take(it: &mut impl Keys) -> Result<(EncodedKey, u64), ApiError> {
    match it.next() {
        Some(Ok(item)) => Ok(item),
        Some(Err(e)) => Err(spill(e)),
        None => Err(spill(IndexError::Corrupt)),
    }
}

/// Whether two sorted streams of distinct byte strings with counts are equal.
fn same(
    mut a: impl Iterator<Item = Result<(Vec<u8>, u64), IndexError>>,
    mut b: impl Iterator<Item = Result<(Vec<u8>, u64), IndexError>>,
) -> Result<bool, IndexError> {
    loop {
        match (a.next().transpose()?, b.next().transpose()?) {
            (None, None) => return Ok(true),
            (Some(x), Some(y)) if x == y => {}
            _ => return Ok(false),
        }
    }
}

/// Applies a commit's staged deltas as validation left them, at `epoch`, atomically across its
/// indexes: what the transaction log holds, without reading it back. Records the committed
/// table's `head` in the same transaction (RFC 0008). Idempotent (ADR 0003).
pub fn apply_staged(
    store: &PersistentStore,
    staged: &Staged,
    indexes: &BTreeMap<ConstraintId, PersistentIndex>,
    epoch: u64,
    head: (&str, TableHead),
) -> Result<(), ApiError> {
    let batch = staged
        .sources()?
        .into_iter()
        .map(|source| {
            let id = source.constraint;
            let index = indexes.get(&id).ok_or_else(|| {
                ApiError::new(ErrorCode::IndexDegraded, format!("no index for {id}"))
            })?;
            Ok((index, source.header, source.writes))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    fault::hit(FaultPoint::DuringIndexApply);
    // One atomic transaction for every index of the commit.
    store
        .apply_streamed(batch, IndexEpoch(epoch), &[head])
        .map_err(|e| api(e, ErrorCode::IndexDegraded))
}

/// Applies a commit's staged deltas, read from the transaction log one chunk at a time (RFC
/// 0006), at `epoch`, atomically across its indexes: how recovery finishes a commit. Idempotent:
/// re-applying after a crash is a no-op (ADR 0003). Records `head` like [`apply_staged`].
pub fn apply(
    store: &PersistentStore,
    log: &TxnLog,
    txn: TxnId,
    staged: &[StagedEntry],
    indexes: &BTreeMap<ConstraintId, PersistentIndex>,
    epoch: u64,
    head: (&str, TableHead),
) -> Result<(), ApiError> {
    let logged =
        |e: integrity_txn::TxnError| ApiError::new(ErrorCode::IndexDegraded, e.to_string());
    let batch = staged
        .iter()
        .map(|entry| {
            let id = entry.constraint_id();
            let index = indexes.get(&id).ok_or_else(|| {
                ApiError::new(ErrorCode::IndexDegraded, format!("no index for {id}"))
            })?;
            let header = entry.header().map_err(logged)?;
            let writes = log.staged_writes(txn, entry).map_err(logged)?;
            Ok((index, header, writes))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    fault::hit(FaultPoint::DuringIndexApply);
    // One atomic transaction for every index of the commit.
    store
        .apply_streamed(batch, IndexEpoch(epoch), &[head])
        .map_err(|e| api(e, ErrorCode::IndexDegraded))
}
