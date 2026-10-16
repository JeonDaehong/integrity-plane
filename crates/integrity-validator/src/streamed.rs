//! Deciding a commit with bounded memory after the merge (ADR 0025).
//!
//! [`CommitScan::decide`] collects every changed key. [`CommitScan::decide_streamed`] takes the
//! merged keys of each keyed constraint as [`MergedKeys`]: a [`FinishedRun`] of
//! `(key, added ‖ removed)` records and what the merge counted. It reads the runs back a batch of
//! at most [`StreamOptions::chunk`] keys at a time, deciding exactly what [`CommitScan::decide`]
//! decides for the same counts, and staging exactly what
//! [`ValidatedDeltas::stage`](crate::ValidatedDeltas::stage) stages.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use integrity_core::{
    ConstraintKind, Digest, EncodedKey, InvalidCertificate, KeyDeltaDigest, NetDelta, Violation,
};
use integrity_index::{
    FinishedRun, IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, Run, StagedDigest,
    StagedHeader, Writes, decode_staged_value, encode_value, stage_from,
};
use integrity_types::{ConstraintId, ErrorCode, SnapshotId};

use crate::{CommitScan, Inconsistency, IndexSet, ValidationError, ViolationDetail, index, probe};

/// The value of a merged key's record: how often the added and the removed rows have it.
pub fn counted_value(added: u64, removed: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&added.to_be_bytes());
    out[8..].copy_from_slice(&removed.to_be_bytes());
    out
}

/// The counts of a [`counted_value`].
pub fn decode_counted(bytes: &[u8]) -> Option<(u64, u64)> {
    let value: [u8; 16] = bytes.try_into().ok()?;
    let mut half = [0u8; 8];
    half.copy_from_slice(&value[..8]);
    let added = u64::from_be_bytes(half);
    half.copy_from_slice(&value[8..]);
    Some((added, u64::from_be_bytes(half)))
}

fn corrupt() -> ValidationError {
    ValidationError::Index(IndexError::Corrupt)
}

/// One constraint's merged keys, in key order, and what the merge counted on the way.
#[derive(Debug)]
pub struct MergedKeys {
    run: FinishedRun,
    changes: u64,
    repeated: bool,
    falls_twice: bool,
}

/// A merged key with how often the added and the removed rows have it.
pub type KeyCounts = (EncodedKey, u64, u64);

impl MergedKeys {
    /// Each key with its counts, in key order.
    pub fn counts(
        &self,
    ) -> Result<impl Iterator<Item = Result<KeyCounts, ValidationError>> + '_, ValidationError>
    {
        Ok(counted_records(Some(&self.run))?.map(|c| c.map(|c| (c.key, c.added, c.removed))))
    }
}

/// Writes [`MergedKeys`]: each key once, in strictly increasing key order.
#[derive(Debug)]
pub struct MergedKeysWriter {
    run: Run,
    changes: u64,
    repeated: bool,
    falls_twice: bool,
}

impl MergedKeysWriter {
    /// Writes into `run`.
    pub fn new(run: Run) -> Self {
        Self {
            run,
            changes: 0,
            repeated: false,
            falls_twice: false,
        }
    }

    /// Adds a key that the added rows have `added` times and the removed ones `removed` times.
    pub fn push(&mut self, key: &[u8], added: u64, removed: u64) -> Result<(), IndexError> {
        self.run.push(key, &counted_value(added, removed))?;
        if added != removed {
            self.changes += 1;
        }
        self.repeated |= added > 1;
        self.falls_twice |= removed > added.saturating_add(1);
        Ok(())
    }

    /// Ends writing.
    pub fn finish(self) -> Result<MergedKeys, IndexError> {
        Ok(MergedKeys {
            run: self.run.finish()?,
            changes: self.changes,
            repeated: self.repeated,
            falls_twice: self.falls_twice,
        })
    }
}

/// One merged key: the key, its counts on each side, and its net change.
struct Counted {
    key: EncodedKey,
    added: u64,
    removed: u64,
    change: i128,
}

/// The records of a run of merged keys.
fn counted_records(
    run: Option<&FinishedRun>,
) -> Result<impl Iterator<Item = Result<Counted, ValidationError>> + '_, ValidationError> {
    let records = match run {
        Some(run) => Some(run.records().map_err(ValidationError::Index)?),
        None => None,
    };
    Ok(records.into_iter().flatten().map(|record| {
        let (key, value) = record.map_err(ValidationError::Index)?;
        let key = EncodedKey::from_vec(key).map_err(|_| corrupt())?;
        let (added, removed) = decode_counted(&value).ok_or_else(corrupt)?;
        Ok(Counted {
            key,
            added,
            removed,
            change: i128::from(added) - i128::from(removed),
        })
    }))
}

/// Calls `f` with consecutive batches of at most `size` changed keys (non-zero net change).
fn for_each_batch(
    run: Option<&FinishedRun>,
    size: usize,
    mut f: impl FnMut(&[Counted]) -> Result<(), ValidationError>,
) -> Result<(), ValidationError> {
    let size = size.max(1);
    let mut batch = Vec::with_capacity(size.min(4096));
    for c in counted_records(run)? {
        let c = c?;
        if c.change != 0 {
            batch.push(c);
            if batch.len() == size {
                f(&batch)?;
                batch.clear();
            }
        }
    }
    if !batch.is_empty() {
        f(&batch)?;
    }
    Ok(())
}

/// How [`CommitScan::decide_streamed`] works.
#[derive(Debug, Clone, Copy)]
pub struct StreamOptions<'a> {
    /// Directory for staged runs that outgrow their memory.
    pub scratch: &'a Path,
    /// Bytes of staged writes kept in memory, over all indexes, before spilling.
    pub memory: usize,
    /// Keys per probe and per staged batch.
    pub chunk: usize,
}

/// The staged writes of one index: what `apply_streamed` and the transaction log take.
#[derive(Debug)]
pub struct StagedRun {
    /// The constraint whose index it changes.
    pub constraint: ConstraintId,
    /// Base epoch, number of writes and digest.
    pub header: StagedHeader,
    run: FinishedRun,
}

impl StagedRun {
    /// The writes, in key order.
    pub fn writes(&self) -> Result<Writes<'_>, IndexError> {
        let records = self.run.records()?;
        Ok(Box::new(records.map(|record| {
            let (key, value) = record?;
            let key = EncodedKey::from_vec(key).map_err(|_| IndexError::Corrupt)?;
            Ok((key, decode_staged_value(&value)?))
        })))
    }
}

/// An accepted commit decided by [`CommitScan::decide_streamed`].
#[derive(Debug)]
pub struct StreamedDeltas {
    key_delta_digest: Result<Digest, InvalidCertificate>,
    /// The staged writes of every index the commit changes, in constraint id order.
    pub staged: Vec<StagedRun>,
}

impl StreamedDeltas {
    /// The RFC 0002 key delta digest of the commit, as
    /// [`ValidatedDeltas::key_delta_digest`](crate::ValidatedDeltas::key_delta_digest).
    pub fn key_delta_digest(&self) -> Result<Digest, InvalidCertificate> {
        self.key_delta_digest
    }
}

/// What [`CommitScan::decide_streamed`] decides.
#[derive(Debug)]
pub enum StreamedDecision {
    /// Every enforced constraint holds after the commit.
    Accepted(StreamedDeltas),
    /// Every violated `(constraint, code)`.
    Rejected(BTreeSet<Violation>),
}

/// The staged writes of one index being written.
struct Stager {
    kind: IndexKind,
    base_epoch: IndexEpoch,
    writes: u64,
    digest: StagedDigest,
    run: Run,
}

impl Stager {
    /// Stages `batch` against the current values of its keys.
    fn stage(
        &mut self,
        snapshot: SnapshotId,
        batch: &[Counted],
        current: &[Option<IndexValue>],
    ) -> Result<(), ValidationError> {
        let changes = batch.iter().map(|c| (c.key.clone(), c.change)).collect();
        let delta = IndexDelta {
            snapshot,
            changes: NetDelta::from_sorted(changes).map_err(ValidationError::Delta)?,
        };
        let part = stage_from(self.kind, &delta, self.base_epoch, current)
            .map_err(ValidationError::Index)?;
        for (key, value) in part.writes() {
            self.digest
                .update(key, value)
                .map_err(ValidationError::Index)?;
            self.run
                .push(key.as_bytes(), encode_value(value).as_slice())
                .map_err(ValidationError::Index)?;
        }
        Ok(())
    }

    fn finish(self, constraint: ConstraintId) -> Result<StagedRun, ValidationError> {
        Ok(StagedRun {
            constraint,
            header: StagedHeader {
                base_epoch: self.base_epoch,
                writes: self.writes,
                digest: self.digest.finish().map_err(ValidationError::Index)?,
            },
            run: self.run.finish().map_err(ValidationError::Index)?,
        })
    }
}

/// The key delta digest (RFC 0002), computed in the pass: the validator keeps its constraints in
/// id order, as the digest takes them (a different order fails the digest, and so the commit).
enum Digesting {
    Here(Box<KeyDeltaDigest>),
    Failed(InvalidCertificate),
}

impl Digesting {
    fn constraint(&mut self, id: ConstraintId, changes: u64) {
        if let Digesting::Here(d) = self
            && let Err(e) = d.constraint(id, changes)
        {
            *self = Digesting::Failed(e);
        }
    }

    fn changes(&mut self, batch: &[Counted]) {
        if let Digesting::Here(d) = self
            && let Some(e) = batch
                .iter()
                .find_map(|c| d.change(c.key.as_bytes(), c.change).err())
        {
            *self = Digesting::Failed(e);
        }
    }
}

impl CommitScan<'_> {
    /// The key delta digest of the merged keys alone, without deciding anything: what
    /// [`Validator::net_key_deltas`](crate::Validator::net_key_deltas) digests for the same rows,
    /// read back one key at a time. `verify` recomputes certificates with it. Fails like
    /// [`CommitScan::decide`] on the errors the rows had.
    pub fn key_delta_digest(
        self,
        merged: &BTreeMap<ConstraintId, MergedKeys>,
    ) -> Result<Result<Digest, InvalidCertificate>, ValidationError> {
        if let Some((_, e)) = self.errors.into_iter().next() {
            return Err(e);
        }
        let keyed = self
            .ctx
            .own
            .iter()
            .filter(|rc| rc.constraint.kind.key().is_some());
        let mut digest = match KeyDeltaDigest::new(keyed.clone().count()) {
            Ok(d) => Digesting::Here(Box::new(d)),
            Err(e) => Digesting::Failed(e),
        };
        for rc in keyed {
            let id = rc.constraint.id;
            let m = merged.get(&id);
            digest.constraint(id, m.map_or(0, |m| m.changes));
            for_each_batch(m.map(|m| &m.run), 4096, |batch| {
                digest.changes(batch);
                Ok(())
            })?;
        }
        Ok(match digest {
            Digesting::Here(d) => d.finish(),
            Digesting::Failed(e) => Err(e),
        })
    }

    /// [`CommitScan::counts`] for every key of `merged`, then [`CommitScan::decide`], then
    /// staging, with bounded memory (ADR 0025). `merged` holds, per keyed constraint, its merged
    /// keys (at least those [`CommitScan::counts_matter`]; a constraint without an entry has none).
    ///
    /// One pass over each constraint's changed keys probes them, records violations, stages them
    /// while nothing is violated (a unique index's current values are the probe's) and digests
    /// them. Duplicates among the added rows are recorded in a pass of their own before (spec §8),
    /// only where the merge saw any.
    pub fn decide_streamed(
        mut self,
        indexes: &impl IndexSet,
        merged: &BTreeMap<ConstraintId, MergedKeys>,
        options: StreamOptions<'_>,
    ) -> Result<(StreamedDecision, BTreeMap<Violation, ViolationDetail>), ValidationError> {
        let keyed = self.keyed();
        let run = |id: &ConstraintId| merged.get(id).map(|m| &m.run);
        let changes = |id: &ConstraintId| merged.get(id).map_or(0, |m| m.changes);
        let duplicate_code = |kind: &ConstraintKind| match kind {
            ConstraintKind::PrimaryKey(_) => Some(ErrorCode::DuplicatePrimaryKey),
            ConstraintKind::Unique(_) => Some(ErrorCode::DuplicateUniqueKey),
            _ => None,
        };

        // What `counts` does for each key: duplicates among the added rows (spec §8, before any
        // probe). Merged keys are distinct: each is reported once.
        for id in &keyed {
            let rc = self
                .ctx
                .own
                .iter()
                .find(|rc| rc.constraint.id == *id)
                .ok_or(ValidationError::MissingIndex(*id))?;
            let Some(code) = duplicate_code(&rc.constraint.kind) else {
                continue;
            };
            if !merged.get(id).is_some_and(|m| m.repeated) {
                continue;
            }
            for c in counted_records(run(id))? {
                let c = c?;
                if c.added > 1 {
                    self.plan.record_distinct_key(*id, code, &c.key);
                }
            }
        }

        // What `decide` does, in the same order.
        let observed = self.validator.observe(&self.ctx, indexes)?;
        if let Some((_, e)) = std::mem::take(&mut self.errors).into_iter().next() {
            return Err(e);
        }
        let mut digest = match KeyDeltaDigest::new(keyed.len()) {
            Ok(d) => Digesting::Here(Box::new(d)),
            Err(e) => Digesting::Failed(e),
        };
        let mut stagers: BTreeMap<ConstraintId, Stager> = BTreeMap::new();
        let staging = keyed.iter().filter(|id| changes(id) > 0).count();
        let budget = options.memory / staging.max(1);
        let snapshot = self.snapshot;
        let chunk = options.chunk;
        for rc in &self.ctx.own {
            let id = rc.constraint.id;
            if !keyed.contains(&id) {
                continue;
            }
            digest.constraint(id, changes(&id));
            let duplicate = duplicate_code(&rc.constraint.kind);
            if duplicate.is_some() && merged.get(&id).is_some_and(|m| m.falls_twice) {
                return Err(ValidationError::Inconsistent(
                    Inconsistency::RemovedKeyNotIndexed(id),
                ));
            }
            let mut stager = if changes(&id) > 0 {
                let base_epoch = *observed.get(&id).ok_or(ValidationError::MissingIndex(id))?;
                Some(Stager {
                    kind: if duplicate.is_some() {
                        IndexKind::Unique
                    } else {
                        IndexKind::Reference
                    },
                    base_epoch,
                    writes: changes(&id),
                    digest: StagedDigest::new(base_epoch, changes(&id)),
                    run: Run::new(options.scratch, budget),
                })
            } else {
                None
            };
            let plan = &mut self.plan;
            for_each_batch(run(&id), chunk, |batch| {
                let keys: Vec<EncodedKey> = batch.iter().map(|c| c.key.clone()).collect();
                let current = match (&rc.constraint.kind, duplicate) {
                    (_, Some(code)) => {
                        let found = probe(indexes, id, &keys)?;
                        for (c, value) in batch.iter().zip(&found) {
                            match (c.change > 0, value) {
                                // Count rises and the key is already present: duplicate (already
                                // recorded if the added rows repeat it).
                                (true, Some(_)) => {
                                    if c.added <= 1 {
                                        plan.record_distinct_key(id, code, &c.key);
                                    }
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
                        Some(found)
                    }
                    (ConstraintKind::ForeignKey(fk), None) => {
                        // Only keys whose count rises need a parent: a key that was already
                        // referenced had a parent before, and the parent table is not written.
                        let rising: Vec<EncodedKey> = batch
                            .iter()
                            .filter(|c| c.change > 0)
                            .map(|c| c.key.clone())
                            .collect();
                        let found = probe(indexes, fk.parent_constraint, &rising)?;
                        for (key, value) in rising.iter().zip(found) {
                            if value.is_none() {
                                plan.record_distinct_key(id, ErrorCode::ForeignKeyViolation, key);
                            }
                        }
                        None
                    }
                    _ => None,
                };
                digest.changes(batch);
                // Staging is useless once anything is violated (and could not resolve a
                // duplicate): stop.
                if !plan.violations.is_empty() {
                    stager = None;
                }
                if let Some(s) = &mut stager {
                    let current = match current {
                        Some(found) => found,
                        None => probe(indexes, id, &keys)?,
                    };
                    s.stage(snapshot, batch, &current)?;
                }
                Ok(())
            })?;
            if let Some(s) = stager {
                stagers.insert(id, s);
            }
        }
        for (rc, parent) in &self.ctx.referencing {
            if !keyed.contains(parent) {
                return Err(ValidationError::Unprovable(rc.constraint.id));
            }
            let child = rc.constraint.id;
            let plan = &mut self.plan;
            // Parent keys that disappear (removed and not re-added).
            for_each_batch(run(parent), chunk, |batch| {
                let gone: Vec<EncodedKey> = batch
                    .iter()
                    .filter(|c| c.change < 0)
                    .map(|c| c.key.clone())
                    .collect();
                for (key, value) in gone.iter().zip(probe(indexes, child, &gone)?) {
                    match value {
                        None => {}
                        Some(IndexValue::Reference { .. }) => {
                            plan.record_distinct_key(child, ErrorCode::ReferencedRowDelete, key);
                        }
                        Some(IndexValue::Unique { .. }) => {
                            return Err(ValidationError::Inconsistent(
                                Inconsistency::WrongIndexKind(child),
                            ));
                        }
                    }
                }
                Ok(())
            })?;
        }
        if !self.plan.violations.is_empty() {
            return Ok((
                StreamedDecision::Rejected(self.plan.violations),
                self.plan.details,
            ));
        }

        // What `ValidatedDeltas::stage` checks first: every index the decision read or writes is
        // still where it was (the staged writes were resolved at those epochs).
        for (&id, &epoch) in &observed {
            if index(indexes, id)?
                .epoch()
                .map_err(ValidationError::Index)?
                != epoch
            {
                return Err(ValidationError::StaleValidation);
            }
        }
        let staged = stagers
            .into_iter()
            .map(|(id, s)| s.finish(id))
            .collect::<Result<Vec<_>, _>>()?;
        let key_delta_digest = match digest {
            Digesting::Here(d) => d.finish(),
            Digesting::Failed(e) => Err(e),
        };
        Ok((
            StreamedDecision::Accepted(StreamedDeltas {
                key_delta_digest,
                staged,
            }),
            BTreeMap::new(),
        ))
    }
}
