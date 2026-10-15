//! Deciding a commit with bounded memory after the merge (ADR 0025).
//!
//! [`CommitScan::decide`] collects every changed key. [`CommitScan::decide_streamed`] takes the
//! merged keys of each keyed constraint as a [`FinishedRun`] of `(key, added ‖ removed)` records
//! and reads it back as often as needed, a batch of at most [`StreamOptions::chunk`] keys at a
//! time: it records duplicates, probes the indexes, stages the accepted changes into runs and
//! computes the key delta digest, deciding exactly what [`CommitScan::decide`] decides for the same
//! counts, and staging exactly what [`ValidatedDeltas::stage`](crate::ValidatedDeltas::stage)
//! stages.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use integrity_core::{
    ConstraintKind, Digest, EncodedKey, InvalidCertificate, KeyDeltaDigest, NetDelta, Violation,
};
use integrity_index::{
    FinishedRun, IndexDelta, IndexEpoch, IndexError, IndexValue, Run, StagedDigest, StagedHeader,
    Writes, decode_staged_value, encode_value,
};
use integrity_types::{ConstraintId, ErrorCode};

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

/// One merged key: the key, its counts on each side, and its net change.
struct Counted {
    key: EncodedKey,
    added: u64,
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
            change: i128::from(added) - i128::from(removed),
        })
    }))
}

/// Calls `f` with consecutive batches of at most `size` changed keys (non-zero net change).
fn for_each_batch(
    run: Option<&FinishedRun>,
    size: usize,
    mut keep: impl FnMut(&Counted) -> bool,
    mut f: impl FnMut(&[Counted]) -> Result<(), ValidationError>,
) -> Result<(), ValidationError> {
    let size = size.max(1);
    let mut batch = Vec::with_capacity(size.min(4096));
    for c in counted_records(run)? {
        let c = c?;
        if c.change != 0 && keep(&c) {
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

/// What [`CommitScan::decide_streamed`] learns while counting a constraint's keys.
#[derive(Debug, Default, Clone, Copy)]
struct Tally {
    changes: u64,
    /// A PK/UNIQUE key whose count falls by more than one (never indexed that often).
    falls_twice: bool,
}

impl CommitScan<'_> {
    /// [`CommitScan::counts`] for every key of `counted`, then [`CommitScan::decide`], then
    /// staging, with bounded memory (ADR 0025). `counted` holds, per keyed constraint, its merged
    /// keys in key order with [`counted_value`]s (at least the keys
    /// [`CommitScan::counts_matter`]; a constraint without a run has no such key).
    pub fn decide_streamed(
        mut self,
        indexes: &impl IndexSet,
        counted: &BTreeMap<ConstraintId, FinishedRun>,
        options: StreamOptions<'_>,
    ) -> Result<(StreamedDecision, BTreeMap<Violation, ViolationDetail>), ValidationError> {
        let keyed = self.keyed();
        let duplicate_code = |id: ConstraintId| {
            self.ctx
                .own
                .iter()
                .find(|rc| rc.constraint.id == id)
                .map(|rc| match rc.constraint.kind {
                    ConstraintKind::PrimaryKey(_) => Some(ErrorCode::DuplicatePrimaryKey),
                    ConstraintKind::Unique(_) => Some(ErrorCode::DuplicateUniqueKey),
                    _ => None,
                })
                .ok_or(ValidationError::MissingIndex(id))
        };

        // What `counts` does for each key: duplicates among the added rows (spec §8, before any
        // probe).
        let mut tallies: BTreeMap<ConstraintId, Tally> = BTreeMap::new();
        for &id in &keyed {
            let duplicate = duplicate_code(id)?;
            let tally = tallies.entry(id).or_default();
            for c in counted_records(counted.get(&id))? {
                let c = c?;
                if let Some(code) = duplicate
                    && c.added > 1
                {
                    // Merged keys are distinct: each is reported once.
                    self.plan.record_distinct_key(id, code, &c.key);
                }
                if c.change != 0 {
                    tally.changes += 1;
                    tally.falls_twice |= duplicate.is_some() && c.change < -1;
                }
            }
        }

        // What `decide` does, in the same order.
        let observed = self.validator.observe(&self.ctx, indexes)?;
        if let Some((_, e)) = std::mem::take(&mut self.errors).into_iter().next() {
            return Err(e);
        }
        let chunk = options.chunk;
        for rc in &self.ctx.own {
            let id = rc.constraint.id;
            let Some(tally) = tallies.get(&id).copied() else {
                continue;
            };
            let run = counted.get(&id);
            match &rc.constraint.kind {
                ConstraintKind::PrimaryKey(_) | ConstraintKind::Unique(_) => {
                    let duplicate = match rc.constraint.kind {
                        ConstraintKind::PrimaryKey(_) => ErrorCode::DuplicatePrimaryKey,
                        _ => ErrorCode::DuplicateUniqueKey,
                    };
                    if tally.falls_twice {
                        return Err(ValidationError::Inconsistent(
                            Inconsistency::RemovedKeyNotIndexed(id),
                        ));
                    }
                    let plan = &mut self.plan;
                    for_each_batch(
                        run,
                        chunk,
                        |_| true,
                        |batch| {
                            let keys: Vec<EncodedKey> =
                                batch.iter().map(|c| c.key.clone()).collect();
                            for (c, value) in batch.iter().zip(probe(indexes, id, &keys)?) {
                                match (c.change > 0, value) {
                                    // Count rises and the key is already present: duplicate (already
                                    // recorded if the added rows repeat it).
                                    (true, Some(_)) => {
                                        if c.added <= 1 {
                                            plan.record_distinct_key(id, duplicate, &c.key);
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
                            Ok(())
                        },
                    )?;
                }
                ConstraintKind::ForeignKey(fk) => {
                    let plan = &mut self.plan;
                    for_each_batch(
                        run,
                        chunk,
                        |c| c.change > 0,
                        |batch| {
                            let keys: Vec<EncodedKey> =
                                batch.iter().map(|c| c.key.clone()).collect();
                            let found = probe(indexes, fk.parent_constraint, &keys)?;
                            for (key, value) in keys.iter().zip(found) {
                                if value.is_none() {
                                    plan.record_distinct_key(
                                        id,
                                        ErrorCode::ForeignKeyViolation,
                                        key,
                                    );
                                }
                            }
                            Ok(())
                        },
                    )?;
                }
                ConstraintKind::NotNull(_) => {}
            }
        }
        for (rc, parent) in &self.ctx.referencing {
            if !tallies.contains_key(parent) {
                return Err(ValidationError::Unprovable(rc.constraint.id));
            }
            let child = rc.constraint.id;
            let plan = &mut self.plan;
            // Parent keys that disappear (removed and not re-added).
            for_each_batch(
                counted.get(parent),
                chunk,
                |c| c.change < 0,
                |batch| {
                    let keys: Vec<EncodedKey> = batch.iter().map(|c| c.key.clone()).collect();
                    for (key, value) in keys.iter().zip(probe(indexes, child, &keys)?) {
                        match value {
                            None => {}
                            Some(IndexValue::Reference { .. }) => {
                                plan.record_distinct_key(
                                    child,
                                    ErrorCode::ReferencedRowDelete,
                                    key,
                                );
                            }
                            Some(IndexValue::Unique { .. }) => {
                                return Err(ValidationError::Inconsistent(
                                    Inconsistency::WrongIndexKind(child),
                                ));
                            }
                        }
                    }
                    Ok(())
                },
            )?;
        }
        if !self.plan.violations.is_empty() {
            return Ok((
                StreamedDecision::Rejected(self.plan.violations),
                self.plan.details,
            ));
        }

        // What `ValidatedDeltas::stage` does: every index the decision read or writes is still
        // where it was; each changed index stages its changes.
        for (&id, &epoch) in &observed {
            if index(indexes, id)?
                .epoch()
                .map_err(ValidationError::Index)?
                != epoch
            {
                return Err(ValidationError::StaleValidation);
            }
        }
        let changed: Vec<(ConstraintId, Tally)> = tallies
            .iter()
            .filter(|(_, t)| t.changes > 0)
            .map(|(id, t)| (*id, *t))
            .collect();
        let budget = options.memory / changed.len().max(1);
        let mut staged = Vec::new();
        for (id, tally) in changed {
            let target = index(indexes, id)?;
            let base_epoch: IndexEpoch = target.epoch().map_err(ValidationError::Index)?;
            let mut digest = StagedDigest::new(base_epoch, tally.changes);
            let mut out = Run::new(options.scratch, budget);
            let snapshot = self.snapshot;
            for_each_batch(
                counted.get(&id),
                chunk,
                |_| true,
                |batch| {
                    let changes = batch.iter().map(|c| (c.key.clone(), c.change)).collect();
                    let delta = IndexDelta {
                        snapshot,
                        changes: NetDelta::from_sorted(changes).map_err(ValidationError::Delta)?,
                    };
                    let part = target.stage(&delta).map_err(ValidationError::Index)?;
                    if part.base_epoch() != base_epoch {
                        return Err(ValidationError::StaleValidation);
                    }
                    for (key, value) in part.writes() {
                        digest.update(key, value).map_err(ValidationError::Index)?;
                        out.push(key.as_bytes(), encode_value(value).as_slice())
                            .map_err(ValidationError::Index)?;
                    }
                    Ok(())
                },
            )?;
            let header = StagedHeader {
                base_epoch,
                writes: tally.changes,
                digest: digest.finish().map_err(ValidationError::Index)?,
            };
            staged.push(StagedRun {
                constraint: id,
                header,
                run: out.finish().map_err(ValidationError::Index)?,
            });
        }

        let key_delta_digest = key_delta_digest(&tallies, counted)?;
        Ok((
            StreamedDecision::Accepted(StreamedDeltas {
                key_delta_digest,
                staged,
            }),
            BTreeMap::new(),
        ))
    }
}

/// The key delta digest (RFC 0002), every keyed constraint in id order. Reading the runs can
/// fail; a delta the digest cannot represent is returned as such, as `key_delta_digest` does.
fn key_delta_digest(
    tallies: &BTreeMap<ConstraintId, Tally>,
    counted: &BTreeMap<ConstraintId, FinishedRun>,
) -> Result<Result<Digest, InvalidCertificate>, ValidationError> {
    let mut d = match KeyDeltaDigest::new(tallies.len()) {
        Ok(d) => d,
        Err(e) => return Ok(Err(e)),
    };
    for (&id, tally) in tallies {
        if let Err(e) = d.constraint(id, tally.changes) {
            return Ok(Err(e));
        }
        for c in counted_records(counted.get(&id))? {
            let c = c?;
            if c.change != 0
                && let Err(e) = d.change(c.key.as_bytes(), c.change)
            {
                return Ok(Err(e));
            }
        }
    }
    Ok(d.finish())
}
