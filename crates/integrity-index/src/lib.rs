//! `KeyIndex` trait (spec §13.2) and its in-memory backend.
//!
//! Indexes store keys and counts, never row locations (spec §13.1):
//!
//! | Index | Key | Value |
//! |---|---|---|
//! | Unique (per PK/UNIQUE) | `EncodedKey` | presence + `last_snapshot`; multiplicity ≤ 1 |
//! | Reference (per FK) | parent `EncodedKey` | `child_count` |
//!
//! Changes go through two steps: [`KeyIndex::stage`] checks a net delta against the current
//! contents and resolves the new values without any visible effect; [`KeyIndex::apply`] makes a
//! staged delta visible atomically. `apply` is idempotent for the same `(staged, epoch)`, so
//! recovery can replay it.

mod memory;
mod overlay;
mod persistent;
mod run;
mod sort;

use std::fmt;

use integrity_core::{EncodedKey, NetDelta};
use integrity_types::SnapshotId;

pub use memory::MemoryIndex;
pub use overlay::Overlay;
pub use persistent::{IndexBuild, PersistentIndex, PersistentStore, Writes};
pub use run::{FinishedRun, Records, Run};
pub use sort::{KeySorter, SortedBytes, SortedKeys};

/// Monotonically increasing version of an index's contents. A new index starts at epoch 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexEpoch(pub u64);

/// Which kind of index this is (spec §13.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// PK/UNIQUE index: each key present at most once.
    Unique,
    /// FK reference index: per parent key, how many child rows reference it.
    Reference,
}

/// A stored index value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexValue {
    /// The key is present in a unique index.
    Unique {
        /// The snapshot that added the key.
        last_snapshot: SnapshotId,
    },
    /// The parent key is referenced by `child_count` child rows (never 0: absent instead).
    Reference {
        /// Number of referencing child rows.
        child_count: u64,
    },
}

impl IndexValue {
    /// The kind of index this value belongs to.
    pub fn kind(&self) -> Option<IndexKind> {
        Some(match self {
            IndexValue::Unique { .. } => IndexKind::Unique,
            IndexValue::Reference { .. } => IndexKind::Reference,
        })
    }
}

/// A commit's net key change for one index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDelta {
    /// The snapshot the change belongs to.
    pub snapshot: SnapshotId,
    /// Per-key signed count changes.
    pub changes: NetDelta,
}

/// A delta resolved against a specific epoch: the exact values to write. Has no effect until
/// [`KeyIndex::apply`]d, and can only be applied while the index is still at `base_epoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedDelta {
    base_epoch: IndexEpoch,
    /// In strictly increasing key order.
    writes: Vec<(EncodedKey, Option<IndexValue>)>,
}

/// What identifies a staged delta without its writes (ADR 0003, ADR 0025): enough to apply it
/// from a stream of its writes and to recognize a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedHeader {
    /// The epoch the delta was resolved against.
    pub base_epoch: IndexEpoch,
    /// Number of writes.
    pub writes: u64,
    /// [`StagedDelta::digest`].
    pub digest: [u8; 32],
}

/// [`StagedDelta::digest`], computed one write at a time.
#[derive(Debug)]
pub struct StagedDigest {
    hasher: blake3::Hasher,
    left: u64,
}

impl StagedDigest {
    /// Starts the digest of a delta resolved at `base_epoch` with `writes` writes.
    pub fn new(base_epoch: IndexEpoch, writes: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"oip-staged-delta-v1");
        hasher.update(&base_epoch.0.to_be_bytes());
        hasher.update(&writes.to_be_bytes());
        Self {
            hasher,
            left: writes,
        }
    }

    /// Adds the next write; more writes than announced is [`IndexError::Corrupt`].
    pub fn update(&mut self, key: &EncodedKey, value: Option<&IndexValue>) -> Result<()> {
        self.left = self.left.checked_sub(1).ok_or(IndexError::Corrupt)?;
        self.hasher
            .update(&(key.as_bytes().len() as u64).to_be_bytes());
        self.hasher.update(key.as_bytes());
        self.hasher.update(encode_value(value).as_slice());
        Ok(())
    }

    /// The digest; fewer writes than announced is [`IndexError::Corrupt`].
    pub fn finish(self) -> Result<[u8; 32]> {
        if self.left != 0 {
            return Err(IndexError::Corrupt);
        }
        Ok(*self.hasher.finalize().as_bytes())
    }
}

impl StagedDelta {
    /// The epoch the delta was resolved against.
    pub fn base_epoch(&self) -> IndexEpoch {
        self.base_epoch
    }

    /// Its [`StagedHeader`].
    pub fn header(&self) -> StagedHeader {
        StagedHeader {
            base_epoch: self.base_epoch,
            writes: self.writes.len() as u64,
            digest: self.digest(),
        }
    }

    /// New value per key; `None` deletes the key.
    pub fn writes(&self) -> impl Iterator<Item = (&EncodedKey, Option<&IndexValue>)> {
        self.writes.iter().map(|(k, v)| (k, v.as_ref()))
    }

    /// Serializes the delta for the transaction log (RFC 0004), so that recovery can replay the
    /// identical apply after a crash.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![STAGED_VERSION];
        out.extend_from_slice(&self.base_epoch.0.to_be_bytes());
        out.extend_from_slice(&(self.writes.len() as u64).to_be_bytes());
        for (key, value) in &self.writes {
            out.extend_from_slice(&(key.as_bytes().len() as u32).to_be_bytes());
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(encode_value(value.as_ref()).as_slice());
        }
        out
    }

    /// Parses [`StagedDelta::encode`] output. Anything else, including keys out of order or
    /// trailing bytes, is [`IndexError::Corrupt`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            if b.len() < n {
                return Err(IndexError::Corrupt);
            }
            let (head, rest) = b.split_at(n);
            *b = rest;
            Ok(head)
        }
        fn u64_at(b: &mut &[u8]) -> Result<u64> {
            let mut a = [0u8; 8];
            a.copy_from_slice(take(b, 8)?);
            Ok(u64::from_be_bytes(a))
        }
        let mut b = bytes;
        if take(&mut b, 1)? != [STAGED_VERSION] {
            return Err(IndexError::Corrupt);
        }
        let base_epoch = IndexEpoch(u64_at(&mut b)?);
        let n = u64_at(&mut b)?;
        let mut writes: Vec<(EncodedKey, Option<IndexValue>)> = Vec::new();
        for _ in 0..n {
            let mut len = [0u8; 4];
            len.copy_from_slice(take(&mut b, 4)?);
            let key = EncodedKey::from_bytes(take(&mut b, u32::from_be_bytes(len) as usize)?)
                .map_err(|_| IndexError::Corrupt)?;
            if writes.last().is_some_and(|(p, _)| *p >= key) {
                return Err(IndexError::Corrupt);
            }
            let value = match take(&mut b, 1)?[0] {
                VALUE_ABSENT => None,
                tag => {
                    let mut v = vec![tag];
                    v.extend_from_slice(take(&mut b, 8)?);
                    Some(decode_value(&v)?)
                }
            };
            writes.push((key, value));
        }
        if !b.is_empty() {
            return Err(IndexError::Corrupt);
        }
        Ok(Self { base_epoch, writes })
    }

    /// BLAKE3 digest of the canonical encoding: the identity used to recognize a replayed
    /// `apply` (ADR 0003).
    pub fn digest(&self) -> [u8; 32] {
        let mut h = StagedDigest::new(self.base_epoch, self.writes.len() as u64);
        for (key, value) in &self.writes {
            // Exactly as many writes as announced: neither call can fail.
            let _ = h.update(key, value.as_ref());
        }
        h.finish().unwrap_or_default()
    }
}

/// Index failures. None of them carry key values, which may be PII.
///
/// Stage-time errors mean the delta is inconsistent with the index: the validator must have
/// rejected such a commit already, so reaching them indicates corruption or a bug, and callers
/// must fail closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    /// A unique index delta adds a key that is already present.
    KeyAlreadyPresent,
    /// A delta removes a key (or more references) than the index holds.
    KeyAbsent,
    /// A unique index delta changes a key's count by something other than ±1.
    InvalidMultiplicity(i128),
    /// A reference count would exceed `u64::MAX`.
    CountOverflow,
    /// The index changed since the delta was staged.
    StaleStage {
        /// Epoch the delta was staged against.
        staged_at: IndexEpoch,
        /// Current epoch.
        current: IndexEpoch,
    },
    /// The epoch is not newer than the current one and is not a replay of the last apply.
    EpochConflict {
        /// Epoch passed to `apply`.
        requested: IndexEpoch,
        /// Current epoch.
        current: IndexEpoch,
    },
    /// Stored data does not match the index kind or format.
    Corrupt,
    /// The backend is unusable (e.g. a writer panicked mid-update).
    Unavailable,
    /// The storage engine failed (I/O, lock, transaction). The message never contains keys.
    Storage(String),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::KeyAlreadyPresent => f.write_str("unique index already holds the key"),
            IndexError::KeyAbsent => f.write_str("delta removes a key the index does not hold"),
            IndexError::InvalidMultiplicity(n) => {
                write!(f, "unique index key count would change by {n}")
            }
            IndexError::CountOverflow => f.write_str("reference count overflow"),
            IndexError::StaleStage { staged_at, current } => write!(
                f,
                "delta staged at epoch {} but index is at epoch {}",
                staged_at.0, current.0
            ),
            IndexError::EpochConflict { requested, current } => write!(
                f,
                "cannot apply epoch {} to index at epoch {}",
                requested.0, current.0
            ),
            IndexError::Corrupt => f.write_str("index data is corrupt"),
            IndexError::Unavailable => f.write_str("index backend unavailable"),
            IndexError::Storage(m) => write!(f, "index storage error: {m}"),
        }
    }
}

impl std::error::Error for IndexError {}

/// Result alias for index operations.
pub type Result<T> = std::result::Result<T, IndexError>;

/// A persistent or in-memory key index (spec §13.2). One instance per constraint.
pub trait KeyIndex: Send + Sync {
    /// The kind of index.
    fn kind(&self) -> IndexKind;

    /// Looks up keys; the result has one entry per input key, in order.
    fn get_many(&self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>>;

    /// Resolves a delta against the current contents. No visible effect.
    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta>;

    /// Atomically makes a staged delta visible and moves the index to `epoch`.
    ///
    /// Requires `epoch` newer than the current epoch and the index still at the staged base
    /// epoch. Re-applying the most recently applied `(staged, epoch)` is a no-op success.
    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> Result<()>;

    /// The current epoch.
    fn epoch(&self) -> Result<IndexEpoch>;

    /// Every entry in key order (used by rebuild comparison and verification).
    fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>>;
}

/// [`KeyIndex::stage`] of an index of `kind` at `base_epoch` whose current values of the delta's
/// keys were already read: `current` holds them in the delta's key order (ADR 0025). The same
/// resolution every backend uses.
pub fn stage_from(
    kind: IndexKind,
    delta: &IndexDelta,
    base_epoch: IndexEpoch,
    current: &[Option<IndexValue>],
) -> Result<StagedDelta> {
    if current.len() != delta.changes.len() {
        return Err(IndexError::Corrupt);
    }
    let mut values = current.iter();
    resolve(kind, delta, base_epoch, |_| {
        values.next().copied().ok_or(IndexError::Corrupt)
    })
}

/// Resolves `delta` against the current values returned by `current` (spec §13.2 `stage`).
/// Shared by every backend so that all of them stage identically.
pub(crate) fn resolve(
    kind: IndexKind,
    delta: &IndexDelta,
    base_epoch: IndexEpoch,
    mut current: impl FnMut(&EncodedKey) -> Result<Option<IndexValue>>,
) -> Result<StagedDelta> {
    // The delta is in key order, so the writes are too.
    let mut writes = Vec::with_capacity(delta.changes.len());
    for (key, change) in delta.changes.iter() {
        let value = current(key)?;
        let new = match kind {
            IndexKind::Unique => match (change, value) {
                (1, None) => Some(IndexValue::Unique {
                    last_snapshot: delta.snapshot,
                }),
                (1, Some(_)) => return Err(IndexError::KeyAlreadyPresent),
                (-1, Some(IndexValue::Unique { .. })) => None,
                (-1, Some(IndexValue::Reference { .. })) => return Err(IndexError::Corrupt),
                (-1, None) => return Err(IndexError::KeyAbsent),
                (n, _) => return Err(IndexError::InvalidMultiplicity(n)),
            },
            IndexKind::Reference => {
                let count = match value {
                    Some(IndexValue::Reference { child_count }) => child_count,
                    None => 0,
                    Some(IndexValue::Unique { .. }) => return Err(IndexError::Corrupt),
                };
                let new = i128::from(count) + change;
                if new < 0 {
                    return Err(IndexError::KeyAbsent);
                }
                let new = u64::try_from(new).map_err(|_| IndexError::CountOverflow)?;
                (new > 0).then_some(IndexValue::Reference { child_count: new })
            }
        };
        writes.push((key.clone(), new));
    }
    Ok(StagedDelta { base_epoch, writes })
}

/// Decides an `apply` from the index's state; shared by every backend (ADR 0003).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyAction {
    /// Write the staged values and move to the new epoch.
    Write,
    /// Replay of the last apply: succeed without writing.
    AlreadyApplied,
}

pub(crate) fn decide_apply(
    staged: &StagedHeader,
    epoch: IndexEpoch,
    current: IndexEpoch,
    last_applied: Option<(IndexEpoch, [u8; 32])>,
) -> Result<ApplyAction> {
    if epoch <= current {
        return match last_applied {
            Some((last_epoch, digest)) if last_epoch == epoch && digest == staged.digest => {
                Ok(ApplyAction::AlreadyApplied)
            }
            _ => Err(IndexError::EpochConflict {
                requested: epoch,
                current,
            }),
        };
    }
    if staged.base_epoch != current {
        return Err(IndexError::StaleStage {
            staged_at: staged.base_epoch,
            current,
        });
    }
    Ok(ApplyAction::Write)
}

const STAGED_VERSION: u8 = 1;
const VALUE_ABSENT: u8 = 0x00;
const VALUE_UNIQUE: u8 = 0x01;
const VALUE_REFERENCE: u8 = 0x02;

/// An encoded index value, without a heap allocation (one is written per changed key).
#[derive(Debug)]
pub struct EncodedValue {
    bytes: [u8; 9],
    len: usize,
}

impl EncodedValue {
    /// The encoded bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// Canonical 1- or 9-byte encoding of an index value (`None` = absent), as in a staged delta
/// (RFC 0004, RFC 0006).
pub fn encode_value(value: Option<&IndexValue>) -> EncodedValue {
    let tagged = |tag: u8, payload: [u8; 8]| {
        let mut bytes = [tag, 0, 0, 0, 0, 0, 0, 0, 0];
        bytes[1..].copy_from_slice(&payload);
        EncodedValue { bytes, len: 9 }
    };
    match value {
        None => EncodedValue {
            bytes: [VALUE_ABSENT, 0, 0, 0, 0, 0, 0, 0, 0],
            len: 1,
        },
        Some(IndexValue::Unique { last_snapshot }) => {
            tagged(VALUE_UNIQUE, last_snapshot.0.to_be_bytes())
        }
        Some(IndexValue::Reference { child_count }) => {
            tagged(VALUE_REFERENCE, child_count.to_be_bytes())
        }
    }
}

/// Decodes [`encode_value`] output, absent included; anything else is corruption.
pub fn decode_staged_value(bytes: &[u8]) -> Result<Option<IndexValue>> {
    if bytes == [VALUE_ABSENT] {
        return Ok(None);
    }
    decode_value(bytes).map(Some)
}

/// Decodes a stored (present) value; anything else is corruption.
pub(crate) fn decode_value(bytes: &[u8]) -> Result<IndexValue> {
    let (&tag, rest) = bytes.split_first().ok_or(IndexError::Corrupt)?;
    let payload: [u8; 8] = rest.try_into().map_err(|_| IndexError::Corrupt)?;
    match tag {
        VALUE_UNIQUE => Ok(IndexValue::Unique {
            last_snapshot: SnapshotId(i64::from_be_bytes(payload)),
        }),
        VALUE_REFERENCE => Ok(IndexValue::Reference {
            child_count: u64::from_be_bytes(payload),
        }),
        _ => Err(IndexError::Corrupt),
    }
}

impl<T: KeyIndex + ?Sized> KeyIndex for Box<T> {
    fn kind(&self) -> IndexKind {
        (**self).kind()
    }
    fn get_many(&self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>> {
        (**self).get_many(keys)
    }
    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta> {
        (**self).stage(delta)
    }
    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> Result<()> {
        (**self).apply(staged, epoch)
    }
    fn epoch(&self) -> Result<IndexEpoch> {
        (**self).epoch()
    }
    fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>> {
        (**self).entries()
    }
}
