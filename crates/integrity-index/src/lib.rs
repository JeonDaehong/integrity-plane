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
mod persistent;

use std::collections::BTreeMap;
use std::fmt;

use integrity_core::{EncodedKey, NetDelta};
use integrity_types::SnapshotId;

pub use memory::MemoryIndex;
pub use persistent::{PersistentIndex, PersistentStore};

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
    writes: BTreeMap<EncodedKey, Option<IndexValue>>,
}

impl StagedDelta {
    /// The epoch the delta was resolved against.
    pub fn base_epoch(&self) -> IndexEpoch {
        self.base_epoch
    }

    /// New value per key; `None` deletes the key.
    pub fn writes(&self) -> impl Iterator<Item = (&EncodedKey, Option<&IndexValue>)> {
        self.writes.iter().map(|(k, v)| (k, v.as_ref()))
    }

    /// BLAKE3 digest of the canonical encoding: the identity used to recognize a replayed
    /// `apply` (ADR 0003).
    pub fn digest(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(b"oip-staged-delta-v1");
        h.update(&self.base_epoch.0.to_be_bytes());
        h.update(&(self.writes.len() as u64).to_be_bytes());
        for (key, value) in &self.writes {
            h.update(&(key.as_bytes().len() as u64).to_be_bytes());
            h.update(key.as_bytes());
            h.update(&encode_value(value.as_ref()));
        }
        *h.finalize().as_bytes()
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

/// Resolves `delta` against the current values returned by `current` (spec §13.2 `stage`).
/// Shared by every backend so that all of them stage identically.
pub(crate) fn resolve(
    kind: IndexKind,
    delta: &IndexDelta,
    base_epoch: IndexEpoch,
    mut current: impl FnMut(&EncodedKey) -> Result<Option<IndexValue>>,
) -> Result<StagedDelta> {
    let mut writes = BTreeMap::new();
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
        writes.insert(key.clone(), new);
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
    staged: &StagedDelta,
    epoch: IndexEpoch,
    current: IndexEpoch,
    last_applied: Option<(IndexEpoch, [u8; 32])>,
) -> Result<ApplyAction> {
    if epoch <= current {
        return match last_applied {
            Some((last_epoch, digest)) if last_epoch == epoch && digest == staged.digest() => {
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

const VALUE_ABSENT: u8 = 0x00;
const VALUE_UNIQUE: u8 = 0x01;
const VALUE_REFERENCE: u8 = 0x02;

/// Canonical 1- or 9-byte encoding of an index value (`None` = absent).
pub(crate) fn encode_value(value: Option<&IndexValue>) -> Vec<u8> {
    match value {
        None => vec![VALUE_ABSENT],
        Some(IndexValue::Unique { last_snapshot }) => {
            let mut v = vec![VALUE_UNIQUE];
            v.extend_from_slice(&last_snapshot.0.to_be_bytes());
            v
        }
        Some(IndexValue::Reference { child_count }) => {
            let mut v = vec![VALUE_REFERENCE];
            v.extend_from_slice(&child_count.to_be_bytes());
            v
        }
    }
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
