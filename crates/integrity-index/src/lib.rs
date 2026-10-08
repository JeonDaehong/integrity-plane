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

use std::collections::BTreeMap;
use std::fmt;

use integrity_core::{EncodedKey, NetDelta};
use integrity_types::SnapshotId;

pub use memory::MemoryIndex;

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
}
