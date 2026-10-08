//! In-memory `BTreeMap` backend (spec §13.4, Phase 2). Not durable.

use std::collections::BTreeMap;
use std::sync::RwLock;

use integrity_core::EncodedKey;

use crate::{
    IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, Result, StagedDelta,
};

/// An in-memory key index.
#[derive(Debug)]
pub struct MemoryIndex {
    kind: IndexKind,
    state: RwLock<State>,
}

#[derive(Debug)]
struct State {
    entries: BTreeMap<EncodedKey, IndexValue>,
    epoch: IndexEpoch,
    last_applied: Option<(IndexEpoch, StagedDelta)>,
}

impl MemoryIndex {
    /// An empty index at epoch 0.
    pub fn new(kind: IndexKind) -> Self {
        Self {
            kind,
            state: RwLock::new(State {
                entries: BTreeMap::new(),
                epoch: IndexEpoch(0),
                last_applied: None,
            }),
        }
    }

    /// All entries in key order.
    pub fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>> {
        let state = self.state.read().map_err(|_| IndexError::Unavailable)?;
        Ok(state.entries.iter().map(|(k, v)| (k.clone(), *v)).collect())
    }
}

impl KeyIndex for MemoryIndex {
    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn get_many(&self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>> {
        let state = self.state.read().map_err(|_| IndexError::Unavailable)?;
        Ok(keys.iter().map(|k| state.entries.get(k).copied()).collect())
    }

    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta> {
        let state = self.state.read().map_err(|_| IndexError::Unavailable)?;
        let mut writes = BTreeMap::new();
        for (key, change) in delta.changes.iter() {
            let current = state.entries.get(key);
            let new = match self.kind {
                IndexKind::Unique => match (change, current) {
                    (1, None) => Some(IndexValue::Unique {
                        last_snapshot: delta.snapshot,
                    }),
                    (1, Some(_)) => return Err(IndexError::KeyAlreadyPresent),
                    (-1, Some(_)) => None,
                    (-1, None) => return Err(IndexError::KeyAbsent),
                    (n, _) => return Err(IndexError::InvalidMultiplicity(n)),
                },
                IndexKind::Reference => {
                    let count = match current {
                        Some(IndexValue::Reference { child_count }) => *child_count,
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
        Ok(StagedDelta {
            base_epoch: state.epoch,
            writes,
        })
    }

    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> Result<()> {
        let mut state = self.state.write().map_err(|_| IndexError::Unavailable)?;
        if epoch <= state.epoch {
            return match &state.last_applied {
                Some((last_epoch, last)) if *last_epoch == epoch && *last == staged => Ok(()),
                _ => Err(IndexError::EpochConflict {
                    requested: epoch,
                    current: state.epoch,
                }),
            };
        }
        if staged.base_epoch != state.epoch {
            return Err(IndexError::StaleStage {
                staged_at: staged.base_epoch,
                current: state.epoch,
            });
        }
        // Writes are infallible from here on, so the update is all-or-nothing.
        for (key, value) in &staged.writes {
            match value {
                Some(v) => {
                    state.entries.insert(key.clone(), *v);
                }
                None => {
                    state.entries.remove(key);
                }
            }
        }
        state.epoch = epoch;
        state.last_applied = Some((epoch, staged));
        Ok(())
    }

    fn epoch(&self) -> Result<IndexEpoch> {
        let state = self.state.read().map_err(|_| IndexError::Unavailable)?;
        Ok(state.epoch)
    }
}
