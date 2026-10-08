//! In-memory `BTreeMap` backend (spec §13.4, Phase 2). Not durable.

use std::collections::BTreeMap;
use std::sync::RwLock;

use integrity_core::EncodedKey;

use crate::{
    ApplyAction, IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, Result,
    StagedDelta, decide_apply, resolve,
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
    last_applied: Option<(IndexEpoch, [u8; 32])>,
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
        resolve(self.kind, delta, state.epoch, |k| {
            Ok(state.entries.get(k).copied())
        })
    }

    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> Result<()> {
        let mut state = self.state.write().map_err(|_| IndexError::Unavailable)?;
        match decide_apply(&staged, epoch, state.epoch, state.last_applied)? {
            ApplyAction::AlreadyApplied => return Ok(()),
            ApplyAction::Write => {}
        }
        let digest = staged.digest();
        // Writes are infallible from here on, so the update is all-or-nothing.
        for (key, value) in staged.writes {
            match value {
                Some(v) => {
                    state.entries.insert(key, v);
                }
                None => {
                    state.entries.remove(&key);
                }
            }
        }
        state.epoch = epoch;
        state.last_applied = Some((epoch, digest));
        Ok(())
    }

    fn epoch(&self) -> Result<IndexEpoch> {
        let state = self.state.read().map_err(|_| IndexError::Unavailable)?;
        Ok(state.epoch)
    }

    fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>> {
        let state = self.state.read().map_err(|_| IndexError::Unavailable)?;
        Ok(state.entries.iter().map(|(k, v)| (k.clone(), *v)).collect())
    }
}
