//! A copy-on-write view over an index, for validating several snapshots of one commit in order
//! (ADR 0008): each validated step is "applied" to the overlay only; the base index is untouched
//! until the upstream commit succeeds.

use std::collections::BTreeMap;
use std::sync::Mutex;

use integrity_core::EncodedKey;

use crate::{
    IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, Result, StagedDelta,
    resolve,
};

/// An index seen through pending writes. `apply` records writes in the overlay (ignoring epochs);
/// reads see the overlay first, then the base. `epoch` is the base's.
#[derive(Debug)]
pub struct Overlay<'a> {
    base: &'a dyn KeyIndex,
    writes: Mutex<BTreeMap<EncodedKey, Option<IndexValue>>>,
}

impl<'a> Overlay<'a> {
    /// An empty overlay over `base`.
    pub fn new(base: &'a dyn KeyIndex) -> Self {
        Self {
            base,
            writes: Mutex::new(BTreeMap::new()),
        }
    }

    fn lookup(
        &self,
        writes: &BTreeMap<EncodedKey, Option<IndexValue>>,
        key: &EncodedKey,
    ) -> Result<Option<IndexValue>> {
        match writes.get(key) {
            Some(v) => Ok(*v),
            None => Ok(self
                .base
                .get_many(std::slice::from_ref(key))?
                .pop()
                .flatten()),
        }
    }
}

impl std::fmt::Debug for dyn KeyIndex + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyIndex({:?})", self.kind())
    }
}

impl KeyIndex for Overlay<'_> {
    fn kind(&self) -> IndexKind {
        self.base.kind()
    }

    fn get_many(&self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>> {
        let writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        let missing: Vec<EncodedKey> = keys
            .iter()
            .filter(|k| !writes.contains_key(*k))
            .cloned()
            .collect();
        let mut from_base = self.base.get_many(&missing)?.into_iter();
        keys.iter()
            .map(|k| match writes.get(k) {
                Some(v) => Ok(*v),
                None => from_base.next().ok_or(IndexError::Corrupt),
            })
            .collect()
    }

    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta> {
        let writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        resolve(self.kind(), delta, self.base.epoch()?, |k| {
            self.lookup(&writes, k)
        })
    }

    fn apply(&self, staged: StagedDelta, _epoch: IndexEpoch) -> Result<()> {
        let mut writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        for (key, value) in staged.writes {
            writes.insert(key, value);
        }
        Ok(())
    }

    fn epoch(&self) -> Result<IndexEpoch> {
        self.base.epoch()
    }

    fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>> {
        let writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        let mut merged: BTreeMap<EncodedKey, IndexValue> =
            self.base.entries()?.into_iter().collect();
        for (k, v) in writes.iter() {
            match v {
                Some(v) => {
                    merged.insert(k.clone(), *v);
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        Ok(merged.into_iter().collect())
    }
}
