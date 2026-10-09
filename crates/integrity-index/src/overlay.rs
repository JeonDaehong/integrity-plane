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
///
/// Values read from the base are remembered: the base must not change while the overlay is in
/// use (the gateway holds the domain queue for the whole commit), so validating and then staging
/// the same keys reads each of them from the base once.
#[derive(Debug)]
pub struct Overlay<'a> {
    base: &'a dyn KeyIndex,
    writes: Mutex<BTreeMap<EncodedKey, Option<IndexValue>>>,
    read: Mutex<BTreeMap<EncodedKey, Option<IndexValue>>>,
}

impl<'a> Overlay<'a> {
    /// An empty overlay over `base`.
    pub fn new(base: &'a dyn KeyIndex) -> Self {
        Self {
            base,
            writes: Mutex::new(BTreeMap::new()),
            read: Mutex::new(BTreeMap::new()),
        }
    }

    /// Base values of `keys`, from memory or one batched base read for the rest.
    fn base_values(&self, keys: &[EncodedKey]) -> Result<BTreeMap<EncodedKey, Option<IndexValue>>> {
        let mut read = self.read.lock().map_err(|_| IndexError::Unavailable)?;
        let mut missing: Vec<EncodedKey> = keys
            .iter()
            .filter(|k| !read.contains_key(*k))
            .cloned()
            .collect();
        missing.sort();
        missing.dedup();
        let found = self.base.get_many(&missing)?;
        if found.len() != missing.len() {
            return Err(IndexError::Corrupt);
        }
        for (k, v) in missing.into_iter().zip(found) {
            read.insert(k, v);
        }
        Ok(keys
            .iter()
            .filter_map(|k| read.get(k).map(|v| (k.clone(), *v)))
            .collect())
    }
}

impl Overlay<'_> {
    /// All pending writes as one delta staged against the base's current epoch: applying it to
    /// the base reproduces every step applied to the overlay.
    pub fn into_staged(self) -> Result<StagedDelta> {
        let base_epoch = self.base.epoch()?;
        let writes = self
            .writes
            .into_inner()
            .map_err(|_| IndexError::Unavailable)?;
        Ok(StagedDelta { base_epoch, writes })
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
        let base = self.base_values(&missing)?;
        keys.iter()
            .map(|k| match writes.get(k) {
                Some(v) => Ok(*v),
                None => base.get(k).copied().ok_or(IndexError::Corrupt),
            })
            .collect()
    }

    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta> {
        let writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        let keys: Vec<EncodedKey> = delta
            .changes
            .iter()
            .map(|(k, _)| k.clone())
            .filter(|k| !writes.contains_key(k))
            .collect();
        let base = self.base_values(&keys)?;
        resolve(self.kind(), delta, self.base.epoch()?, |k| {
            match writes.get(k) {
                Some(v) => Ok(*v),
                None => base.get(k).copied().ok_or(IndexError::Corrupt),
            }
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
