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

/// Values by key, in strictly increasing key order: looked up by binary search and extended in
/// bulk, since keys arrive in key order (a commit's delta, its probes).
#[derive(Debug, Default)]
struct Sorted(Vec<(EncodedKey, Option<IndexValue>)>);

impl Sorted {
    /// The value of `key`. `hint` is where to look first and is left after the key: lookups in
    /// key order (the usual case) cost one comparison each.
    fn find(&self, key: &EncodedKey, hint: &mut usize) -> Option<Option<IndexValue>> {
        if let Some((k, v)) = self.0.get(*hint)
            && k == key
        {
            *hint += 1;
            return Some(*v);
        }
        match self.0.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(i) => {
                *hint = i + 1;
                Some(self.0[i].1)
            }
            Err(i) => {
                *hint = i;
                None
            }
        }
    }

    /// Adds `entries` (strictly increasing), replacing the values of keys already present.
    fn merge(&mut self, entries: Vec<(EncodedKey, Option<IndexValue>)>) {
        let Some((first, _)) = entries.first() else {
            return;
        };
        if self.0.last().is_none_or(|(last, _)| last < first) {
            self.0.extend(entries);
            return;
        }
        let old = std::mem::take(&mut self.0);
        let mut out = Vec::with_capacity(old.len() + entries.len());
        let (mut old, mut new) = (old.into_iter().peekable(), entries.into_iter().peekable());
        loop {
            let order = match (old.peek(), new.peek()) {
                (None, None) => break,
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some((a, _)), Some((b, _))) => a.cmp(b),
            };
            match order {
                std::cmp::Ordering::Less => out.extend(old.next()),
                std::cmp::Ordering::Greater => out.extend(new.next()),
                // The new value replaces the old one.
                std::cmp::Ordering::Equal => {
                    old.next();
                    out.extend(new.next());
                }
            }
        }
        self.0 = out;
    }
}

/// An index seen through pending writes. `apply` records writes in the overlay (ignoring epochs);
/// reads see the overlay first, then the base. `epoch` is the base's.
///
/// Values read from the base are remembered: the base must not change while the overlay is in
/// use (the gateway holds the domain queue for the whole commit), so validating and then staging
/// the same keys reads each of them from the base once.
#[derive(Debug)]
pub struct Overlay<'a> {
    base: &'a dyn KeyIndex,
    /// Locked before `read` wherever both are needed.
    writes: Mutex<Sorted>,
    read: Mutex<Sorted>,
}

impl<'a> Overlay<'a> {
    /// An empty overlay over `base`.
    pub fn new(base: &'a dyn KeyIndex) -> Self {
        Self {
            base,
            writes: Mutex::new(Sorted::default()),
            read: Mutex::new(Sorted::default()),
        }
    }

    /// Reads from the base, in one batch, the values of `keys` neither written nor read before.
    fn read_through<'k>(
        &self,
        writes: &Sorted,
        read: &mut Sorted,
        keys: impl Iterator<Item = &'k EncodedKey>,
    ) -> Result<()> {
        let (mut w, mut r) = (0, 0);
        let mut missing: Vec<EncodedKey> = keys
            .filter(|k| writes.find(k, &mut w).is_none() && read.find(k, &mut r).is_none())
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        if !missing.is_sorted() {
            missing.sort();
        }
        missing.dedup();
        let found = self.base.get_many(&missing)?;
        if found.len() != missing.len() {
            return Err(IndexError::Corrupt);
        }
        read.merge(missing.into_iter().zip(found).collect());
        Ok(())
    }
}

/// Values as the overlay sees them, once [`Overlay::read_through`] has run for the keys asked.
struct Seen<'s> {
    writes: &'s Sorted,
    read: &'s Sorted,
    hints: (usize, usize),
}

impl<'s> Seen<'s> {
    fn new(writes: &'s Sorted, read: &'s Sorted) -> Self {
        Self {
            writes,
            read,
            hints: (0, 0),
        }
    }

    fn get(&mut self, key: &EncodedKey) -> Result<Option<IndexValue>> {
        self.writes
            .find(key, &mut self.hints.0)
            .or_else(|| self.read.find(key, &mut self.hints.1))
            .ok_or(IndexError::Corrupt)
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
            .map_err(|_| IndexError::Unavailable)?
            .0;
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
        let mut read = self.read.lock().map_err(|_| IndexError::Unavailable)?;
        self.read_through(&writes, &mut read, keys.iter())?;
        let mut seen = Seen::new(&writes, &read);
        keys.iter().map(|k| seen.get(k)).collect()
    }

    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta> {
        let writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        let mut read = self.read.lock().map_err(|_| IndexError::Unavailable)?;
        self.read_through(&writes, &mut read, delta.changes.iter().map(|(k, _)| k))?;
        let mut seen = Seen::new(&writes, &read);
        resolve(self.kind(), delta, self.base.epoch()?, |k| seen.get(k))
    }

    fn apply(&self, staged: StagedDelta, _epoch: IndexEpoch) -> Result<()> {
        let mut writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        writes.merge(staged.writes);
        Ok(())
    }

    fn epoch(&self) -> Result<IndexEpoch> {
        self.base.epoch()
    }

    fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>> {
        let writes = self.writes.lock().map_err(|_| IndexError::Unavailable)?;
        let mut merged: BTreeMap<EncodedKey, IndexValue> =
            self.base.entries()?.into_iter().collect();
        for (k, v) in &writes.0 {
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
