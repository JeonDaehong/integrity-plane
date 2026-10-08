//! Key multisets and commit key deltas (spec §8).
//!
//! A commit's effect on one constraint is a pair of multisets, `added` and `removed`. Two facts
//! drive validation and are deliberately kept apart here:
//!
//! - **multiplicities of `added`** decide intra-commit PK/UNIQUE duplicates, and MUST be checked
//!   before any index probe ([`KeyMultiset::duplicates`]);
//! - the **net delta** `added − removed` ([`KeyDelta::net`]) is what changes the index; a key
//!   removed and re-added (copy-on-write rewrite) nets to zero.
//!
//! Netting first would mask duplicates: `added = {k, k}`, `removed = {k}` nets to `+1`, yet the
//! post-commit state holds `k` twice.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;

use crate::key::EncodedKey;

/// A multiset of encoded keys. Never stores zero counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyMultiset {
    counts: BTreeMap<EncodedKey, u64>,
}

impl KeyMultiset {
    /// An empty multiset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one occurrence of `key`.
    pub fn insert(&mut self, key: EncodedKey) -> Result<(), DeltaError> {
        self.insert_n(key, 1)
    }

    /// Adds `n` occurrences of `key`.
    pub fn insert_n(&mut self, key: EncodedKey, n: u64) -> Result<(), DeltaError> {
        if n == 0 {
            return Ok(());
        }
        let count = self.counts.entry(key).or_insert(0);
        *count = count.checked_add(n).ok_or(DeltaError::Overflow)?;
        Ok(())
    }

    /// Occurrences of `key` (0 if absent).
    pub fn count(&self, key: &EncodedKey) -> u64 {
        self.counts.get(key).copied().unwrap_or(0)
    }

    /// Number of distinct keys.
    pub fn distinct_len(&self) -> usize {
        self.counts.len()
    }

    /// `true` if the multiset has no keys.
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Distinct keys with their counts, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&EncodedKey, u64)> {
        self.counts.iter().map(|(k, &n)| (k, n))
    }

    /// Keys that occur more than once, with their counts, in key order.
    pub fn duplicates(&self) -> impl Iterator<Item = (&EncodedKey, u64)> {
        self.iter().filter(|&(_, n)| n > 1)
    }

    /// Applies a net delta atomically: either every count changes or none does.
    ///
    /// Fails without modifying `self` if a count would drop below zero (the delta removes
    /// keys that are not present) or exceed `u64::MAX`.
    pub fn apply(&mut self, delta: &NetDelta) -> Result<(), DeltaError> {
        let mut updated = Vec::with_capacity(delta.changes.len());
        for (key, &change) in &delta.changes {
            let new = i128::from(self.count(key)) + change;
            if new < 0 {
                return Err(DeltaError::Underflow);
            }
            let new = u64::try_from(new).map_err(|_| DeltaError::Overflow)?;
            updated.push((key, new));
        }
        for (key, new) in updated {
            match self.counts.entry(key.clone()) {
                Entry::Occupied(e) if new == 0 => {
                    e.remove();
                }
                Entry::Occupied(mut e) => *e.get_mut() = new,
                Entry::Vacant(e) if new != 0 => {
                    e.insert(new);
                }
                Entry::Vacant(_) => {}
            }
        }
        Ok(())
    }
}

impl KeyMultiset {
    /// Collects keys, counting repeats.
    pub fn from_keys(keys: impl IntoIterator<Item = EncodedKey>) -> Result<Self, DeltaError> {
        let mut set = Self::new();
        for key in keys {
            set.insert(key)?;
        }
        Ok(set)
    }
}

/// A commit's key changes for one constraint: keys of added rows and keys of removed rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyDelta {
    /// Keys of rows the commit adds.
    pub added: KeyMultiset,
    /// Keys of rows the commit removes.
    pub removed: KeyMultiset,
}

impl KeyDelta {
    /// The net change `added − removed`; keys with no net change are omitted.
    pub fn net(&self) -> NetDelta {
        let mut changes: BTreeMap<EncodedKey, i128> = BTreeMap::new();
        for (key, n) in self.added.iter() {
            changes.insert(key.clone(), i128::from(n));
        }
        for (key, n) in self.removed.iter() {
            *changes.entry(key.clone()).or_insert(0) -= i128::from(n);
        }
        changes.retain(|_, c| *c != 0);
        NetDelta { changes }
    }
}

/// Signed per-key count changes. Never stores zero changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetDelta {
    changes: BTreeMap<EncodedKey, i128>,
}

impl NetDelta {
    /// The change for `key` (0 if unchanged).
    pub fn get(&self, key: &EncodedKey) -> i128 {
        self.changes.get(key).copied().unwrap_or(0)
    }

    /// `true` if no key changes.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Changed keys with their signed changes, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&EncodedKey, i128)> {
        self.changes.iter().map(|(k, &c)| (k, c))
    }

    /// Keys whose count increases.
    pub fn increases(&self) -> impl Iterator<Item = (&EncodedKey, i128)> {
        self.iter().filter(|&(_, c)| c > 0)
    }

    /// Keys whose count decreases, with the (negative) change.
    pub fn decreases(&self) -> impl Iterator<Item = (&EncodedKey, i128)> {
        self.iter().filter(|&(_, c)| c < 0)
    }

    /// The inverse delta: applying `d` then `d.negate()` is the identity.
    pub fn negate(&self) -> Self {
        Self {
            changes: self.changes.iter().map(|(k, &c)| (k.clone(), -c)).collect(),
        }
    }
}

/// Error applying or accumulating key counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaError {
    /// A delta removes more occurrences of a key than are present.
    Underflow,
    /// A count would exceed `u64::MAX`.
    Overflow,
}

impl fmt::Display for DeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaError::Underflow => f.write_str("delta removes a key that is not present"),
            DeltaError::Overflow => f.write_str("key count overflow"),
        }
    }
}

impl std::error::Error for DeltaError {}
