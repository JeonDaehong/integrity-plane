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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::sync::Arc;

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
        for (key, change) in delta.iter() {
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
        let added = self.added.iter().map(|(k, n)| (k, i128::from(n)));
        let removed = self.removed.iter().map(|(k, n)| (k, -i128::from(n)));
        NetDelta::merged(added, removed, |a, b| Some(a + b))
    }
}

/// Signed per-key count changes, in key order. Never stores zero changes. Cloning is cheap (the
/// changes are shared): a commit's delta is handed to the certificate and to every index stage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetDelta {
    changes: Arc<[(EncodedKey, i128)]>,
}

impl NetDelta {
    /// A delta from changes already in strictly increasing key order, none of them zero (as a
    /// merge of sorted keys produces them); anything else is [`DeltaError::NotSorted`].
    pub fn from_sorted(changes: Vec<(EncodedKey, i128)>) -> Result<Self, DeltaError> {
        let ordered = changes.windows(2).all(|w| w[0].0 < w[1].0);
        if !ordered || changes.iter().any(|(_, c)| *c == 0) {
            return Err(DeltaError::NotSorted);
        }
        Ok(Self {
            changes: changes.into(),
        })
    }

    /// The per-key sum of `self` and `other`.
    pub fn plus(&self, other: &NetDelta) -> Result<NetDelta, DeltaError> {
        let mut overflow = false;
        let sum = Self::merged(self.iter(), other.iter(), |a, b| {
            let s = a.checked_add(b);
            overflow |= s.is_none();
            s
        });
        if overflow {
            return Err(DeltaError::Overflow);
        }
        Ok(sum)
    }

    /// Merges two key-ordered sequences of changes, combining equal keys with `both` (`None`
    /// drops the key) and dropping zero results.
    fn merged<'a>(
        a: impl Iterator<Item = (&'a EncodedKey, i128)>,
        b: impl Iterator<Item = (&'a EncodedKey, i128)>,
        mut both: impl FnMut(i128, i128) -> Option<i128>,
    ) -> NetDelta {
        let (mut a, mut b) = (a.peekable(), b.peekable());
        let mut out = Vec::new();
        loop {
            let order = match (a.peek(), b.peek()) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((x, _)), Some((y, _))) => x.cmp(y),
            };
            let item = match order {
                Ordering::Less => a.next().map(|(k, c)| (k, Some(c))),
                Ordering::Greater => b.next().map(|(k, c)| (k, Some(c))),
                Ordering::Equal => match (a.next(), b.next()) {
                    (Some((k, x)), Some((_, y))) => Some((k, both(x, y))),
                    _ => None,
                },
            };
            if let Some((k, Some(c))) = item
                && c != 0
            {
                out.push((k.clone(), c));
            }
        }
        NetDelta {
            changes: out.into(),
        }
    }

    /// The change for `key` (0 if unchanged).
    pub fn get(&self, key: &EncodedKey) -> i128 {
        self.changes
            .binary_search_by(|(k, _)| k.cmp(key))
            .map_or(0, |i| self.changes[i].1)
    }

    /// `true` if no key changes.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Number of changed keys.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Changed keys with their signed changes, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&EncodedKey, i128)> {
        self.changes.iter().map(|(k, c)| (k, *c))
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
            changes: self.changes.iter().map(|(k, c)| (k.clone(), -c)).collect(),
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
    /// Changes given as sorted were out of key order, repeated a key or held a zero change.
    NotSorted,
}

impl fmt::Display for DeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaError::Underflow => f.write_str("delta removes a key that is not present"),
            DeltaError::Overflow => f.write_str("key count overflow"),
            DeltaError::NotSorted => f.write_str("key changes out of order"),
        }
    }
}

impl std::error::Error for DeltaError {}
