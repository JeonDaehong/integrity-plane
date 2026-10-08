//! Shared helpers for index tests: keys, deltas, backends and temporary stores.

#![allow(dead_code, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use integrity_core::{EncodedKey, KeyDelta, KeySchema, KeyValue, TypeFamily};
use integrity_index::{
    IndexDelta, IndexEpoch, IndexError, IndexKind, KeyIndex, MemoryIndex, PersistentStore,
};
use integrity_types::{ConstraintId, SnapshotId};

pub fn k(v: i64) -> EncodedKey {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap()
}

/// A delta from `(key, signed change)` pairs.
pub fn delta(snapshot: i64, changes: &[(i64, i64)]) -> IndexDelta {
    let mut d = KeyDelta::default();
    for &(key, n) in changes {
        let side = if n > 0 { &mut d.added } else { &mut d.removed };
        side.insert_n(k(key), n.unsigned_abs()).unwrap();
    }
    IndexDelta {
        snapshot: SnapshotId(snapshot),
        changes: d.net(),
    }
}

/// Stage and apply at the next epoch.
pub fn commit(index: &dyn KeyIndex, d: &IndexDelta) -> Result<(), IndexError> {
    let staged = index.stage(d)?;
    let next = IndexEpoch(index.epoch()?.0 + 1);
    index.apply(staged, next)
}

/// A directory under the system temp dir, removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "oip-{tag}-{}-{nanos}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Memory,
    Persistent,
}

pub const BACKENDS: [Backend; 2] = [Backend::Memory, Backend::Persistent];

/// An index plus whatever keeps it alive.
pub struct Fixture {
    pub index: Box<dyn KeyIndex>,
    _dir: Option<TempDir>,
}

impl std::ops::Deref for Fixture {
    type Target = dyn KeyIndex;
    fn deref(&self) -> &Self::Target {
        self.index.as_ref()
    }
}

pub fn make(backend: Backend, kind: IndexKind) -> Fixture {
    match backend {
        Backend::Memory => Fixture {
            index: Box::new(MemoryIndex::new(kind)),
            _dir: None,
        },
        Backend::Persistent => {
            let dir = TempDir::new("index");
            let store = PersistentStore::open(dir.path().join("index.redb")).unwrap();
            Fixture {
                index: Box::new(store.index(ConstraintId(1), kind).unwrap()),
                _dir: Some(dir),
            }
        }
    }
}
