//! Embedded persistent backend (spec §13.4, ADR 0005), built on redb.
//!
//! redb types never appear in this crate's public API. One store (one file) holds the indexes of
//! many constraints: a table of entries per index and a metadata table with each index's kind,
//! epoch and last applied `(epoch, digest)`. Every `apply` is one redb write transaction that
//! updates entries and metadata together, committed durably with two-phase commit.
//!
//! Rebuilds (spec §19) write new contents into a build table beside the live one, in many small
//! transactions, then [`PersistentStore::install`] swaps the build tables of all indexes in one
//! durable transaction: readers see the old or the new indexes, never a mix, and a crash before the
//! swap leaves the old ones untouched.
//!
//! redb can panic (`unreachable!`) on some corrupted files instead of returning an error. Every
//! call into redb therefore runs under `catch_unwind`: a panic becomes [`IndexError::Corrupt`] and
//! poisons the store, so all later calls fail closed too. This requires `panic = "unwind"`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use integrity_core::EncodedKey;
use integrity_types::ConstraintId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::{
    ApplyAction, IndexDelta, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, Result,
    StagedDelta, decide_apply, decode_value, encode_value, resolve,
};

const META: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/index-meta/v1");
const META_VERSION: u8 = 1;
const META_LEN: usize = 1 + 1 + 8 + 1 + 8 + 32;
/// Identity of the store file, created once: lets the owner notice that the file was deleted and
/// recreated (all indexes empty) instead of trusting it (ADR 0005).
const STORE: TableDefinition<&str, &[u8]> = TableDefinition::new("oip/index-store/v1");

/// Converts any redb error. Messages from redb describe files and pages, never key contents.
fn storage(e: impl Into<redb::Error>) -> IndexError {
    match e.into() {
        redb::Error::Corrupted(_) => IndexError::Corrupt,
        other => IndexError::Storage(other.to_string()),
    }
}

#[derive(Debug)]
struct Shared {
    db: Database,
    poisoned: AtomicBool,
    id: String,
    scratch: PathBuf,
}

impl Shared {
    /// Runs `f` against the database, converting a panic inside redb into `Corrupt`.
    fn guard<T>(&self, f: impl FnOnce(&Database) -> Result<T>) -> Result<T> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(IndexError::Corrupt);
        }
        match catch_unwind(AssertUnwindSafe(|| f(&self.db))) {
            Ok(result) => result,
            Err(_) => {
                self.poisoned.store(true, Ordering::SeqCst);
                Err(IndexError::Corrupt)
            }
        }
    }
}

/// A file holding the persistent indexes of one or more constraints.
#[derive(Debug, Clone)]
pub struct PersistentStore {
    shared: Arc<Shared>,
}

impl PersistentStore {
    /// Opens the store at `path`, creating it if absent, and verifies every checksum.
    ///
    /// If the integrity check fails, or redb had to repair the file, the store is reported as
    /// [`IndexError::Corrupt`]: a repaired file may have lost applied commits, so its indexes
    /// cannot be trusted and must be rebuilt (spec §19).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let opened = catch_unwind(AssertUnwindSafe(|| -> Result<Database> {
            let mut db = Database::create(path).map_err(storage)?;
            match db.check_integrity() {
                Ok(true) => Ok(db),
                Ok(false) => Err(IndexError::Corrupt),
                Err(e) => Err(storage(e)),
            }
        }));
        let db = opened.map_err(|_| IndexError::Corrupt)??;
        let id = catch_unwind(AssertUnwindSafe(|| store_id(&db, path)))
            .map_err(|_| IndexError::Corrupt)??;
        let mut scratch = path.as_os_str().to_owned();
        scratch.push(".scratch");
        let scratch = PathBuf::from(scratch);
        // Sort runs of a scan that did not finish (the process died).
        let _ = std::fs::remove_dir_all(&scratch);
        Ok(Self {
            shared: Arc::new(Shared {
                db,
                poisoned: AtomicBool::new(false),
                id,
                scratch,
            }),
        })
    }

    /// A directory for the temporary files of scans (sort runs), next to the store file and
    /// emptied when the store is opened.
    pub fn scratch_dir(&self) -> &Path {
        &self.shared.scratch
    }

    /// Starts new contents for the index of constraint `id` (creating the index if needed) in a
    /// build table beside the live one (spec §19). Readers keep seeing the live contents until
    /// [`PersistentStore::install`]. A build of `id` left over from a crash is discarded.
    pub fn build(&self, id: ConstraintId, kind: IndexKind) -> Result<IndexBuild> {
        self.index(id, kind)?;
        let build = IndexBuild {
            shared: Arc::clone(&self.shared),
            id: id.0,
            table: format!("oip/index-build/v1/{}", id.0),
            kind,
            pending: Vec::new(),
            len: 0,
        };
        self.shared.guard(|db| {
            let txn = db.begin_write().map_err(storage)?;
            txn.delete_table(build.table()).map_err(storage)?;
            txn.open_table(build.table()).map_err(storage)?;
            txn.commit().map_err(storage)
        })?;
        Ok(build)
    }

    /// Makes every build the contents of its index, all at one epoch above the current epoch of
    /// every one of them, in one atomic, durable transaction (spec §19 step 5): either every index
    /// switches or none does. Returns that epoch. Clears the replay identity of each index: the
    /// next apply must be a new epoch.
    pub fn install(&self, builds: Vec<IndexBuild>) -> Result<IndexEpoch> {
        let mut builds = builds;
        for b in &mut builds {
            if !Arc::ptr_eq(&b.shared, &self.shared) {
                return Err(IndexError::Storage("build of another store".into()));
            }
            b.flush()?;
        }
        self.shared.guard(|db| {
            let mut txn = db.begin_write().map_err(storage)?;
            txn.set_two_phase_commit(true);
            let epoch = {
                let mut meta_table = txn.open_table(META).map_err(storage)?;
                let mut current = 0;
                for b in &builds {
                    current = current.max(b.live().read_meta(&meta_table)?.epoch.0);
                }
                let epoch = IndexEpoch(current + 1);
                for b in &builds {
                    let live = b.live();
                    txn.delete_table(live.table()).map_err(storage)?;
                    txn.rename_table(b.table(), live.table()).map_err(storage)?;
                    let updated = Meta {
                        kind: b.kind,
                        epoch,
                        last_applied: None,
                    };
                    meta_table
                        .insert(b.id, updated.encode().as_slice())
                        .map_err(storage)?;
                }
                epoch
            };
            txn.commit().map_err(storage)?;
            Ok(epoch)
        })
    }

    /// The identity of this store file (hex), assigned when the file was created.
    pub fn id(&self) -> &str {
        &self.shared.id
    }

    /// Applies the staged deltas of several indexes of this store at `epoch` in one atomic,
    /// durable transaction: either every index changes or none does. Each index is idempotent as
    /// with [`KeyIndex::apply`] (one already applied at `epoch` with the same delta is skipped).
    pub fn apply_all(
        &self,
        batch: &[(&PersistentIndex, &StagedDelta)],
        epoch: IndexEpoch,
    ) -> Result<()> {
        if batch
            .iter()
            .any(|(index, _)| !Arc::ptr_eq(&index.shared, &self.shared))
        {
            return Err(IndexError::Storage("index of another store".into()));
        }
        self.shared.guard(|db| {
            let mut txn = db.begin_write().map_err(storage)?;
            txn.set_two_phase_commit(true);
            let mut wrote = false;
            for (index, staged) in batch {
                wrote |= index.write_in(&txn, staged, epoch)?;
            }
            if wrote {
                txn.commit().map_err(storage)?;
            }
            Ok(())
        })
    }

    /// The index of constraint `id`, created empty at epoch 0 if it does not exist yet.
    /// Fails with [`IndexError::Corrupt`] if it exists with a different kind.
    pub fn index(&self, id: ConstraintId, kind: IndexKind) -> Result<PersistentIndex> {
        let index = PersistentIndex {
            shared: Arc::clone(&self.shared),
            id: id.0,
            table: format!("oip/index/v1/{}", id.0),
            kind,
        };
        self.shared.guard(|db| index.create(db))?;
        Ok(index)
    }
}

/// Reads the store's identity, creating it on first open. Not secret, only distinct.
fn store_id(db: &Database, path: &Path) -> Result<String> {
    let mut txn = db.begin_write().map_err(storage)?;
    txn.set_two_phase_commit(true);
    let id = {
        let mut table = txn.open_table(STORE).map_err(storage)?;
        let existing = table
            .get("id")
            .map_err(storage)?
            .map(|v| v.value().to_vec());
        match existing {
            Some(bytes) if bytes.len() == 32 => bytes,
            Some(_) => return Err(IndexError::Corrupt),
            None => {
                let mut h = blake3::Hasher::new();
                h.update(b"oip-index-store");
                h.update(path.to_string_lossy().as_bytes());
                h.update(&std::process::id().to_be_bytes());
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos());
                h.update(&now.to_be_bytes());
                let fresh = h.finalize().as_bytes().to_vec();
                table.insert("id", fresh.as_slice()).map_err(storage)?;
                fresh
            }
        }
    };
    txn.commit().map_err(storage)?;
    Ok(id.iter().map(|b| format!("{b:02x}")).collect())
}

/// Entries written per transaction while building.
const BUILD_BATCH: usize = 100_000;

/// New contents of one index, written beside the live contents until installed
/// ([`PersistentStore::build`]). Dropping it without installing leaves the build table, which the
/// next build of the same index discards.
#[derive(Debug)]
pub struct IndexBuild {
    shared: Arc<Shared>,
    id: u64,
    table: String,
    kind: IndexKind,
    pending: Vec<(EncodedKey, IndexValue)>,
    len: u64,
}

impl IndexBuild {
    fn table(&self) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
        TableDefinition::new(&self.table)
    }

    fn live(&self) -> PersistentIndex {
        PersistentIndex {
            shared: Arc::clone(&self.shared),
            id: self.id,
            table: format!("oip/index/v1/{}", self.id),
            kind: self.kind,
        }
    }

    /// The constraint whose index this builds.
    pub fn constraint(&self) -> ConstraintId {
        ConstraintId(self.id)
    }

    /// Entries added so far.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether no entry was added.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds an entry (keys in order write fastest). A key added twice keeps the last value.
    pub fn push(&mut self, key: EncodedKey, value: IndexValue) -> Result<()> {
        if value.kind() != Some(self.kind) {
            return Err(IndexError::Corrupt);
        }
        self.pending.push((key, value));
        self.len += 1;
        if self.pending.len() >= BUILD_BATCH {
            self.flush()?;
        }
        Ok(())
    }

    /// Writes the pending entries to the build table.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.shared.guard(|db| {
            let txn = db.begin_write().map_err(storage)?;
            {
                let mut table = txn.open_table(self.table()).map_err(storage)?;
                for (key, value) in &pending {
                    table
                        .insert(key.as_bytes(), encode_value(Some(value)).as_slice())
                        .map_err(storage)?;
                }
            }
            txn.commit().map_err(storage)
        })
    }

    /// The values of `keys` in the build so far (FK parents during a scan).
    pub fn get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>> {
        self.flush()?;
        self.shared.guard(|db| {
            let txn = db.begin_read().map_err(storage)?;
            let table = txn.open_table(self.table()).map_err(storage)?;
            keys.iter()
                .map(|k| match table.get(k.as_bytes()).map_err(storage)? {
                    Some(g) => decode_value(g.value()).map(Some),
                    None => Ok(None),
                })
                .collect()
        })
    }

    /// Deletes the build table.
    pub fn discard(self) -> Result<()> {
        self.shared.guard(|db| {
            let txn = db.begin_write().map_err(storage)?;
            txn.delete_table(self.table()).map_err(storage)?;
            txn.commit().map_err(storage)
        })
    }
}

/// The persistent index of one constraint.
#[derive(Debug, Clone)]
pub struct PersistentIndex {
    shared: Arc<Shared>,
    id: u64,
    table: String,
    kind: IndexKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Meta {
    kind: IndexKind,
    epoch: IndexEpoch,
    last_applied: Option<(IndexEpoch, [u8; 32])>,
}

impl Meta {
    fn encode(&self) -> [u8; META_LEN] {
        let mut out = [0u8; META_LEN];
        out[0] = META_VERSION;
        out[1] = match self.kind {
            IndexKind::Unique => 1,
            IndexKind::Reference => 2,
        };
        out[2..10].copy_from_slice(&self.epoch.0.to_be_bytes());
        if let Some((epoch, digest)) = self.last_applied {
            out[10] = 1;
            out[11..19].copy_from_slice(&epoch.0.to_be_bytes());
            out[19..51].copy_from_slice(&digest);
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; META_LEN] = bytes.try_into().map_err(|_| IndexError::Corrupt)?;
        if bytes[0] != META_VERSION {
            return Err(IndexError::Corrupt);
        }
        let kind = match bytes[1] {
            1 => IndexKind::Unique,
            2 => IndexKind::Reference,
            _ => return Err(IndexError::Corrupt),
        };
        let u64_at = |i: usize| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&bytes[i..i + 8]);
            u64::from_be_bytes(b)
        };
        let last_applied = match bytes[10] {
            0 if bytes[11..].iter().all(|&b| b == 0) => None,
            1 => {
                let mut digest = [0u8; 32];
                digest.copy_from_slice(&bytes[19..51]);
                Some((IndexEpoch(u64_at(11)), digest))
            }
            _ => return Err(IndexError::Corrupt),
        };
        Ok(Self {
            kind,
            epoch: IndexEpoch(u64_at(2)),
            last_applied,
        })
    }
}

impl PersistentIndex {
    fn table(&self) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
        TableDefinition::new(&self.table)
    }

    /// Registers the index in the metadata table if needed, checking its kind.
    fn create(&self, db: &Database) -> Result<()> {
        // Usually the index exists: check without a (durable) write transaction.
        {
            let read = db.begin_read().map_err(storage)?;
            if let Ok(meta) = read.open_table(META) {
                let existing = meta
                    .get(self.id)
                    .map_err(storage)?
                    .map(|g| Meta::decode(g.value()))
                    .transpose()?;
                match existing {
                    Some(m) if m.kind != self.kind => return Err(IndexError::Corrupt),
                    Some(_) if read.open_table(self.table()).is_ok() => return Ok(()),
                    _ => {}
                }
            }
        }
        let mut txn = db.begin_write().map_err(storage)?;
        txn.set_two_phase_commit(true);
        {
            let mut meta = txn.open_table(META).map_err(storage)?;
            let existing = meta
                .get(self.id)
                .map_err(storage)?
                .map(|g| Meta::decode(g.value()))
                .transpose()?;
            match existing {
                Some(m) if m.kind != self.kind => return Err(IndexError::Corrupt),
                Some(_) => {}
                None => {
                    let fresh = Meta {
                        kind: self.kind,
                        epoch: IndexEpoch(0),
                        last_applied: None,
                    };
                    meta.insert(self.id, fresh.encode().as_slice())
                        .map_err(storage)?;
                }
            }
            // Create the entries table so that readers can always open it.
            txn.open_table(self.table()).map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }

    fn read_meta(&self, meta: &impl ReadableTable<u64, &'static [u8]>) -> Result<Meta> {
        let m = meta
            .get(self.id)
            .map_err(storage)?
            .ok_or(IndexError::Corrupt)
            .and_then(|g| Meta::decode(g.value()))?;
        if m.kind != self.kind {
            return Err(IndexError::Corrupt);
        }
        Ok(m)
    }

    fn get(
        &self,
        table: &impl ReadableTable<&'static [u8], &'static [u8]>,
        key: &EncodedKey,
    ) -> Result<Option<IndexValue>> {
        match table.get(key.as_bytes()).map_err(storage)? {
            Some(g) => decode_value(g.value()).map(Some),
            None => Ok(None),
        }
    }

    fn apply_in(&self, db: &Database, staged: &StagedDelta, epoch: IndexEpoch) -> Result<()> {
        let mut txn = db.begin_write().map_err(storage)?;
        txn.set_two_phase_commit(true);
        if self.write_in(&txn, staged, epoch)? {
            txn.commit().map_err(storage)?;
        }
        Ok(())
    }

    /// Writes `staged` within `txn`; `false` if it was already applied (nothing written).
    fn write_in(
        &self,
        txn: &redb::WriteTransaction,
        staged: &StagedDelta,
        epoch: IndexEpoch,
    ) -> Result<bool> {
        {
            let mut meta_table = txn.open_table(META).map_err(storage)?;
            let meta = self.read_meta(&meta_table)?;
            match decide_apply(staged, epoch, meta.epoch, meta.last_applied)? {
                ApplyAction::AlreadyApplied => return Ok(false),
                ApplyAction::Write => {}
            }
            let digest = staged.digest();
            let mut table = txn.open_table(self.table()).map_err(storage)?;
            for (key, value) in staged.writes() {
                match value {
                    Some(v) => {
                        table
                            .insert(key.as_bytes(), encode_value(Some(v)).as_slice())
                            .map_err(storage)?;
                    }
                    None => {
                        table.remove(key.as_bytes()).map_err(storage)?;
                    }
                }
            }
            let updated = Meta {
                kind: self.kind,
                epoch,
                last_applied: Some((epoch, digest)),
            };
            meta_table
                .insert(self.id, updated.encode().as_slice())
                .map_err(storage)?;
        }
        Ok(true)
    }
}

impl KeyIndex for PersistentIndex {
    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn get_many(&self, keys: &[EncodedKey]) -> Result<Vec<Option<IndexValue>>> {
        self.shared.guard(|db| {
            let txn = db.begin_read().map_err(storage)?;
            let table = txn.open_table(self.table()).map_err(storage)?;
            keys.iter().map(|k| self.get(&table, k)).collect()
        })
    }

    fn stage(&self, delta: &IndexDelta) -> Result<StagedDelta> {
        self.shared.guard(|db| {
            let txn = db.begin_read().map_err(storage)?;
            let meta = self.read_meta(&txn.open_table(META).map_err(storage)?)?;
            let table = txn.open_table(self.table()).map_err(storage)?;
            resolve(self.kind, delta, meta.epoch, |k| self.get(&table, k))
        })
    }

    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> Result<()> {
        self.shared.guard(|db| self.apply_in(db, &staged, epoch))
    }

    fn epoch(&self) -> Result<IndexEpoch> {
        self.shared.guard(|db| {
            let txn = db.begin_read().map_err(storage)?;
            Ok(self
                .read_meta(&txn.open_table(META).map_err(storage)?)?
                .epoch)
        })
    }

    fn entries(&self) -> Result<Vec<(EncodedKey, IndexValue)>> {
        self.shared.guard(|db| {
            let txn = db.begin_read().map_err(storage)?;
            let table = txn.open_table(self.table()).map_err(storage)?;
            let mut out = Vec::new();
            for item in table.iter().map_err(storage)? {
                let (k, v) = item.map_err(storage)?;
                let key = EncodedKey::from_bytes(k.value()).map_err(|_| IndexError::Corrupt)?;
                out.push((key, decode_value(v.value())?));
            }
            Ok(out)
        })
    }
}
