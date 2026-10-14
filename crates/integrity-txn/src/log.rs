//! The durable transaction log (spec §16, RFC 0004, retention RFC 0006).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use integrity_core::EncodedKey;
use integrity_index::{
    IndexEpoch, IndexError, IndexValue, StagedDelta, StagedDigest, StagedHeader, Writes,
    decode_staged_value, encode_value,
};
use integrity_types::ConstraintId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::fault::{self, FaultPoint};
use crate::{TxnError, TxnId, TxnState};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/txn-log/v1");
const STAGED: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/txn-staged/v1");
const STAGED_V2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("oip/txn-staged/v2");
/// Default [`LogOptions::chunk_writes`] (RFC 0006).
pub const DEFAULT_CHUNK_WRITES: usize = 100_000;
const META: TableDefinition<&str, u64> = TableDefinition::new("oip/txn-meta/v1");
const VERSION: u8 = 1;

/// Default `[txn_log] retention` (RFC 0006).
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(24 * 3600);
/// Smallest allowed retention: the `idempotency-key-lifetime` advertised in `/v1/config`.
pub const MIN_RETENTION: Duration = Duration::from_secs(30 * 60);
/// Pruning runs at most this often after terminal records (milliseconds).
const PRUNE_EVERY_MS: u64 = 60_000;
/// Transactions pruned per durable write.
const PRUNE_BATCH: usize = 10_000;

/// The current time in Unix milliseconds.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// How the log keeps finished transactions.
#[derive(Clone)]
pub struct LogOptions {
    /// How long a transaction is kept after its terminal record (RFC 0006).
    pub retention: Duration,
    /// Where `finished_at` and the pruning cutoff come from.
    pub clock: Clock,
    /// Writes per staged chunk (RFC 0006); readers accept any chunk size.
    pub chunk_writes: usize,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            retention: DEFAULT_RETENTION,
            clock: Arc::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            }),
            chunk_writes: DEFAULT_CHUNK_WRITES,
        }
    }
}

impl fmt::Debug for LogOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogOptions")
            .field("retention", &self.retention)
            .field("chunk_writes", &self.chunk_writes)
            .finish_non_exhaustive()
    }
}

/// What is known when a commit arrives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prepared {
    /// The client's `Idempotency-Key`, if any.
    pub request_id: Option<String>,
    /// Table UUID.
    pub table: String,
    /// Table identifier.
    pub identifier: String,
    /// REST path to reload the table during recovery.
    pub load_path: String,
}

/// What recovery needs to finish a validated commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Validated {
    /// `main` before the commit.
    pub base_snapshot: Option<i64>,
    /// New snapshots, oldest first.
    pub snapshots: Vec<i64>,
    /// The snapshot whose presence upstream proves the commit happened.
    pub final_snapshot: i64,
    /// Constraint set version the commit was validated under.
    pub constraint_set_version: u64,
    /// Index epoch the staged deltas are applied at.
    pub epoch: u64,
    /// Certificates, hex, one per snapshot.
    pub certificates: Vec<String>,
}

/// A response recorded for a terminal state, returned again for a repeated request id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    /// HTTP status.
    pub status: u16,
    /// Response body.
    pub body: Value,
}

/// A transaction that recovery must resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolved {
    /// Its id.
    pub txn: TxnId,
    /// `Prepared`, `Validated` or `Committing`.
    pub state: TxnState,
    /// The prepared record.
    pub prepared: Prepared,
    /// The validated record, if it got that far.
    pub validated: Option<Validated>,
    /// Staged index deltas, if validated; their writes are read with [`TxnLog::staged_writes`].
    pub staged: Vec<StagedEntry>,
}

/// The payload of a terminal record: the decision and, since RFC 0006, when it was made.
#[derive(Serialize, Deserialize)]
struct Terminal {
    status: u16,
    body: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<u64>,
}

/// Everything recorded about one transaction (`GET /v1/integrity/transactions/{id}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnSummary {
    /// Its latest state.
    pub state: TxnState,
    /// The prepared record.
    pub prepared: Prepared,
    /// The validated record, if it got that far.
    pub validated: Option<Validated>,
    /// The recorded response, once terminal.
    pub decision: Option<Decision>,
}

/// What the log keeps in memory about a transaction; its payloads are read back from `seqs`.
#[derive(Debug, Clone, Default)]
struct Txn {
    state: Option<TxnState>,
    /// Its records, in order.
    seqs: Vec<u64>,
    request_id: Option<String>,
    /// When it reached its terminal state (Unix milliseconds).
    finished_at: Option<u64>,
}

#[derive(Debug, Default)]
struct Index {
    next_seq: u64,
    next_txn: u64,
    txns: BTreeMap<u64, Txn>,
    by_request: BTreeMap<String, u64>,
    /// Transactions not in a terminal state (what recovery must resolve).
    open: BTreeSet<u64>,
    /// When pruning last ran (Unix milliseconds).
    last_prune: u64,
}

fn is_terminal(state: TxnState) -> bool {
    matches!(
        state,
        TxnState::Committed | TxnState::Aborted | TxnState::Rejected
    )
}

/// The transaction log. All writes are durable two-phase commits.
pub struct TxnLog {
    db: Database,
    index: Mutex<Index>,
    options: LogOptions,
}

impl fmt::Debug for TxnLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnLog")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

fn poisoned<T>(_: T) -> TxnError {
    TxnError::Storage("log lock poisoned".into())
}

fn corrupt<T>(_: T) -> TxnError {
    TxnError::Corrupt
}

fn storage(e: impl Into<redb::Error>) -> TxnError {
    match e.into() {
        redb::Error::Corrupted(_) => TxnError::Corrupt,
        other => TxnError::Storage(other.to_string()),
    }
}

fn kind_of(state: TxnState) -> u8 {
    match state {
        TxnState::Prepared => 1,
        TxnState::Validated => 2,
        TxnState::Committing => 3,
        TxnState::Committed => 4,
        TxnState::Aborted => 5,
        TxnState::Rejected => 6,
    }
}

fn state_of(kind: u8) -> Result<TxnState, TxnError> {
    Ok(match kind {
        1 => TxnState::Prepared,
        2 => TxnState::Validated,
        3 => TxnState::Committing,
        4 => TxnState::Committed,
        5 => TxnState::Aborted,
        6 => TxnState::Rejected,
        _ => return Err(TxnError::Corrupt),
    })
}

fn checksum(kind: u8, txn: u64, payload: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"oip-txn-record-v1");
    h.update(&[VERSION, kind]);
    h.update(&txn.to_be_bytes());
    h.update(&(payload.len() as u32).to_be_bytes());
    h.update(payload);
    *h.finalize().as_bytes()
}

/// Encodes one record (RFC 0004).
pub fn encode_record(state: TxnState, txn: TxnId, payload: &[u8]) -> Vec<u8> {
    let kind = kind_of(state);
    let mut out = vec![VERSION, kind];
    out.extend_from_slice(&txn.0.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&checksum(kind, txn.0, payload));
    out
}

/// Decodes and verifies one record.
pub fn decode_record(bytes: &[u8]) -> Result<(TxnState, TxnId, Vec<u8>), TxnError> {
    if bytes.len() < 1 + 1 + 8 + 4 + 32 || bytes[0] != VERSION {
        return Err(TxnError::Corrupt);
    }
    let kind = bytes[1];
    let mut txn = [0u8; 8];
    txn.copy_from_slice(&bytes[2..10]);
    let txn = u64::from_be_bytes(txn);
    let mut len = [0u8; 4];
    len.copy_from_slice(&bytes[10..14]);
    let len = u32::from_be_bytes(len) as usize;
    if bytes.len() != 14 + len + 32 {
        return Err(TxnError::Corrupt);
    }
    let payload = &bytes[14..14 + len];
    if bytes[14 + len..] != checksum(kind, txn, payload) {
        return Err(TxnError::Corrupt);
    }
    Ok((state_of(kind)?, TxnId(txn), payload.to_vec()))
}

#[cfg(test)]
fn encode_staged(staged: &[(ConstraintId, StagedDelta)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(staged.len() as u32).to_be_bytes());
    for (id, delta) in staged {
        let bytes = delta.encode();
        body.extend_from_slice(&id.0.to_be_bytes());
        body.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        body.extend_from_slice(&bytes);
    }
    let mut out = blake3::hash(&body).as_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

fn decode_staged(bytes: &[u8]) -> Result<Vec<(ConstraintId, StagedDelta)>, TxnError> {
    if bytes.len() < 36 || blake3::hash(&bytes[32..]).as_bytes() != &bytes[..32] {
        return Err(TxnError::Corrupt);
    }
    let mut b = &bytes[32..];
    let mut take = |n: usize| -> Result<Vec<u8>, TxnError> {
        if b.len() < n {
            return Err(TxnError::Corrupt);
        }
        let (h, r) = b.split_at(n);
        b = r;
        Ok(h.to_vec())
    };
    let count = u32::from_be_bytes(take(4)?.try_into().map_err(|_| TxnError::Corrupt)?);
    let mut out = Vec::new();
    for _ in 0..count {
        let id = u64::from_be_bytes(take(8)?.try_into().map_err(|_| TxnError::Corrupt)?);
        let len = u32::from_be_bytes(take(4)?.try_into().map_err(|_| TxnError::Corrupt)?) as usize;
        let delta = StagedDelta::decode(&take(len)?).map_err(|_| TxnError::Corrupt)?;
        out.push((ConstraintId(id), delta));
    }
    if !take(0)?.is_empty() || !b.is_empty() {
        return Err(TxnError::Corrupt);
    }
    Ok(out)
}

/// One index's staged delta in a `VALIDATED` record (RFC 0006): what its chunks must add up to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedEntry {
    /// The constraint whose index it changes.
    pub constraint: u64,
    /// The epoch it was resolved against.
    pub base_epoch: u64,
    /// Number of chunks in `oip/txn-staged/v2`.
    pub chunks: u32,
    /// Number of writes over all chunks.
    pub writes: u64,
    /// [`StagedDelta::digest`], hex.
    pub digest: String,
    /// Read from `oip/txn-staged/v1` (a transaction written before RFC 0006).
    #[serde(skip)]
    legacy: bool,
}

impl StagedEntry {
    /// The constraint whose index it changes.
    pub fn constraint_id(&self) -> ConstraintId {
        ConstraintId(self.constraint)
    }

    /// The header [`integrity_index::PersistentStore::apply_streamed`] checks the writes against.
    pub fn header(&self) -> Result<StagedHeader, TxnError> {
        let bytes = hex_decode(&self.digest).ok_or(TxnError::Corrupt)?;
        Ok(StagedHeader {
            base_epoch: IndexEpoch(self.base_epoch),
            writes: self.writes,
            digest: bytes.try_into().map_err(corrupt)?,
        })
    }

    fn of(constraint: ConstraintId, header: &StagedHeader, chunks: u32, legacy: bool) -> Self {
        Self {
            constraint: constraint.0,
            base_epoch: header.base_epoch.0,
            chunks,
            writes: header.writes,
            digest: hex_encode(&header.digest),
            legacy,
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A staged delta to log: its constraint, header, and writes in strictly increasing key order.
pub struct StagedSource<'a> {
    /// The constraint whose index it changes.
    pub constraint: ConstraintId,
    /// What the writes must add up to.
    pub header: StagedHeader,
    /// The writes.
    pub writes: Writes<'a>,
}

impl fmt::Debug for StagedSource<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagedSource")
            .field("constraint", &self.constraint)
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

/// Sources for staged deltas held in memory.
pub fn sources(staged: &[(ConstraintId, StagedDelta)]) -> Vec<StagedSource<'_>> {
    staged
        .iter()
        .map(|(id, delta)| StagedSource {
            constraint: *id,
            header: delta.header(),
            writes: Box::new(delta.writes().map(|(k, v)| Ok((k.clone(), v.copied())))),
        })
        .collect()
}

/// The `VALIDATED` payload: the validated record and, since RFC 0006, the staged entries.
#[derive(Serialize, Deserialize)]
struct ValidatedRecord {
    #[serde(flatten)]
    validated: Validated,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staged: Option<Vec<StagedEntry>>,
}

const CHUNK_VERSION: u8 = 2;

fn chunk_key(txn: u64, constraint: u64, chunk: u32) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[..8].copy_from_slice(&txn.to_be_bytes());
    key[8..16].copy_from_slice(&constraint.to_be_bytes());
    key[16..].copy_from_slice(&chunk.to_be_bytes());
    key
}

/// Every chunk key of `txn`, as a range.
fn chunk_range(txn: u64) -> ([u8; 20], [u8; 20]) {
    (chunk_key(txn, 0, 0), chunk_key(txn, u64::MAX, u32::MAX))
}

fn chunk_checksum(key: &[u8; 20], body: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"oip-staged-chunk-v2");
    h.update(key);
    h.update(body);
    *h.finalize().as_bytes()
}

/// RFC 0006 chunk encoding.
fn encode_chunk(
    key: &[u8; 20],
    base_epoch: IndexEpoch,
    writes: &[(EncodedKey, Option<IndexValue>)],
) -> Result<Vec<u8>, TxnError> {
    let mut out = vec![CHUNK_VERSION];
    out.extend_from_slice(&base_epoch.0.to_be_bytes());
    let n = u32::try_from(writes.len()).map_err(corrupt)?;
    out.extend_from_slice(&n.to_be_bytes());
    for (k, v) in writes {
        let len = u32::try_from(k.as_bytes().len()).map_err(corrupt)?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(encode_value(v.as_ref()).as_slice());
    }
    let checksum = chunk_checksum(key, &out);
    out.extend_from_slice(&checksum);
    Ok(out)
}

/// Parses [`encode_chunk`] output; anything else, keys out of order included, is corruption.
fn decode_chunk(
    key: &[u8; 20],
    base_epoch: IndexEpoch,
    bytes: &[u8],
) -> Result<Vec<(EncodedKey, Option<IndexValue>)>, TxnError> {
    let split = bytes.len().checked_sub(32).ok_or(TxnError::Corrupt)?;
    let (body, checksum) = bytes.split_at(split);
    if chunk_checksum(key, body) != checksum {
        return Err(TxnError::Corrupt);
    }
    let mut b = body;
    let mut take = |n: usize| -> Result<&[u8], TxnError> {
        if b.len() < n {
            return Err(TxnError::Corrupt);
        }
        let (head, rest) = b.split_at(n);
        b = rest;
        Ok(head)
    };
    if take(1)? != [CHUNK_VERSION] {
        return Err(TxnError::Corrupt);
    }
    if u64::from_be_bytes(take(8)?.try_into().map_err(corrupt)?) != base_epoch.0 {
        return Err(TxnError::Corrupt);
    }
    let n = u32::from_be_bytes(take(4)?.try_into().map_err(corrupt)?);
    let mut out: Vec<(EncodedKey, Option<IndexValue>)> = Vec::new();
    for _ in 0..n {
        let len = u32::from_be_bytes(take(4)?.try_into().map_err(corrupt)?) as usize;
        let k = EncodedKey::from_bytes(take(len)?).map_err(corrupt)?;
        if out.last().is_some_and(|(p, _)| *p >= k) {
            return Err(TxnError::Corrupt);
        }
        let tag = take(1)?;
        let value = if tag == [0] {
            None
        } else {
            let mut v = tag.to_vec();
            v.extend_from_slice(take(8)?);
            decode_staged_value(&v).map_err(corrupt)?
        };
        out.push((k, value));
    }
    if !b.is_empty() {
        return Err(TxnError::Corrupt);
    }
    Ok(out)
}

fn to_index(e: TxnError) -> IndexError {
    match e {
        TxnError::Storage(m) => IndexError::Storage(m),
        TxnError::Corrupt | TxnError::IllegalTransition { .. } => IndexError::Corrupt,
    }
}

/// The writes of a logged staged delta, read one chunk at a time.
struct ChunkReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    txn: u64,
    entry: StagedEntry,
    next_chunk: u32,
    current: std::vec::IntoIter<(EncodedKey, Option<IndexValue>)>,
    failed: bool,
}

impl ChunkReader {
    fn load_next(&mut self) -> Result<bool, TxnError> {
        if self.next_chunk >= self.entry.chunks {
            return Ok(false);
        }
        let key = chunk_key(self.txn, self.entry.constraint, self.next_chunk);
        let bytes = self
            .table
            .get(key.as_slice())
            .map_err(storage)?
            .ok_or(TxnError::Corrupt)?;
        let writes = decode_chunk(&key, IndexEpoch(self.entry.base_epoch), bytes.value())?;
        self.current = writes.into_iter();
        self.next_chunk += 1;
        Ok(true)
    }
}

impl Iterator for ChunkReader {
    type Item = Result<(EncodedKey, Option<IndexValue>), IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(write) = self.current.next() {
                return Some(Ok(write));
            }
            match self.load_next() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => {
                    self.failed = true;
                    return Some(Err(to_index(e)));
                }
            }
        }
    }
}

/// Whether `to` may follow `from` (RFC 0004 state machine).
pub fn allowed(from: Option<TxnState>, to: TxnState) -> bool {
    use TxnState::*;
    matches!(
        (from, to),
        (None, Prepared)
            | (Some(Prepared), Validated | Rejected | Aborted)
            | (Some(Validated), Committing | Aborted)
            | (Some(Committing), Committed | Aborted)
    )
}

impl TxnLog {
    /// Opens (or creates) the log with the default [`LogOptions`], verifying every record.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TxnError> {
        Self::open_with(path, LogOptions::default())
    }

    /// Opens (or creates) the log, verifying every record; deletes the staged deltas of finished
    /// transactions and prunes those past the retention period (RFC 0006).
    pub fn open_with(path: impl AsRef<Path>, options: LogOptions) -> Result<Self, TxnError> {
        let path = path.as_ref();
        let db = catch_unwind(AssertUnwindSafe(|| Database::create(path)))
            .map_err(corrupt)?
            .map_err(storage)?;
        let now = (options.clock)();
        let mut index = Index {
            next_txn: 1,
            ..Index::default()
        };
        let mut w = db.begin_write().map_err(storage)?;
        w.set_two_phase_commit(true);
        {
            let log = w.open_table(LOG).map_err(storage)?;
            let mut staged = w.open_table(STAGED).map_err(storage)?;
            let mut meta = w.open_table(META).map_err(storage)?;
            // Terminal records written before RFC 0006 have no `finished_at`: they count as
            // finished when a log was first opened by a version that prunes.
            let legacy_finished_at = match meta.get("opened_at").map_err(storage)? {
                Some(v) => v.value(),
                None => now,
            };
            meta.insert("opened_at", legacy_finished_at)
                .map_err(storage)?;
            if let Some(v) = meta.get("next_txn").map_err(storage)? {
                index.next_txn = index.next_txn.max(v.value());
            }
            if let Some(v) = meta.get("next_seq").map_err(storage)? {
                index.next_seq = index.next_seq.max(v.value());
            }
            for item in log.iter().map_err(storage)? {
                let (seq, record) = item.map_err(storage)?;
                let seq = seq.value();
                let (state, id, payload) = decode_record(record.value())?;
                index.next_seq = index.next_seq.max(seq + 1);
                index.next_txn = index.next_txn.max(id.0 + 1);
                let t = index.txns.entry(id.0).or_default();
                if !allowed(t.state, state) {
                    return Err(TxnError::Corrupt);
                }
                t.state = Some(state);
                t.seqs.push(seq);
                match state {
                    TxnState::Prepared => {
                        let p: Prepared = serde_json::from_slice(&payload).map_err(corrupt)?;
                        t.request_id = p.request_id;
                    }
                    TxnState::Validated => {
                        serde_json::from_slice::<Validated>(&payload).map_err(corrupt)?;
                    }
                    TxnState::Committing => {}
                    TxnState::Committed | TxnState::Aborted | TxnState::Rejected => {
                        let d: Terminal = serde_json::from_slice(&payload).map_err(corrupt)?;
                        t.finished_at = Some(d.finished_at.unwrap_or(legacy_finished_at));
                    }
                }
            }
            // Staged deltas are only kept for transactions recovery may still apply.
            let mut stale = Vec::new();
            for item in staged.iter().map_err(storage)? {
                let id = item.map_err(storage)?.0.value();
                let needed = index
                    .txns
                    .get(&id)
                    .and_then(|t| t.state)
                    .is_some_and(|s| matches!(s, TxnState::Validated | TxnState::Committing));
                if !needed {
                    stale.push(id);
                }
            }
            for id in stale {
                staged.remove(id).map_err(storage)?;
            }
            let mut chunks = w.open_table(STAGED_V2).map_err(storage)?;
            let mut stale = Vec::new();
            for item in chunks.iter().map_err(storage)? {
                let key = item.map_err(storage)?.0.value().to_vec();
                let id = u64::from_be_bytes(
                    key.get(..8)
                        .ok_or(TxnError::Corrupt)?
                        .try_into()
                        .map_err(corrupt)?,
                );
                let needed = index
                    .txns
                    .get(&id)
                    .and_then(|t| t.state)
                    .is_some_and(|s| matches!(s, TxnState::Validated | TxnState::Committing));
                if !needed {
                    stale.push(key);
                }
            }
            for key in stale {
                chunks.remove(key.as_slice()).map_err(storage)?;
            }
        }
        w.commit().map_err(storage)?;
        let Index {
            txns,
            by_request,
            open,
            ..
        } = &mut index;
        for (id, t) in txns.iter() {
            if let Some(r) = &t.request_id {
                by_request.insert(r.clone(), *id);
            }
            if !t.state.is_some_and(is_terminal) {
                open.insert(*id);
            }
        }
        let log = Self {
            db,
            index: Mutex::new(index),
            options,
        };
        log.prune(now)?;
        Ok(log)
    }

    /// Deletes every transaction that has been terminal for longer than the retention period, in
    /// durable writes of at most [`PRUNE_BATCH`] transactions, each also recording the next
    /// transaction id and sequence number so that neither is reused (RFC 0006).
    fn prune(&self, now: u64) -> Result<(), TxnError> {
        let retention = u64::try_from(self.options.retention.as_millis()).unwrap_or(u64::MAX);
        let mut index = self.index.lock().map_err(poisoned)?;
        index.last_prune = now;
        loop {
            let batch: Vec<u64> = index
                .txns
                .iter()
                .filter(|(_, t)| {
                    t.finished_at
                        .is_some_and(|f| f.saturating_add(retention) < now)
                })
                .map(|(id, _)| *id)
                .take(PRUNE_BATCH)
                .collect();
            if batch.is_empty() {
                return Ok(());
            }
            let mut w = self.db.begin_write().map_err(storage)?;
            w.set_two_phase_commit(true);
            {
                let mut log = w.open_table(LOG).map_err(storage)?;
                let mut staged = w.open_table(STAGED).map_err(storage)?;
                let mut meta = w.open_table(META).map_err(storage)?;
                let mut chunks = w.open_table(STAGED_V2).map_err(storage)?;
                for id in &batch {
                    if let Some(t) = index.txns.get(id) {
                        for seq in &t.seqs {
                            log.remove(*seq).map_err(storage)?;
                        }
                    }
                    staged.remove(*id).map_err(storage)?;
                    remove_chunks(&mut chunks, *id)?;
                }
                meta.insert("next_txn", index.next_txn).map_err(storage)?;
                meta.insert("next_seq", index.next_seq).map_err(storage)?;
            }
            w.commit().map_err(storage)?;
            for id in batch {
                if let Some(t) = index.txns.remove(&id)
                    && let Some(r) = t.request_id
                    && index.by_request.get(&r) == Some(&id)
                {
                    index.by_request.remove(&r);
                }
            }
        }
    }

    /// The records at `seqs`, decoded.
    fn read(&self, seqs: &[u64]) -> Result<Vec<(TxnState, Vec<u8>)>, TxnError> {
        let read = self.db.begin_read().map_err(storage)?;
        let log = read.open_table(LOG).map_err(storage)?;
        seqs.iter()
            .map(|seq| {
                let record = log.get(*seq).map_err(storage)?.ok_or(TxnError::Corrupt)?;
                let (state, _, payload) = decode_record(record.value())?;
                Ok((state, payload))
            })
            .collect()
    }

    /// The in-memory entry of `txn`, if the log still has it.
    fn entry(&self, txn: TxnId) -> Result<Option<Txn>, TxnError> {
        Ok(self
            .index
            .lock()
            .map_err(poisoned)?
            .txns
            .get(&txn.0)
            .cloned())
    }

    /// Everything recorded about `t`, read back from the log.
    fn load(&self, t: &Txn) -> Result<TxnSummary, TxnError> {
        Ok(self.load_with_entries(t)?.0)
    }

    /// [`TxnLog::load`], and the staged entries of a `VALIDATED` record written since RFC 0006.
    fn load_with_entries(
        &self,
        t: &Txn,
    ) -> Result<(TxnSummary, Option<Vec<StagedEntry>>), TxnError> {
        let mut prepared = None;
        let mut validated = None;
        let mut entries = None;
        let mut decision = None;
        for (state, payload) in self.read(&t.seqs)? {
            match state {
                TxnState::Prepared => {
                    prepared = Some(serde_json::from_slice(&payload).map_err(corrupt)?);
                }
                TxnState::Validated => {
                    let record: ValidatedRecord =
                        serde_json::from_slice(&payload).map_err(corrupt)?;
                    validated = Some(record.validated);
                    entries = record.staged;
                }
                TxnState::Committing => {}
                TxnState::Committed | TxnState::Aborted | TxnState::Rejected => {
                    let d: Terminal = serde_json::from_slice(&payload).map_err(corrupt)?;
                    decision = Some(Decision {
                        status: d.status,
                        body: d.body,
                    });
                }
            }
        }
        let summary = TxnSummary {
            state: t.state.ok_or(TxnError::Corrupt)?,
            prepared: prepared.ok_or(TxnError::Corrupt)?,
            validated,
            decision,
        };
        Ok((summary, entries))
    }

    fn append(&self, txn: TxnId, state: TxnState, payload: &[u8]) -> Result<(), TxnError> {
        self.append_all(txn, &[(state, payload)], &[])
    }

    /// Appends consecutive records of one transaction, and staged chunks, in a single durable
    /// write. A terminal record deletes the transaction's staged deltas in the same write (RFC
    /// 0006).
    fn append_all(
        &self,
        txn: TxnId,
        records: &[(TxnState, &[u8])],
        chunks: &[([u8; 20], Vec<u8>)],
    ) -> Result<(), TxnError> {
        let mut index = self.index.lock().map_err(poisoned)?;
        let mut current = index.txns.get(&txn.0).and_then(|t| t.state);
        for &(state, _) in records {
            if !allowed(current, state) {
                return Err(TxnError::IllegalTransition {
                    from: current,
                    to: state,
                });
            }
            current = Some(state);
        }
        let first_seq = index.next_seq;
        let mut w = self.db.begin_write().map_err(storage)?;
        w.set_two_phase_commit(true);
        {
            let mut log = w.open_table(LOG).map_err(storage)?;
            for (n, &(state, payload)) in records.iter().enumerate() {
                let record = encode_record(state, txn, payload);
                log.insert(first_seq + n as u64, record.as_slice())
                    .map_err(storage)?;
            }
            let mut table = w.open_table(STAGED_V2).map_err(storage)?;
            for (key, bytes) in chunks {
                table
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(storage)?;
            }
            if current.is_some_and(is_terminal) {
                remove_chunks(&mut table, txn.0)?;
                w.open_table(STAGED)
                    .map_err(storage)?
                    .remove(txn.0)
                    .map_err(storage)?;
            }
        }
        w.commit().map_err(storage)?;
        index.next_seq = first_seq + records.len() as u64;
        if let Some(state) = current {
            let t = index.txns.entry(txn.0).or_default();
            t.state = Some(state);
            t.seqs.extend(first_seq..first_seq + records.len() as u64);
            if is_terminal(state) {
                index.open.remove(&txn.0);
            } else {
                index.open.insert(txn.0);
            }
        }
        Ok(())
    }

    /// Starts a transaction (`PREPARED`).
    pub fn begin(&self, prepared: Prepared) -> Result<TxnId, TxnError> {
        let id = {
            let mut index = self.index.lock().map_err(poisoned)?;
            let id = index.next_txn;
            index.next_txn += 1;
            TxnId(id)
        };
        let payload =
            serde_json::to_vec(&prepared).map_err(|e| TxnError::Storage(e.to_string()))?;
        self.append(id, TxnState::Prepared, &payload)?;
        let mut index = self.index.lock().map_err(poisoned)?;
        if let Some(r) = &prepared.request_id {
            index.by_request.insert(r.clone(), id.0);
        }
        index.txns.entry(id.0).or_default().request_id = prepared.request_id;
        Ok(id)
    }

    /// Records a validated commit and its staged deltas (`VALIDATED`, RFC 0006).
    pub fn validated(
        &self,
        txn: TxnId,
        validated: Validated,
        staged: &[(ConstraintId, StagedDelta)],
    ) -> Result<Vec<StagedEntry>, TxnError> {
        self.write_validated(txn, validated, sources(staged), false)
    }

    /// Records `VALIDATED` (with the staged deltas) and `COMMITTING`, for a commit about to be
    /// forwarded. Same records as [`TxnLog::validated`] then [`TxnLog::committing`].
    pub fn validated_and_committing(
        &self,
        txn: TxnId,
        validated: Validated,
        staged: &[(ConstraintId, StagedDelta)],
    ) -> Result<Vec<StagedEntry>, TxnError> {
        self.write_validated(txn, validated, sources(staged), true)
    }

    /// [`TxnLog::validated_and_committing`] for staged deltas read as streams (ADR 0025).
    pub fn validated_and_committing_streamed(
        &self,
        txn: TxnId,
        validated: Validated,
        staged: Vec<StagedSource<'_>>,
    ) -> Result<Vec<StagedEntry>, TxnError> {
        self.write_validated(txn, validated, staged, true)
    }

    /// Writes the staged deltas in chunks of [`LogOptions::chunk_writes`]: every chunk but the
    /// last of each delta in a durable write of its own, then the last ones with `VALIDATED` (and
    /// `COMMITTING`) in one durable write (RFC 0006). Each delta's writes must be in strictly
    /// increasing key order and add up to its header, or nothing is validated
    /// ([`TxnError::Corrupt`]; chunks already written go with the transaction's terminal record).
    fn write_validated(
        &self,
        txn: TxnId,
        validated: Validated,
        staged: Vec<StagedSource<'_>>,
        committing: bool,
    ) -> Result<Vec<StagedEntry>, TxnError> {
        {
            let index = self.index.lock().map_err(poisoned)?;
            let current = index.txns.get(&txn.0).and_then(|t| t.state);
            if !allowed(current, TxnState::Validated) {
                return Err(TxnError::IllegalTransition {
                    from: current,
                    to: TxnState::Validated,
                });
            }
        }
        let size = self.options.chunk_writes.max(1);
        let mut entries = Vec::new();
        let mut last_chunks = Vec::new();
        for source in staged {
            let StagedSource {
                constraint,
                header,
                writes,
            } = source;
            let mut digest = StagedDigest::new(header.base_epoch, header.writes);
            let mut chunk: Vec<(EncodedKey, Option<IndexValue>)> =
                Vec::with_capacity(size.min(usize::try_from(header.writes).unwrap_or(size)));
            let mut previous: Option<EncodedKey> = None;
            let mut n = 0u32;
            let mut writes = writes.peekable();
            while let Some(write) = writes.next() {
                let (key, value) = write.map_err(|e| match e {
                    IndexError::Storage(m) => TxnError::Storage(m),
                    _ => TxnError::Corrupt,
                })?;
                let before = chunk.last().map(|(k, _)| k).or(previous.as_ref());
                if before.is_some_and(|p| *p >= key) {
                    return Err(TxnError::Corrupt);
                }
                digest.update(&key, value.as_ref()).map_err(corrupt)?;
                chunk.push((key, value));
                if chunk.len() == size && writes.peek().is_some() {
                    let key = chunk_key(txn.0, constraint.0, n);
                    let bytes = encode_chunk(&key, header.base_epoch, &chunk)?;
                    let mut w = self.db.begin_write().map_err(storage)?;
                    w.set_two_phase_commit(true);
                    w.open_table(STAGED_V2)
                        .map_err(storage)?
                        .insert(key.as_slice(), bytes.as_slice())
                        .map_err(storage)?;
                    w.commit().map_err(storage)?;
                    fault::hit(FaultPoint::BetweenStagedChunks);
                    n = n.checked_add(1).ok_or(TxnError::Corrupt)?;
                    previous = chunk.pop().map(|(k, _)| k);
                    chunk.clear();
                }
            }
            if digest.finish().map_err(corrupt)? != header.digest {
                return Err(TxnError::Corrupt);
            }
            let key = chunk_key(txn.0, constraint.0, n);
            last_chunks.push((key, encode_chunk(&key, header.base_epoch, &chunk)?));
            entries.push(StagedEntry::of(constraint, &header, n + 1, false));
        }
        let payload = serde_json::to_vec(&ValidatedRecord {
            validated,
            staged: Some(entries.clone()),
        })
        .map_err(|e| TxnError::Storage(e.to_string()))?;
        let mut records = vec![(TxnState::Validated, payload.as_slice())];
        if committing {
            records.push((TxnState::Committing, b"{}".as_slice()));
        }
        self.append_all(txn, &records, &last_chunks)?;
        Ok(entries)
    }

    /// Marks the commit as being forwarded upstream (`COMMITTING`).
    pub fn committing(&self, txn: TxnId) -> Result<(), TxnError> {
        self.append(txn, TxnState::Committing, b"{}")
    }

    /// Records a terminal state and the response given, and prunes transactions past the
    /// retention period if that has not been done for a minute.
    pub fn finish(&self, txn: TxnId, state: TxnState, decision: Decision) -> Result<(), TxnError> {
        if !is_terminal(state) {
            return Err(TxnError::IllegalTransition {
                from: None,
                to: state,
            });
        }
        let now = (self.options.clock)();
        let payload = serde_json::to_vec(&Terminal {
            status: decision.status,
            body: decision.body,
            finished_at: Some(now),
        })
        .map_err(|e| TxnError::Storage(e.to_string()))?;
        self.append(txn, state, &payload)?;
        let due = {
            let mut index = self.index.lock().map_err(poisoned)?;
            if let Some(t) = index.txns.get_mut(&txn.0) {
                t.finished_at = Some(now);
            }
            now.saturating_sub(index.last_prune) >= PRUNE_EVERY_MS
        };
        if due {
            // The terminal record is durable; a pruning that fails is retried by a later one.
            let _ = self.prune(now);
        }
        Ok(())
    }

    /// The current state of a transaction.
    pub fn state(&self, txn: TxnId) -> Option<TxnState> {
        self.index.lock().ok()?.txns.get(&txn.0)?.state
    }

    /// The id the next transaction will get: every id below it was assigned by this log.
    pub fn next_txn_id(&self) -> u64 {
        self.index.lock().map_or(u64::MAX, |i| i.next_txn)
    }

    /// Everything recorded about a transaction, unless it was pruned.
    pub fn summary(&self, txn: TxnId) -> Option<TxnSummary> {
        let t = self.entry(txn).ok()??;
        self.load(&t).ok()
    }

    /// The recorded decision of a finished transaction.
    pub fn decision(&self, txn: TxnId) -> Option<Decision> {
        self.summary(txn)?.decision
    }

    /// Whether a transaction with this request id was started (finished or not) and not pruned.
    pub fn knows_request(&self, request_id: &str) -> bool {
        self.index
            .lock()
            .is_ok_and(|i| i.by_request.contains_key(request_id))
    }

    /// The recorded decision for a request id, if its transaction finished and was not pruned.
    /// Fails if the log cannot be read: the caller must not execute the request again then.
    pub fn decision_for(&self, request_id: &str) -> Result<Option<Decision>, TxnError> {
        let t = {
            let index = self.index.lock().map_err(poisoned)?;
            match index
                .by_request
                .get(request_id)
                .and_then(|id| index.txns.get(id))
            {
                Some(t) if t.state.is_some_and(is_terminal) => t.clone(),
                _ => return Ok(None),
            }
        };
        Ok(self.load(&t)?.decision)
    }

    /// Transactions recovery must resolve, oldest first.
    pub fn unresolved(&self) -> Result<Vec<Unresolved>, TxnError> {
        let pending: Vec<(u64, Txn)> = {
            let index = self.index.lock().map_err(poisoned)?;
            index
                .open
                .iter()
                .filter_map(|id| index.txns.get(id).map(|t| (*id, t.clone())))
                .collect()
        };
        let mut out = Vec::new();
        for (id, t) in pending {
            let (summary, entries) = self.load_with_entries(&t)?;
            let staged = match entries {
                Some(entries) => entries,
                None => self
                    .legacy_staged(id)?
                    .iter()
                    .map(|(c, d)| StagedEntry::of(*c, &d.header(), 1, true))
                    .collect(),
            };
            if summary.validated.is_none() && !staged.is_empty() {
                return Err(TxnError::Corrupt);
            }
            out.push(Unresolved {
                txn: TxnId(id),
                state: summary.state,
                prepared: summary.prepared,
                validated: summary.validated,
                staged,
            });
        }
        Ok(out)
    }

    /// The staged deltas of a transaction written before RFC 0006 (`oip/txn-staged/v1`); none if
    /// it has no `VALIDATED` record, corruption if it has one without them.
    fn legacy_staged(&self, txn: u64) -> Result<Vec<(ConstraintId, StagedDelta)>, TxnError> {
        let validated = {
            let index = self.index.lock().map_err(poisoned)?;
            index
                .txns
                .get(&txn)
                .and_then(|t| t.state)
                .is_some_and(|s| !matches!(s, TxnState::Prepared))
        };
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(STAGED).map_err(storage)?;
        match table.get(txn).map_err(storage)? {
            Some(bytes) => decode_staged(bytes.value()),
            None if validated => Err(TxnError::Corrupt),
            None => Ok(Vec::new()),
        }
    }

    /// The writes of one staged delta of an unresolved transaction, read from the log one chunk
    /// at a time, for [`integrity_index::PersistentStore::apply_streamed`] (which checks them
    /// against [`StagedEntry::header`]).
    pub fn staged_writes(
        &self,
        txn: TxnId,
        entry: &StagedEntry,
    ) -> Result<Writes<'static>, TxnError> {
        if entry.legacy {
            let delta = self
                .legacy_staged(txn.0)?
                .into_iter()
                .find(|(c, _)| c.0 == entry.constraint)
                .map(|(_, d)| d)
                .ok_or(TxnError::Corrupt)?;
            let writes: Vec<_> = delta
                .writes()
                .map(|(k, v)| Ok((k.clone(), v.copied())))
                .collect();
            return Ok(Box::new(writes.into_iter()));
        }
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(STAGED_V2).map_err(storage)?;
        Ok(Box::new(ChunkReader {
            table,
            txn: txn.0,
            entry: entry.clone(),
            next_chunk: 0,
            current: Vec::new().into_iter(),
            failed: false,
        }))
    }

    /// Raw records in log order (verification and tests).
    pub fn records(&self) -> Result<Vec<(u64, Vec<u8>)>, TxnError> {
        let read = self.db.begin_read().map_err(storage)?;
        let log = read.open_table(LOG).map_err(storage)?;
        let mut out = Vec::new();
        for item in log.iter().map_err(storage)? {
            let (seq, record) = item.map_err(storage)?;
            out.push((seq.value(), record.value().to_vec()));
        }
        Ok(out)
    }
}

/// Deletes every staged chunk of `txn`.
fn remove_chunks(
    table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    txn: u64,
) -> Result<(), TxnError> {
    let (from, to) = chunk_range(txn);
    let keys: Vec<Vec<u8>> = table
        .range(from.as_slice()..=to.as_slice())
        .map_err(storage)?
        .map(|item| item.map(|(k, _)| k.value().to_vec()))
        .collect::<Result<_, _>>()
        .map_err(storage)?;
    for key in keys {
        table.remove(key.as_slice()).map_err(storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use integrity_core::{KeyDelta, KeySchema, KeyValue, TypeFamily};
    use integrity_index::{IndexDelta, IndexKind, KeyIndex, MemoryIndex};
    use integrity_types::SnapshotId;

    #[test]
    fn an_unresolved_transaction_written_before_rfc_0006_still_replays() {
        let dir = std::env::temp_dir().join(format!("oip-legacy-staged-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("txn.redb");
        let schema = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
        let mut d = KeyDelta::default();
        for v in [3, 1, 2] {
            let key = EncodedKey::encode(&schema, &[Some(KeyValue::Integer(v))]);
            d.added.insert(key.unwrap()).unwrap();
        }
        let delta = IndexDelta {
            snapshot: SnapshotId(31),
            changes: d.net(),
        };
        let staged = vec![(
            ConstraintId(4),
            MemoryIndex::new(IndexKind::Unique).stage(&delta).unwrap(),
        )];
        let validated = Validated {
            base_snapshot: None,
            snapshots: vec![31],
            final_snapshot: 31,
            constraint_set_version: 1,
            epoch: 7,
            certificates: vec!["00".repeat(32)],
        };
        // What 0.0.2 wrote: v1 records and the staged deltas in oip/txn-staged/v1.
        {
            let db = Database::create(&path).unwrap();
            let w = db.begin_write().unwrap();
            {
                let mut log = w.open_table(LOG).unwrap();
                let txn = TxnId(1);
                let p = Prepared {
                    request_id: None,
                    table: "t".into(),
                    identifier: "db.t".into(),
                    load_path: "/t".into(),
                };
                let records = [
                    (TxnState::Prepared, serde_json::to_vec(&p).unwrap()),
                    (TxnState::Validated, serde_json::to_vec(&validated).unwrap()),
                    (TxnState::Committing, b"{}".to_vec()),
                ];
                for (seq, (state, payload)) in records.iter().enumerate() {
                    let record = encode_record(*state, txn, payload);
                    log.insert(seq as u64, record.as_slice()).unwrap();
                }
                w.open_table(STAGED)
                    .unwrap()
                    .insert(1, encode_staged(&staged).as_slice())
                    .unwrap();
            }
            w.commit().unwrap();
        }
        let log = TxnLog::open(&path).unwrap();
        let u = log.unresolved().unwrap();
        assert_eq!(u.len(), 1);
        assert_eq!(u[0].validated, Some(validated));
        assert_eq!(u[0].staged.len(), 1);
        let entry = &u[0].staged[0];
        assert_eq!(entry.constraint_id(), ConstraintId(4));
        assert_eq!(entry.header().ok(), Some(staged[0].1.header()));
        let writes: Vec<_> = log
            .staged_writes(u[0].txn, entry)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let expected: Vec<_> = staged[0]
            .1
            .writes()
            .map(|(k, v)| (k.clone(), v.copied()))
            .collect();
        assert_eq!(writes, expected);
        // Finishing it deletes the v1 staged value.
        log.finish(
            u[0].txn,
            TxnState::Committed,
            Decision {
                status: 200,
                body: Value::Null,
            },
        )
        .unwrap();
        drop(log);
        let db = Database::create(&path).unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_table(STAGED).unwrap();
        assert!(table.get(1).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
