//! The durable transaction log (spec §16, RFC 0004).

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Mutex;

use integrity_index::StagedDelta;
use integrity_types::ConstraintId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{TxnError, TxnId, TxnState};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/txn-log/v1");
const STAGED: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/txn-staged/v1");
const VERSION: u8 = 1;

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
    /// Staged index deltas, if validated.
    pub staged: Vec<(ConstraintId, StagedDelta)>,
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

#[derive(Debug, Clone, Default)]
struct Txn {
    state: Option<TxnState>,
    prepared: Option<Prepared>,
    validated: Option<Validated>,
    decision: Option<Decision>,
}

#[derive(Debug, Default)]
struct Index {
    next_seq: u64,
    next_txn: u64,
    txns: BTreeMap<u64, Txn>,
    by_request: BTreeMap<String, u64>,
}

/// The transaction log. All writes are durable two-phase commits.
#[derive(Debug)]
pub struct TxnLog {
    db: Database,
    index: Mutex<Index>,
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
    /// Opens (or creates) the log, verifying every record.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TxnError> {
        let path = path.as_ref();
        let db = catch_unwind(AssertUnwindSafe(|| Database::create(path)))
            .map_err(|_| TxnError::Corrupt)?
            .map_err(storage)?;
        let mut index = Index {
            next_txn: 1,
            ..Index::default()
        };
        let txn = db.begin_write().map_err(storage)?;
        {
            let log = txn.open_table(LOG).map_err(storage)?;
            txn.open_table(STAGED).map_err(storage)?;
            for item in log.iter().map_err(storage)? {
                let (seq, record) = item.map_err(storage)?;
                let (state, id, payload) = decode_record(record.value())?;
                index.next_seq = seq.value() + 1;
                index.next_txn = index.next_txn.max(id.0 + 1);
                let t = index.txns.entry(id.0).or_default();
                if !allowed(t.state, state) {
                    return Err(TxnError::Corrupt);
                }
                t.state = Some(state);
                match state {
                    TxnState::Prepared => {
                        let p: Prepared =
                            serde_json::from_slice(&payload).map_err(|_| TxnError::Corrupt)?;
                        if let Some(r) = &p.request_id {
                            index.by_request.insert(r.clone(), id.0);
                        }
                        t.prepared = Some(p);
                    }
                    TxnState::Validated => {
                        t.validated =
                            Some(serde_json::from_slice(&payload).map_err(|_| TxnError::Corrupt)?);
                    }
                    TxnState::Committing => {}
                    TxnState::Committed | TxnState::Aborted | TxnState::Rejected => {
                        t.decision =
                            Some(serde_json::from_slice(&payload).map_err(|_| TxnError::Corrupt)?);
                    }
                }
            }
        }
        txn.commit().map_err(storage)?;
        Ok(Self {
            db,
            index: Mutex::new(index),
        })
    }

    fn append(
        &self,
        txn: TxnId,
        state: TxnState,
        payload: &[u8],
        staged: Option<&[(ConstraintId, StagedDelta)]>,
    ) -> Result<(), TxnError> {
        let mut index = self
            .index
            .lock()
            .map_err(|_| TxnError::Storage("log lock poisoned".into()))?;
        let current = index.txns.get(&txn.0).and_then(|t| t.state);
        if !allowed(current, state) {
            return Err(TxnError::IllegalTransition {
                from: current,
                to: state,
            });
        }
        let seq = index.next_seq;
        let record = encode_record(state, txn, payload);
        let mut w = self.db.begin_write().map_err(storage)?;
        w.set_two_phase_commit(true);
        {
            let mut log = w.open_table(LOG).map_err(storage)?;
            log.insert(seq, record.as_slice()).map_err(storage)?;
            if let Some(staged) = staged {
                let mut table = w.open_table(STAGED).map_err(storage)?;
                table
                    .insert(txn.0, encode_staged(staged).as_slice())
                    .map_err(storage)?;
            }
        }
        w.commit().map_err(storage)?;
        index.next_seq = seq + 1;
        index.txns.entry(txn.0).or_default().state = Some(state);
        Ok(())
    }

    /// Starts a transaction (`PREPARED`).
    pub fn begin(&self, prepared: Prepared) -> Result<TxnId, TxnError> {
        let id = {
            let mut index = self
                .index
                .lock()
                .map_err(|_| TxnError::Storage("log lock poisoned".into()))?;
            let id = index.next_txn;
            index.next_txn += 1;
            TxnId(id)
        };
        let payload =
            serde_json::to_vec(&prepared).map_err(|e| TxnError::Storage(e.to_string()))?;
        self.append(id, TxnState::Prepared, &payload, None)?;
        let mut index = self
            .index
            .lock()
            .map_err(|_| TxnError::Storage("log lock poisoned".into()))?;
        if let Some(r) = &prepared.request_id {
            index.by_request.insert(r.clone(), id.0);
        }
        index.txns.entry(id.0).or_default().prepared = Some(prepared);
        Ok(id)
    }

    /// Records a validated commit and its staged deltas atomically (`VALIDATED`).
    pub fn validated(
        &self,
        txn: TxnId,
        validated: Validated,
        staged: &[(ConstraintId, StagedDelta)],
    ) -> Result<(), TxnError> {
        let payload =
            serde_json::to_vec(&validated).map_err(|e| TxnError::Storage(e.to_string()))?;
        self.append(txn, TxnState::Validated, &payload, Some(staged))?;
        if let Ok(mut index) = self.index.lock() {
            index.txns.entry(txn.0).or_default().validated = Some(validated);
        }
        Ok(())
    }

    /// Marks the commit as being forwarded upstream (`COMMITTING`).
    pub fn committing(&self, txn: TxnId) -> Result<(), TxnError> {
        self.append(txn, TxnState::Committing, b"{}", None)
    }

    /// Records a terminal state and the response given.
    pub fn finish(&self, txn: TxnId, state: TxnState, decision: Decision) -> Result<(), TxnError> {
        if !matches!(
            state,
            TxnState::Committed | TxnState::Aborted | TxnState::Rejected
        ) {
            return Err(TxnError::IllegalTransition {
                from: None,
                to: state,
            });
        }
        let payload =
            serde_json::to_vec(&decision).map_err(|e| TxnError::Storage(e.to_string()))?;
        self.append(txn, state, &payload, None)?;
        if let Ok(mut index) = self.index.lock() {
            index.txns.entry(txn.0).or_default().decision = Some(decision);
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

    /// Everything recorded about a transaction.
    pub fn summary(&self, txn: TxnId) -> Option<TxnSummary> {
        let index = self.index.lock().ok()?;
        let t = index.txns.get(&txn.0)?;
        Some(TxnSummary {
            state: t.state?,
            prepared: t.prepared.clone()?,
            validated: t.validated.clone(),
            decision: t.decision.clone(),
        })
    }

    /// The recorded decision of a finished transaction.
    pub fn decision(&self, txn: TxnId) -> Option<Decision> {
        self.index.lock().ok()?.txns.get(&txn.0)?.decision.clone()
    }

    /// The recorded decision for a request id, if its transaction finished.
    pub fn decision_for(&self, request_id: &str) -> Option<Decision> {
        let index = self.index.lock().ok()?;
        let t = index.txns.get(index.by_request.get(request_id)?)?;
        t.decision.clone()
    }

    /// Transactions recovery must resolve, oldest first.
    pub fn unresolved(&self) -> Result<Vec<Unresolved>, TxnError> {
        let pending: Vec<(u64, Txn)> = {
            let index = self
                .index
                .lock()
                .map_err(|_| TxnError::Storage("log lock poisoned".into()))?;
            index
                .txns
                .iter()
                .filter(|(_, t)| {
                    matches!(
                        t.state,
                        Some(TxnState::Prepared | TxnState::Validated | TxnState::Committing)
                    )
                })
                .map(|(id, t)| (*id, t.clone()))
                .collect()
        };
        let read = self.db.begin_read().map_err(storage)?;
        let staged_table = read.open_table(STAGED).map_err(storage)?;
        let mut out = Vec::new();
        for (id, t) in pending {
            let staged = match staged_table.get(id).map_err(storage)? {
                Some(bytes) => decode_staged(bytes.value())?,
                None if t.validated.is_some() => return Err(TxnError::Corrupt),
                None => Vec::new(),
            };
            out.push(Unresolved {
                txn: TxnId(id),
                state: t.state.ok_or(TxnError::Corrupt)?,
                prepared: t.prepared.ok_or(TxnError::Corrupt)?,
                validated: t.validated,
                staged,
            });
        }
        Ok(out)
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
