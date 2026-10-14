//! Transaction log retention (RFC 0006): staged deltas go with the terminal record, terminal
//! transactions are pruned after the retention period, ids are never reused.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use integrity_core::{EncodedKey, KeyDelta, KeySchema, KeyValue, TypeFamily};
use integrity_index::{IndexDelta, IndexKind, KeyIndex, MemoryIndex, StagedDelta};
use integrity_txn::log::{LogOptions, encode_record};
use integrity_txn::{Decision, Prepared, TxnId, TxnLog, TxnState, Validated};
use integrity_types::{ConstraintId, SnapshotId};
use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use serde_json::json;

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/txn-log/v1");
const STAGED: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/txn-staged/v1");

const HOUR: u64 = 3_600_000;

struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "oip-retention-{}-{nanos}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn log(&self) -> PathBuf {
        self.0.join("txn.redb")
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A clock the test moves by hand (Unix milliseconds).
#[derive(Clone)]
struct Clock(Arc<AtomicU64>);

impl Clock {
    fn at(ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(ms)))
    }
    fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
    fn options(&self, retention: Duration) -> LogOptions {
        let now = Arc::clone(&self.0);
        LogOptions {
            retention,
            clock: Arc::new(move || now.load(Ordering::SeqCst)),
        }
    }
}

fn open(dir: &Dir, clock: &Clock) -> TxnLog {
    TxnLog::open_with(dir.log(), clock.options(Duration::from_secs(24 * 3600))).unwrap()
}

fn prepared(request: Option<&str>) -> Prepared {
    Prepared {
        request_id: request.map(str::to_owned),
        table: "6c1b2d42-0000-4000-8000-000000000001".into(),
        identifier: "db.orders".into(),
        load_path: "/v1/namespaces/db/tables/orders".into(),
    }
}

fn validated() -> Validated {
    Validated {
        base_snapshot: Some(20),
        snapshots: vec![31],
        final_snapshot: 31,
        constraint_set_version: 1,
        epoch: 7,
        certificates: vec!["00".repeat(32)],
    }
}

fn staged() -> Vec<(ConstraintId, StagedDelta)> {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    let k = |v| EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap();
    let index = MemoryIndex::new(IndexKind::Unique);
    let mut d = KeyDelta::default();
    d.added.insert(k(1)).unwrap();
    let delta = IndexDelta {
        snapshot: SnapshotId(31),
        changes: d.net(),
    };
    vec![(ConstraintId(1), index.stage(&delta).unwrap())]
}

fn ok() -> Decision {
    Decision {
        status: 200,
        body: json!({"metadata": {"x": 1}}),
    }
}

/// A committed transaction with request id `request`.
fn commit(log: &TxnLog, request: &str) -> TxnId {
    let txn = log.begin(prepared(Some(request))).unwrap();
    log.validated_and_committing(txn, validated(), &staged())
        .unwrap();
    log.finish(txn, TxnState::Committed, ok()).unwrap();
    txn
}

/// Rows of the log and staged tables, read with the log closed.
fn rows(dir: &Dir) -> (u64, u64) {
    let db = Database::create(dir.log()).unwrap();
    let read = db.begin_read().unwrap();
    let log = read.open_table(LOG).unwrap().len().unwrap();
    let staged = read.open_table(STAGED).unwrap().len().unwrap();
    (log, staged)
}

#[test]
fn staged_deltas_go_with_the_terminal_record() {
    let dir = Dir::new();
    let clock = Clock::at(100 * HOUR);
    {
        let log = open(&dir, &clock);
        let committed = commit(&log, "a");
        let open_txn = log.begin(prepared(None)).unwrap();
        log.validated_and_committing(open_txn, validated(), &staged())
            .unwrap();
        let aborted = log.begin(prepared(None)).unwrap();
        log.validated(aborted, validated(), &staged()).unwrap();
        log.finish(aborted, TxnState::Aborted, ok()).unwrap();
        assert_eq!(log.decision(committed), Some(ok()));
    }
    // Records: prepared, validated, committing, committed; prepared, validated, committing;
    // prepared, validated, aborted. Only the open transaction keeps its staged deltas, which
    // recovery still needs.
    assert_eq!(rows(&dir), (4 + 3 + 3, 1));
    let log = open(&dir, &clock);
    let u = log.unresolved().unwrap();
    assert_eq!(u.len(), 1);
    assert_eq!(u[0].staged, staged());
}

#[test]
fn terminal_transactions_are_pruned_after_the_retention_period() {
    let dir = Dir::new();
    let clock = Clock::at(100 * HOUR);
    let (old, open_txn) = {
        let log = open(&dir, &clock);
        let old = commit(&log, "old");
        let open_txn = log.begin(prepared(Some("open"))).unwrap();
        (old, open_txn)
    };
    // One hour short of the retention period: everything is kept.
    clock.set(123 * HOUR);
    let log = open(&dir, &clock);
    assert_eq!(log.decision_for("old"), Ok(Some(ok())));
    assert!(log.summary(old).is_some());
    drop(log);

    // Past it: the old transaction is gone, the unresolved one is not.
    clock.set(125 * HOUR);
    let log = open(&dir, &clock);
    assert_eq!(log.summary(old), None);
    assert_eq!(log.state(old), None);
    assert_eq!(log.decision_for("old"), Ok(None));
    assert!(!log.knows_request("old"));
    assert!(log.knows_request("open"));
    assert_eq!(log.unresolved().unwrap().len(), 1);
    assert_eq!(log.unresolved().unwrap()[0].txn, open_txn);
    drop(log);
    assert_eq!(rows(&dir), (1, 0));
}

#[test]
fn a_running_log_prunes_after_a_later_terminal_record() {
    let dir = Dir::new();
    let clock = Clock::at(100 * HOUR);
    let log = open(&dir, &clock);
    let first = commit(&log, "first");
    clock.set(125 * HOUR);
    let second = commit(&log, "second");
    assert_eq!(log.summary(first), None);
    assert!(!log.knows_request("first"));
    assert_eq!(log.decision_for("second"), Ok(Some(ok())));
    assert_eq!(log.state(second), Some(TxnState::Committed));
}

#[test]
fn pruning_runs_at_most_once_a_minute() {
    let t0 = 100 * HOUR;
    let dir = Dir::new();
    let clock = Clock::at(t0);
    // Opening prunes (at t0).
    let log = open(&dir, &clock);
    // 30 s later: too soon to prune again, nothing to prune anyway.
    clock.set(t0 + 30_000);
    commit(&log, "b");
    // 24 h after the open: prunes, but `b` is 30 s short of the retention period.
    let x = t0 + 24 * HOUR;
    clock.set(x);
    commit(&log, "c");
    assert!(log.knows_request("b"));
    // 40 s later `b` is past it, but the last pruning was 40 s ago.
    clock.set(x + 40_000);
    commit(&log, "d");
    assert!(log.knows_request("b"));
    // A minute after the last pruning, the next terminal record prunes again.
    clock.set(x + 61_000);
    commit(&log, "e");
    assert!(!log.knows_request("b"));
    assert!(log.knows_request("c"));
}

#[test]
fn ids_are_not_reused_after_the_newest_transactions_are_pruned() {
    let dir = Dir::new();
    let clock = Clock::at(100 * HOUR);
    let last = {
        let log = open(&dir, &clock);
        commit(&log, "a");
        commit(&log, "b")
    };
    clock.set(200 * HOUR);
    let log = open(&dir, &clock);
    assert_eq!(log.summary(last), None);
    assert_eq!(log.next_txn_id(), last.0 + 1);
    let next = log.begin(prepared(None)).unwrap();
    assert_eq!(next, TxnId(last.0 + 1));
    drop(log);
    assert_eq!(rows(&dir).0, 1);
}

#[test]
fn a_log_written_before_retention_opens_and_ages_from_the_upgrade() {
    // A 0.0.2 log: terminal records without `finished_at`, staged deltas of finished
    // transactions still present.
    let dir = Dir::new();
    {
        let db = Database::create(dir.log()).unwrap();
        let w = db.begin_write().unwrap();
        {
            let mut log = w.open_table(LOG).unwrap();
            let mut staged_table = w.open_table(STAGED).unwrap();
            let txn = TxnId(1);
            let p = serde_json::to_vec(&prepared(Some("legacy"))).unwrap();
            let v = serde_json::to_vec(&validated()).unwrap();
            let d = serde_json::to_vec(&ok()).unwrap();
            log.insert(0, encode_record(TxnState::Prepared, txn, &p).as_slice())
                .unwrap();
            log.insert(1, encode_record(TxnState::Validated, txn, &v).as_slice())
                .unwrap();
            log.insert(
                2,
                encode_record(TxnState::Committing, txn, b"{}").as_slice(),
            )
            .unwrap();
            log.insert(3, encode_record(TxnState::Committed, txn, &d).as_slice())
                .unwrap();
            staged_table.insert(1, b"leftover".as_slice()).unwrap();
        }
        w.commit().unwrap();
    }
    let clock = Clock::at(500 * HOUR);
    {
        let log = open(&dir, &clock);
        assert_eq!(log.decision_for("legacy"), Ok(Some(ok())));
        assert_eq!(log.state(TxnId(1)), Some(TxnState::Committed));
    }
    // The leftover staged value of the finished transaction is gone at the first open.
    assert_eq!(rows(&dir), (4, 0));
    // It counts as finished when the log was opened at 500 h.
    clock.set(523 * HOUR);
    assert!(open(&dir, &clock).knows_request("legacy"));
    clock.set(525 * HOUR);
    let log = open(&dir, &clock);
    assert!(!log.knows_request("legacy"));
    assert_eq!(log.next_txn_id(), 2);
}
