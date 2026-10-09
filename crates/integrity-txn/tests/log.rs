//! Transaction log (RFC 0004): state machine, durability across reopen, recovery inputs,
//! recorded decisions and corruption detection.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use integrity_core::{EncodedKey, KeyDelta, KeySchema, KeyValue, TypeFamily};
use integrity_index::{IndexDelta, IndexKind, KeyIndex, MemoryIndex, StagedDelta};
use integrity_txn::log::{allowed, decode_record, encode_record};
use integrity_txn::{Decision, Prepared, TxnError, TxnId, TxnLog, TxnState, Validated};
use integrity_types::{ConstraintId, SnapshotId};
use serde_json::json;

struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "oip-txn-{}-{nanos}-{}",
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
        snapshots: vec![30, 31],
        final_snapshot: 31,
        constraint_set_version: 1,
        epoch: 7,
        certificates: vec!["00".repeat(32), "11".repeat(32)],
    }
}

fn staged() -> Vec<(ConstraintId, StagedDelta)> {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    let k = |v| EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap();
    let index = MemoryIndex::new(IndexKind::Unique);
    let mut d = KeyDelta::default();
    d.added.insert(k(1)).unwrap();
    d.added.insert(k(2)).unwrap();
    let delta = IndexDelta {
        snapshot: SnapshotId(31),
        changes: d.net(),
    };
    vec![(ConstraintId(1), index.stage(&delta).unwrap())]
}

fn ok(status: u16) -> Decision {
    Decision {
        status,
        body: json!({"metadata": {"x": 1}}),
    }
}

#[test]
fn state_machine_allows_exactly_the_rfc_transitions() {
    use TxnState::*;
    let states = [
        Prepared, Validated, Committing, Committed, Aborted, Rejected,
    ];
    let mut allowed_pairs = Vec::new();
    for to in states {
        if allowed(None, to) {
            allowed_pairs.push((None, to));
        }
        for from in states {
            if allowed(Some(from), to) {
                allowed_pairs.push((Some(from), to));
            }
        }
    }
    assert_eq!(
        allowed_pairs,
        vec![
            (None, Prepared),
            (Some(Prepared), Validated),
            (Some(Validated), Committing),
            (Some(Committing), Committed),
            (Some(Prepared), Aborted),
            (Some(Validated), Aborted),
            (Some(Committing), Aborted),
            (Some(Prepared), Rejected),
        ]
    );
}

#[test]
fn a_commit_survives_reopen_with_its_decision() {
    let dir = Dir::new();
    let txn = {
        let log = TxnLog::open(dir.log()).unwrap();
        let txn = log.begin(prepared(Some("req-1"))).unwrap();
        log.validated(txn, validated(), &staged()).unwrap();
        log.committing(txn).unwrap();
        log.finish(txn, TxnState::Committed, ok(200)).unwrap();
        txn
    };
    let log = TxnLog::open(dir.log()).unwrap();
    assert_eq!(log.state(txn), Some(TxnState::Committed));
    assert_eq!(log.decision_for("req-1"), Some(ok(200)));
    assert_eq!(log.decision_for("other"), None);
    assert!(log.unresolved().unwrap().is_empty());
    // Ids keep increasing after reopen.
    let next = log.begin(prepared(None)).unwrap();
    assert!(next > txn);
}

#[test]
fn unresolved_transactions_carry_what_recovery_needs() {
    let dir = Dir::new();
    let (a, b, c) = {
        let log = TxnLog::open(dir.log()).unwrap();
        let a = log.begin(prepared(Some("a"))).unwrap();
        log.finish(a, TxnState::Aborted, ok(409)).unwrap(); // resolved: not reported
        let b = log.begin(prepared(Some("b"))).unwrap();
        log.validated(b, validated(), &staged()).unwrap(); // crashed before forwarding
        let c = log.begin(prepared(Some("c"))).unwrap();
        log.validated(c, validated(), &staged()).unwrap();
        log.committing(c).unwrap(); // crashed with the outcome unknown
        (a, b, c)
    };
    let log = TxnLog::open(dir.log()).unwrap();
    let unresolved = log.unresolved().unwrap();
    let states: Vec<_> = unresolved.iter().map(|u| (u.txn, u.state)).collect();
    assert_eq!(
        states,
        vec![(b, TxnState::Validated), (c, TxnState::Committing)]
    );
    assert_eq!(log.state(a), Some(TxnState::Aborted));
    for u in &unresolved {
        assert_eq!(u.validated, Some(validated()));
        assert_eq!(u.staged, staged(), "staged deltas replay identically");
        assert_eq!(u.prepared.load_path, "/v1/namespaces/db/tables/orders");
    }
    // A decision only exists once a transaction finished.
    assert_eq!(log.decision_for("b"), None);
}

#[test]
fn prepared_only_transactions_are_unresolved_too() {
    let dir = Dir::new();
    let txn = TxnLog::open(dir.log())
        .unwrap()
        .begin(prepared(None))
        .unwrap();
    let u = TxnLog::open(dir.log()).unwrap().unresolved().unwrap();
    assert_eq!(u.len(), 1);
    assert_eq!((u[0].txn, u[0].state), (txn, TxnState::Prepared));
    assert!(u[0].validated.is_none() && u[0].staged.is_empty());
}

#[test]
fn illegal_transitions_are_refused() {
    let dir = Dir::new();
    let log = TxnLog::open(dir.log()).unwrap();
    let txn = log.begin(prepared(None)).unwrap();
    assert!(matches!(
        log.committing(txn),
        Err(TxnError::IllegalTransition { .. })
    ));
    log.finish(txn, TxnState::Rejected, ok(400)).unwrap();
    assert!(matches!(
        log.finish(txn, TxnState::Committed, ok(200)),
        Err(TxnError::IllegalTransition { .. })
    ));
    assert!(matches!(
        log.validated(TxnId(999), validated(), &[]),
        Err(TxnError::IllegalTransition { .. })
    ));
    assert!(matches!(
        log.finish(txn, TxnState::Validated, ok(200)),
        Err(TxnError::IllegalTransition { .. })
    ));
}

#[test]
fn every_flipped_byte_of_a_record_is_detected() {
    let record = encode_record(
        TxnState::Committed,
        TxnId(42),
        br#"{"status":200,"body":null}"#,
    );
    let (state, txn, payload) = decode_record(&record).unwrap();
    assert_eq!((state, txn), (TxnState::Committed, TxnId(42)));
    assert_eq!(payload, br#"{"status":200,"body":null}"#);
    for i in 0..record.len() {
        for bit in [0x01u8, 0x80] {
            let mut bad = record.clone();
            bad[i] ^= bit;
            assert_eq!(
                decode_record(&bad),
                Err(TxnError::Corrupt),
                "byte {i} bit {bit:#x}"
            );
        }
    }
    assert_eq!(
        decode_record(&record[..record.len() - 1]),
        Err(TxnError::Corrupt)
    );
}

#[test]
fn a_tampered_log_refuses_to_open() {
    let dir = Dir::new();
    {
        let log = TxnLog::open(dir.log()).unwrap();
        let txn = log.begin(prepared(Some("x"))).unwrap();
        log.finish(txn, TxnState::Rejected, ok(400)).unwrap();
    }
    // Rewrite the second record with a valid redb write but a wrong checksum.
    {
        let db = redb::Database::create(dir.log()).unwrap();
        let def: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("oip/txn-log/v1");
        let w = db.begin_write().unwrap();
        {
            let mut t = w.open_table(def).unwrap();
            let mut record = encode_record(
                TxnState::Rejected,
                TxnId(1),
                br#"{"status":400,"body":null}"#,
            );
            let last = record.len() - 1;
            record[last] ^= 1;
            t.insert(1u64, record.as_slice()).unwrap();
        }
        w.commit().unwrap();
    }
    assert_eq!(TxnLog::open(dir.log()).err(), Some(TxnError::Corrupt));
}
