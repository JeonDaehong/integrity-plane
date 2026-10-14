//! Staged deltas in chunks (RFC 0006): any chunk size reads back the same writes, a chunk that is
//! tampered with or missing is corruption, and writes that do not add up to their header are
//! never validated.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use integrity_core::{EncodedKey, KeyDelta, KeySchema, KeyValue, TypeFamily};
use integrity_index::{
    IndexDelta, IndexError, IndexKind, IndexValue, KeyIndex, MemoryIndex, StagedDelta,
};
use integrity_txn::log::{LogOptions, sources};
use integrity_txn::{Decision, Prepared, TxnError, TxnLog, TxnState, Validated};
use integrity_types::{ConstraintId, SnapshotId};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde_json::json;

const STAGED_V2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("oip/txn-staged/v2");

struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "oip-chunks-{}-{nanos}-{}",
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

fn open(dir: &Dir, chunk_writes: usize) -> TxnLog {
    TxnLog::open_with(
        dir.log(),
        LogOptions {
            chunk_writes,
            ..LogOptions::default()
        },
    )
    .unwrap()
}

fn prepared() -> Prepared {
    Prepared {
        request_id: None,
        table: "6c1b2d42-0000-4000-8000-000000000001".into(),
        identifier: "db.orders".into(),
        load_path: "/v1/namespaces/db/tables/orders".into(),
    }
}

fn validated() -> Validated {
    Validated {
        base_snapshot: None,
        snapshots: vec![31],
        final_snapshot: 31,
        constraint_set_version: 1,
        epoch: 7,
        certificates: vec!["00".repeat(32)],
    }
}

fn k(v: i64) -> EncodedKey {
    let s = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
    EncodedKey::encode(&s, &[Some(KeyValue::Integer(v))]).unwrap()
}

/// Two staged deltas: `n` keys added to a unique index, `m` references to another.
fn staged(n: i64, m: i64) -> Vec<(ConstraintId, StagedDelta)> {
    let unique = MemoryIndex::new(IndexKind::Unique);
    let reference = MemoryIndex::new(IndexKind::Reference);
    let mut a = KeyDelta::default();
    for v in 0..n {
        a.added.insert(k(v)).unwrap();
    }
    let mut b = KeyDelta::default();
    for v in 0..m {
        b.added.insert_n(k(v * 3), 2).unwrap();
    }
    let delta = |d: KeyDelta| IndexDelta {
        snapshot: SnapshotId(31),
        changes: d.net(),
    };
    vec![
        (ConstraintId(4), unique.stage(&delta(a)).unwrap()),
        (ConstraintId(9), reference.stage(&delta(b)).unwrap()),
    ]
}

type Writes = Vec<(EncodedKey, Option<IndexValue>)>;

fn writes_of(d: &StagedDelta) -> Writes {
    d.writes().map(|(k, v)| (k.clone(), v.copied())).collect()
}

fn read_back(log: &TxnLog) -> Vec<(ConstraintId, Result<Writes, IndexError>)> {
    let u = log.unresolved().unwrap();
    assert_eq!(u.len(), 1);
    u[0].staged
        .iter()
        .map(|e| {
            let writes = log
                .staged_writes(u[0].txn, e)
                .unwrap()
                .collect::<Result<Writes, _>>();
            (e.constraint_id(), writes)
        })
        .collect()
}

fn chunk_rows(dir: &Dir) -> u64 {
    let db = Database::create(dir.log()).unwrap();
    let read = db.begin_read().unwrap();
    read.open_table(STAGED_V2).unwrap().len().unwrap()
}

#[test]
fn any_chunk_size_reads_back_the_same_writes() {
    let deltas = staged(7, 5);
    for chunk_writes in [1, 2, 3, 6, 7, 8, 100_000] {
        let dir = Dir::new();
        {
            let log = open(&dir, chunk_writes);
            let txn = log.begin(prepared()).unwrap();
            log.validated_and_committing(txn, validated(), &deltas)
                .unwrap();
            let u = log.unresolved().unwrap();
            let expected: Vec<u32> = deltas
                .iter()
                .map(|(_, d)| {
                    let w = d.writes().count();
                    u32::try_from(w.div_ceil(chunk_writes).max(1)).unwrap()
                })
                .collect();
            let chunks: Vec<u32> = u[0].staged.iter().map(|e| e.chunks).collect();
            assert_eq!(chunks, expected, "chunk size {chunk_writes}");
            for (e, (id, d)) in u[0].staged.iter().zip(&deltas) {
                assert_eq!(e.constraint_id(), *id);
                assert_eq!(e.header().unwrap(), d.header());
            }
        }
        // After a restart, with another chunk size: the same writes.
        let log = open(&dir, 1_000);
        for ((id, writes), (expected_id, d)) in read_back(&log).into_iter().zip(&deltas) {
            assert_eq!(id, *expected_id);
            assert_eq!(writes.unwrap(), writes_of(d), "chunk size {chunk_writes}");
        }
        let txn = log.unresolved().unwrap()[0].txn;
        log.finish(
            txn,
            TxnState::Committed,
            Decision {
                status: 200,
                body: json!({}),
            },
        )
        .unwrap();
        drop(log);
        assert_eq!(chunk_rows(&dir), 0, "chunks go with the terminal record");
    }
}

#[test]
fn an_empty_delta_has_one_empty_chunk() {
    let dir = Dir::new();
    let log = open(&dir, 2);
    let txn = log.begin(prepared()).unwrap();
    log.validated_and_committing(txn, validated(), &staged(0, 0))
        .unwrap();
    let u = log.unresolved().unwrap();
    assert!(u[0].staged.iter().all(|e| e.chunks == 1 && e.writes == 0));
    assert!(
        read_back(&log)
            .iter()
            .all(|(_, w)| w.as_ref().unwrap().is_empty())
    );
}

/// Rewrites the stored chunks with `f` (the log closed).
fn tamper(dir: &Dir, f: impl Fn(&mut redb::Table<&[u8], &[u8]>, Vec<(Vec<u8>, Vec<u8>)>)) {
    let db = Database::create(dir.log()).unwrap();
    let w = db.begin_write().unwrap();
    {
        let mut table = w.open_table(STAGED_V2).unwrap();
        let rows: Vec<(Vec<u8>, Vec<u8>)> = table
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value().to_vec(), v.value().to_vec())
            })
            .collect();
        f(&mut table, rows);
    }
    w.commit().unwrap();
}

#[test]
fn tampered_missing_or_moved_chunks_are_corruption() {
    type Change = Box<dyn Fn(&mut redb::Table<&[u8], &[u8]>, Vec<(Vec<u8>, Vec<u8>)>)>;
    let changes: Vec<(&str, Change)> = vec![
        (
            "a flipped byte",
            Box::new(|t, rows| {
                let (k, mut v) = rows[1].clone();
                v[20] ^= 1;
                t.insert(k.as_slice(), v.as_slice()).unwrap();
            }),
        ),
        (
            "a missing chunk",
            Box::new(|t, rows| {
                t.remove(rows[1].0.as_slice()).unwrap();
            }),
        ),
        (
            "two chunks swapped",
            Box::new(|t, rows| {
                t.insert(rows[0].0.as_slice(), rows[1].1.as_slice())
                    .unwrap();
                t.insert(rows[1].0.as_slice(), rows[0].1.as_slice())
                    .unwrap();
            }),
        ),
        (
            "a chunk of another transaction",
            Box::new(|t, rows| {
                let mut k = rows[1].0.clone();
                k[7] ^= 1;
                t.remove(rows[1].0.as_slice()).unwrap();
                t.insert(k.as_slice(), rows[1].1.as_slice()).unwrap();
            }),
        ),
    ];
    for (what, change) in changes {
        let dir = Dir::new();
        {
            let log = open(&dir, 2);
            let txn = log.begin(prepared()).unwrap();
            log.validated_and_committing(txn, validated(), &staged(5, 0))
                .unwrap();
        }
        tamper(&dir, change);
        let log = open(&dir, 2);
        let first = read_back(&log).into_iter().next().unwrap();
        assert_eq!(first.1, Err(IndexError::Corrupt), "{what}");
    }
}

#[test]
fn writes_that_do_not_add_up_to_their_header_are_never_validated() {
    let deltas = staged(5, 0);
    let (id, delta) = &deltas[0];
    let mut wrong: Vec<Writes> = Vec::new();
    let mut short = writes_of(delta);
    short.pop();
    wrong.push(short);
    let mut swapped = writes_of(delta);
    swapped.swap(1, 2);
    wrong.push(swapped);
    let mut changed = writes_of(delta);
    changed[3].1 = None;
    wrong.push(changed);
    for writes in wrong {
        let dir = Dir::new();
        let log = open(&dir, 2);
        let txn = log.begin(prepared()).unwrap();
        let mut source = sources(&deltas[..1]);
        source[0].writes = Box::new(writes.into_iter().map(Ok));
        assert_eq!(
            log.validated_and_committing_streamed(txn, validated(), source),
            Err(TxnError::Corrupt)
        );
        assert_eq!(log.state(txn), Some(TxnState::Prepared));
        // Recovery aborts it; the chunks written before the mismatch go with it.
        log.finish(
            txn,
            TxnState::Aborted,
            Decision {
                status: 500,
                body: json!({}),
            },
        )
        .unwrap();
        drop(log);
        assert_eq!(chunk_rows(&dir), 0);
        let _ = id;
    }
}

#[test]
fn chunks_of_transactions_without_validated_are_deleted_at_open() {
    let dir = Dir::new();
    let txn = {
        let log = open(&dir, 1);
        let txn = log.begin(prepared()).unwrap();
        // A crash after some chunks were written: write all but the last, then fail.
        let deltas = staged(4, 0);
        let mut source = sources(&deltas[..1]);
        let mut writes = writes_of(&deltas[0].1);
        writes.pop();
        source[0].writes = Box::new(writes.into_iter().map(Ok));
        assert!(
            log.validated_and_committing_streamed(txn, validated(), source)
                .is_err()
        );
        txn
    };
    assert!(chunk_rows(&dir) > 0);
    let log = open(&dir, 1);
    assert_eq!(log.state(txn), Some(TxnState::Prepared));
    drop(log);
    assert_eq!(chunk_rows(&dir), 0);
}
