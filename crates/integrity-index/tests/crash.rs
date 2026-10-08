//! Phase 4 exit criterion: crash tests on `apply`.
//!
//! The parent test repeatedly starts a child process (this same test binary, running the ignored
//! `crash_child` test) that applies a deterministic sequence of deltas to a persistent index and
//! acknowledges each apply on stdout, then kills it (`TerminateProcess` / `SIGKILL`) at an
//! arbitrary moment. After every kill the store must reopen cleanly and hold exactly the state
//! after some prefix of the sequence: never a partial apply, and never less than what was
//! acknowledged.
//!
//! A process kill keeps the OS page cache, so this proves atomicity and recovery of the commit
//! protocol, not survival of power loss.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use integrity_index::{IndexDelta, IndexEpoch, IndexKind, IndexValue, KeyIndex, PersistentStore};
use integrity_types::ConstraintId;

const DB_ENV: &str = "OIP_CRASH_CHILD_DB";
const KEYS_PER_STEP: i64 = 20;

/// Step `i` (1-based) adds one reference to 20 keys and removes one from the first 5 keys
/// added by step `i - 1`, so counts never go negative and removals are exercised.
fn step_changes(i: i64) -> Vec<(i64, i64)> {
    let keys = |s: i64| (0..KEYS_PER_STEP).map(move |j| (s * 7 + j * 13) % 101);
    let mut changes: Vec<(i64, i64)> = keys(i).map(|key| (key, 1)).collect();
    if i > 1 {
        changes.extend(keys(i - 1).take(5).map(|key| (key, -1)));
    }
    changes
}

fn step_delta(i: i64) -> IndexDelta {
    delta(i, &step_changes(i))
}

/// The index contents after steps `1..=n`.
fn expected_after(n: u64) -> Vec<(Vec<u8>, u64)> {
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    for i in 1..=n as i64 {
        for (key, c) in step_changes(i) {
            *counts.entry(key).or_insert(0) += c;
        }
    }
    let mut out: Vec<(Vec<u8>, u64)> = counts
        .into_iter()
        .filter(|&(_, c)| c > 0)
        .map(|(key, c)| (k(key).as_bytes().to_vec(), c as u64))
        .collect();
    out.sort();
    out
}

fn contents(index: &dyn KeyIndex) -> Vec<(Vec<u8>, u64)> {
    index
        .entries()
        .unwrap()
        .into_iter()
        .map(|(key, v)| match v {
            IndexValue::Reference { child_count } => (key.as_bytes().to_vec(), child_count),
            IndexValue::Unique { .. } => panic!("unexpected unique value"),
        })
        .collect()
}

/// Child side: apply steps forever, acknowledging each one after `apply` returns.
#[test]
#[ignore = "spawned by kill_during_apply_leaves_an_acknowledged_prefix"]
fn crash_child() {
    let Ok(path) = std::env::var(DB_ENV) else {
        return;
    };
    let store = PersistentStore::open(&path).unwrap();
    let index = store.index(ConstraintId(1), IndexKind::Reference).unwrap();
    let mut i = index.epoch().unwrap().0 as i64 + 1;
    loop {
        let staged = index.stage(&step_delta(i)).unwrap();
        index.apply(staged, IndexEpoch(i as u64)).unwrap();
        println!("applied {i}");
        i += 1;
    }
}

#[test]
fn kill_during_apply_leaves_an_acknowledged_prefix() {
    let dir = TempDir::new("crash");
    let path = dir.path().join("index.redb");
    let exe = std::env::current_exe().unwrap();
    let mut total_kills = 0;

    for round in 0..12u64 {
        let mut child = Command::new(&exe)
            .args([
                "crash_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DB_ENV, &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let acked = Arc::new(Mutex::new(0u64));
        let reader = {
            let acked = Arc::clone(&acked);
            let stdout = child.stdout.take().unwrap();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if let Some(n) = line.strip_prefix("applied ") {
                        *acked.lock().unwrap() = n.trim().parse().unwrap();
                    }
                }
            })
        };

        // Kill at a varying moment, after the child has made some progress.
        let start = std::time::Instant::now();
        let progress_before = *acked.lock().unwrap();
        while *acked.lock().unwrap() <= progress_before && start.elapsed() < Duration::from_secs(20)
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(7 + (round * 37) % 120));
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        total_kills += 1;

        let acknowledged = *acked.lock().unwrap();
        assert!(acknowledged > 0, "round {round}: child made no progress");

        let store = PersistentStore::open(&path)
            .unwrap_or_else(|e| panic!("round {round}: store did not reopen after kill: {e}"));
        let index = store.index(ConstraintId(1), IndexKind::Reference).unwrap();
        let epoch = index.epoch().unwrap().0;
        // Every acknowledged apply survived; at most one more (killed after commit, before ack).
        assert!(
            epoch == acknowledged || epoch == acknowledged + 1,
            "round {round}: epoch {epoch} but {acknowledged} acknowledged"
        );
        assert_eq!(
            contents(&index),
            expected_after(epoch),
            "round {round}: contents are not the state after {epoch} whole steps"
        );
    }
    assert_eq!(total_kills, 12);
}
