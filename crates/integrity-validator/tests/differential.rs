//! Phase 3 and Phase 4 exit criteria: differential tests against the reference oracle on random
//! operation sequences, with the in-memory and the persistent index backend. Verdicts (including
//! the full set of violated `(constraint, code)` pairs) must match at every step, and the final
//! indexes must equal indexes rebuilt from the oracle's state.

mod common;

use std::collections::BTreeMap;

use common::*;
use integrity_reference::Verdict;
use integrity_types::ErrorCode;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, TestRunner};

fn sequence() -> impl Strategy<Value = Vec<OpSeed>> {
    proptest::collection::vec(op_seed(), 1..40)
}

proptest! {
    #![proptest_config(Config { cases: 512, ..Config::default() })]

    #[test]
    fn engine_matches_oracle(seeds in sequence()) {
        if let Err(e) = run(&seeds, Backend::Memory) {
            prop_assert!(false, "{}", e);
        }
    }
}

proptest! {
    // Every accepted commit is a durable two-phase commit per index; fewer cases.
    #![proptest_config(Config { cases: 64, ..Config::default() })]

    #[test]
    fn persistent_engine_matches_oracle(seeds in sequence()) {
        if let Err(e) = run(&seeds, Backend::Persistent) {
            prop_assert!(false, "{}", e);
        }
    }
}

/// Guards against a vacuous differential test: over a fixed-seed sample, every verdict kind the
/// fixture can produce must actually occur.
#[test]
fn differential_runs_exercise_every_verdict() {
    let mut runner = TestRunner::deterministic();
    let strategy = sequence();
    let mut accepted = 0usize;
    let mut codes: BTreeMap<ErrorCode, usize> = BTreeMap::new();
    for _ in 0..300 {
        let seeds = strategy.new_tree(&mut runner).unwrap().current();
        for v in run(&seeds, Backend::Memory).unwrap() {
            match v {
                Verdict::Accepted => accepted += 1,
                Verdict::Rejected(vs) => {
                    for violation in vs {
                        *codes.entry(violation.code).or_default() += 1;
                    }
                }
            }
        }
    }
    eprintln!("accepted={accepted} rejections={codes:?}");
    assert!(accepted > 100, "only {accepted} accepted commits");
    for code in [
        ErrorCode::DuplicatePrimaryKey,
        ErrorCode::DuplicateUniqueKey,
        ErrorCode::ForeignKeyViolation,
        ErrorCode::ReferencedRowDelete,
        ErrorCode::NotNullViolation,
    ] {
        assert!(
            codes.get(&code).copied().unwrap_or(0) > 5,
            "{code} barely exercised: {codes:?}"
        );
    }
}
