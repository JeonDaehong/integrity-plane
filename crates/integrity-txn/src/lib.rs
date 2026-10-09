//! Transaction state machine, durable transaction log and fault points (spec §16, RFC 0004).

pub mod fault;
pub mod log;

use std::fmt;

pub use fault::FaultPoint;
pub use log::{Decision, Prepared, TxnLog, TxnSummary, Unresolved, Validated};

/// A transaction id, allocated by the log, never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn-{}", self.0)
    }
}

/// Transaction states (spec §16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnState {
    /// Received; validating.
    Prepared,
    /// Validated; staged deltas recorded; not yet forwarded.
    Validated,
    /// Being forwarded upstream; outcome possibly unknown.
    Committing,
    /// Published and applied.
    Committed,
    /// Not published.
    Aborted,
    /// Failed validation.
    Rejected,
}

/// Log failures. All of them stop commits to the domain (fail closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnError {
    /// A record or staged delta does not verify.
    Corrupt,
    /// The storage engine failed.
    Storage(String),
    /// The state machine forbids this transition.
    IllegalTransition {
        /// Current state.
        from: Option<TxnState>,
        /// Requested state.
        to: TxnState,
    },
}

impl fmt::Display for TxnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TxnError::Corrupt => f.write_str("transaction log is corrupt"),
            TxnError::Storage(m) => write!(f, "transaction log storage error: {m}"),
            TxnError::IllegalTransition { from, to } => {
                write!(f, "illegal transaction transition {from:?} -> {to:?}")
            }
        }
    }
}

impl std::error::Error for TxnError {}
