//! Verdict vocabulary shared by the engine and the reference oracle.

use integrity_types::{ConstraintId, ErrorCode};

/// One violated constraint and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Violation {
    /// The violated constraint.
    pub constraint: ConstraintId,
    /// The violation code.
    pub code: ErrorCode,
}
