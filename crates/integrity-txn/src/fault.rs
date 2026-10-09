//! Fault points for crash testing (spec §28).
//!
//! With the `fault-injection` feature, [`hit`] aborts the process when the environment variable
//! `OIP_FAULT` names the point. Without the feature it compiles to nothing: production builds never
//! read the variable.

/// Places in the commit protocol where a crash must be survivable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// After `PREPARED` is durable, during validation.
    AfterPreparedLog,
    /// After `VALIDATED` (with staged deltas) is durable.
    AfterValidatedLog,
    /// After `COMMITTING`, before the request reaches upstream.
    BeforeUpstream,
    /// Upstream accepted the commit; nothing applied or logged yet.
    AfterUpstreamBeforeLog,
    /// Upstream answered with an unknown outcome (5xx or no answer).
    AfterUpstreamUnknown,
    /// Between applying two indexes.
    DuringIndexApply,
    /// Indexes applied, `COMMITTED` not yet logged.
    BeforeCommittedLog,
    /// Rebuild or onboarding: between replacing the contents of two indexes.
    DuringRebuildSwap,
}

impl FaultPoint {
    /// Every point, in protocol order.
    pub const ALL: [FaultPoint; 8] = [
        FaultPoint::AfterPreparedLog,
        FaultPoint::AfterValidatedLog,
        FaultPoint::BeforeUpstream,
        FaultPoint::AfterUpstreamBeforeLog,
        FaultPoint::AfterUpstreamUnknown,
        FaultPoint::DuringIndexApply,
        FaultPoint::BeforeCommittedLog,
        FaultPoint::DuringRebuildSwap,
    ];

    /// The `OIP_FAULT` value selecting this point.
    pub fn name(self) -> &'static str {
        match self {
            FaultPoint::AfterPreparedLog => "AfterPreparedLog",
            FaultPoint::AfterValidatedLog => "AfterValidatedLog",
            FaultPoint::BeforeUpstream => "BeforeUpstream",
            FaultPoint::AfterUpstreamBeforeLog => "AfterUpstreamBeforeLog",
            FaultPoint::AfterUpstreamUnknown => "AfterUpstreamUnknown",
            FaultPoint::DuringIndexApply => "DuringIndexApply",
            FaultPoint::BeforeCommittedLog => "BeforeCommittedLog",
            FaultPoint::DuringRebuildSwap => "DuringRebuildSwap",
        }
    }
}

/// Aborts the process if `OIP_FAULT` selects `point` (feature `fault-injection` only).
#[inline]
pub fn hit(point: FaultPoint) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("OIP_FAULT").is_ok_and(|v| v == point.name()) {
        eprintln!("fault injection: aborting at {}", point.name());
        std::process::abort();
    }
    #[cfg(not(feature = "fault-injection"))]
    let _ = point;
}
