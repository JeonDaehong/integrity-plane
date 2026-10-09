//! Prometheus metrics (spec §25), in the text exposition format. Labels never carry key values or
//! table names, so cardinality stays fixed.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A counter of durations: total and count, exported as a Prometheus summary without quantiles.
#[derive(Debug, Default)]
pub struct Timer {
    micros: AtomicU64,
    count: AtomicU64,
}

impl Timer {
    /// Records one observation.
    pub fn observe(&self, d: Duration) {
        self.micros.fetch_add(
            u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Every metric the gateway exports.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Commits forwarded upstream and committed.
    pub accepted: AtomicU64,
    /// Commits refused for constraint violations.
    pub rejected: AtomicU64,
    /// Commits refused for any other reason, or refused upstream.
    pub aborted: AtomicU64,
    /// Time spent reading files and validating.
    pub validation: Timer,
    /// Time spent waiting for a domain's queue.
    pub queue_wait: Timer,
    /// Transactions resolved by recovery as committed.
    pub recovered_committed: AtomicU64,
    /// Transactions resolved by recovery as aborted.
    pub recovered_aborted: AtomicU64,
    /// Commits refused because a domain member's head was not certified.
    pub bypass_detected: AtomicU64,
}

fn counter(out: &mut String, name: &str, help: &str, values: &[(&str, u64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    for (labels, v) in values {
        let _ = writeln!(out, "{name}{labels} {v}");
    }
}

fn summary(out: &mut String, name: &str, help: &str, t: &Timer) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} summary");
    let micros = t.micros.load(Ordering::Relaxed);
    let _ = writeln!(out, "{name}_sum {}", micros as f64 / 1e6);
    let _ = writeln!(out, "{name}_count {}", t.count.load(Ordering::Relaxed));
}

impl Metrics {
    /// The exposition text. `healthy` and `degraded` count tables with an anchor.
    pub fn render(&self, healthy: usize, degraded: usize, unresolved: usize) -> String {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut out = String::new();
        counter(
            &mut out,
            "integrity_commits_total",
            "Commits to constrained tables by verdict.",
            &[
                ("{verdict=\"accepted\"}", get(&self.accepted)),
                ("{verdict=\"rejected\"}", get(&self.rejected)),
                ("{verdict=\"aborted\"}", get(&self.aborted)),
            ],
        );
        summary(
            &mut out,
            "integrity_validation_seconds",
            "Time spent reading files and validating commits.",
            &self.validation,
        );
        summary(
            &mut out,
            "integrity_domain_queue_wait_seconds",
            "Time commits waited for their domain's queue.",
            &self.queue_wait,
        );
        counter(
            &mut out,
            "integrity_recovery_total",
            "Transactions resolved by recovery, by outcome.",
            &[
                ("{outcome=\"committed\"}", get(&self.recovered_committed)),
                ("{outcome=\"aborted\"}", get(&self.recovered_aborted)),
            ],
        );
        counter(
            &mut out,
            "integrity_bypass_detected_total",
            "Commits refused because a table's main was not committed through the Plane.",
            &[("", get(&self.bypass_detected))],
        );
        let _ = writeln!(
            out,
            "# HELP integrity_domain_state Constrained tables by the state of their domain."
        );
        let _ = writeln!(out, "# TYPE integrity_domain_state gauge");
        let _ = writeln!(out, "integrity_domain_state{{state=\"healthy\"}} {healthy}");
        let _ = writeln!(
            out,
            "integrity_domain_state{{state=\"degraded\"}} {degraded}"
        );
        let _ = writeln!(
            out,
            "# HELP integrity_unresolved_transactions Transactions waiting for recovery."
        );
        let _ = writeln!(out, "# TYPE integrity_unresolved_transactions gauge");
        let _ = writeln!(out, "integrity_unresolved_transactions {unresolved}");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_exposition_format() {
        let m = Metrics::default();
        m.accepted.fetch_add(2, Ordering::Relaxed);
        m.validation.observe(Duration::from_millis(1500));
        let text = m.render(3, 1, 0);
        assert!(text.contains("integrity_commits_total{verdict=\"accepted\"} 2\n"));
        assert!(text.contains("integrity_validation_seconds_sum 1.5\n"));
        assert!(text.contains("integrity_validation_seconds_count 1\n"));
        assert!(text.contains("integrity_bypass_detected_total 0\n"));
        assert!(text.contains("integrity_domain_state{state=\"degraded\"} 1\n"));
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (name, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
            assert!(name.starts_with("integrity_"), "{line}");
        }
    }
}
