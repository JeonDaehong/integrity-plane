//! Prometheus metrics (spec §25), in the text exposition format. Labels never carry key values or
//! table names, so cardinality stays fixed.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Upper bounds (seconds) of the latency histogram buckets.
pub const BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// A latency histogram (cumulative buckets are computed when rendering).
#[derive(Debug, Default)]
pub struct Timer {
    micros: AtomicU64,
    count: AtomicU64,
    /// Observations per bucket (non-cumulative); the last slot is `+Inf`.
    buckets: [AtomicU64; BUCKETS.len() + 1],
}

impl Timer {
    /// Records one observation.
    pub fn observe(&self, d: Duration) {
        self.micros.fetch_add(
            u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.count.fetch_add(1, Ordering::Relaxed);
        let secs = d.as_secs_f64();
        let slot = BUCKETS
            .iter()
            .position(|&b| secs <= b)
            .unwrap_or(BUCKETS.len());
        self.buckets[slot].fetch_add(1, Ordering::Relaxed);
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
    /// Bytes read from storage to validate commits.
    pub bytes_read: AtomicU64,
    /// Rows whose keys were extracted and validated.
    pub keys_validated: AtomicU64,
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

fn histogram(out: &mut String, name: &str, help: &str, t: &Timer) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} histogram");
    let mut cumulative = 0;
    for (i, bound) in BUCKETS.iter().enumerate() {
        cumulative += t.buckets[i].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
    }
    cumulative += t.buckets[BUCKETS.len()].load(Ordering::Relaxed);
    let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
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
        histogram(
            &mut out,
            "integrity_validation_seconds",
            "Time spent reading files and validating commits.",
            &self.validation,
        );
        histogram(
            &mut out,
            "integrity_domain_queue_wait_seconds",
            "Time commits waited for their domain's queue.",
            &self.queue_wait,
        );
        counter(
            &mut out,
            "integrity_bytes_read_total",
            "Bytes read from storage (manifest lists, manifests, data files) to validate commits.",
            &[("", get(&self.bytes_read))],
        );
        counter(
            &mut out,
            "integrity_keys_validated_total",
            "Rows whose keys were extracted and validated.",
            &[("", get(&self.keys_validated))],
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
        m.validation.observe(Duration::from_micros(300));
        m.validation.observe(Duration::from_secs(60));
        let text = m.render(3, 1, 0);
        assert!(text.contains("integrity_commits_total{verdict=\"accepted\"} 2\n"));
        assert!(text.contains("integrity_validation_seconds_sum 61.5003\n"));
        assert!(text.contains("integrity_validation_seconds_count 3\n"));
        assert!(text.contains("integrity_validation_seconds_bucket{le=\"0.0005\"} 1\n"));
        assert!(text.contains("integrity_validation_seconds_bucket{le=\"1\"} 1\n"));
        assert!(text.contains("integrity_validation_seconds_bucket{le=\"2.5\"} 2\n"));
        assert!(text.contains("integrity_validation_seconds_bucket{le=\"10\"} 2\n"));
        assert!(text.contains("integrity_validation_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(text.contains("integrity_bypass_detected_total 0\n"));
        assert!(text.contains("integrity_domain_state{state=\"degraded\"} 1\n"));
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (name, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
            assert!(name.starts_with("integrity_"), "{line}");
        }
    }
}
