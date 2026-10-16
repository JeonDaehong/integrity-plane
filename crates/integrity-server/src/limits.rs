//! Request limits (RFC 0007): commit rates per client and failed integrity API authentications.
//!
//! Both take the current time as an argument, so tests drive them without sleeping.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderName};

/// Clients tracked at most by each limiter.
pub const MAX_CLIENTS: usize = 65_536;

/// Failed authentications within [`AUTH_WINDOW`] after which a client is refused.
pub const AUTH_FAILURES: u32 = 10;

/// How long failed authentications count.
pub const AUTH_WINDOW: Duration = Duration::from_secs(60);

/// Whole seconds until `d` has passed, at least 1 (a `Retry-After` value).
fn retry_after(d: Duration) -> u64 {
    let ms = d.as_millis();
    u64::try_from(ms.div_ceil(1000)).unwrap_or(u64::MAX).max(1)
}

/// Who sent a request: the first value of `header` when it is configured and present, else the
/// connection's peer address; empty when neither is known (those requests share one client).
pub fn client_key(
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    header: Option<&HeaderName>,
) -> String {
    let forwarded = header
        .and_then(|h| headers.get(h))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.chars().take(64).collect());
    forwarded
        .or_else(|| peer.map(|p| p.to_string()))
        .unwrap_or_default()
}

/// Makes room for one more client in `map`: drops those `idle` says can go, then, if that freed
/// nothing, an arbitrary one.
fn make_room<V>(map: &mut HashMap<String, V>, idle: impl Fn(&V) -> bool) {
    if map.len() < MAX_CLIENTS {
        return;
    }
    map.retain(|_, v| !idle(v));
    if map.len() >= MAX_CLIENTS
        && let Some(k) = map.keys().next().cloned()
    {
        map.remove(&k);
    }
}

/// One token bucket per client: `rate` requests per second, bursts of up to `rate`.
#[derive(Debug)]
pub struct TokenBuckets {
    rate: f64,
    /// Tokens left and when they were counted.
    buckets: HashMap<String, (f64, Instant)>,
}

impl TokenBuckets {
    /// Buckets refilled at `per_second` tokens per second (at least 1).
    pub fn new(per_second: u32) -> Self {
        Self {
            rate: f64::from(per_second.max(1)),
            buckets: HashMap::new(),
        }
    }

    /// Takes a token from `client`'s bucket, or says in how many seconds one will be there.
    pub fn take(&mut self, client: &str, now: Instant) -> Result<(), u64> {
        let rate = self.rate;
        let full = |(tokens, at): &(f64, Instant)| {
            tokens + now.saturating_duration_since(*at).as_secs_f64() * rate >= rate
        };
        if !self.buckets.contains_key(client) {
            make_room(&mut self.buckets, full);
        }
        let (tokens, at) = self.buckets.entry(client.to_owned()).or_insert((rate, now));
        let refilled =
            (*tokens + now.saturating_duration_since(*at).as_secs_f64() * rate).min(rate);
        *at = now;
        if refilled >= 1.0 {
            *tokens = refilled - 1.0;
            Ok(())
        } else {
            *tokens = refilled;
            Err(retry_after(Duration::from_secs_f64(
                (1.0 - refilled) / rate,
            )))
        }
    }

    /// Clients tracked.
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// No client tracked.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

/// Failed authentications per client: [`AUTH_FAILURES`] within [`AUTH_WINDOW`] of the first of
/// them lock the client out until that window ends.
#[derive(Debug, Default)]
pub struct AuthFailures {
    /// Failures and when the first of them happened.
    clients: HashMap<String, (u32, Instant)>,
}

impl AuthFailures {
    /// Seconds until `client` may try again, if it is locked out.
    pub fn locked(&mut self, client: &str, now: Instant) -> Option<u64> {
        let &(count, first) = self.clients.get(client)?;
        let elapsed = now.saturating_duration_since(first);
        if elapsed >= AUTH_WINDOW {
            self.clients.remove(client);
            None
        } else if count >= AUTH_FAILURES {
            Some(retry_after(AUTH_WINDOW - elapsed))
        } else {
            None
        }
    }

    /// Records a failed authentication of `client`.
    pub fn failed(&mut self, client: &str, now: Instant) {
        if !self.clients.contains_key(client) {
            make_room(&mut self.clients, |(_, first)| {
                now.saturating_duration_since(*first) >= AUTH_WINDOW
            });
        }
        let entry = self.clients.entry(client.to_owned()).or_insert((0, now));
        if now.saturating_duration_since(entry.1) >= AUTH_WINDOW {
            *entry = (0, now);
        }
        entry.0 = entry.0.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_allows_bursts_of_its_rate_then_refills() {
        let t0 = Instant::now();
        let mut b = TokenBuckets::new(2);
        assert_eq!(b.take("a", t0), Ok(()));
        assert_eq!(b.take("a", t0), Ok(()));
        // Empty: one token comes back after half a second, rounded up to a whole second.
        assert_eq!(b.take("a", t0), Err(1));
        // Other clients have buckets of their own.
        assert_eq!(b.take("b", t0), Ok(()));
        assert_eq!(b.take("a", t0 + Duration::from_millis(500)), Ok(()));
        assert_eq!(b.take("a", t0 + Duration::from_millis(500)), Err(1));
        // A long pause refills to the burst, not beyond.
        let later = t0 + Duration::from_secs(60);
        assert_eq!(b.take("a", later), Ok(()));
        assert_eq!(b.take("a", later), Ok(()));
        assert_eq!(b.take("a", later), Err(1));
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        let t0 = Instant::now();
        let mut b = TokenBuckets::new(1);
        assert_eq!(b.take("a", t0), Ok(()));
        assert_eq!(b.take("a", t0), Err(1));
        assert_eq!(b.take("a", t0 + Duration::from_millis(999)), Err(1));
        assert_eq!(b.take("a", t0 + Duration::from_millis(1000)), Ok(()));
        assert_eq!(retry_after(Duration::from_millis(1001)), 2);
        assert_eq!(retry_after(Duration::ZERO), 1);
    }

    #[test]
    fn buckets_stay_bounded() {
        let t0 = Instant::now();
        let mut b = TokenBuckets::new(1);
        for i in 0..MAX_CLIENTS {
            assert!(b.take(&i.to_string(), t0).is_ok());
        }
        // All empty: one arbitrary client makes room.
        assert!(b.take("new", t0).is_ok());
        assert_eq!(b.len(), MAX_CLIENTS);
        // All full again a second later: they are forgotten at once.
        assert!(b.take("newer", t0 + Duration::from_secs(1)).is_ok());
        assert!(b.len() <= 2, "{}", b.len());
    }

    #[test]
    fn failed_authentications_lock_out_until_the_window_ends() {
        let t0 = Instant::now();
        let mut f = AuthFailures::default();
        for i in 0..AUTH_FAILURES {
            assert_eq!(f.locked("a", t0), None, "after {i} failures");
            f.failed("a", t0 + Duration::from_secs(u64::from(i)));
        }
        assert_eq!(f.locked("a", t0 + Duration::from_secs(10)), Some(50));
        assert_eq!(f.locked("b", t0 + Duration::from_secs(10)), None);
        assert_eq!(f.locked("a", t0 + Duration::from_millis(59_500)), Some(1));
        assert_eq!(f.locked("a", t0 + AUTH_WINDOW), None);
        // A failure after the window starts a new one.
        f.failed("a", t0 + AUTH_WINDOW);
        assert_eq!(f.locked("a", t0 + AUTH_WINDOW), None);
    }

    #[test]
    fn failures_spread_over_windows_never_lock_out() {
        let t0 = Instant::now();
        let mut f = AuthFailures::default();
        for i in 0..100u64 {
            let now = t0 + Duration::from_secs(i * 7);
            f.failed("a", now);
            assert_eq!(f.locked("a", now), None, "at {i}");
        }
    }

    #[test]
    fn the_client_is_the_configured_header_else_the_peer() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", " 10.0.0.7 , 10.0.0.1".parse().unwrap());
        let peer: IpAddr = "192.168.1.5".parse().unwrap();
        let xff = HeaderName::from_static("x-forwarded-for");
        assert_eq!(client_key(&h, Some(peer), Some(&xff)), "10.0.0.7");
        // Not configured: the header is anyone's to send, so it is ignored.
        assert_eq!(client_key(&h, Some(peer), None), "192.168.1.5");
        assert_eq!(
            client_key(&HeaderMap::new(), Some(peer), Some(&xff)),
            "192.168.1.5"
        );
        assert_eq!(client_key(&HeaderMap::new(), None, None), "");
    }
}
