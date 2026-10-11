//! File access for commit inspection, with the inline validation budget (spec §17.1).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;

/// Why a file could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// The storage layer failed or the file does not exist.
    Io(String),
    /// Reading the file would exceed the validation budget.
    BudgetExceeded {
        /// The configured limit in bytes.
        limit: u64,
    },
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::Io(m) => write!(f, "cannot read file: {m}"),
            ReadError::BudgetExceeded { limit } => {
                write!(f, "inline validation would read more than {limit} bytes")
            }
        }
    }
}

impl std::error::Error for ReadError {}

/// Reads by Iceberg location (object store, local disk, …). Independent files are read from
/// several threads at once ([`concurrently`]).
pub trait FileIo: Sync {
    /// The contents of the file at `location`.
    fn read(&self, location: &str) -> Result<Bytes, ReadError>;

    /// The size of the file at `location` in bytes.
    fn size(&self, location: &str) -> Result<u64, ReadError> {
        Ok(self.read(location)?.len() as u64)
    }

    /// Bytes `range` of the file at `location` (ranged GET on object stores). Reading past the end
    /// is an error.
    fn read_range(&self, location: &str, range: std::ops::Range<u64>) -> Result<Bytes, ReadError> {
        let all = self.read(location)?;
        slice(&all, location, range)
    }
}

/// Requests in flight at once when reading independent files (manifests, delete files): object
/// stores answer each in tens of milliseconds, so one at a time a commit touching a few hundred
/// files waits seconds.
pub const READ_CONCURRENCY: usize = 16;

/// `f` applied to every item, up to [`READ_CONCURRENCY`] at a time; the results in item order, or
/// the error of the first failing item in item order (items after a failure are not started).
pub(crate) fn concurrently<T: Sync, R: Send, E: Send>(
    items: &[T],
    f: impl Fn(&T) -> Result<R, E> + Sync,
) -> Result<Vec<R>, E> {
    let workers = READ_CONCURRENCY.min(items.len());
    if workers <= 1 {
        return items.iter().map(f).collect();
    }
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let mut done: Vec<(usize, Result<R, E>)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                s.spawn(|| {
                    let mut out = Vec::new();
                    while !failed.load(Ordering::SeqCst) {
                        let i = next.fetch_add(1, Ordering::SeqCst);
                        let Some(item) = items.get(i) else { break };
                        let r = f(item);
                        if r.is_err() {
                            failed.store(true, Ordering::SeqCst);
                        }
                        out.push((i, r));
                    }
                    out
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            .collect()
    });
    // Items are taken in order, so every item before a failed one has a result: the first error
    // in item order comes before any item that was not started.
    done.sort_by_key(|(i, _)| *i);
    done.into_iter().map(|(_, r)| r).collect()
}

/// `bytes[range]`, or an error naming `location` if the range is out of bounds.
pub fn slice(
    bytes: &Bytes,
    location: &str,
    range: std::ops::Range<u64>,
) -> Result<Bytes, ReadError> {
    let (start, end) = (
        usize::try_from(range.start).unwrap_or(usize::MAX),
        usize::try_from(range.end).unwrap_or(usize::MAX),
    );
    if start > end || end > bytes.len() {
        return Err(ReadError::Io(format!(
            "{location}: range {start}..{end} beyond {} bytes",
            bytes.len()
        )));
    }
    Ok(bytes.slice(start..end))
}

/// One file in memory, at any location (for [`crate::extract_rows`] on bytes).
pub(crate) struct Single(pub Bytes);

impl FileIo for Single {
    fn read(&self, _location: &str) -> Result<Bytes, ReadError> {
        Ok(self.0.clone())
    }
}

/// Files held in memory, keyed by location (tests, fixtures).
#[derive(Debug, Clone, Default)]
pub struct MemoryIo {
    files: BTreeMap<String, Bytes>,
}

impl MemoryIo {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a file.
    pub fn insert(&mut self, location: impl Into<String>, contents: impl Into<Bytes>) {
        self.files.insert(location.into(), contents.into());
    }
}

impl FileIo for MemoryIo {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        self.files
            .get(location)
            .cloned()
            .ok_or_else(|| ReadError::Io(format!("not found: {location}")))
    }
}

/// Counts bytes read through `inner` and refuses reads beyond `limit`.
///
/// Every byte returned is counted (whole files, or only the ranges read from Parquet files).
#[derive(Debug)]
pub struct Budgeted<'a, I: ?Sized> {
    inner: &'a I,
    limit: u64,
    used: AtomicU64,
}

impl<'a, I: FileIo + ?Sized> Budgeted<'a, I> {
    /// Wraps `inner` with a budget of `limit` bytes.
    pub fn new(inner: &'a I, limit: u64) -> Self {
        Self {
            inner,
            limit,
            used: AtomicU64::new(0),
        }
    }

    /// Bytes read so far.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::SeqCst)
    }
}

impl<I: FileIo + ?Sized> Budgeted<'_, I> {
    fn charge(&self, bytes: &Bytes) -> Result<(), ReadError> {
        let used = self.used.fetch_add(bytes.len() as u64, Ordering::SeqCst) + bytes.len() as u64;
        if used > self.limit {
            return Err(ReadError::BudgetExceeded { limit: self.limit });
        }
        Ok(())
    }
}

impl<I: FileIo + ?Sized> FileIo for Budgeted<'_, I> {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        if self.used() > self.limit {
            return Err(ReadError::BudgetExceeded { limit: self.limit });
        }
        let bytes = self.inner.read(location)?;
        self.charge(&bytes)?;
        Ok(bytes)
    }

    fn size(&self, location: &str) -> Result<u64, ReadError> {
        self.inner.size(location)
    }

    fn read_range(&self, location: &str, range: std::ops::Range<u64>) -> Result<Bytes, ReadError> {
        if self.used() > self.limit {
            return Err(ReadError::BudgetExceeded { limit: self.limit });
        }
        let bytes = self.inner.read_range(location, range)?;
        self.charge(&bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Item `i` takes longer the smaller `i` is, so later items finish first.
    fn slow(i: &u64) {
        std::thread::sleep(std::time::Duration::from_millis(40 - *i));
    }

    #[test]
    fn concurrent_results_keep_item_order() {
        let items: Vec<u64> = (0..40).collect();
        let out = concurrently(&items, |i| {
            slow(i);
            Ok::<_, ()>(i * 2)
        });
        assert_eq!(out, Ok(items.iter().map(|i| i * 2).collect()));
        assert_eq!(concurrently(&[] as &[u64], |i| Ok::<_, ()>(*i)), Ok(vec![]));
    }

    #[test]
    fn the_first_failing_item_in_order_is_reported() {
        let items: Vec<u64> = (0..40).collect();
        for _ in 0..10 {
            let out = concurrently(&items, |i| {
                slow(i);
                if *i == 5 || *i == 30 { Err(*i) } else { Ok(*i) }
            });
            assert_eq!(out, Err(5));
        }
    }

    #[test]
    fn items_after_a_failure_are_not_started() {
        let started = AtomicUsize::new(0);
        let items: Vec<u64> = (0..1000).collect();
        let out = concurrently(&items, |i| {
            started.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(1));
            if *i == 0 { Err(()) } else { Ok(()) }
        });
        assert_eq!(out, Err(()));
        assert!(started.load(Ordering::SeqCst) < 100);
    }
}
