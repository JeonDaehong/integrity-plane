//! File access for commit inspection, with the inline validation budget (spec §17.1).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Reads by Iceberg location (object store, local disk, …).
pub trait FileIo {
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
