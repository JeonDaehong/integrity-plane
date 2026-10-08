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

/// Whole-file reads by Iceberg location (object store, local disk, …).
pub trait FileIo {
    /// The contents of the file at `location`.
    fn read(&self, location: &str) -> Result<Bytes, ReadError>;
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
/// Whole files are counted, which over-estimates key-column bytes; the budget therefore errs on
/// the side of rejecting (`VALIDATION_BUDGET_EXCEEDED`), never of reading too much.
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

impl<I: FileIo + ?Sized> FileIo for Budgeted<'_, I> {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        if self.used() > self.limit {
            return Err(ReadError::BudgetExceeded { limit: self.limit });
        }
        let bytes = self.inner.read(location)?;
        let used = self.used.fetch_add(bytes.len() as u64, Ordering::SeqCst) + bytes.len() as u64;
        if used > self.limit {
            return Err(ReadError::BudgetExceeded { limit: self.limit });
        }
        Ok(bytes)
    }
}
