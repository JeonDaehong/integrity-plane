//! Runs: `(key, value)` records produced in strictly increasing key order and read back, any
//! number of times, in the same order, with bounded memory (ADR 0025).
//!
//! Records are kept in memory until they would take more than the run's budget, then everything
//! goes to one file in a private directory, removed on drop. A run holds key bytes, which can be
//! personal data (spec §24), only while the commit that made it is being decided.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{IndexError, Result};

/// Write and read buffer of a run file.
const IO_BUFFER: usize = 64 * 1024;

fn io(e: std::io::Error) -> IndexError {
    IndexError::Storage(format!("run spill: {e}"))
}

/// A private directory holding one run file, removed with it on drop.
#[derive(Debug)]
struct Dir(PathBuf);

impl Dir {
    fn new(parent: &Path) -> Result<Self> {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let dir = parent.join(format!(
            "run-{}-{nanos}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).map_err(io)?;
        Ok(Self(dir))
    }

    fn file(&self) -> PathBuf {
        self.0.join("records")
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Appends one record: key length, value length (both `u32`, little-endian), key, value.
fn put(out: &mut impl Write, key: &[u8], value: &[u8]) -> Result<()> {
    let k = u32::try_from(key.len()).map_err(|_| IndexError::Corrupt)?;
    let v = u32::try_from(value.len()).map_err(|_| IndexError::Corrupt)?;
    out.write_all(&k.to_le_bytes()).map_err(io)?;
    out.write_all(&v.to_le_bytes()).map_err(io)?;
    out.write_all(key).map_err(io)?;
    out.write_all(value).map_err(io)
}

/// A run being written.
#[derive(Debug)]
pub struct Run {
    parent: PathBuf,
    budget: usize,
    memory: Vec<u8>,
    spilled: Option<(Dir, BufWriter<File>)>,
    last: Option<Vec<u8>>,
    len: u64,
}

impl Run {
    /// An empty run keeping at most about `budget` bytes in memory, spilling to a directory
    /// created under `parent`.
    pub fn new(parent: impl Into<PathBuf>, budget: usize) -> Self {
        Self {
            parent: parent.into(),
            budget,
            memory: Vec::new(),
            spilled: None,
            last: None,
            len: 0,
        }
    }

    /// Appends a record. Keys must be strictly increasing ([`IndexError::Corrupt`] otherwise).
    pub fn push(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.last.as_deref().is_some_and(|last| last >= key) {
            return Err(IndexError::Corrupt);
        }
        match &mut self.last {
            Some(last) => {
                last.clear();
                last.extend_from_slice(key);
            }
            None => self.last = Some(key.to_vec()),
        }
        self.len += 1;
        if let Some((_, w)) = &mut self.spilled {
            return put(w, key, value);
        }
        put(&mut self.memory, key, value)?;
        if self.memory.capacity() > self.budget {
            let dir = Dir::new(&self.parent)?;
            let mut w = BufWriter::with_capacity(IO_BUFFER, File::create(dir.file()).map_err(io)?);
            w.write_all(&self.memory).map_err(io)?;
            self.memory = Vec::new();
            self.spilled = Some((dir, w));
        }
        Ok(())
    }

    /// Records pushed so far.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether no record was pushed.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Ends writing; the records can then be read any number of times.
    pub fn finish(self) -> Result<FinishedRun> {
        let stored = match self.spilled {
            None => Stored::Memory(self.memory),
            Some((dir, mut w)) => {
                w.flush().map_err(io)?;
                drop(w);
                Stored::File(dir)
            }
        };
        Ok(FinishedRun {
            stored,
            len: self.len,
        })
    }
}

#[derive(Debug)]
enum Stored {
    Memory(Vec<u8>),
    File(Dir),
}

/// A written run.
#[derive(Debug)]
pub struct FinishedRun {
    stored: Stored,
    len: u64,
}

impl FinishedRun {
    /// Number of records.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the run has no record.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the records went to a file.
    pub fn spilled(&self) -> bool {
        matches!(self.stored, Stored::File(_))
    }

    /// The records, in the order they were pushed.
    pub fn records(&self) -> Result<Records<'_>> {
        let source = match &self.stored {
            Stored::Memory(bytes) => Source::Memory(bytes.as_slice()),
            Stored::File(dir) => Source::File(BufReader::with_capacity(
                IO_BUFFER,
                File::open(dir.file()).map_err(io)?,
            )),
        };
        Ok(Records {
            source,
            left: self.len,
        })
    }
}

#[derive(Debug)]
enum Source<'a> {
    Memory(&'a [u8]),
    File(BufReader<File>),
}

impl Read for Source<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Source::Memory(bytes) => bytes.read(buf),
            Source::File(file) => file.read(buf),
        }
    }
}

/// The records of a [`FinishedRun`]: `(key, value)` pairs.
#[derive(Debug)]
pub struct Records<'a> {
    source: Source<'a>,
    left: u64,
}

impl Records<'_> {
    fn next_record(&mut self) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut lens = [0u8; 8];
        self.source.read_exact(&mut lens).map_err(io)?;
        let k = u32::from_le_bytes([lens[0], lens[1], lens[2], lens[3]]) as usize;
        let v = u32::from_le_bytes([lens[4], lens[5], lens[6], lens[7]]) as usize;
        let mut key = vec![0u8; k];
        self.source.read_exact(&mut key).map_err(io)?;
        let mut value = vec![0u8; v];
        self.source.read_exact(&mut value).map_err(io)?;
        Ok((key, value))
    }
}

impl Iterator for Records<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let record = self.next_record();
        if record.is_err() {
            self.left = 0;
        }
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn scratch() -> PathBuf {
        std::env::temp_dir().join(format!("oip-run-test-{}", std::process::id()))
    }

    fn records() -> impl Strategy<Value = Vec<(Vec<u8>, Vec<u8>)>> {
        proptest::collection::btree_map(
            proptest::collection::vec(any::<u8>(), 0..12),
            proptest::collection::vec(any::<u8>(), 0..20),
            0..200,
        )
        .prop_map(|m| m.into_iter().collect())
    }

    proptest! {
        #[test]
        fn records_come_back_in_order_at_any_budget(
            input in records(),
            budget in prop_oneof![Just(0usize), 1usize..64, Just(1 << 20)],
        ) {
            let mut run = Run::new(scratch(), budget);
            for (k, v) in &input {
                run.push(k, v).unwrap();
            }
            prop_assert_eq!(run.len(), input.len() as u64);
            let run = run.finish().unwrap();
            // Read twice: a run can be read any number of times.
            for _ in 0..2 {
                let back: Vec<_> = run.records().unwrap().collect::<Result<_>>().unwrap();
                prop_assert_eq!(&back, &input);
            }
            if budget == 0 && !input.is_empty() {
                prop_assert!(run.spilled());
            }
            if budget == 1 << 20 {
                prop_assert!(!run.spilled());
            }
        }
    }

    #[test]
    fn keys_must_strictly_increase() {
        let mut run = Run::new(scratch(), 1 << 10);
        run.push(b"b", b"").unwrap();
        assert_eq!(run.push(b"b", b""), Err(IndexError::Corrupt));
        assert_eq!(run.push(b"a", b""), Err(IndexError::Corrupt));
        run.push(b"c", b"").unwrap();
    }

    #[test]
    fn the_spill_directory_goes_with_the_run() {
        let parent = scratch().join("dropped");
        let mut run = Run::new(&parent, 0);
        run.push(b"a", b"1").unwrap();
        let run = run.finish().unwrap();
        assert!(run.spilled());
        assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 1);
        drop(run);
        assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    }
}
