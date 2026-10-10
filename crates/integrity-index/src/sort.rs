//! External sort of encoded keys with bounded memory (onboarding and rebuild, ADR 0011).
//!
//! Keys are buffered up to a memory budget, then sorted, collapsed into `(key, count)` and written
//! to a run file. Reading merges the runs (at most [`FAN_IN`] at once, in several passes if
//! needed) and yields every distinct key once, in byte order (= key order, spec §9.2), with the
//! number of times it was pushed. Run files live in a private directory removed on drop; they hold
//! key bytes, which can be personal data (spec §24), only while the sort runs.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use integrity_core::EncodedKey;

use crate::{IndexError, Result};

/// Runs merged at once.
const FAN_IN: usize = 64;
/// Bytes per buffered key on top of its length (its span).
const PER_KEY: usize = std::mem::size_of::<(usize, u32)>();
/// Write and read buffer of each run file.
const IO_BUFFER: usize = 64 * 1024;

fn io(e: std::io::Error) -> IndexError {
    IndexError::Storage(format!("sort spill: {e}"))
}

/// A directory of run files, removed with everything in it on drop.
#[derive(Debug)]
struct Spill {
    dir: PathBuf,
    next: u64,
}

impl Spill {
    fn new(parent: &Path) -> Result<Self> {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let dir = parent.join(format!(
            "sort-{}-{nanos}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).map_err(io)?;
        Ok(Self { dir, next: 0 })
    }

    fn run_path(&mut self) -> PathBuf {
        self.next += 1;
        self.dir.join(format!("run-{}", self.next))
    }

    /// Writes sorted, distinct `(key, count)` records to a new run file.
    fn write<K: AsRef<[u8]>>(
        &mut self,
        records: impl Iterator<Item = Result<(K, u64)>>,
    ) -> Result<PathBuf> {
        let path = self.run_path();
        let mut w = BufWriter::with_capacity(IO_BUFFER, File::create(&path).map_err(io)?);
        for record in records {
            let (key, count) = record?;
            let key = key.as_ref();
            let len = u32::try_from(key.len()).map_err(|_| IndexError::Corrupt)?;
            w.write_all(&len.to_le_bytes()).map_err(io)?;
            w.write_all(key).map_err(io)?;
            w.write_all(&count.to_le_bytes()).map_err(io)?;
        }
        w.flush().map_err(io)?;
        Ok(path)
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Keys packed into one allocation: their bytes back to back, and an `(offset, length)` span
/// per key. Millions of keys cost two allocations, not one each.
#[derive(Debug, Default)]
struct Packed {
    data: Vec<u8>,
    spans: Vec<(usize, u32)>,
}

/// The capacity a vector grows to when it must hold `needed` elements (doubling, as `Vec` does).
fn grown(capacity: usize, needed: usize) -> usize {
    if needed <= capacity {
        capacity
    } else {
        needed.max(capacity.saturating_mul(2)).max(8)
    }
}

impl Packed {
    /// The memory the buffer would have allocated after adding a key of `len` bytes: what the
    /// budget is checked against, so that growth never overshoots it.
    fn allocated_with(&self, len: usize) -> usize {
        grown(self.data.capacity(), self.data.len() + len)
            + grown(self.spans.capacity(), self.spans.len() + 1) * PER_KEY
    }

    fn push(&mut self, bytes: &[u8]) -> Result<()> {
        let len = u32::try_from(bytes.len()).map_err(|_| IndexError::Corrupt)?;
        // Grow exactly as `allocated_with` assumed.
        let data = grown(self.data.capacity(), self.data.len() + bytes.len());
        self.data.reserve_exact(data - self.data.len());
        let spans = grown(self.spans.capacity(), self.spans.len() + 1);
        self.spans.reserve_exact(spans - self.spans.len());
        self.spans.push((self.data.len(), len));
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    fn key(&self, (offset, len): (usize, u32)) -> &[u8] {
        &self.data[offset..offset + len as usize]
    }

    fn sort(&mut self) {
        let data = &self.data;
        self.spans.sort_unstable_by(|&(a, la), &(b, lb)| {
            data[a..a + la as usize].cmp(&data[b..b + lb as usize])
        });
    }

    /// The distinct keys from span `at` on, collapsed: the next key and its count, and where the
    /// following key starts. The spans must be sorted.
    fn next_from(&self, at: usize) -> Option<(&[u8], u64, usize)> {
        let first = *self.spans.get(at)?;
        let key = self.key(first);
        let mut end = at + 1;
        while end < self.spans.len() && self.key(self.spans[end]) == key {
            end += 1;
        }
        Some((key, (end - at) as u64, end))
    }

    /// Sorted, collapsed records, for a run file.
    fn records(&self) -> impl Iterator<Item = Result<(&[u8], u64)>> {
        let mut at = 0;
        std::iter::from_fn(move || {
            let (key, count, next) = self.next_from(at)?;
            at = next;
            Some(Ok((key, count)))
        })
    }

    fn clear(&mut self) {
        self.data.clear();
        self.spans.clear();
    }
}

/// Sorts keys within a memory budget, spilling sorted runs to disk.
#[derive(Debug)]
pub struct KeySorter {
    parent: PathBuf,
    spill: Option<Spill>,
    budget: usize,
    buffer: Packed,
    runs: Vec<PathBuf>,
}

impl KeySorter {
    /// A sorter that keeps at most about `budget` bytes of keys in memory and spills to a new
    /// directory under `parent`.
    pub fn new(parent: impl Into<PathBuf>, budget: usize) -> Self {
        Self {
            parent: parent.into(),
            spill: None,
            budget: budget.max(1),
            buffer: Packed::default(),
            runs: Vec::new(),
        }
    }

    /// Adds one occurrence of `key`.
    pub fn push(&mut self, key: &EncodedKey) -> Result<()> {
        self.push_bytes(key.as_bytes())
    }

    /// Adds one occurrence of an arbitrary byte string (read back with
    /// [`KeySorter::finish_bytes`]).
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        // Spill before the buffer would grow past the budget (allocated memory, not just used).
        if !self.buffer.is_empty() && self.buffer.allocated_with(bytes.len()) > self.budget {
            self.spill_buffer()?;
        }
        self.buffer.push(bytes)
    }

    /// Number of run files written so far.
    pub fn runs(&self) -> usize {
        self.runs.len()
    }

    /// Sorts the buffer into a new run file and empties it (keeping its allocation for reuse).
    fn spill_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort();
        let spill = match &mut self.spill {
            Some(s) => s,
            None => self.spill.insert(Spill::new(&self.parent)?),
        };
        let path = spill.write(self.buffer.records())?;
        self.runs.push(path);
        self.buffer.clear();
        Ok(())
    }

    /// Every distinct key pushed, in order, with its number of occurrences.
    pub fn finish(self) -> Result<SortedKeys> {
        Ok(SortedKeys(self.finish_bytes()?))
    }

    /// Every distinct byte string pushed, in byte order, with its number of occurrences.
    pub fn finish_bytes(mut self) -> Result<SortedBytes> {
        if self.spill.is_none() {
            let mut packed = std::mem::take(&mut self.buffer);
            packed.sort();
            return Ok(SortedBytes {
                inner: Inner::Memory { packed, at: 0 },
                _spill: None,
            });
        }
        self.spill_buffer()?;
        let Some(mut spill) = self.spill.take() else {
            return Err(IndexError::Corrupt);
        };
        // The buffer is not needed any more: free it before merging.
        self.buffer = Packed::default();
        let mut runs = std::mem::take(&mut self.runs);
        while runs.len() > FAN_IN {
            let mut merged = Vec::new();
            for group in runs.chunks(FAN_IN) {
                let merge = Merge::open(group)?;
                merged.push(spill.write(merge)?);
                for path in group {
                    let _ = std::fs::remove_file(path);
                }
            }
            runs = merged;
        }
        Ok(SortedBytes {
            inner: Inner::Merge(Merge::open(&runs)?),
            _spill: Some(spill),
        })
    }
}

/// Reads the records of one run file.
struct Run(BufReader<File>);

impl Run {
    fn next(&mut self) -> Result<Option<(Vec<u8>, u64)>> {
        let mut len = [0u8; 4];
        match self.0.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(io(e)),
        }
        let mut key = vec![0u8; u32::from_le_bytes(len) as usize];
        self.0.read_exact(&mut key).map_err(io)?;
        let mut count = [0u8; 8];
        self.0.read_exact(&mut count).map_err(io)?;
        Ok(Some((key, u64::from_le_bytes(count))))
    }
}

/// K-way merge of run files, summing the counts of equal keys.
struct Merge {
    runs: Vec<Run>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize, u64)>>,
}

impl Merge {
    fn open(paths: &[PathBuf]) -> Result<Self> {
        let mut runs = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::with_capacity(paths.len());
        for (i, path) in paths.iter().enumerate() {
            let mut run = Run(BufReader::with_capacity(
                IO_BUFFER,
                File::open(path).map_err(io)?,
            ));
            if let Some((key, count)) = run.next()? {
                heap.push(Reverse((key, i, count)));
            }
            runs.push(run);
        }
        Ok(Self { runs, heap })
    }

    fn advance(&mut self, run: usize) -> Result<()> {
        if let Some((key, count)) = self.runs[run].next()? {
            self.heap.push(Reverse((key, run, count)));
        }
        Ok(())
    }
}

impl Iterator for Merge {
    type Item = Result<(Vec<u8>, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((key, run, mut total)) = self.heap.pop()?;
        if let Err(e) = self.advance(run) {
            return Some(Err(e));
        }
        while let Some(Reverse((next, _, _))) = self.heap.peek() {
            if *next != key {
                break;
            }
            let Some(Reverse((_, run, count))) = self.heap.pop() else {
                break;
            };
            total = match total.checked_add(count) {
                Some(t) => t,
                None => return Some(Err(IndexError::CountOverflow)),
            };
            if let Err(e) = self.advance(run) {
                return Some(Err(e));
            }
        }
        Some(Ok((key, total)))
    }
}

enum Inner {
    Memory { packed: Packed, at: usize },
    Merge(Merge),
}

/// The distinct byte strings of a [`KeySorter`] in order, with their counts. Keeps the run files
/// alive until dropped.
pub struct SortedBytes {
    inner: Inner,
    _spill: Option<Spill>,
}

impl std::fmt::Debug for SortedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SortedBytes")
    }
}

impl Iterator for SortedBytes {
    type Item = Result<(Vec<u8>, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            Inner::Memory { packed, at } => {
                let (key, count, next) = packed.next_from(*at)?;
                let key = key.to_vec();
                *at = next;
                Some(Ok((key, count)))
            }
            Inner::Merge(m) => m.next(),
        }
    }
}

/// The distinct keys of a [`KeySorter`] in order, with their counts.
#[derive(Debug)]
pub struct SortedKeys(SortedBytes);

impl Iterator for SortedKeys {
    type Item = Result<(EncodedKey, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.0.next()?;
        Some(record.and_then(|(key, count)| {
            EncodedKey::from_bytes(&key)
                .map(|k| (k, count))
                .map_err(|_| IndexError::Corrupt)
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use integrity_core::{KeySchema, KeyValue, TypeFamily};
    use proptest::prelude::*;

    use super::*;

    fn key(v: i64) -> EncodedKey {
        let schema = KeySchema::new(vec![TypeFamily::Integer]).unwrap();
        EncodedKey::encode(&schema, &[Some(KeyValue::Integer(v))]).unwrap()
    }

    fn temp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "oip-sort-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ))
    }

    fn sorted(values: &[i64], budget: usize) -> (Vec<(EncodedKey, u64)>, usize, PathBuf) {
        let dir = temp();
        let mut s = KeySorter::new(&dir, budget);
        for v in values {
            s.push(&key(*v)).unwrap();
        }
        let runs = s.runs();
        let out = s.finish().unwrap().collect::<Result<Vec<_>>>().unwrap();
        (out, runs, dir)
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config { cases: 128, ..Default::default() })]

        /// Any budget, from all in memory to one key per run (several merge passes), gives the
        /// distinct keys in order with their multiplicities.
        #[test]
        fn equals_a_counted_map(values in proptest::collection::vec(-50i64..50, 0..300), budget in 1usize..3000) {
            let mut want: BTreeMap<EncodedKey, u64> = BTreeMap::new();
            for v in &values {
                *want.entry(key(*v)).or_default() += 1;
            }
            let (got, _, dir) = sorted(&values, budget);
            prop_assert_eq!(got, want.into_iter().collect::<Vec<_>>());
            // Run files are gone once the keys are consumed and dropped.
            let leftover = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
            prop_assert_eq!(leftover, 0);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// The buffer's allocated memory, not only its contents, stays within the budget.
    #[test]
    fn allocated_memory_stays_within_the_budget() {
        let dir = temp();
        let budget = 64 * 1024;
        let mut s = KeySorter::new(&dir, budget);
        for i in 0..50_000 {
            s.push(&key(i)).unwrap();
            let allocated = s.buffer.data.capacity() + s.buffer.spans.capacity() * PER_KEY;
            assert!(allocated <= budget, "{allocated} > {budget} after {i} keys");
        }
        assert!(s.runs() > 10);
        assert_eq!(s.finish().unwrap().count(), 50_000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn many_runs_merge_in_several_passes() {
        let values: Vec<i64> = (0..400).map(|i| (i * 7919) % 100).collect();
        let (got, runs, dir) = sorted(&values, 1);
        assert!(runs > 2 * FAN_IN, "{runs} runs");
        assert_eq!(got.len(), 100);
        assert!(got.iter().all(|(_, c)| *c == 4));
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
