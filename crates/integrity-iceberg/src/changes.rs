//! What a new `main` snapshot changes, and the rows it adds and removes (spec §14 step 5,
//! §15 data-level rows).
//!
//! The diff trusts neither the snapshot summary nor the client-written entry status and snapshot
//! ids. Manifests are immutable files, so a manifest present in both the parent's and the new
//! manifest list is unchanged and skipped. The live files of the manifests that appear only in the
//! new list are compared with the live files of those that disappeared: files only in the former
//! are added, files only in the latter are removed. A file re-listed by a new manifest while still
//! live elsewhere therefore counts as added, so its keys are checked again (and duplicate keys are
//! caught by the validator).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use integrity_core::{CommitRows, Datum, LogicalType, RowBatch, Side};
use integrity_types::{ErrorCode, FieldId, SnapshotId, TableId};

use crate::classify::Operation;
use crate::io::{FileIo, ReadError};
use crate::manifest::{
    DataFile, FileContent, ManifestContent, ManifestError, ManifestFile, read_manifest,
    read_manifest_list,
};
use crate::parquet_keys::{ExtractError, extract_rows_from};

/// Why a main change could not be turned into rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InspectError {
    /// A file could not be read, or the budget ran out.
    Read(ReadError),
    /// A manifest or manifest list is invalid.
    Manifest(ManifestError),
    /// A data file could not be read.
    Extract(ExtractError),
    /// The change is outside the 0.1 capability matrix.
    Unsupported(String),
}

impl InspectError {
    /// The integrity code.
    pub fn code(&self) -> ErrorCode {
        match self {
            InspectError::Read(ReadError::BudgetExceeded { .. }) => {
                ErrorCode::ValidationBudgetExceeded
            }
            InspectError::Read(ReadError::Io(_)) => ErrorCode::StorageReadFailed,
            InspectError::Manifest(_) | InspectError::Extract(_) | InspectError::Unsupported(_) => {
                ErrorCode::UnsupportedCommitOperation
            }
        }
    }
}

impl fmt::Display for InspectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InspectError::Read(e) => write!(f, "{e}"),
            InspectError::Manifest(e) => write!(f, "{e}"),
            InspectError::Extract(e) => write!(f, "{e}"),
            InspectError::Unsupported(m) => write!(f, "{}: {m}", self.code()),
        }
    }
}

impl std::error::Error for InspectError {}

impl From<ReadError> for InspectError {
    fn from(e: ReadError) -> Self {
        InspectError::Read(e)
    }
}

impl From<ManifestError> for InspectError {
    fn from(e: ManifestError) -> Self {
        InspectError::Manifest(e)
    }
}

impl From<ExtractError> for InspectError {
    fn from(e: ExtractError) -> Self {
        match e {
            // Storage failures and the budget keep their own codes.
            ExtractError::Read(r) => InspectError::Read(r),
            other => InspectError::Extract(other),
        }
    }
}

fn unsupported(m: impl Into<String>) -> InspectError {
    InspectError::Unsupported(m.into())
}

/// Content files a snapshot adds and removes relative to its parent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileChanges {
    /// Data files live in the new snapshot only.
    pub added: Vec<DataFile>,
    /// Data files live in the parent only.
    pub removed: Vec<DataFile>,
    /// Equality delete files added by the snapshot, all on the same field set (ADR 0009).
    pub equality_deletes: Vec<DataFile>,
    /// Position deletes, when either snapshot has any (ADR 0017).
    pub positions: Option<PositionDeletes>,
}

/// The position delete files of both snapshots and the data files they may refer to (ADR 0017).
///
/// A data file's live rows are its rows minus the positions its snapshot's position delete files
/// name. The rows a commit removes are the live rows of removed data files plus the rows newly
/// deleted in kept data files; the rows it adds are the live rows of added data files plus rows
/// whose deletion it undoes (by removing a delete file).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PositionDeletes {
    /// Live position delete files of the parent snapshot.
    pub before: Vec<DataFile>,
    /// Live position delete files of the new snapshot.
    pub after: Vec<DataFile>,
    /// Paths of the position delete files the commit adds or removes.
    pub changed: BTreeSet<String>,
    /// Data files live in both snapshots, by path.
    pub kept: BTreeMap<String, DataFile>,
}

type Live = BTreeMap<String, (DataFile, Option<i64>)>;

fn manifests(io: &impl FileIo, list: Option<&str>) -> Result<Vec<ManifestFile>, InspectError> {
    match list {
        None => Ok(Vec::new()),
        Some(location) => Ok(read_manifest_list(&io.read(location)?)?),
    }
}

/// Live files of `manifests`, keyed by path, with their explicit sequence numbers; a path live
/// twice is invalid.
fn live_files(
    io: &impl FileIo,
    manifests: &[&ManifestFile],
) -> Result<BTreeMap<String, (DataFile, Option<i64>)>, InspectError> {
    let mut out = BTreeMap::new();
    for m in manifests {
        for entry in read_manifest(&io.read(&m.path)?)? {
            if !entry.status.is_live() {
                continue;
            }
            let declared = match m.content {
                ManifestContent::Data => entry.file.content == FileContent::Data,
                ManifestContent::Deletes => entry.file.content != FileContent::Data,
            };
            if !declared {
                return Err(InspectError::Manifest(ManifestError(format!(
                    "{} lists a file of the wrong content type",
                    m.path
                ))));
            }
            let path = entry.file.path.clone();
            if out
                .insert(path.clone(), (entry.file, entry.sequence_number))
                .is_some()
            {
                return Err(unsupported(format!("file listed twice: {path}")));
            }
        }
    }
    Ok(out)
}

/// Diffs the new snapshot's manifest list against its parent's (spec §15 data-level rows).
///
/// Delete files (ADR 0008, 0009): added equality deletes are returned (one field set, inherited
/// sequence number); added position deletes, removed delete files, and data files removed while
/// delete files exist are unsupported.
pub fn diff_snapshots(
    io: &impl FileIo,
    parent_list: Option<&str>,
    new_list: &str,
) -> Result<FileChanges, InspectError> {
    let parent = manifests(io, parent_list)?;
    let new = manifests(io, Some(new_list))?;
    let parent_paths: BTreeSet<&str> = parent.iter().map(|m| m.path.as_str()).collect();
    let new_paths: BTreeSet<&str> = new.iter().map(|m| m.path.as_str()).collect();
    if parent_paths.len() != parent.len() || new_paths.len() != new.len() {
        return Err(unsupported("manifest listed twice in a manifest list"));
    }

    let dropped: Vec<&ManifestFile> = parent
        .iter()
        .filter(|m| !new_paths.contains(m.path.as_str()))
        .collect();
    let introduced: Vec<&ManifestFile> = new
        .iter()
        .filter(|m| !parent_paths.contains(m.path.as_str()))
        .collect();

    let before = live_files(io, &dropped)?;
    let after = live_files(io, &introduced)?;
    let unchanged =
        |path: &str, file: &DataFile, other: &Live| other.get(path).is_some_and(|(f, _)| f == file);

    let mut changes = FileChanges::default();
    let mut position_changed = BTreeSet::new();
    for (path, (file, sequence_number)) in &after {
        if unchanged(path, file, &before) {
            continue;
        }
        match file.content {
            FileContent::Data => changes.added.push(file.clone()),
            FileContent::EqualityDeletes => {
                // A new equality delete must apply to exactly the rows that existed before this
                // commit, which an inherited sequence number guarantees (ADR 0009).
                if sequence_number.is_some() {
                    return Err(unsupported(
                        "equality delete file with an explicit sequence number",
                    ));
                }
                changes.equality_deletes.push(file.clone());
            }
            FileContent::PositionDeletes => {
                position_changed.insert(path.clone());
            }
        }
    }
    for (path, (file, _)) in &before {
        if unchanged(path, file, &after) {
            continue;
        }
        match file.content {
            FileContent::Data => changes.removed.push(file.clone()),
            FileContent::PositionDeletes => {
                position_changed.insert(path.clone());
            }
            FileContent::EqualityDeletes => {
                return Err(unsupported("commit removes equality delete files"));
            }
        }
    }

    // With delete manifests on either side, position deletes need the complete picture: every
    // live delete file (unchanged ones included) and every data file kept by the commit.
    let any_deletes = parent
        .iter()
        .chain(new.iter())
        .any(|m| m.content == ManifestContent::Deletes);
    let (parent_all, new_all) = if any_deletes {
        (
            live_files(io, &parent.iter().collect::<Vec<_>>())?,
            live_files(io, &new.iter().collect::<Vec<_>>())?,
        )
    } else {
        (Live::new(), Live::new())
    };
    let of = |live: &Live, content: FileContent| -> Vec<DataFile> {
        live.values()
            .filter(|(f, _)| f.content == content)
            .map(|(f, _)| f.clone())
            .collect()
    };
    let positions_before = of(&parent_all, FileContent::PositionDeletes);
    let positions_after = of(&new_all, FileContent::PositionDeletes);
    let equality_live = !of(&parent_all, FileContent::EqualityDeletes).is_empty()
        || !of(&new_all, FileContent::EqualityDeletes).is_empty();
    if !positions_before.is_empty() || !positions_after.is_empty() {
        if equality_live {
            return Err(unsupported(
                "position and equality deletes in the same table",
            ));
        }
        if let Some(f) = positions_before
            .iter()
            .chain(&positions_after)
            .find(|f| !f.format.eq_ignore_ascii_case("parquet") && !is_deletion_vector(f))
        {
            return Err(unsupported(format!(
                "position deletes in {} format without a complete deletion vector reference",
                f.format
            )));
        }
        let kept = new_all
            .iter()
            .filter(|(path, (f, _))| {
                f.content == FileContent::Data && unchanged(path, f, &parent_all)
            })
            .map(|(path, (f, _))| (path.clone(), f.clone()))
            .collect();
        changes.positions = Some(PositionDeletes {
            before: positions_before,
            after: positions_after,
            changed: position_changed,
            kept,
        });
    } else if (equality_live || !changes.equality_deletes.is_empty()) && !changes.removed.is_empty()
    {
        return Err(unsupported(
            "commit removes data files from a table with equality delete files",
        ));
    }
    let mut field_sets = changes.equality_deletes.iter().map(|f| {
        let mut ids = f.equality_ids.clone().unwrap_or_default();
        ids.sort_unstable();
        ids
    });
    if let Some(first) = field_sets.next()
        && (first.is_empty() || field_sets.any(|ids| ids != first))
    {
        return Err(unsupported(
            "equality delete files without, or with different, equality fields",
        ));
    }
    let kept_touched = changes.positions.iter().flat_map(|p| p.kept.values());
    for f in changes
        .added
        .iter()
        .chain(&changes.removed)
        .chain(&changes.equality_deletes)
        .chain(kept_touched)
    {
        if !f.format.eq_ignore_ascii_case("parquet") {
            return Err(unsupported(format!("data file format {}", f.format)));
        }
    }
    Ok(changes)
}

/// Reads the projected rows of every added and removed data file.
///
/// `columns` are the constrained columns with their current table types
/// (`Validator::projection`). The extracted row count of each file must equal the manifest's
/// `record_count`.
pub fn commit_rows(
    io: &impl FileIo,
    table: TableId,
    snapshot: SnapshotId,
    changes: &FileChanges,
    columns: &[(FieldId, LogicalType)],
) -> Result<CommitRows, InspectError> {
    // Rows of a data file, in file order (row position = index).
    let file_rows = |file: &DataFile| -> Result<RowBatch, InspectError> {
        let rows = extract_rows_from(io, &file.path, columns)?;
        if rows.len() as i64 != file.record_count {
            return Err(unsupported(format!(
                "{} has {} rows but its manifest says {}",
                file.path,
                rows.len(),
                file.record_count
            )));
        }
        Ok(rows)
    };
    let new_batch = || RowBatch::new(columns.iter().map(|(f, _)| *f).collect());
    let push = |batch: &mut RowBatch, row: &[Datum]| -> Result<(), InspectError> {
        batch
            .push(row.to_vec())
            .map_err(|e| unsupported(e.to_string()))
    };
    let read = |files: &[DataFile]| -> Result<RowBatch, InspectError> {
        let mut batch = new_batch();
        for file in files {
            for row in file_rows(file)?.rows() {
                push(&mut batch, row)?;
            }
        }
        Ok(batch)
    };
    let equality_deletes = match changes.equality_deletes.first() {
        None => None,
        Some(first) => {
            let fields: Vec<FieldId> = first
                .equality_ids
                .iter()
                .flatten()
                .map(|&id| FieldId(id))
                .collect();
            let typed: Vec<(FieldId, LogicalType)> = fields
                .iter()
                .map(|f| {
                    columns
                        .iter()
                        .find(|(c, _)| c == f)
                        .cloned()
                        .ok_or_else(|| unsupported(format!("equality delete on unconstrained {f}")))
                })
                .collect::<Result<_, _>>()?;
            let mut batch = RowBatch::new(fields);
            for file in &changes.equality_deletes {
                let rows = extract_rows_from(io, &file.path, &typed)?;
                if rows.len() as i64 != file.record_count {
                    return Err(unsupported(format!(
                        "{} has {} rows but its manifest says {}",
                        file.path,
                        rows.len(),
                        file.record_count
                    )));
                }
                for row in rows.rows() {
                    batch
                        .push(row.clone())
                        .map_err(|e| unsupported(e.to_string()))?;
                }
            }
            Some(batch)
        }
    };
    let Some(pd) = &changes.positions else {
        return Ok(CommitRows {
            table,
            snapshot,
            added: read(&changes.added)?,
            removed: read(&changes.removed)?,
            equality_deletes,
        });
    };

    // Position deletes (ADR 0017): deleted positions per data file, before and after.
    let mut by_file: HashMap<String, BTreeMap<String, BTreeSet<i64>>> = HashMap::new();
    let mut deleted =
        |files: &[DataFile]| -> Result<BTreeMap<String, BTreeSet<i64>>, InspectError> {
            let mut out: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
            for f in files {
                if !by_file.contains_key(&f.path) {
                    by_file.insert(f.path.clone(), position_deletes(io, f)?);
                }
                for (path, positions) in &by_file[&f.path] {
                    out.entry(path.clone()).or_default().extend(positions);
                }
            }
            Ok(out)
        };
    let before = deleted(&pd.before)?;
    let after = deleted(&pd.after)?;
    let empty = BTreeSet::new();
    let live =
        |file: &DataFile, gone: &BTreeSet<i64>, batch: &mut RowBatch| -> Result<(), InspectError> {
            let rows = file_rows(file)?;
            check_positions(file, gone, rows.len())?;
            for (pos, row) in rows.rows().iter().enumerate() {
                if !gone.contains(&(pos as i64)) {
                    push(batch, row)?;
                }
            }
            Ok(())
        };
    let mut added = new_batch();
    let mut removed = new_batch();
    for f in &changes.added {
        live(f, after.get(&f.path).unwrap_or(&empty), &mut added)?;
    }
    for f in &changes.removed {
        live(f, before.get(&f.path).unwrap_or(&empty), &mut removed)?;
    }
    // Kept data files whose deleted positions the commit changes.
    for (path, file) in &pd.kept {
        let b = before.get(path).unwrap_or(&empty);
        let a = after.get(path).unwrap_or(&empty);
        if b == a {
            continue;
        }
        let rows = file_rows(file)?;
        check_positions(file, a, rows.len())?;
        check_positions(file, b, rows.len())?;
        for pos in a.difference(b) {
            push(&mut removed, &rows.rows()[*pos as usize])?;
        }
        for pos in b.difference(a) {
            push(&mut added, &rows.rows()[*pos as usize])?;
        }
    }
    Ok(CommitRows {
        table,
        snapshot,
        added,
        removed,
        equality_deletes,
    })
}

/// Hands every live row of a whole table to `f`: the rows [`commit_rows`] would put in `added` for
/// a snapshot diffed against no parent (`diff_snapshots(io, None, list)`), in the same order, a
/// few thousand at a time. Unlike `commit_rows` it never holds the table: memory is one row group
/// of key columns plus the table's deleted positions (onboarding and rebuild, ADR 0011).
pub fn for_each_live_batch<E: From<InspectError>>(
    io: &impl FileIo,
    changes: &FileChanges,
    columns: &[(FieldId, LogicalType)],
    mut f: impl FnMut(RowBatch) -> Result<(), E>,
) -> Result<(), E> {
    if !changes.removed.is_empty()
        || changes
            .positions
            .as_ref()
            .is_some_and(|p| !p.before.is_empty() || !p.kept.is_empty())
    {
        return Err(
            unsupported("a whole-table scan needs a snapshot diffed against no parent").into(),
        );
    }
    for_each_commit_batch(io, changes, columns, |_, batch| f(batch))
}

/// Hands the rows [`commit_rows`] would read to `f`, side by side, a few thousand at a time and in
/// the same order within each side, without holding them (ADR 0019): memory is one row group of
/// key columns plus the deleted positions of both snapshots. Commits with equality deletes are
/// refused here; they go through `commit_rows`.
pub fn for_each_commit_batch<E: From<InspectError>>(
    io: &impl FileIo,
    changes: &FileChanges,
    columns: &[(FieldId, LogicalType)],
    mut f: impl FnMut(Side, RowBatch) -> Result<(), E>,
) -> Result<(), E> {
    if !changes.equality_deletes.is_empty() {
        return Err(unsupported("equality deletes are read with commit_rows").into());
    }
    let empty = BTreeSet::new();
    let (before, after) = match &changes.positions {
        None => (HashMap::new(), HashMap::new()),
        Some(pd) => {
            let mut by_file: HashMap<String, BTreeMap<String, BTreeSet<i64>>> = HashMap::new();
            let mut deleted =
                |files: &[DataFile]| -> Result<HashMap<String, BTreeSet<i64>>, InspectError> {
                    let mut out: HashMap<String, BTreeSet<i64>> = HashMap::new();
                    for d in files {
                        if !by_file.contains_key(&d.path) {
                            by_file.insert(d.path.clone(), position_deletes(io, d)?);
                        }
                        for (path, positions) in &by_file[&d.path] {
                            out.entry(path.clone()).or_default().extend(positions);
                        }
                    }
                    Ok(out)
                };
            (deleted(&pd.before)?, deleted(&pd.after)?)
        }
    };
    // Live rows of whole data files.
    for (side, files, gone) in [
        (Side::Added, &changes.added, &after),
        (Side::Removed, &changes.removed, &before),
    ] {
        for file in files {
            let gone = gone.get(&file.path).unwrap_or(&empty);
            stream_file(io, file, columns, &[gone], &mut |batch, start| {
                let live = without(batch, start, gone, columns)?;
                if live.is_empty() {
                    Ok(())
                } else {
                    f(side, live).map_err(Failed::Caller)
                }
            })?;
        }
    }
    // Kept data files whose deleted positions the commit changes.
    if let Some(pd) = &changes.positions {
        for (path, file) in &pd.kept {
            let b = before.get(path).unwrap_or(&empty);
            let a = after.get(path).unwrap_or(&empty);
            if b == a {
                continue;
            }
            let newly: BTreeSet<i64> = a.difference(b).copied().collect();
            let restored: BTreeSet<i64> = b.difference(a).copied().collect();
            stream_file(io, file, columns, &[a, b], &mut |batch, start| {
                let removed = only(&batch, start, &newly, columns)?;
                let added = only(&batch, start, &restored, columns)?;
                if !removed.is_empty() {
                    f(Side::Removed, removed).map_err(Failed::Caller)?;
                }
                if !added.is_empty() {
                    f(Side::Added, added).map_err(Failed::Caller)?;
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}

/// The caller's error, or one of reading a file.
enum Failed<E> {
    Inspect(InspectError),
    Caller(E),
}

impl<E> From<ExtractError> for Failed<E> {
    fn from(e: ExtractError) -> Self {
        Failed::Inspect(e.into())
    }
}

impl<E> From<InspectError> for Failed<E> {
    fn from(e: InspectError) -> Self {
        Failed::Inspect(e)
    }
}

/// Streams one data file to `g(batch, position of its first row)`, then checks its row count
/// against the manifest and that every position of every set in `deleted` is within the file.
fn stream_file<E: From<InspectError>>(
    io: &impl FileIo,
    file: &DataFile,
    columns: &[(FieldId, LogicalType)],
    deleted: &[&BTreeSet<i64>],
    g: &mut dyn FnMut(RowBatch, i64) -> Result<(), Failed<E>>,
) -> Result<(), E> {
    let mut rows: usize = 0;
    let read = crate::parquet_keys::for_each_batch_from(io, &file.path, columns, |batch| {
        let start = rows as i64;
        rows += batch.len();
        g(batch, start)
    });
    match read {
        Ok(()) => {}
        Err(Failed::Inspect(e)) => return Err(e.into()),
        Err(Failed::Caller(e)) => return Err(e),
    }
    if rows as i64 != file.record_count {
        return Err(unsupported(format!(
            "{} has {rows} rows but its manifest says {}",
            file.path, file.record_count
        ))
        .into());
    }
    for positions in deleted {
        check_positions(file, positions, rows)?;
    }
    Ok(())
}

/// The positions of `set` that fall in `batch`, whose first row is at `start`, relative to it.
fn positions_in(batch: &RowBatch, start: i64, set: &BTreeSet<i64>) -> Vec<usize> {
    set.range(start..start + batch.len() as i64)
        .map(|p| (p - start) as usize)
        .collect()
}

/// `batch` without the rows at positions in `gone` (the batch itself if none is).
fn without<E>(
    batch: RowBatch,
    start: i64,
    gone: &BTreeSet<i64>,
    columns: &[(FieldId, LogicalType)],
) -> Result<RowBatch, Failed<E>> {
    let skip = positions_in(&batch, start, gone);
    if skip.is_empty() {
        return Ok(batch);
    }
    let mut out = RowBatch::new(columns.iter().map(|(c, _)| *c).collect());
    let mut skip = skip.into_iter().peekable();
    for (i, row) in batch.rows().iter().enumerate() {
        if skip.peek() == Some(&i) {
            skip.next();
        } else {
            push(&mut out, row)?;
        }
    }
    Ok(out)
}

/// The rows of `batch` at positions in `keep`.
fn only<E>(
    batch: &RowBatch,
    start: i64,
    keep: &BTreeSet<i64>,
    columns: &[(FieldId, LogicalType)],
) -> Result<RowBatch, Failed<E>> {
    let mut out = RowBatch::new(columns.iter().map(|(c, _)| *c).collect());
    for i in positions_in(batch, start, keep) {
        push(&mut out, &batch.rows()[i])?;
    }
    Ok(out)
}

fn push<E>(batch: &mut RowBatch, row: &[Datum]) -> Result<(), Failed<E>> {
    batch
        .push(row.to_vec())
        .map_err(|e| Failed::Inspect(unsupported(e.to_string())))
}

/// Field ids of `file_path` and `pos` in position delete files (Iceberg spec, reserved ids).
const DELETE_FILE_PATH: FieldId = FieldId(2_147_483_546);
const DELETE_POS: FieldId = FieldId(2_147_483_545);

/// A v3 deletion vector: a Puffin blob for exactly one data file.
fn is_deletion_vector(f: &DataFile) -> bool {
    f.format.eq_ignore_ascii_case("puffin")
        && f.referenced_data_file.is_some()
        && f.content_offset.is_some()
        && f.content_size_in_bytes.is_some()
}

/// Reads a position delete file or deletion vector: deleted positions per data file path.
fn position_deletes(
    io: &impl FileIo,
    file: &DataFile,
) -> Result<BTreeMap<String, BTreeSet<i64>>, InspectError> {
    if let (true, Some(target), Some(offset), Some(size)) = (
        is_deletion_vector(file),
        &file.referenced_data_file,
        file.content_offset,
        file.content_size_in_bytes,
    ) {
        // Only the blob is read, not the whole Puffin file.
        let (start, len) = (
            u64::try_from(offset).map_err(|_| unsupported("negative deletion vector offset"))?,
            u64::try_from(size).map_err(|_| unsupported("negative deletion vector size"))?,
        );
        let end = start
            .checked_add(len)
            .ok_or_else(|| unsupported("deletion vector range overflows"))?;
        let positions = crate::deletion_vector::decode(&io.read_range(&file.path, start..end)?)
            .map_err(|e| unsupported(format!("{}: {e}", file.path)))?;
        if positions.len() as i64 != file.record_count {
            return Err(unsupported(format!(
                "{} deletes {} rows but its manifest says {}",
                file.path,
                positions.len(),
                file.record_count
            )));
        }
        return Ok(BTreeMap::from([(target.clone(), positions)]));
    }
    let rows = extract_rows_from(
        io,
        &file.path,
        &[
            (DELETE_FILE_PATH, LogicalType::String),
            (DELETE_POS, LogicalType::Long),
        ],
    )?;
    if rows.len() as i64 != file.record_count {
        return Err(unsupported(format!(
            "{} has {} rows but its manifest says {}",
            file.path,
            rows.len(),
            file.record_count
        )));
    }
    let mut out: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for row in rows.rows() {
        match (&row[0], &row[1]) {
            (
                Datum::Value(integrity_core::KeyValue::String(path)),
                Datum::Value(integrity_core::KeyValue::Integer(pos)),
            ) => {
                out.entry(path.clone()).or_default().insert(*pos);
            }
            _ => {
                return Err(unsupported(format!(
                    "{} is not a valid position delete file",
                    file.path
                )));
            }
        }
    }
    Ok(out)
}

/// Every deleted position must name a row of the file.
fn check_positions(
    file: &DataFile,
    positions: &BTreeSet<i64>,
    rows: usize,
) -> Result<(), InspectError> {
    match positions.iter().find(|&&p| p < 0 || p as usize >= rows) {
        Some(p) => Err(unsupported(format!(
            "position delete {p} beyond the {rows} rows of {}",
            file.path
        ))),
        None => Ok(()),
    }
}

/// Checks the declared operation against what the commit actually does (spec §15): `append`
/// removes nothing, `delete` adds nothing, and `replace` must leave the multiset of projected rows,
/// hence every key multiset, unchanged.
pub fn check_operation(operation: Operation, rows: &CommitRows) -> Result<(), InspectError> {
    check_sides(
        operation,
        !rows.added.is_empty(),
        !rows.removed.is_empty(),
        || {
            let mut counts: HashMap<&[Datum], i64> = HashMap::new();
            for row in rows.added.rows() {
                *counts.entry(row.as_slice()).or_insert(0) += 1;
            }
            for row in rows.removed.rows() {
                *counts.entry(row.as_slice()).or_insert(0) -= 1;
            }
            Ok(counts.values().all(|&c| c == 0))
        },
    )
}

/// [`check_operation`] from what a commit's rows are, for callers that stream them (ADR 0019):
/// whether it adds rows, whether it removes rows, and (asked only for `replace`) whether both
/// sides hold the same rows.
pub fn check_sides<E: From<InspectError>>(
    operation: Operation,
    adds: bool,
    removes: bool,
    same_rows: impl FnOnce() -> Result<bool, E>,
) -> Result<(), E> {
    match operation {
        Operation::Append if removes => {
            Err(unsupported("append snapshot removes data files").into())
        }
        Operation::Delete if adds => Err(unsupported("delete snapshot adds data files").into()),
        Operation::Replace => {
            if same_rows()? {
                Ok(())
            } else {
                Err(unsupported("replace snapshot changes constrained data").into())
            }
        }
        Operation::Append | Operation::Delete | Operation::Overwrite => Ok(()),
    }
}
