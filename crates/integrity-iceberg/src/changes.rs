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

use integrity_core::{CommitRows, Datum, LogicalType, RowBatch};
use integrity_types::{ErrorCode, FieldId, SnapshotId, TableId};

use crate::classify::Operation;
use crate::io::{FileIo, ReadError};
use crate::manifest::{
    DataFile, FileContent, ManifestContent, ManifestError, ManifestFile, read_manifest,
    read_manifest_list,
};
use crate::parquet_keys::{ExtractError, extract_rows};

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
            InspectError::Read(ReadError::Io(_))
            | InspectError::Manifest(_)
            | InspectError::Extract(_)
            | InspectError::Unsupported(_) => ErrorCode::UnsupportedCommitOperation,
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
        InspectError::Extract(e)
    }
}

fn unsupported(m: impl Into<String>) -> InspectError {
    InspectError::Unsupported(m.into())
}

/// Data files a snapshot adds and removes relative to its parent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileChanges {
    /// Data files live in the new snapshot only.
    pub added: Vec<DataFile>,
    /// Data files live in the parent only.
    pub removed: Vec<DataFile>,
}

fn manifests(io: &impl FileIo, list: Option<&str>) -> Result<Vec<ManifestFile>, InspectError> {
    match list {
        None => Ok(Vec::new()),
        Some(location) => Ok(read_manifest_list(&io.read(location)?)?),
    }
}

/// Live files of `manifests`, keyed by path; a path live twice is invalid.
fn live_files(
    io: &impl FileIo,
    manifests: &[&ManifestFile],
) -> Result<BTreeMap<String, DataFile>, InspectError> {
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
            if out.insert(path.clone(), entry.file).is_some() {
                return Err(unsupported(format!("file listed twice: {path}")));
            }
        }
    }
    Ok(out)
}

/// Diffs the new snapshot's manifest list against its parent's (spec §15 data-level rows).
///
/// Delete files are outside this step: any delete file added or removed, and any data file
/// removed while the parent has delete manifests, is unsupported.
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

    let mut changes = FileChanges::default();
    for (path, file) in &after {
        if before.get(path) == Some(file) {
            continue;
        }
        if file.content != FileContent::Data {
            return Err(unsupported(match file.content {
                FileContent::EqualityDeletes => "commit adds equality delete files",
                _ => "commit adds position deletes or deletion vectors (merge-on-read)",
            }));
        }
        changes.added.push(file.clone());
    }
    for (path, file) in &before {
        if after.get(path) == Some(file) {
            continue;
        }
        if file.content != FileContent::Data {
            return Err(unsupported("commit removes delete files"));
        }
        changes.removed.push(file.clone());
    }

    let parent_has_deletes = parent.iter().any(|m| m.content == ManifestContent::Deletes);
    if parent_has_deletes && !changes.removed.is_empty() {
        return Err(unsupported(
            "commit removes data files from a table with delete files",
        ));
    }
    for f in changes.added.iter().chain(&changes.removed) {
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
    let read = |files: &[DataFile]| -> Result<RowBatch, InspectError> {
        let mut batch = RowBatch::new(columns.iter().map(|(f, _)| *f).collect());
        for file in files {
            let rows = extract_rows(io.read(&file.path)?, columns)?;
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
        Ok(batch)
    };
    Ok(CommitRows {
        table,
        snapshot,
        added: read(&changes.added)?,
        removed: read(&changes.removed)?,
    })
}

/// Checks the declared operation against what the commit actually does (spec §15): `append`
/// removes nothing, `delete` adds nothing, and `replace` must leave the multiset of projected rows,
/// hence every key multiset, unchanged.
pub fn check_operation(operation: Operation, rows: &CommitRows) -> Result<(), InspectError> {
    match operation {
        Operation::Append if !rows.removed.is_empty() => {
            Err(unsupported("append snapshot removes data files"))
        }
        Operation::Delete if !rows.added.is_empty() => {
            Err(unsupported("delete snapshot adds data files"))
        }
        Operation::Replace => {
            let mut counts: HashMap<&[Datum], i64> = HashMap::new();
            for row in rows.added.rows() {
                *counts.entry(row.as_slice()).or_insert(0) += 1;
            }
            for row in rows.removed.rows() {
                *counts.entry(row.as_slice()).or_insert(0) -= 1;
            }
            if counts.values().all(|&c| c == 0) {
                Ok(())
            } else {
                Err(unsupported("replace snapshot changes constrained data"))
            }
        }
        Operation::Append | Operation::Delete | Operation::Overwrite => Ok(()),
    }
}
