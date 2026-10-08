//! Manifest lists and manifests (Iceberg spec "Manifest Lists", "Manifests"), read from Avro.
//!
//! Only the fields the Plane needs are read, by name. Optional fields are Avro unions with null.
//! Format v1 files lack `content`; it then defaults to data.

use std::fmt;

use apache_avro::types::Value;
use bytes::Bytes;

/// What a manifest tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestContent {
    /// Data files.
    Data,
    /// Delete files.
    Deletes,
}

/// One entry of a manifest list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestFile {
    /// Location of the manifest.
    pub path: String,
    /// Data or delete manifest.
    pub content: ManifestContent,
    /// Snapshot that wrote the manifest (client-declared; never trusted for classification).
    pub added_snapshot_id: i64,
}

/// Manifest entry status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// Live, carried over.
    Existing,
    /// Live, added by the manifest's snapshot.
    Added,
    /// Removed by the manifest's snapshot; not live.
    Deleted,
}

impl EntryStatus {
    /// Whether the file is part of the snapshot.
    pub fn is_live(self) -> bool {
        matches!(self, EntryStatus::Existing | EntryStatus::Added)
    }
}

/// What a content file holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileContent {
    /// Rows.
    Data,
    /// Position deletes or deletion vectors.
    PositionDeletes,
    /// Equality deletes.
    EqualityDeletes,
}

/// A data or delete file referenced by a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFile {
    /// Content type.
    pub content: FileContent,
    /// Location.
    pub path: String,
    /// `PARQUET`, `AVRO`, `ORC`, `PUFFIN`.
    pub format: String,
    /// Number of records.
    pub record_count: i64,
    /// Field ids of an equality delete.
    pub equality_ids: Option<Vec<i32>>,
}

/// A manifest entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Status.
    pub status: EntryStatus,
    /// Explicit data sequence number; `None` when inherited from the manifest (v2+).
    pub sequence_number: Option<i64>,
    /// The file.
    pub file: DataFile,
}

/// A manifest or manifest list that is not valid Avro or lacks required fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestError(pub String);

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid manifest: {}", self.0)
    }
}

impl std::error::Error for ManifestError {}

fn err(m: impl Into<String>) -> ManifestError {
    ManifestError(m.into())
}

fn records(bytes: &Bytes) -> Result<Vec<Vec<(String, Value)>>, ManifestError> {
    let reader = apache_avro::Reader::new(&bytes[..]).map_err(|e| err(e.to_string()))?;
    reader
        .map(|v| match v.map_err(|e| err(e.to_string()))? {
            Value::Record(fields) => Ok(fields),
            other => Err(err(format!("expected a record, got {other:?}"))),
        })
        .collect()
}

/// A field by name, unwrapping `[null, T]` unions; `None` if absent or null.
fn get<'v>(record: &'v [(String, Value)], name: &str) -> Option<&'v Value> {
    let v = record.iter().find(|(n, _)| n == name).map(|(_, v)| v)?;
    match v {
        Value::Union(_, inner) => match inner.as_ref() {
            Value::Null => None,
            inner => Some(inner),
        },
        Value::Null => None,
        v => Some(v),
    }
}

fn int(record: &[(String, Value)], name: &str) -> Result<Option<i64>, ManifestError> {
    match get(record, name) {
        None => Ok(None),
        Some(Value::Int(i)) => Ok(Some(i64::from(*i))),
        Some(Value::Long(i)) => Ok(Some(*i)),
        Some(other) => Err(err(format!("`{name}` is not an integer: {other:?}"))),
    }
}

fn required_int(record: &[(String, Value)], name: &str) -> Result<i64, ManifestError> {
    int(record, name)?.ok_or_else(|| err(format!("missing `{name}`")))
}

fn string(record: &[(String, Value)], name: &str) -> Result<String, ManifestError> {
    match get(record, name) {
        Some(Value::String(s)) => Ok(s.clone()),
        other => Err(err(format!("`{name}` is not a string: {other:?}"))),
    }
}

/// Reads a manifest list.
pub fn read_manifest_list(bytes: &Bytes) -> Result<Vec<ManifestFile>, ManifestError> {
    records(bytes)?
        .iter()
        .map(|r| {
            let content = match int(r, "content")? {
                None | Some(0) => ManifestContent::Data,
                Some(1) => ManifestContent::Deletes,
                Some(c) => return Err(err(format!("unknown manifest content {c}"))),
            };
            Ok(ManifestFile {
                path: string(r, "manifest_path")?,
                content,
                added_snapshot_id: required_int(r, "added_snapshot_id")?,
            })
        })
        .collect()
}

/// Reads a manifest's entries.
pub fn read_manifest(bytes: &Bytes) -> Result<Vec<ManifestEntry>, ManifestError> {
    records(bytes)?
        .iter()
        .map(|r| {
            let status = match required_int(r, "status")? {
                0 => EntryStatus::Existing,
                1 => EntryStatus::Added,
                2 => EntryStatus::Deleted,
                s => return Err(err(format!("unknown entry status {s}"))),
            };
            let Some(Value::Record(df)) = get(r, "data_file") else {
                return Err(err("missing `data_file`"));
            };
            let content = match int(df, "content")? {
                None | Some(0) => FileContent::Data,
                Some(1) => FileContent::PositionDeletes,
                Some(2) => FileContent::EqualityDeletes,
                Some(c) => return Err(err(format!("unknown file content {c}"))),
            };
            let equality_ids = match get(df, "equality_ids") {
                None => None,
                Some(Value::Array(ids)) => Some(
                    ids.iter()
                        .map(|v| match v {
                            Value::Int(i) => Ok(*i),
                            other => Err(err(format!("equality id {other:?}"))),
                        })
                        .collect::<Result<_, _>>()?,
                ),
                Some(other) => return Err(err(format!("equality_ids {other:?}"))),
            };
            Ok(ManifestEntry {
                status,
                sequence_number: int(r, "sequence_number")?,
                file: DataFile {
                    content,
                    path: string(df, "file_path")?,
                    format: string(df, "file_format")?,
                    record_count: required_int(df, "record_count")?,
                    equality_ids,
                },
            })
        })
        .collect()
}
