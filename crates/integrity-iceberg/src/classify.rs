//! Commit classification against the 0.1 capability matrix (spec §15, update level).
//!
//! Decides from the current table metadata and the request alone, before any file is read:
//! whether the commit leaves `main`'s data untouched (pass-through), moves `main` to one new child
//! snapshot (data validation follows), is stale (the client must refresh and retry), or is
//! unsupported. Data-level rows of §15 (append, overwrite, replace, delete, delete files) are decided
//! later from the manifest diff; the snapshot `operation` is only a hint here.

use std::collections::BTreeSet;
use std::fmt;

use integrity_core::LogicalType;
use integrity_types::{ErrorCode, FieldId, SnapshotId};

use crate::metadata::{FieldLookup, MAIN, Schema, TableMetadata, logical_type};
use crate::request::{CommitRequest, Requirement, Update};

/// The snapshot `operation` from the summary (a hint, spec §15).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// `append`.
    Append,
    /// `replace` (compaction, manifest rewrites).
    Replace,
    /// `overwrite`.
    Overwrite,
    /// `delete`.
    Delete,
}

/// `main` moves to a new child snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MainChange {
    /// `main` before the commit (`None` for an empty table).
    pub parent: Option<SnapshotId>,
    /// The new snapshot.
    pub snapshot: SnapshotId,
    /// Declared operation.
    pub operation: Operation,
    /// The new snapshot's manifest list.
    pub manifest_list: String,
    /// Index of its `add-snapshot` update (where the certificate goes).
    pub update_index: usize,
}

/// How the commit is handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classification {
    /// `main`'s data does not change (metadata updates, other refs, expiry). Forwarded without
    /// data validation; snapshots on other refs are uncertified.
    PassThrough,
    /// `main` moves to a new snapshot whose data must be validated.
    MainChange(MainChange),
}

/// Why a commit is not forwarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// The client's base is out of date; it should refresh and retry (HTTP 409).
    Stale(String),
    /// The commit contains a change the Plane cannot prove (spec §15).
    Unsupported(String),
}

impl Rejection {
    /// The integrity code.
    pub fn code(&self) -> ErrorCode {
        match self {
            Rejection::Stale(_) => ErrorCode::StaleBaseSnapshot,
            Rejection::Unsupported(_) => ErrorCode::UnsupportedCommitOperation,
        }
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rejection::Stale(m) => write!(f, "{}: {m}", ErrorCode::StaleBaseSnapshot),
            Rejection::Unsupported(m) => {
                write!(f, "{}: {m}", ErrorCode::UnsupportedCommitOperation)
            }
        }
    }
}

impl std::error::Error for Rejection {}

fn unsupported(m: impl Into<String>) -> Rejection {
    Rejection::Unsupported(m.into())
}

fn stale(m: impl Into<String>) -> Rejection {
    Rejection::Stale(m.into())
}

/// Checks the request's requirements against current metadata (spec §14 step 3).
pub fn check_requirements(meta: &TableMetadata, request: &CommitRequest) -> Result<(), Rejection> {
    for req in &request.requirements {
        let ok = match req {
            Requirement::AssertCreate => {
                return Err(unsupported("assert-create on an existing table"));
            }
            Requirement::AssertTableUuid(uuid) => *uuid == meta.table_uuid,
            Requirement::AssertRefSnapshotId {
                ref_name,
                snapshot_id,
            } => {
                let current = if ref_name == MAIN {
                    meta.main_snapshot_id().map(|s| s.0)
                } else {
                    meta.refs.get(ref_name).map(|r| r.snapshot_id)
                };
                current == *snapshot_id
            }
            Requirement::AssertLastAssignedFieldId(id) => meta.last_column_id == Some(*id),
            Requirement::AssertCurrentSchemaId(id) => {
                meta.current_schema().map(|s| i64::from(s.schema_id)) == Some(*id)
            }
            Requirement::AssertLastAssignedPartitionId(id) => meta.last_partition_id == Some(*id),
            Requirement::AssertDefaultSpecId(id) => meta.default_spec_id == Some(*id),
            Requirement::AssertDefaultSortOrderId(id) => meta.default_sort_order_id == Some(*id),
            Requirement::Unknown(kind) => {
                return Err(unsupported(format!("unknown requirement `{kind}`")));
            }
        };
        if !ok {
            return Err(stale(format!("requirement failed: {req:?}")));
        }
    }
    Ok(())
}

/// Whether a constrained column may change type from `old` to `new` (spec §15: same type,
/// `int → long`, decimal precision widening, and `float → double` for NOT NULL-only columns).
fn allowed_promotion(old: &LogicalType, new: &LogicalType) -> bool {
    match (old, new) {
        (a, b) if a == b => true,
        (LogicalType::Int, LogicalType::Long) | (LogicalType::Float, LogicalType::Double) => true,
        (
            LogicalType::Decimal {
                precision: p1,
                scale: s1,
            },
            LogicalType::Decimal {
                precision: p2,
                scale: s2,
            },
        ) => s1 == s2 && p2 >= p1,
        _ => false,
    }
}

/// Rejects a schema that would change any constrained field other than by an allowed promotion.
fn check_schema(
    old: &Schema,
    new: &Schema,
    constrained: &BTreeSet<FieldId>,
) -> Result<(), Rejection> {
    for &field in constrained {
        let FieldLookup::TopLevel(before) = old.lookup(field) else {
            return Err(unsupported(format!(
                "constrained {field} is not a top-level column"
            )));
        };
        let FieldLookup::TopLevel(after) = new.lookup(field) else {
            return Err(unsupported(format!(
                "schema change removes or moves constrained {field}"
            )));
        };
        if after.initial_default.is_some() {
            return Err(unsupported(format!(
                "constrained {field} has an initial-default"
            )));
        }
        let (a, b) = (
            logical_type(&before.field_type),
            logical_type(&after.field_type),
        );
        if !allowed_promotion(&a, &b) {
            return Err(unsupported(format!(
                "schema change alters constrained {field} from {a:?} to {b:?}"
            )));
        }
    }
    Ok(())
}

/// Classifies a commit. `constrained` holds every field of an enforced constraint on the table.
pub fn classify(
    meta: &TableMetadata,
    request: &CommitRequest,
    constrained: &BTreeSet<FieldId>,
) -> Result<Classification, Rejection> {
    if meta.format_version > 3 || meta.format_version == 0 {
        return Err(unsupported(format!(
            "format version {}",
            meta.format_version
        )));
    }
    let current_schema = meta
        .current_schema()
        .ok_or_else(|| unsupported("table has no current schema"))?;

    let mut added_schemas: Vec<&Schema> = Vec::new();
    let mut added_snapshots = Vec::new();
    let mut main_moves = Vec::new();

    for (index, update) in request.updates.iter().enumerate() {
        match update {
            Update::AssignUuid => return Err(unsupported("assign-uuid on an existing table")),
            Update::UpgradeFormatVersion(v) if *v > 3 => {
                return Err(unsupported(format!("upgrade to format version {v}")));
            }
            Update::UpgradeFormatVersion(_) | Update::RemoveSnapshots | Update::Metadata(_) => {}
            Update::AddSchema(schema) => added_schemas.push(schema),
            Update::SetCurrentSchema(id) => {
                let target = if *id == -1 {
                    added_schemas.last().copied()
                } else {
                    added_schemas
                        .iter()
                        .copied()
                        .chain(meta.schemas.iter())
                        .find(|s| i64::from(s.schema_id) == *id)
                };
                let target =
                    target.ok_or_else(|| unsupported(format!("unknown schema id {id}")))?;
                check_schema(current_schema, target, constrained)?;
            }
            Update::AddSnapshot(snapshot) => {
                if meta.snapshot(SnapshotId(snapshot.snapshot_id)).is_some() {
                    return Err(unsupported("add-snapshot reuses an existing snapshot id"));
                }
                added_snapshots.push((index, snapshot));
            }
            Update::SetSnapshotRef {
                ref_name,
                snapshot_id,
                ref_type,
            } if ref_name == MAIN => {
                if ref_type != "branch" {
                    return Err(unsupported("main must be a branch"));
                }
                main_moves.push(*snapshot_id);
            }
            Update::SetSnapshotRef { .. } => {}
            Update::RemoveSnapshotRef(name) if name == MAIN => {
                return Err(unsupported("remove-snapshot-ref main"));
            }
            Update::RemoveSnapshotRef(_) => {}
            Update::Unknown(action) => {
                return Err(unsupported(format!("unknown update action `{action}`")));
            }
        }
    }

    let target = match main_moves.as_slice() {
        [] => return Ok(Classification::PassThrough),
        [one] => *one,
        _ => return Err(unsupported("main moves more than once in one commit")),
    };
    let current_main = meta.main_snapshot_id();
    if current_main == Some(SnapshotId(target)) {
        return Ok(Classification::PassThrough);
    }
    let Some(&(update_index, snapshot)) = added_snapshots
        .iter()
        .find(|(_, s)| s.snapshot_id == target)
    else {
        return Err(unsupported(
            "main moves to an existing snapshot (rollback, cherry-pick or set-current-snapshot)",
        ));
    };
    if snapshot.parent_snapshot_id.map(SnapshotId) != current_main {
        return Err(stale(
            "new main snapshot is not a child of the current main snapshot",
        ));
    }
    let operation = match snapshot.summary.get("operation").map(String::as_str) {
        Some("append") => Operation::Append,
        Some("replace") => Operation::Replace,
        Some("overwrite") => Operation::Overwrite,
        Some("delete") => Operation::Delete,
        other => return Err(unsupported(format!("snapshot operation {other:?}"))),
    };
    let manifest_list = snapshot
        .manifest_list
        .clone()
        .ok_or_else(|| unsupported("snapshot without a manifest list (v1 embedded manifests)"))?;
    Ok(Classification::MainChange(MainChange {
        parent: current_main,
        snapshot: SnapshotId(target),
        operation,
        manifest_list,
        update_index,
    }))
}
