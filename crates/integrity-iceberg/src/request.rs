//! Iceberg REST `CommitTableRequest` (requirements and updates), parsed from JSON.
//!
//! The original JSON is kept so the gateway can forward the request unchanged except for the
//! certificate fields it adds to the new snapshot's summary.

use std::fmt;

use serde_json::Value;

use crate::metadata::{Schema, Snapshot};

/// A parsed commit request plus its original JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitRequest {
    raw: Value,
    /// Preconditions the catalog must check.
    pub requirements: Vec<Requirement>,
    /// Changes, in order.
    pub updates: Vec<Update>,
}

/// A table requirement (REST spec `TableRequirement`).
#[derive(Debug, Clone, PartialEq)]
pub enum Requirement {
    /// The table must not exist yet.
    AssertCreate,
    /// The table's UUID.
    AssertTableUuid(String),
    /// A ref points at a snapshot, or does not exist (`None`).
    AssertRefSnapshotId {
        /// Ref name.
        ref_name: String,
        /// Expected snapshot.
        snapshot_id: Option<i64>,
    },
    /// Highest assigned field id.
    AssertLastAssignedFieldId(i64),
    /// Current schema id.
    AssertCurrentSchemaId(i64),
    /// Highest assigned partition field id.
    AssertLastAssignedPartitionId(i64),
    /// Default partition spec id.
    AssertDefaultSpecId(i64),
    /// Default sort order id.
    AssertDefaultSortOrderId(i64),
    /// A requirement type this version does not know.
    Unknown(String),
}

/// A table update (REST spec `TableUpdate`).
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    /// `assign-uuid`.
    AssignUuid,
    /// `upgrade-format-version`.
    UpgradeFormatVersion(i64),
    /// `add-schema`.
    AddSchema(Schema),
    /// `set-current-schema`; `-1` means the schema added last in this request.
    SetCurrentSchema(i64),
    /// `add-snapshot`.
    AddSnapshot(Snapshot),
    /// `set-snapshot-ref`.
    SetSnapshotRef {
        /// Ref name.
        ref_name: String,
        /// Target snapshot.
        snapshot_id: i64,
        /// `branch` or `tag`.
        ref_type: String,
    },
    /// `remove-snapshot-ref`.
    RemoveSnapshotRef(String),
    /// `remove-snapshots` (expiry).
    RemoveSnapshots,
    /// An update that cannot change the rows of any snapshot (properties, specs, sort orders,
    /// statistics, location, encryption keys, schema/spec removal).
    Metadata(String),
    /// An action this version does not know.
    Unknown(String),
}

/// The request JSON does not have the shape the REST spec requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MalformedRequest(pub String);

impl fmt::Display for MalformedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed commit request: {}", self.0)
    }
}

impl std::error::Error for MalformedRequest {}

fn bad(what: impl Into<String>) -> MalformedRequest {
    MalformedRequest(what.into())
}

fn str_field<'a>(v: &'a Value, key: &str) -> Result<&'a str, MalformedRequest> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| bad(format!("missing string `{key}`")))
}

fn int_field(v: &Value, key: &str) -> Result<i64, MalformedRequest> {
    v.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| bad(format!("missing integer `{key}`")))
}

const METADATA_ONLY: &[&str] = &[
    "add-spec",
    "set-default-spec",
    "add-sort-order",
    "set-default-sort-order",
    "set-location",
    "set-properties",
    "remove-properties",
    "set-statistics",
    "remove-statistics",
    "set-partition-statistics",
    "remove-partition-statistics",
    "remove-partition-specs",
    "remove-schemas",
    "add-encryption-key",
    "remove-encryption-key",
];

impl CommitRequest {
    /// Parses a request body.
    pub fn from_json(raw: Value) -> Result<Self, MalformedRequest> {
        let requirements = raw
            .get("requirements")
            .and_then(Value::as_array)
            .ok_or_else(|| bad("missing `requirements` array"))?
            .iter()
            .map(parse_requirement)
            .collect::<Result<_, _>>()?;
        let updates = raw
            .get("updates")
            .and_then(Value::as_array)
            .ok_or_else(|| bad("missing `updates` array"))?
            .iter()
            .map(parse_update)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            raw,
            requirements,
            updates,
        })
    }

    /// The original JSON.
    pub fn json(&self) -> &Value {
        &self.raw
    }

    /// Adds `fields` to the summary of the snapshot in update number `index` (which must be an
    /// `add-snapshot`), in both the parsed and the JSON form.
    pub fn set_summary_fields(
        &mut self,
        index: usize,
        fields: &[(&str, String)],
    ) -> Result<(), MalformedRequest> {
        let Some(Update::AddSnapshot(snapshot)) = self.updates.get_mut(index) else {
            return Err(bad("update is not add-snapshot"));
        };
        let summary = self
            .raw
            .get_mut("updates")
            .and_then(|u| u.get_mut(index))
            .and_then(|u| u.get_mut("snapshot"))
            .and_then(|s| s.get_mut("summary"))
            .and_then(Value::as_object_mut)
            .ok_or_else(|| bad("add-snapshot without summary"))?;
        for (key, value) in fields {
            summary.insert((*key).to_owned(), Value::String(value.clone()));
            snapshot.summary.insert((*key).to_owned(), value.clone());
        }
        Ok(())
    }
}

fn parse_requirement(v: &Value) -> Result<Requirement, MalformedRequest> {
    let kind = str_field(v, "type")?;
    Ok(match kind {
        "assert-create" => Requirement::AssertCreate,
        "assert-table-uuid" => Requirement::AssertTableUuid(str_field(v, "uuid")?.to_owned()),
        "assert-ref-snapshot-id" => Requirement::AssertRefSnapshotId {
            ref_name: str_field(v, "ref")?.to_owned(),
            snapshot_id: match v.get("snapshot-id") {
                None | Some(Value::Null) => None,
                Some(id) => Some(
                    id.as_i64()
                        .ok_or_else(|| bad("snapshot-id not an integer"))?,
                ),
            },
        },
        "assert-last-assigned-field-id" => {
            Requirement::AssertLastAssignedFieldId(int_field(v, "last-assigned-field-id")?)
        }
        "assert-current-schema-id" => {
            Requirement::AssertCurrentSchemaId(int_field(v, "current-schema-id")?)
        }
        "assert-last-assigned-partition-id" => {
            Requirement::AssertLastAssignedPartitionId(int_field(v, "last-assigned-partition-id")?)
        }
        "assert-default-spec-id" => {
            Requirement::AssertDefaultSpecId(int_field(v, "default-spec-id")?)
        }
        "assert-default-sort-order-id" => {
            Requirement::AssertDefaultSortOrderId(int_field(v, "default-sort-order-id")?)
        }
        other => Requirement::Unknown(other.to_owned()),
    })
}

fn parse_update(v: &Value) -> Result<Update, MalformedRequest> {
    let action = str_field(v, "action")?;
    Ok(match action {
        "assign-uuid" => Update::AssignUuid,
        "upgrade-format-version" => Update::UpgradeFormatVersion(int_field(v, "format-version")?),
        "add-schema" => Update::AddSchema(
            serde_json::from_value(
                v.get("schema")
                    .cloned()
                    .ok_or_else(|| bad("missing schema"))?,
            )
            .map_err(|e| bad(format!("schema: {e}")))?,
        ),
        "set-current-schema" => Update::SetCurrentSchema(int_field(v, "schema-id")?),
        "add-snapshot" => {
            let snapshot = v.get("snapshot").ok_or_else(|| bad("missing snapshot"))?;
            if !snapshot.get("summary").is_some_and(Value::is_object) {
                return Err(bad("snapshot without summary"));
            }
            Update::AddSnapshot(
                serde_json::from_value(snapshot.clone())
                    .map_err(|e| bad(format!("snapshot: {e}")))?,
            )
        }
        "set-snapshot-ref" => Update::SetSnapshotRef {
            ref_name: str_field(v, "ref-name")?.to_owned(),
            snapshot_id: int_field(v, "snapshot-id")?,
            ref_type: str_field(v, "type")?.to_owned(),
        },
        "remove-snapshot-ref" => Update::RemoveSnapshotRef(str_field(v, "ref-name")?.to_owned()),
        "remove-snapshots" => Update::RemoveSnapshots,
        a if METADATA_ONLY.contains(&a) => Update::Metadata(a.to_owned()),
        other => Update::Unknown(other.to_owned()),
    })
}
