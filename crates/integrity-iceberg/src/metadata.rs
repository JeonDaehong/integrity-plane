//! The subset of Iceberg table metadata the Plane reads (Iceberg spec "Table Metadata").
//!
//! Parsed strictly where it matters for integrity (snapshots, refs, schemas, field types) and
//! leniently elsewhere: fields the Plane does not use are ignored, never interpreted.

use std::collections::BTreeMap;

use integrity_core::LogicalType;
use integrity_types::{FieldId, SnapshotId};
use serde::Deserialize;
use serde_json::Value;

/// The branch whose snapshots are validated and certified.
pub const MAIN: &str = "main";

/// Table metadata (subset).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// 1, 2 or 3.
    pub format_version: u8,
    /// Stable table identity; the Plane's `TableId`.
    pub table_uuid: String,
    /// v2+: all schemas.
    #[serde(default)]
    pub schemas: Vec<Schema>,
    /// v1: the single schema.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Id of the current schema (v2+).
    #[serde(default)]
    pub current_schema_id: Option<i32>,
    /// v1 fallback for `refs["main"]`; `-1` or absent when the table has no snapshot.
    #[serde(default)]
    pub current_snapshot_id: Option<i64>,
    /// Known snapshots.
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    /// Named branches and tags.
    #[serde(default)]
    pub refs: BTreeMap<String, SnapshotRef>,
    /// Highest assigned field id.
    #[serde(default)]
    pub last_column_id: Option<i64>,
    /// Highest assigned partition field id.
    #[serde(default)]
    pub last_partition_id: Option<i64>,
    /// Default partition spec id.
    #[serde(default)]
    pub default_spec_id: Option<i64>,
    /// Default sort order id.
    #[serde(default)]
    pub default_sort_order_id: Option<i64>,
}

/// A table schema.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Schema id (absent in some v1 metadata, treated as 0).
    #[serde(default)]
    pub schema_id: i32,
    /// Top-level fields.
    pub fields: Vec<NestedField>,
}

/// A schema field.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NestedField {
    /// Field id.
    pub id: i32,
    /// Current name (informational; the Plane never matches by name).
    pub name: String,
    /// Required (NOT NULL in the format).
    pub required: bool,
    /// Iceberg type JSON.
    #[serde(rename = "type")]
    pub field_type: Value,
    /// v3 default for rows written before the field existed.
    #[serde(default)]
    pub initial_default: Option<Value>,
}

/// A snapshot.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    /// Snapshot id.
    pub snapshot_id: i64,
    /// Parent on the branch it was committed to.
    #[serde(default)]
    pub parent_snapshot_id: Option<i64>,
    /// Location of the manifest list (absent only in old v1 metadata, which is unsupported).
    #[serde(default)]
    pub manifest_list: Option<String>,
    /// Summary; `operation` is required by the spec.
    #[serde(default)]
    pub summary: BTreeMap<String, String>,
}

/// A branch or tag.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    /// Referenced snapshot.
    pub snapshot_id: i64,
    /// `branch` or `tag`.
    #[serde(rename = "type")]
    pub ref_type: String,
}

impl TableMetadata {
    /// Parses metadata JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// The snapshot `main` points to, if any.
    pub fn main_snapshot_id(&self) -> Option<SnapshotId> {
        match self.refs.get(MAIN) {
            Some(r) => Some(SnapshotId(r.snapshot_id)),
            None => self
                .current_snapshot_id
                .filter(|&id| id != -1)
                .map(SnapshotId),
        }
    }

    /// A snapshot by id.
    pub fn snapshot(&self, id: SnapshotId) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id.0)
    }

    /// The current schema.
    pub fn current_schema(&self) -> Option<&Schema> {
        match self.current_schema_id {
            Some(id) => self.schemas.iter().find(|s| s.schema_id == id),
            None => self.schema.as_ref().or_else(|| self.schemas.first()),
        }
    }
}

/// Where a field id was found in a schema.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldLookup<'a> {
    /// A top-level field.
    TopLevel(&'a NestedField),
    /// Inside a struct, list or map.
    Nested,
    /// Not in the schema.
    Missing,
}

impl Schema {
    /// Finds a field id anywhere in the schema.
    pub fn lookup(&self, id: FieldId) -> FieldLookup<'_> {
        if let Some(f) = self.fields.iter().find(|f| f.id == id.0) {
            return FieldLookup::TopLevel(f);
        }
        if self.fields.iter().any(|f| contains_id(&f.field_type, id.0)) {
            FieldLookup::Nested
        } else {
            FieldLookup::Missing
        }
    }
}

fn contains_id(ty: &Value, id: i32) -> bool {
    let Value::Object(obj) = ty else { return false };
    let id_matches = |key: &str| obj.get(key).and_then(Value::as_i64) == Some(i64::from(id));
    if id_matches("element-id") || id_matches("key-id") || id_matches("value-id") {
        return true;
    }
    let nested = |key: &str| obj.get(key).is_some_and(|t| contains_id(t, id));
    if nested("element") || nested("key") || nested("value") {
        return true;
    }
    obj.get("fields")
        .and_then(Value::as_array)
        .is_some_and(|fields| {
            fields.iter().any(|f| {
                f.get("id").and_then(Value::as_i64) == Some(i64::from(id))
                    || f.get("type").is_some_and(|t| contains_id(t, id))
            })
        })
}

/// Maps an Iceberg type JSON to the Plane's logical type.
pub fn logical_type(ty: &Value) -> LogicalType {
    match ty {
        Value::Object(_) => LogicalType::Nested,
        Value::String(s) => primitive(s.trim()),
        _ => LogicalType::Other(ty.to_string()),
    }
}

fn primitive(s: &str) -> LogicalType {
    match s {
        "boolean" => LogicalType::Boolean,
        "int" => LogicalType::Int,
        "long" => LogicalType::Long,
        "float" => LogicalType::Float,
        "double" => LogicalType::Double,
        "date" => LogicalType::Date,
        "time" => LogicalType::Time,
        "timestamp" => LogicalType::Timestamp,
        "timestamptz" => LogicalType::TimestampTz,
        "timestamp_ns" => LogicalType::TimestampNs,
        "timestamptz_ns" => LogicalType::TimestampTzNs,
        "string" => LogicalType::String,
        "uuid" => LogicalType::Uuid,
        "binary" => LogicalType::Binary,
        "variant" => LogicalType::Variant,
        _ if s.starts_with("geometry") || s.starts_with("geography") => LogicalType::Geospatial,
        _ => {
            if let Some(args) = s.strip_prefix("decimal(").and_then(|r| r.strip_suffix(')'))
                && let Some((p, sc)) = args.split_once(',')
                && let (Ok(precision), Ok(scale)) = (p.trim().parse(), sc.trim().parse())
            {
                return LogicalType::Decimal { precision, scale };
            }
            if let Some(len) = s.strip_prefix("fixed[").and_then(|r| r.strip_suffix(']'))
                && let Ok(len) = len.trim().parse()
            {
                return LogicalType::Fixed(len);
            }
            LogicalType::Other(s.to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn primitive_types() {
        assert_eq!(logical_type(&json!("long")), LogicalType::Long);
        assert_eq!(
            logical_type(&json!("decimal(10, 2)")),
            LogicalType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            logical_type(&json!("decimal(38,6)")),
            LogicalType::Decimal {
                precision: 38,
                scale: 6
            }
        );
        assert_eq!(logical_type(&json!("fixed[16]")), LogicalType::Fixed(16));
        assert_eq!(
            logical_type(&json!("timestamptz_ns")),
            LogicalType::TimestampTzNs
        );
        assert_eq!(
            logical_type(&json!("geometry(srid:4326)")),
            LogicalType::Geospatial
        );
        assert_eq!(
            logical_type(&json!({"type": "struct", "fields": []})),
            LogicalType::Nested
        );
        assert!(matches!(
            logical_type(&json!("decimal(x,2)")),
            LogicalType::Other(_)
        ));
        assert!(matches!(
            logical_type(&json!("unknown")),
            LogicalType::Other(_)
        ));
    }

    #[test]
    fn nested_lookup() {
        let schema: Schema = serde_json::from_value(json!({
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "s", "required": false, "type": {"type": "struct", "fields": [
                    {"id": 3, "name": "k", "required": false, "type": "long"}
                ]}},
                {"id": 4, "name": "l", "required": false, "type": {
                    "type": "list", "element-id": 5, "element": "string", "element-required": false
                }}
            ]
        }))
        .unwrap();
        assert!(matches!(
            schema.lookup(FieldId(1)),
            FieldLookup::TopLevel(_)
        ));
        assert_eq!(schema.lookup(FieldId(3)), FieldLookup::Nested);
        assert_eq!(schema.lookup(FieldId(5)), FieldLookup::Nested);
        assert_eq!(schema.lookup(FieldId(9)), FieldLookup::Missing);
    }

    #[test]
    fn main_falls_back_to_current_snapshot_id() {
        let meta: TableMetadata = serde_json::from_value(json!({
            "format-version": 1, "table-uuid": "u", "current-snapshot-id": 7,
            "schema": {"fields": []}
        }))
        .unwrap();
        assert_eq!(meta.main_snapshot_id(), Some(SnapshotId(7)));
        let empty: TableMetadata = serde_json::from_value(json!({
            "format-version": 2, "table-uuid": "u", "current-snapshot-id": -1, "schemas": []
        }))
        .unwrap();
        assert_eq!(empty.main_snapshot_id(), None);
    }
}
