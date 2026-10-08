//! Configured constraints bound to the tables the upstream catalog actually holds.
//!
//! Constraints name tables by identifier; the core works on table UUIDs. A table is bound when the
//! Plane first loads it; every table connected by foreign keys must be bound before any of them is
//! validated, so the validator always sees the whole integrity domain.

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{
    Constraint, ConstraintKind, EnforcementMode, ForeignKeySpec, KeySpec, LogicalType, MatchMode,
    NullsMode, ReferentialAction, RegistrationContext, UniqueSpec,
};
use integrity_iceberg::TableMetadata;
use integrity_iceberg::metadata::{FieldLookup, logical_type};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, TableId};
use integrity_validator::ResolvedConstraint;

use crate::config::ConstraintConfig;
use crate::error::ApiError;

/// A configured table bound to its upstream identity.
#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    /// The table UUID.
    pub table: TableId,
    /// Types of the constrained columns, from the current schema.
    pub columns: BTreeMap<FieldId, LogicalType>,
}

fn misconfigured(m: impl Into<String>) -> ApiError {
    ApiError::new(
        ErrorCode::IndexDegraded,
        format!("constraint configuration: {}", m.into()),
    )
}

/// Every table identifier FK-connected to `identifier` (including itself).
pub fn component(configs: &[ConstraintConfig], identifier: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::from([identifier.to_owned()]);
    loop {
        let before = out.len();
        for c in configs {
            if let Some(r) = &c.references
                && (out.contains(&c.table) || out.contains(&r.table))
            {
                out.insert(c.table.clone());
                out.insert(r.table.clone());
            }
        }
        if out.len() == before {
            return out;
        }
    }
}

/// Whether any constraint is declared on `identifier` or references it.
pub fn is_constrained(configs: &[ConstraintConfig], identifier: &str) -> bool {
    configs.iter().any(|c| {
        c.table == identifier || c.references.as_ref().is_some_and(|r| r.table == identifier)
    })
}

/// Binds a table from its metadata: its UUID and the types of its constrained columns.
pub fn bind(
    configs: &[ConstraintConfig],
    identifier: &str,
    meta: &TableMetadata,
) -> Result<Binding, ApiError> {
    let schema = meta
        .current_schema()
        .ok_or_else(|| misconfigured(format!("{identifier} has no current schema")))?;
    let mut columns = BTreeMap::new();
    for c in configs.iter().filter(|c| c.table == identifier) {
        for &f in &c.columns {
            match schema.lookup(FieldId(f)) {
                FieldLookup::TopLevel(field) => {
                    columns.insert(FieldId(f), logical_type(&field.field_type));
                }
                FieldLookup::Nested | FieldLookup::Missing => {
                    return Err(misconfigured(format!(
                        "constraint {} names field {f}, which is not a top-level column of {identifier}",
                        c.name
                    )));
                }
            }
        }
    }
    Ok(Binding {
        table: TableId::new(meta.table_uuid.to_lowercase()),
        columns,
    })
}

struct Ctx<'a> {
    bindings: &'a BTreeMap<String, Binding>,
    built: BTreeMap<ConstraintId, Constraint>,
}

impl RegistrationContext for Ctx<'_> {
    fn column_type(&self, table: &TableId, field: FieldId) -> Option<LogicalType> {
        self.bindings
            .values()
            .find(|b| b.table == *table)
            .and_then(|b| b.columns.get(&field).cloned())
    }

    fn constraint(&self, id: ConstraintId) -> Option<&Constraint> {
        self.built.get(&id)
    }
}

/// Resolves every configured constraint whose tables are bound. PK/UNIQUE before FK.
pub fn resolve(
    configs: &[ConstraintConfig],
    bindings: &BTreeMap<String, Binding>,
) -> Result<Vec<ResolvedConstraint>, ApiError> {
    let table_of = |ident: &str| bindings.get(ident).map(|b| b.table.clone());
    let mut ordered: Vec<&ConstraintConfig> = configs.iter().collect();
    ordered.sort_by_key(|c| (c.kind == "foreign_key", c.id));

    let mut ctx = Ctx {
        bindings,
        built: BTreeMap::new(),
    };
    let mut resolved = Vec::new();
    for c in ordered {
        let Some(table) = table_of(&c.table) else {
            continue;
        };
        let key = KeySpec {
            columns: c.columns.iter().map(|&f| FieldId(f)).collect(),
        };
        let kind = match c.kind.as_str() {
            "primary_key" => ConstraintKind::PrimaryKey(key),
            "unique" => ConstraintKind::Unique(UniqueSpec {
                key,
                nulls: match c.nulls.as_deref() {
                    None | Some("distinct") => NullsMode::Distinct,
                    Some("not_distinct") => NullsMode::NotDistinct,
                    Some(other) => return Err(misconfigured(format!("nulls = {other}"))),
                },
            }),
            "not_null" => match c.columns.as_slice() {
                [f] => ConstraintKind::NotNull(FieldId(*f)),
                _ => return Err(misconfigured(format!("{} must name one column", c.name))),
            },
            "foreign_key" => {
                let r = c
                    .references
                    .as_ref()
                    .ok_or_else(|| misconfigured(format!("{} has no references", c.name)))?;
                let Some(parent_table) = table_of(&r.table) else {
                    continue;
                };
                ConstraintKind::ForeignKey(ForeignKeySpec {
                    child: key,
                    parent_table,
                    parent_constraint: ConstraintId(r.constraint),
                    match_mode: match c.match_mode.as_deref() {
                        None | Some("simple") => MatchMode::Simple,
                        Some("full") => MatchMode::Full,
                        Some(other) => return Err(misconfigured(format!("match = {other}"))),
                    },
                    on_delete: ReferentialAction::Restrict,
                })
            }
            other => return Err(misconfigured(format!("type = {other}"))),
        };
        let constraint = Constraint {
            id: ConstraintId(c.id),
            table,
            name: c.name.clone(),
            kind,
            mode: EnforcementMode::Enforced,
            version: ConstraintSetVersion(1),
        };
        let rc = ResolvedConstraint::resolve(constraint.clone(), &ctx)
            .map_err(|e| misconfigured(format!("{}: {e}", c.name)))?;
        ctx.built.insert(constraint.id, constraint);
        resolved.push(rc);
    }
    Ok(resolved)
}
