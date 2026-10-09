//! The onboarding and rebuild scan (spec §19, §20, ADR 0011), run on a blocking thread.
//!
//! Every table of a domain is read at its pinned `main` snapshot and validated as one insert
//! against in-memory indexes, FK parents first, so the result is exactly the index contents the
//! data implies, or the constraints the data violates.

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::EncodedKey;
use integrity_iceberg::{Budgeted, FileIo, TableMetadata, commit_rows, diff_snapshots};
use integrity_index::{IndexEpoch, IndexKind, IndexValue, KeyIndex, MemoryIndex};
use integrity_types::{ConstraintId, ErrorCode};
use integrity_validator::{Decision, Validator};

use crate::config::ConstraintConfig;
use crate::error::ApiError;
use crate::{registry, report};

/// A table of the domain, loaded at the snapshot the scan pins.
#[derive(Debug, Clone)]
pub struct Member {
    /// Table identifier.
    pub identifier: String,
    /// Its metadata; the scan reads its `main`.
    pub meta: TableMetadata,
}

/// What the scan found.
#[derive(Debug)]
pub enum ScanOutcome {
    /// The data satisfies every constraint: the full contents of every index of the domain.
    Clean(BTreeMap<ConstraintId, (IndexKind, Vec<(EncodedKey, IndexValue)>)>),
    /// The first table (in FK order) whose data violates constraints: one report entry per
    /// violated constraint (`report::violations`).
    Violations(Vec<serde_json::Value>),
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidConstraint, m)
}

fn unsupported(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::UnsupportedCommitOperation, m)
}

/// Orders `members` so that every FK parent table comes before its children. A self-reference is
/// allowed; a cycle between distinct tables is not.
pub fn fk_order(
    configs: &[ConstraintConfig],
    members: &BTreeSet<String>,
) -> Result<Vec<String>, ApiError> {
    let mut parents: BTreeMap<&str, BTreeSet<&str>> = members
        .iter()
        .map(|m| (m.as_str(), BTreeSet::new()))
        .collect();
    for c in configs {
        if let Some(r) = &c.references
            && c.table != r.table
            && members.contains(&r.table)
            && let Some(p) = parents.get_mut(c.table.as_str())
        {
            p.insert(r.table.as_str());
        }
    }
    let mut out: Vec<String> = Vec::new();
    while out.len() < members.len() {
        let ready: Vec<&str> = parents
            .iter()
            .filter(|(t, ps)| {
                !out.iter().any(|o| o == *t) && ps.iter().all(|p| out.iter().any(|o| o == p))
            })
            .map(|(t, _)| *t)
            .collect();
        if ready.is_empty() {
            return Err(invalid(
                "foreign keys form a cycle between tables, which 0.1 cannot onboard",
            ));
        }
        out.extend(ready.into_iter().map(str::to_owned));
    }
    Ok(out)
}

/// Scans the domain formed by `members` under `configs`.
pub fn scan(
    io: &(dyn FileIo + Send + Sync),
    configs: &[ConstraintConfig],
    members: &[Member],
    redact_keys: bool,
) -> Result<ScanOutcome, ApiError> {
    let io = Budgeted::new(io, u64::MAX);
    let mut bindings = BTreeMap::new();
    let mut column_names = report::ColumnNames::new();
    for m in members {
        report::add_names(&mut column_names, &m.identifier, &m.meta);
        let binding =
            registry::bind(configs, &m.identifier, &m.meta).map_err(|e| invalid(e.message))?;
        bindings.insert(m.identifier.clone(), binding);
    }
    let resolved = registry::resolve(configs, &bindings).map_err(|e| invalid(e.message))?;
    let indexes: BTreeMap<ConstraintId, MemoryIndex> = resolved
        .iter()
        .filter_map(|rc| {
            rc.index_kind()
                .map(|k| (rc.constraint.id, MemoryIndex::new(k)))
        })
        .collect();
    let validator = Validator::new(resolved);
    let names: BTreeSet<String> = members.iter().map(|m| m.identifier.clone()).collect();
    let by_name: BTreeMap<&str, &Member> =
        members.iter().map(|m| (m.identifier.as_str(), m)).collect();

    let mut epoch = 0;
    for ident in fk_order(configs, &names)? {
        let member = by_name[ident.as_str()];
        let Some(head) = member.meta.main_snapshot_id() else {
            continue;
        };
        let list = member
            .meta
            .snapshot(head)
            .and_then(|s| s.manifest_list.clone())
            .ok_or_else(|| unsupported(format!("{ident}: snapshot without manifest list")))?;
        let changes = diff_snapshots(&io, None, &list)
            .map_err(|e| ApiError::new(e.code(), format!("{ident}: {e}")))?;
        if !changes.equality_deletes.is_empty() {
            return Err(unsupported(format!(
                "{ident} has delete files; compact it before onboarding"
            )));
        }
        let binding = &bindings[&ident];
        let columns = validator
            .projection(&binding.table)
            .into_iter()
            .map(|f| {
                binding
                    .columns
                    .get(&f)
                    .cloned()
                    .map(|t| (f, t))
                    .ok_or_else(|| invalid(format!("{ident}: no type for {f}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = commit_rows(&io, binding.table.clone(), head, &changes, &columns)
            .map_err(|e| ApiError::new(e.code(), format!("{ident}: {e}")))?;
        match validator
            .validate_with_details(&rows, &indexes)
            .map_err(|e| ApiError::new(e.code(), format!("{ident}: {e}")))?
        {
            (Decision::Rejected(_), details) => {
                return Ok(ScanOutcome::Violations(report::violations(
                    &details,
                    configs,
                    &column_names,
                    redact_keys,
                )));
            }
            (Decision::Accepted(validated), _) => {
                epoch += 1;
                for (id, staged) in validated
                    .stage(&indexes)
                    .map_err(|e| ApiError::new(e.code(), e.to_string()))?
                {
                    indexes[&id]
                        .apply(staged, IndexEpoch(epoch))
                        .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, e.to_string()))?;
                }
            }
        }
    }
    let mut out = BTreeMap::new();
    for (id, index) in indexes {
        let entries = index
            .entries()
            .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, e.to_string()))?;
        out.insert(id, (index.kind(), entries));
    }
    Ok(ScanOutcome::Clean(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReferenceConfig;

    fn fk(id: u64, table: &str, parent: &str) -> ConstraintConfig {
        ConstraintConfig {
            id,
            table: table.into(),
            name: format!("fk{id}"),
            kind: "foreign_key".into(),
            columns: vec![2],
            nulls: None,
            references: Some(ReferenceConfig {
                table: parent.into(),
                constraint: 1,
            }),
            match_mode: None,
            column_names: None,
        }
    }

    #[test]
    fn parents_come_first_and_cycles_are_refused() {
        let members: BTreeSet<String> = ["a.child", "a.parent", "a.grandchild"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let configs = vec![
            fk(10, "a.grandchild", "a.child"),
            fk(11, "a.child", "a.parent"),
            fk(12, "a.parent", "a.parent"),
        ];
        assert_eq!(
            fk_order(&configs, &members).unwrap(),
            ["a.parent", "a.child", "a.grandchild"]
        );
        let mut cyclic = configs.clone();
        cyclic.push(fk(13, "a.parent", "a.grandchild"));
        assert_eq!(
            fk_order(&cyclic, &members).unwrap_err().code,
            ErrorCode::InvalidConstraint
        );
    }
}
