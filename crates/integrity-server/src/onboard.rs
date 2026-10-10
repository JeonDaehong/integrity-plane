//! The onboarding and rebuild scan (spec §19, §20, ADR 0011), run on a blocking thread.
//!
//! Every table of a domain is read at its pinned `main` snapshot and validated as one insert into
//! empty indexes, FK parents first, so the result is exactly the index contents the data implies,
//! or the constraints the data violates.
//!
//! [`scan`] does this with bounded memory, whatever the table size: rows are read a row group at a
//! time, the keys of each constraint are sorted externally (spilling to the index store's scratch
//! directory), and duplicates, FK parents and reference counts are checked on the sorted keys. The
//! new contents go into build tables beside the live indexes, installed later in one transaction.
//! [`scan_in_memory`] is the reference it is tested against.

use std::collections::{BTreeMap, BTreeSet};

use integrity_core::{EncodedKey, LogicalType, Violation};
use integrity_iceberg::{
    Budgeted, FileChanges, FileIo, InspectError, TableMetadata, commit_rows, diff_snapshots,
    for_each_live_batch,
};
use integrity_index::{
    IndexBuild, IndexEpoch, IndexError, IndexKind, IndexValue, KeyIndex, KeySorter, MemoryIndex,
    PersistentStore,
};
use integrity_types::{ConstraintId, ErrorCode, FieldId, SnapshotId, TableId};
use integrity_validator::{
    Decision, KeyCheck, ResolvedConstraint, ValidationError, Validator, ViolationDetail,
};

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
    /// The data satisfies every constraint: the new contents of every index of the domain, built
    /// beside the live ones, for [`PersistentStore::install`].
    Clean(Vec<IndexBuild>),
    /// The first table (in FK order) whose data violates constraints: one report entry per
    /// violated constraint (`report::violations`).
    Violations(Vec<serde_json::Value>),
}

/// Full index contents per constraint.
pub type IndexContents = BTreeMap<ConstraintId, (IndexKind, Vec<(EncodedKey, IndexValue)>)>;

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidConstraint, m)
}

fn unsupported(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::UnsupportedCommitOperation, m)
}

fn degraded(e: IndexError) -> ApiError {
    ApiError::new(ErrorCode::IndexDegraded, format!("index store: {e}"))
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

/// The domain resolved for a scan.
struct Prepared<'m> {
    resolved: Vec<ResolvedConstraint>,
    bindings: BTreeMap<String, registry::Binding>,
    column_names: report::ColumnNames,
    by_name: BTreeMap<&'m str, &'m Member>,
    order: Vec<String>,
}

impl<'m> Prepared<'m> {
    fn new(configs: &[ConstraintConfig], members: &'m [Member]) -> Result<Self, ApiError> {
        let mut bindings = BTreeMap::new();
        let mut column_names = report::ColumnNames::new();
        for m in members {
            report::add_names(&mut column_names, &m.identifier, &m.meta);
            let binding =
                registry::bind(configs, &m.identifier, &m.meta).map_err(|e| invalid(e.message))?;
            bindings.insert(m.identifier.clone(), binding);
        }
        let resolved = registry::resolve(configs, &bindings).map_err(|e| invalid(e.message))?;
        let names: BTreeSet<String> = members.iter().map(|m| m.identifier.clone()).collect();
        Ok(Self {
            resolved,
            bindings,
            column_names,
            by_name: members.iter().map(|m| (m.identifier.as_str(), m)).collect(),
            order: fk_order(configs, &names)?,
        })
    }

    /// What to read of table `ident`: its head snapshot, every live data file and the projected
    /// columns. `None` for a table without snapshots.
    fn table(
        &self,
        io: &impl FileIo,
        validator: &Validator,
        ident: &str,
    ) -> Result<Option<Table>, ApiError> {
        let member = self.by_name[ident];
        let Some(head) = member.meta.main_snapshot_id() else {
            return Ok(None);
        };
        let list = member
            .meta
            .snapshot(head)
            .and_then(|s| s.manifest_list.clone())
            .ok_or_else(|| unsupported(format!("{ident}: snapshot without manifest list")))?;
        let changes = diff_snapshots(io, None, &list)
            .map_err(|e| ApiError::new(e.code(), format!("{ident}: {e}")))?;
        if !changes.equality_deletes.is_empty() {
            return Err(unsupported(format!(
                "{ident} has equality delete files; compact it before onboarding"
            )));
        }
        let binding = &self.bindings[ident];
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
        Ok(Some(Table {
            id: binding.table.clone(),
            head,
            changes,
            columns,
        }))
    }
}

/// A table with data, as the scan reads it.
struct Table {
    id: TableId,
    head: SnapshotId,
    changes: FileChanges,
    columns: Vec<(FieldId, LogicalType)>,
}

/// FK keys looked up in the parent's build at once.
const PARENT_PROBE_BATCH: usize = 4096;

/// Scans the domain formed by `members` under `configs` into build tables of `store`, holding
/// about `memory` bytes of keys per table at most (the rest spills to the store's scratch
/// directory). On violations or errors every build is discarded.
pub fn scan(
    io: &(dyn FileIo + Send + Sync),
    store: &PersistentStore,
    memory: usize,
    configs: &[ConstraintConfig],
    members: &[Member],
    redact_keys: bool,
) -> Result<ScanOutcome, ApiError> {
    let prepared = Prepared::new(configs, members)?;
    let mut builds = BTreeMap::new();
    for rc in &prepared.resolved {
        if let Some(kind) = rc.index_kind() {
            let id = rc.constraint.id;
            match store.build(id, kind) {
                Ok(b) => {
                    builds.insert(id, b);
                }
                Err(e) => {
                    discard(builds);
                    return Err(degraded(e));
                }
            }
        }
    }
    let validator = Validator::new(prepared.resolved.clone());
    let io = Budgeted::new(io, u64::MAX);
    match scan_into(&io, store, memory, &prepared, &validator, &mut builds) {
        Ok(None) => Ok(ScanOutcome::Clean(builds.into_values().collect())),
        Ok(Some(details)) => {
            discard(builds);
            Ok(ScanOutcome::Violations(report::violations(
                &details,
                configs,
                &prepared.column_names,
                redact_keys,
            )))
        }
        Err(e) => {
            discard(builds);
            Err(e)
        }
    }
}

fn discard(builds: BTreeMap<ConstraintId, IndexBuild>) {
    for b in builds.into_values() {
        // A build table left behind is discarded by the next build of the same index.
        let _ = b.discard();
    }
}

/// Reading a table or sorting its keys failed.
enum Failed {
    Inspect(InspectError),
    Api(ApiError),
}

impl From<InspectError> for Failed {
    fn from(e: InspectError) -> Self {
        Failed::Inspect(e)
    }
}

/// Fills `builds` table by table, FK parents first; the violations of the first violating table.
fn scan_into(
    io: &impl FileIo,
    store: &PersistentStore,
    memory: usize,
    prepared: &Prepared<'_>,
    validator: &Validator,
    builds: &mut BTreeMap<ConstraintId, IndexBuild>,
) -> Result<Option<BTreeMap<Violation, ViolationDetail>>, ApiError> {
    for ident in &prepared.order {
        let Some(table) = prepared.table(io, validator, ident)? else {
            continue;
        };
        let failed = |e: ValidationError| ApiError::new(e.code(), format!("{ident}: {e}"));
        let mut scan = validator.table_scan(&table.id).map_err(failed)?;
        let checks = scan.checks();
        let share = memory / checks.len().max(1);
        let mut sorters: BTreeMap<ConstraintId, KeySorter> = checks
            .iter()
            .map(|(id, _)| (*id, KeySorter::new(store.scratch_dir(), share)))
            .collect();
        let read = for_each_live_batch(io, &table.changes, &table.columns, |batch| {
            scan.feed(&batch, |id, key| {
                sorters
                    .get_mut(&id)
                    .ok_or(ValidationError::MissingIndex(id))?
                    .push(&key)
                    .map_err(ValidationError::Index)
            })
            .map_err(|e| Failed::Api(failed(e)))
        });
        match read {
            Ok(()) => {}
            Err(Failed::Inspect(e)) => {
                return Err(ApiError::new(e.code(), format!("{ident}: {e}")));
            }
            Err(Failed::Api(e)) => return Err(e),
        }

        let missing = |id| failed(ValidationError::MissingIndex(id));
        for (id, check) in checks {
            let keys = sorters
                .remove(&id)
                .ok_or_else(|| missing(id))?
                .finish()
                .map_err(degraded)?;
            match check {
                KeyCheck::Unique(code) => {
                    let build = builds.get_mut(&id).ok_or_else(|| missing(id))?;
                    for item in keys {
                        let (key, count) = item.map_err(degraded)?;
                        if count > 1 {
                            scan.record_key(id, code, &key);
                        } else {
                            let value = IndexValue::Unique {
                                last_snapshot: table.head,
                            };
                            build.push(key, value).map_err(degraded)?;
                        }
                    }
                }
                KeyCheck::Parent(parent) => {
                    let mut keys = keys.peekable();
                    while keys.peek().is_some() {
                        let chunk: Vec<(EncodedKey, u64)> = keys
                            .by_ref()
                            .take(PARENT_PROBE_BATCH)
                            .collect::<Result<_, _>>()
                            .map_err(degraded)?;
                        let probe: Vec<EncodedKey> = chunk.iter().map(|(k, _)| k.clone()).collect();
                        let found = builds
                            .get_mut(&parent)
                            .ok_or_else(|| missing(parent))?
                            .get_many(&probe)
                            .map_err(degraded)?;
                        let build = builds.get_mut(&id).ok_or_else(|| missing(id))?;
                        for ((key, count), value) in chunk.into_iter().zip(found) {
                            match value {
                                None => scan.record_key(id, ErrorCode::ForeignKeyViolation, &key),
                                Some(IndexValue::Unique { .. }) => {
                                    let value = IndexValue::Reference { child_count: count };
                                    build.push(key, value).map_err(degraded)?;
                                }
                                Some(IndexValue::Reference { .. }) => {
                                    return Err(degraded(IndexError::Corrupt));
                                }
                            }
                        }
                    }
                }
            }
        }
        let (violations, details) = scan.finish();
        if !violations.is_empty() {
            return Ok(Some(details));
        }
    }
    Ok(None)
}

/// The reference scan: every table validated as one commit against in-memory indexes, so it holds
/// every key of the domain in memory. [`scan`] must find exactly the same; tests compare them.
/// `Ok(Err(report))` when the data violates constraints.
pub fn scan_in_memory(
    io: &(dyn FileIo + Send + Sync),
    configs: &[ConstraintConfig],
    members: &[Member],
    redact_keys: bool,
) -> Result<Result<IndexContents, Vec<serde_json::Value>>, ApiError> {
    let prepared = Prepared::new(configs, members)?;
    let io = Budgeted::new(io, u64::MAX);
    let indexes: BTreeMap<ConstraintId, MemoryIndex> = prepared
        .resolved
        .iter()
        .filter_map(|rc| {
            rc.index_kind()
                .map(|k| (rc.constraint.id, MemoryIndex::new(k)))
        })
        .collect();
    let validator = Validator::new(prepared.resolved.clone());
    let mut epoch = 0;
    for ident in &prepared.order {
        let Some(table) = prepared.table(&io, &validator, ident)? else {
            continue;
        };
        let rows = commit_rows(&io, table.id, table.head, &table.changes, &table.columns)
            .map_err(|e| ApiError::new(e.code(), format!("{ident}: {e}")))?;
        match validator
            .validate_with_details(&rows, &indexes)
            .map_err(|e| ApiError::new(e.code(), format!("{ident}: {e}")))?
        {
            (Decision::Rejected(_), details) => {
                return Ok(Err(report::violations(
                    &details,
                    configs,
                    &prepared.column_names,
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
    Ok(Ok(out))
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
