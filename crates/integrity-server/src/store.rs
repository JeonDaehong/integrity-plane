//! The constraint registry and audit log (spec §23, §25), in `registry.redb` in the control store.
//!
//! The registry is one small document (constraints, per-table constraint set versions, certificate
//! chain anchors) rewritten atomically with a BLAKE3 checksum; the audit log is append-only.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::config::ConstraintConfig;

const DOC: TableDefinition<&str, &[u8]> = TableDefinition::new("oip/registry/v1");
const AUDIT: TableDefinition<u64, &[u8]> = TableDefinition::new("oip/audit/v1");

/// Where a table's certificate chain (re)starts: its `main` snapshot when the Plane began (or
/// resumed, after a rebuild) enforcing its constraints. A commit whose parent is uncertified is
/// accepted only if that parent is the anchor (spec §18, ADR 0011).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    /// Table UUID the anchor belongs to.
    pub table_uuid: String,
    /// `main` at that moment (`None`: the table was empty).
    pub snapshot: Option<i64>,
    /// Constraint set version at that moment.
    pub version: u64,
}

/// The registry document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryDoc {
    /// Registered constraints by id.
    pub constraints: BTreeMap<u64, ConstraintConfig>,
    /// Constraint set version per table identifier (spec §6).
    pub versions: BTreeMap<String, u64>,
    /// Constraint sets by version, per table identifier: the constraints governing the table and
    /// the parent keys of its FKs (what `verify` needs to recompute certificates).
    #[serde(default)]
    pub history: BTreeMap<String, BTreeMap<u64, Vec<ConstraintConfig>>>,
    /// Latest chain anchor per table identifier.
    pub anchors: BTreeMap<String, Anchor>,
    /// Earlier anchor snapshots per table (same table UUID): `verify` stops at any of them.
    #[serde(default)]
    pub retired_anchors: BTreeMap<String, Vec<Option<i64>>>,
    /// Tables whose domain is degraded until rebuilt, with the reason (spec §19).
    #[serde(default)]
    pub degraded: BTreeMap<String, String>,
    /// Tables whose domain an operator disabled (spec §19): commits pass through unchecked and
    /// uncertified until the domain is rebuilt. Value: the reason given.
    #[serde(default)]
    pub disabled: BTreeMap<String, String>,
    /// Identity of the index store the anchors were built with: a different one means the index
    /// file was lost and recreated empty.
    #[serde(default)]
    pub index_store: Option<String>,
    /// Next constraint id to assign.
    pub next_id: u64,
}

impl RegistryDoc {
    /// All constraints, in id order.
    pub fn list(&self) -> Vec<ConstraintConfig> {
        self.constraints.values().cloned().collect()
    }

    /// The current constraint set version of a table (0 if never constrained).
    pub fn version(&self, identifier: &str) -> u64 {
        self.versions.get(identifier).copied().unwrap_or(0)
    }

    /// The constraints governing `identifier` (declared on it or referencing it) plus the parent
    /// keys its FKs reference.
    pub fn governing(&self, identifier: &str) -> Vec<ConstraintConfig> {
        let mut ids: Vec<u64> = Vec::new();
        for c in self.constraints.values() {
            if c.table == identifier || c.references.as_ref().is_some_and(|r| r.table == identifier)
            {
                ids.push(c.id);
                if c.table == identifier
                    && let Some(r) = &c.references
                {
                    ids.push(r.constraint);
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids.iter()
            .filter_map(|id| self.constraints.get(id).cloned())
            .collect()
    }

    /// Records a new anchor, retiring the previous one of the same table.
    pub fn set_anchor(&mut self, identifier: &str, anchor: Anchor) {
        let uuid = anchor.table_uuid.clone();
        if let Some(old) = self.anchors.insert(identifier.to_owned(), anchor) {
            let retired = self
                .retired_anchors
                .entry(identifier.to_owned())
                .or_default();
            if old.table_uuid == uuid {
                retired.push(old.snapshot);
            } else {
                retired.clear();
            }
        }
    }

    /// Removes a table's anchor (it is no longer constrained), retiring it.
    pub fn remove_anchor(&mut self, identifier: &str) {
        if let Some(old) = self.anchors.remove(identifier) {
            self.retired_anchors
                .entry(identifier.to_owned())
                .or_default()
                .push(old.snapshot);
        }
    }

    /// Whether `snapshot` is, or was, where the table's certificate chain starts.
    pub fn is_anchor(&self, identifier: &str, snapshot: Option<i64>) -> bool {
        self.anchors
            .get(identifier)
            .is_some_and(|a| a.snapshot == snapshot)
            || self
                .retired_anchors
                .get(identifier)
                .is_some_and(|r| r.contains(&snapshot))
    }

    /// Whether any constraint governs `identifier`.
    pub fn is_constrained(&self, identifier: &str) -> bool {
        self.constraints.values().any(|c| {
            c.table == identifier || c.references.as_ref().is_some_and(|r| r.table == identifier)
        })
    }

    /// Bumps a table's version and records the constraint set of the new version.
    pub fn bump(&mut self, identifier: &str) -> u64 {
        let v = self.version(identifier) + 1;
        self.versions.insert(identifier.to_owned(), v);
        let set = self.governing(identifier);
        self.history
            .entry(identifier.to_owned())
            .or_default()
            .insert(v, set);
        v
    }
}

/// One audit event (spec §25). Never contains key values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Position in the log.
    #[serde(default)]
    pub seq: u64,
    /// Milliseconds since the epoch.
    #[serde(default)]
    pub at_ms: u64,
    /// `CONSTRAINT_REGISTERED`, `COMMIT_ACCEPTED`, …
    pub kind: String,
    /// Who caused the event, as the request stated it (`X-Integrity-Actor`, else the client's
    /// `User-Agent`); `plane` for the Plane's own actions. Not authenticated.
    #[serde(default)]
    pub actor: Option<String>,
    /// Table identifier.
    pub table: Option<String>,
    /// Transaction id.
    pub txn: Option<u64>,
    /// Constraint ids involved.
    #[serde(default)]
    pub constraints: Vec<u64>,
    /// `main` before.
    pub base_snapshot: Option<i64>,
    /// `main` after.
    pub result_snapshot: Option<i64>,
    /// Verdict or error code.
    pub verdict: Option<String>,
    /// Certificate (hex) of the result snapshot.
    pub certificate: Option<String>,
    /// Free-form detail.
    pub detail: Option<String>,
    /// Structured error of a refused commit (spec §24).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<serde_json::Value>,
}

impl AuditEvent {
    /// An event of `kind` about `table`.
    pub fn new(kind: &str, table: Option<&str>) -> Self {
        Self {
            seq: 0,
            at_ms: 0,
            kind: kind.to_owned(),
            actor: None,
            table: table.map(str::to_owned),
            txn: None,
            constraints: Vec::new(),
            base_snapshot: None,
            result_snapshot: None,
            verdict: None,
            certificate: None,
            detail: None,
            error: None,
        }
    }
}

/// Registry and audit failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "registry: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

fn err(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

/// The registry store.
#[derive(Debug)]
pub struct Registry {
    db: Database,
    doc: Mutex<RegistryDoc>,
    next_seq: Mutex<u64>,
}

fn encode(doc: &RegistryDoc) -> Result<Vec<u8>, StoreError> {
    let json = serde_json::to_vec(doc).map_err(err)?;
    let mut out = blake3::hash(&json).as_bytes().to_vec();
    out.extend_from_slice(&json);
    Ok(out)
}

fn decode(bytes: &[u8]) -> Result<RegistryDoc, StoreError> {
    if bytes.len() < 32 || blake3::hash(&bytes[32..]).as_bytes() != &bytes[..32] {
        return Err(StoreError("registry document is corrupt".into()));
    }
    serde_json::from_slice(&bytes[32..]).map_err(err)
}

impl Registry {
    /// Opens the registry, importing `seed` (constraints from the configuration file) if it is
    /// empty. Constraints already registered win over the file.
    pub fn open(path: impl AsRef<Path>, seed: &[ConstraintConfig]) -> Result<Self, StoreError> {
        let db = Database::create(path.as_ref()).map_err(err)?;
        let (doc, next_seq) = {
            let w = db.begin_write().map_err(err)?;
            let doc = {
                let mut table = w.open_table(DOC).map_err(err)?;
                let existing = table.get("doc").map_err(err)?.map(|v| decode(v.value()));
                match existing {
                    Some(doc) => doc?,
                    None => {
                        let mut doc = RegistryDoc {
                            next_id: 1,
                            ..RegistryDoc::default()
                        };
                        for c in seed {
                            doc.constraints.insert(c.id, c.clone());
                            doc.next_id = doc.next_id.max(c.id + 1);
                        }
                        let mut tables = std::collections::BTreeSet::new();
                        for c in seed {
                            tables.insert(c.table.clone());
                            if let Some(r) = &c.references {
                                tables.insert(r.table.clone());
                            }
                        }
                        for t in tables {
                            if doc.version(&t) == 0 {
                                doc.bump(&t);
                            }
                        }
                        table.insert("doc", encode(&doc)?.as_slice()).map_err(err)?;
                        doc
                    }
                }
            };
            let next_seq = {
                let audit = w.open_table(AUDIT).map_err(err)?;
                audit.last().map_err(err)?.map_or(0, |(k, _)| k.value() + 1)
            };
            w.commit().map_err(err)?;
            (doc, next_seq)
        };
        Ok(Self {
            db,
            doc: Mutex::new(doc),
            next_seq: Mutex::new(next_seq),
        })
    }

    /// The current document.
    pub fn snapshot(&self) -> RegistryDoc {
        match self.doc.lock() {
            Ok(d) => d.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Changes the document durably; nothing changes if `f` fails.
    pub fn update<T, E: From<StoreError>>(
        &self,
        f: impl FnOnce(&mut RegistryDoc) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut guard = self
            .doc
            .lock()
            .map_err(|_| StoreError("registry lock poisoned".into()))?;
        let mut next = guard.clone();
        let out = f(&mut next)?;
        let mut w = self.db.begin_write().map_err(err)?;
        w.set_two_phase_commit(true);
        {
            let mut table = w.open_table(DOC).map_err(err)?;
            table
                .insert("doc", encode(&next)?.as_slice())
                .map_err(err)?;
        }
        w.commit().map_err(err)?;
        *guard = next;
        Ok(out)
    }

    /// Appends an audit event.
    pub fn audit(&self, mut event: AuditEvent) -> Result<(), StoreError> {
        let mut seq = self
            .next_seq
            .lock()
            .map_err(|_| StoreError("audit lock poisoned".into()))?;
        event.seq = *seq;
        event.at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        let mut w = self.db.begin_write().map_err(err)?;
        w.set_two_phase_commit(true);
        {
            let mut t = w.open_table(AUDIT).map_err(err)?;
            t.insert(*seq, serde_json::to_vec(&event).map_err(err)?.as_slice())
                .map_err(err)?;
        }
        w.commit().map_err(err)?;
        *seq += 1;
        Ok(())
    }

    /// The highest transaction id in the audit log, if any.
    pub fn max_audited_txn(&self) -> Result<Option<u64>, StoreError> {
        Ok(self
            .audit_since(None, 0)?
            .iter()
            .filter_map(|e| e.txn)
            .max())
    }

    /// The structured error recorded for transaction `txn`, if it was refused with one.
    pub fn error_of(&self, txn: u64) -> Result<Option<serde_json::Value>, StoreError> {
        Ok(self
            .audit_since(None, 0)?
            .into_iter()
            .rev()
            .find(|e| e.txn == Some(txn) && e.error.is_some())
            .and_then(|e| e.error))
    }

    /// Audit events from `since` (sequence number), optionally for one table.
    pub fn audit_since(
        &self,
        table: Option<&str>,
        since: u64,
    ) -> Result<Vec<AuditEvent>, StoreError> {
        let r = self.db.begin_read().map_err(err)?;
        let t = r.open_table(AUDIT).map_err(err)?;
        let mut out = Vec::new();
        for item in t.range(since..).map_err(err)? {
            let (_, v) = item.map_err(err)?;
            let e: AuditEvent = serde_json::from_slice(v.value()).map_err(err)?;
            if table.is_none_or(|t| e.table.as_deref() == Some(t)) {
                out.push(e);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(id: u64, table: &str) -> ConstraintConfig {
        ConstraintConfig {
            id,
            table: table.into(),
            name: format!("pk{id}"),
            kind: "primary_key".into(),
            columns: vec![1],
            nulls: None,
            references: None,
            match_mode: None,
            column_names: None,
        }
    }

    fn temp(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "oip-registry-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn seeds_once_and_survives_reopen() {
        let path = temp("seed");
        {
            let r = Registry::open(&path, &[pk(4, "db.a")]).unwrap();
            let doc = r.snapshot();
            assert_eq!(doc.version("db.a"), 1);
            assert_eq!(doc.next_id, 5);
            r.update(|d| {
                d.constraints.remove(&4);
                d.bump("db.a");
                Ok::<_, StoreError>(())
            })
            .unwrap();
            r.audit(AuditEvent::new("CONSTRAINT_DROPPED", Some("db.a")))
                .unwrap();
        }
        // The file's constraints no longer apply: the registry is the source of truth.
        let r = Registry::open(&path, &[pk(4, "db.a"), pk(9, "db.b")]).unwrap();
        let doc = r.snapshot();
        assert!(doc.constraints.is_empty());
        assert_eq!(doc.version("db.a"), 2);
        assert_eq!(doc.history["db.a"][&1], vec![pk(4, "db.a")]);
        assert!(doc.history["db.a"][&2].is_empty());
        r.audit(AuditEvent::new("CONSTRAINT_REGISTERED", Some("db.b")))
            .unwrap();
        let events = r.audit_since(None, 0).unwrap();
        assert_eq!(
            events.iter().map(|e| e.seq).collect::<Vec<_>>(),
            [0, 1],
            "sequence continues after reopen"
        );
        assert_eq!(r.audit_since(Some("db.b"), 0).unwrap().len(), 1);
        drop(r);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_failed_update_changes_nothing() {
        let path = temp("fail");
        let r = Registry::open(&path, &[]).unwrap();
        let out: Result<(), StoreError> = r.update(|d| {
            d.next_id = 77;
            Err(StoreError("no".into()))
        });
        assert!(out.is_err());
        assert_eq!(r.snapshot().next_id, 1);
        drop(r);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_corrupted_document_is_refused() {
        let doc = RegistryDoc::default();
        let mut bytes = encode(&doc).unwrap();
        assert_eq!(decode(&bytes).unwrap(), doc);
        let last = bytes.len() - 2;
        bytes[last] ^= 1;
        assert!(decode(&bytes).is_err());
        assert!(decode(&bytes[..10]).is_err());
    }
}
