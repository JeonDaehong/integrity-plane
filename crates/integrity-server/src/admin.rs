//! Constraint registration, drop and rebuild (spec §19, §20, §23, ADR 0011).
//!
//! Each operation holds the gateway's exclusive admin lock, so no commit runs meanwhile, resolves
//! unfinished transactions first, and changes the registry in one transaction at the end.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::http::HeaderMap;
use integrity_core::EncodedKey;
use integrity_iceberg::TableMetadata;
use integrity_iceberg::metadata::FieldLookup;
use integrity_index::{IndexEpoch, IndexKind, IndexValue, KeyIndex};
use integrity_txn::{FaultPoint, TxnId, fault};
use integrity_types::{ConstraintId, ErrorCode, FieldId};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::{ConstraintConfig, ReferenceConfig};
use crate::error::ApiError;
use crate::gateway::{Gateway, Loaded, TablePath, actor_of};
use crate::onboard::{self, Member, ScanOutcome};
use crate::registry;
use crate::store::{Anchor, AuditEvent, StoreError};
use crate::verify;

/// A column given by name or by Iceberg field id.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ColumnRef {
    /// Field id.
    Id(i32),
    /// Current column name.
    Name(String),
}

/// A constraint given by name or by id.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ConstraintRef {
    /// Constraint id.
    Id(u64),
    /// Constraint name.
    Name(String),
}

/// FK target in a registration request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceRequest {
    /// Parent table identifier.
    pub table: String,
    /// Parent PRIMARY KEY or UNIQUE constraint.
    pub constraint: ConstraintRef,
}

/// `POST /v1/integrity/constraints` (spec §23).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterRequest {
    /// Table identifier `namespace.table`.
    pub table: String,
    /// Name, unique per table.
    pub name: String,
    /// `PRIMARY_KEY`, `UNIQUE`, `FOREIGN_KEY` or `NOT_NULL` (any case).
    #[serde(rename = "type")]
    pub kind: String,
    /// Key columns in order (one for `NOT_NULL`).
    pub columns: Vec<ColumnRef>,
    /// UNIQUE: `DISTINCT` (default) or `NOT_DISTINCT`.
    #[serde(default)]
    pub nulls: Option<String>,
    /// FK target.
    #[serde(default)]
    pub references: Option<ReferenceRequest>,
    /// FK: `SIMPLE` (default) or `FULL`.
    #[serde(default, rename = "match")]
    pub match_mode: Option<String>,
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidConstraint, m)
}

fn store_error(e: StoreError) -> ApiError {
    ApiError::new(ErrorCode::IndexDegraded, e.to_string())
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        store_error(e)
    }
}

fn normalize(s: &str) -> String {
    s.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

/// The result of scanning a domain: index contents and the snapshot each table was read at.
struct Scanned {
    indexes: BTreeMap<ConstraintId, (IndexKind, Vec<(EncodedKey, IndexValue)>)>,
    anchors: BTreeMap<String, (String, Option<i64>)>,
}

impl Gateway {
    fn table_path(&self, identifier: &str) -> Result<TablePath, ApiError> {
        TablePath::of(&self.base, identifier)
            .ok_or_else(|| invalid(format!("{identifier} is not namespace.table")))
    }

    async fn load_member(
        &self,
        identifier: &str,
        headers: &HeaderMap,
    ) -> Result<TableMetadata, ApiError> {
        let path = self.table_path(identifier)?.path();
        match self.load(&path, headers).await? {
            Loaded::Table(meta, _) => Ok(*meta),
            Loaded::Missing => Err(invalid(format!("table {identifier} does not exist"))),
        }
    }

    /// Scans every table of `members` under `configs` at its current `main`.
    async fn scan_domain(
        &self,
        configs: &[ConstraintConfig],
        members: &BTreeSet<String>,
        headers: &HeaderMap,
    ) -> Result<Result<Scanned, Value>, ApiError> {
        let mut loaded = Vec::new();
        let mut anchors = BTreeMap::new();
        for ident in members {
            let meta = self.load_member(ident, headers).await?;
            anchors.insert(
                ident.clone(),
                (
                    meta.table_uuid.to_lowercase(),
                    meta.main_snapshot_id().map(|s| s.0),
                ),
            );
            loaded.push(Member {
                identifier: ident.clone(),
                meta,
            });
        }
        let io = Arc::clone(&self.io);
        let configs = configs.to_vec();
        let redact = self.redact_keys;
        let outcome = tokio::task::spawn_blocking(move || {
            onboard::scan(io.as_ref(), &configs, &loaded, redact)
        })
        .await
        .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, format!("scan task failed: {e}")))??;
        Ok(match outcome {
            ScanOutcome::Clean(indexes) => Ok(Scanned { indexes, anchors }),
            ScanOutcome::Violations(v) => Err(json!({ "violations": v })),
        })
    }

    /// Replaces the contents of every scanned index at one epoch above all of them.
    fn install(&self, scanned: &Scanned) -> Result<(), ApiError> {
        let degraded = |e: integrity_index::IndexError| {
            ApiError::new(ErrorCode::IndexDegraded, format!("index store: {e}"))
        };
        let mut indexes = Vec::new();
        let mut epoch = 0;
        for (id, (kind, entries)) in &scanned.indexes {
            let index = self.store.index(*id, *kind).map_err(degraded)?;
            epoch = epoch.max(index.epoch().map_err(degraded)?.0);
            indexes.push((index, entries));
        }
        for (n, (index, entries)) in indexes.into_iter().enumerate() {
            if n > 0 {
                fault::hit(FaultPoint::DuringRebuildSwap);
            }
            index
                .replace_all(entries, IndexEpoch(epoch + 1))
                .map_err(degraded)?;
        }
        Ok(())
    }

    fn clear_transient(&self, members: &BTreeSet<String>) {
        if let Ok(mut t) = self.transient.lock() {
            t.retain(|k, _| !members.contains(k));
        }
    }

    /// Registers a constraint, onboarding its domain (spec §20).
    pub async fn register(
        &self,
        req: RegisterRequest,
        headers: &HeaderMap,
    ) -> Result<Value, ApiError> {
        let _exclusive = self.admin.write().await;
        self.recover(headers, None).await?;
        let doc = self.registry.snapshot();
        if req.name.trim().is_empty() {
            return Err(invalid("name is empty"));
        }
        if doc
            .constraints
            .values()
            .any(|c| c.table == req.table && c.name == req.name)
        {
            return Err(invalid(format!(
                "{} already has a constraint named {}",
                req.table, req.name
            )));
        }
        let kind = match normalize(&req.kind).as_str() {
            "primary_key" => "primary_key",
            "unique" => "unique",
            "foreign_key" => "foreign_key",
            "not_null" => "not_null",
            other => return Err(invalid(format!("unknown constraint type {other}"))),
        };
        let nulls = match req.nulls.as_deref().map(normalize).as_deref() {
            None => None,
            Some("distinct") if kind == "unique" => Some("distinct".to_owned()),
            Some("not_distinct") if kind == "unique" => Some("not_distinct".to_owned()),
            Some(other) => return Err(invalid(format!("nulls = {other}"))),
        };
        let match_mode = match req.match_mode.as_deref().map(normalize).as_deref() {
            None => None,
            Some("simple") if kind == "foreign_key" => Some("simple".to_owned()),
            Some("full") if kind == "foreign_key" => Some("full".to_owned()),
            Some(other) => return Err(invalid(format!("match = {other}"))),
        };

        let meta = self.load_member(&req.table, headers).await?;
        let schema = meta
            .current_schema()
            .ok_or_else(|| invalid(format!("{} has no current schema", req.table)))?;
        let mut columns = Vec::new();
        let mut column_names = Vec::new();
        for c in &req.columns {
            let field = match c {
                ColumnRef::Id(id) => match schema.lookup(FieldId(*id)) {
                    FieldLookup::TopLevel(f) => f,
                    _ => return Err(invalid(format!("no top-level field with id {id}"))),
                },
                ColumnRef::Name(name) => schema
                    .fields
                    .iter()
                    .find(|f| f.name == *name)
                    .ok_or_else(|| invalid(format!("{} has no column {name}", req.table)))?,
            };
            columns.push(field.id);
            column_names.push(field.name.clone());
        }
        if columns.is_empty() {
            return Err(invalid("no columns"));
        }
        let references = match (&req.references, kind) {
            (None, "foreign_key") => return Err(invalid("a foreign key needs references")),
            (None, _) => None,
            (Some(_), k) if k != "foreign_key" => {
                return Err(invalid("only a foreign key has references"));
            }
            (Some(r), _) => {
                let parent = doc
                    .constraints
                    .values()
                    .find(|c| {
                        c.table == r.table
                            && match &r.constraint {
                                ConstraintRef::Id(id) => c.id == *id,
                                ConstraintRef::Name(n) => c.name == *n,
                            }
                    })
                    .ok_or_else(|| {
                        ApiError::new(
                            ErrorCode::ConstraintNotFound,
                            format!("no such constraint on {}", r.table),
                        )
                    })?;
                if parent.kind != "primary_key" && parent.kind != "unique" {
                    return Err(invalid(format!(
                        "{} is not a PRIMARY KEY or UNIQUE constraint",
                        parent.name
                    )));
                }
                Some(ReferenceConfig {
                    table: r.table.clone(),
                    constraint: parent.id,
                })
            }
        };

        let config = ConstraintConfig {
            id: doc.next_id,
            table: req.table.clone(),
            name: req.name.clone(),
            kind: kind.to_owned(),
            columns,
            nulls,
            references,
            match_mode,
            column_names: Some(column_names),
        };
        let mut candidate = doc.list();
        candidate.push(config.clone());
        let members = registry::component(&candidate, &config.table);
        let scanned = match self.scan_domain(&candidate, &members, headers).await? {
            Ok(s) => s,
            Err(report) => {
                let mut event = AuditEvent::new("CONSTRAINT_REJECTED", Some(&config.table));
                event.actor = Some(actor_of(headers));
                event.constraints = vec![config.id];
                event.verdict = Some(ErrorCode::OnboardingViolations.code().to_owned());
                event.detail = Some(report.to_string());
                self.audit(event);
                return Err(ApiError::new(
                    ErrorCode::OnboardingViolations,
                    format!(
                        "existing data violates the constraint set; {} was not registered",
                        config.name
                    ),
                )
                .with_report(report));
            }
        };
        self.install(&scanned)?;
        let registered = config.clone();
        self.registry.update(|d| {
            d.constraints.insert(registered.id, registered.clone());
            d.next_id = registered.id + 1;
            d.bump(&registered.table);
            if let Some(r) = &registered.references
                && r.table != registered.table
            {
                d.bump(&r.table);
            }
            for (ident, (uuid, snapshot)) in &scanned.anchors {
                let anchor = Anchor {
                    table_uuid: uuid.clone(),
                    snapshot: *snapshot,
                    version: d.version(ident),
                };
                d.set_anchor(ident, anchor);
                d.degraded.remove(ident);
                d.disabled.remove(ident);
            }
            Ok::<_, ApiError>(())
        })?;
        self.clear_transient(&members);
        self.reset_domains();
        let mut event = AuditEvent::new("CONSTRAINT_REGISTERED", Some(&config.table));
        event.actor = Some(actor_of(headers));
        event.constraints = vec![config.id];
        event.detail = Some(format!("onboarded {}", join(&members)));
        self.audit(event);
        Ok(json!(config))
    }

    /// Drops a constraint. A PK/UNIQUE that a foreign key references cannot be dropped first.
    pub async fn drop_constraint(&self, id: u64, headers: &HeaderMap) -> Result<Value, ApiError> {
        let _exclusive = self.admin.write().await;
        self.recover(headers, None).await?;
        let doc = self.registry.snapshot();
        let config = doc.constraints.get(&id).cloned().ok_or_else(|| {
            ApiError::new(ErrorCode::ConstraintNotFound, format!("no constraint {id}"))
        })?;
        if let Some(fk) = doc
            .constraints
            .values()
            .find(|c| c.references.as_ref().is_some_and(|r| r.constraint == id))
        {
            return Err(invalid(format!(
                "{} is referenced by {}; drop that first",
                config.name, fk.name
            )));
        }
        self.registry.update(|d| {
            d.constraints.remove(&id);
            let mut tables = vec![config.table.clone()];
            if let Some(r) = &config.references {
                tables.push(r.table.clone());
            }
            for t in tables {
                d.bump(&t);
                if !d.is_constrained(&t) {
                    // Re-registering later onboards the table again.
                    d.remove_anchor(&t);
                    d.degraded.remove(&t);
                    d.disabled.remove(&t);
                }
            }
            Ok::<_, ApiError>(())
        })?;
        self.reset_domains();
        let mut event = AuditEvent::new("CONSTRAINT_DROPPED", Some(&config.table));
        event.actor = Some(actor_of(headers));
        event.constraints = vec![id];
        self.audit(event);
        Ok(json!(config))
    }

    /// Rebuilds the indexes of the domain of constraint `id` from the data and re-anchors its
    /// tables (spec §19). On violations the domain stays (or becomes) degraded.
    pub async fn rebuild(&self, id: u64, headers: &HeaderMap) -> Result<Value, ApiError> {
        let _exclusive = self.admin.write().await;
        self.recover(headers, None).await?;
        let doc = self.registry.snapshot();
        let config = doc.constraints.get(&id).cloned().ok_or_else(|| {
            ApiError::new(ErrorCode::ConstraintNotFound, format!("no constraint {id}"))
        })?;
        let constraints = doc.list();
        let members = registry::component(&constraints, &config.table);
        let scanned = match self.scan_domain(&constraints, &members, headers).await? {
            Ok(s) => s,
            Err(report) => {
                let reason = "rebuild found constraint violations in the data".to_owned();
                self.registry.update(|d| {
                    for m in &members {
                        d.degraded.insert(m.clone(), reason.clone());
                    }
                    Ok::<_, ApiError>(())
                })?;
                let mut event = AuditEvent::new("DOMAIN_DEGRADED", Some(&config.table));
                event.actor = Some(actor_of(headers));
                event.verdict = Some(ErrorCode::OnboardingViolations.code().to_owned());
                event.detail = Some(report.to_string());
                self.audit(event);
                return Err(
                    ApiError::new(ErrorCode::OnboardingViolations, reason).with_report(report)
                );
            }
        };
        self.install(&scanned)?;
        let previous = doc.anchors.clone();
        self.registry.update(|d| {
            for (ident, (uuid, snapshot)) in &scanned.anchors {
                let anchor = Anchor {
                    table_uuid: uuid.clone(),
                    snapshot: *snapshot,
                    version: d.version(ident),
                };
                d.set_anchor(ident, anchor);
                d.degraded.remove(ident);
                d.disabled.remove(ident);
            }
            Ok::<_, ApiError>(())
        })?;
        self.clear_transient(&members);
        self.reset_domains();
        let relinks: Vec<Value> = scanned
            .anchors
            .iter()
            .map(|(ident, (_, snapshot))| {
                json!({
                    "table": ident,
                    "previous_anchor": previous.get(ident).and_then(|a| a.snapshot),
                    "anchor": snapshot,
                })
            })
            .collect();
        let mut event = AuditEvent::new("INDEX_REBUILT", Some(&config.table));
        event.actor = Some(actor_of(headers));
        event.constraints = scanned.indexes.keys().map(|c| c.0).collect();
        event.detail = Some(Value::Array(relinks.clone()).to_string());
        self.audit(event);
        Ok(json!({ "domain": members, "anchors": relinks }))
    }

    /// Disables the domain of `table` (spec §19): its commits are forwarded without validation and
    /// without certificates until an operator rebuilds it. For emergencies, e.g. a degraded domain
    /// that must accept writes; audited.
    pub async fn disable(
        &self,
        table: &str,
        reason: &str,
        headers: &HeaderMap,
    ) -> Result<Value, ApiError> {
        let _exclusive = self.admin.write().await;
        let doc = self.registry.snapshot();
        if !doc.is_constrained(table) {
            return Err(ApiError::new(
                ErrorCode::ConstraintNotFound,
                format!("{table} has no constraints"),
            ));
        }
        if reason.trim().is_empty() {
            return Err(invalid("disabling a domain needs a reason"));
        }
        let members = registry::component(&doc.list(), table);
        self.registry.update(|d| {
            for m in &members {
                d.disabled.insert(m.clone(), reason.to_owned());
            }
            Ok::<_, ApiError>(())
        })?;
        self.reset_domains();
        let mut event = AuditEvent::new("DOMAIN_DISABLED", Some(table));
        event.actor = Some(actor_of(headers));
        event.detail = Some(format!("{reason} (tables: {})", join(&members)));
        self.audit(event);
        Ok(json!({ "domain": members, "state": "Disabled", "reason": reason }))
    }

    /// `GET /v1/integrity/constraints?table=…`.
    pub fn constraints(&self, table: Option<&str>) -> Value {
        let doc = self.registry.snapshot();
        let list: Vec<&ConstraintConfig> = doc
            .constraints
            .values()
            .filter(|c| table.is_none_or(|t| c.table == t))
            .collect();
        json!({ "constraints": list })
    }

    /// `GET /v1/integrity/audit?table=…&since=…`.
    pub fn audit_events(&self, table: Option<&str>, since: u64) -> Result<Value, ApiError> {
        let events = self.registry.audit_since(table, since)?;
        Ok(json!({ "events": events }))
    }
}

/// Read-only views: verification, transactions, domains, metrics (spec §18, §23, §25).
impl Gateway {
    /// `GET /v1/integrity/verify?table=…`: recomputes the certificate chain of `main` from the
    /// data files. A broken chain is a bypass: the domain is degraded until rebuilt (spec §18).
    pub async fn verify(&self, table: &str, headers: &HeaderMap) -> Result<Value, ApiError> {
        let _shared = self.admin.read().await;
        let doc = self.registry.snapshot();
        let history = doc.history.get(table).cloned().unwrap_or_default();
        if history.is_empty() {
            return Err(ApiError::new(
                ErrorCode::ConstraintNotFound,
                format!("{table} has never had constraints"),
            ));
        }
        let meta = self.load_member(table, headers).await?;
        let mut others = BTreeMap::new();
        for c in history.values().flatten() {
            let mut names = vec![c.table.clone()];
            if let Some(r) = &c.references {
                names.push(r.table.clone());
            }
            for name in names {
                if name != table && !others.contains_key(&name) {
                    // A table that cannot be loaded makes its versions unverifiable, not an error.
                    if let Ok(m) = self.load_member(&name, headers).await {
                        others.insert(name, m);
                    }
                }
            }
        }
        let mut anchors: Vec<Option<i64>> =
            doc.retired_anchors.get(table).cloned().unwrap_or_default();
        if let Some(a) = doc.anchors.get(table) {
            anchors.push(a.snapshot);
        }
        let input = verify::Input {
            identifier: table.to_owned(),
            meta,
            history,
            others,
            anchors,
        };
        let io = Arc::clone(&self.io);
        let report = tokio::task::spawn_blocking(move || verify::verify(io.as_ref(), &input))
            .await
            .map_err(|e| {
                ApiError::new(ErrorCode::IndexDegraded, format!("verify task failed: {e}"))
            })?;
        if let Some(broken) = report.first_broken {
            let reason = format!("{table}: certificate chain broken at snapshot {broken}");
            let newly = self.registry.update(|d| {
                Ok::<_, ApiError>(
                    d.degraded
                        .insert(table.to_owned(), reason.clone())
                        .is_none(),
                )
            })?;
            if newly {
                self.metrics.bypass_detected.fetch_add(1, Ordering::Relaxed);
                let mut event = AuditEvent::new("BYPASS_DETECTED", Some(table));
                event.result_snapshot = Some(broken);
                event.detail = Some(reason);
                self.audit(event);
            }
        }
        Ok(json!(report))
    }

    /// `GET /v1/integrity/transactions/{id}`: everything the log recorded, including the
    /// structured error of a refused commit (spec §22).
    pub fn transaction(&self, id: u64) -> Option<Value> {
        let t = self.log.summary(TxnId(id))?;
        let error = self.registry.error_of(id).ok().flatten();
        Some(json!({
            "txn": id,
            "state": format!("{:?}", t.state).to_ascii_uppercase(),
            "table": t.prepared.identifier,
            "table_uuid": t.prepared.table,
            "request_id": t.prepared.request_id,
            "validated": t.validated,
            "decision": t.decision,
            "error": error,
        }))
    }

    /// `GET /v1/integrity/domains/{table}`: the domain `table` belongs to (spec §19).
    pub fn domain_state(&self, table: &str) -> Option<Value> {
        let doc = self.registry.snapshot();
        if !doc.is_constrained(table) {
            return None;
        }
        let constraints = doc.list();
        let members = registry::component(&constraints, table);
        let transient = self.transient.lock().map(|t| t.clone()).unwrap_or_default();
        let reasons: BTreeMap<&String, &String> = members
            .iter()
            .filter_map(|m| {
                doc.degraded
                    .get(m)
                    .or_else(|| transient.get(m))
                    .map(|r| (m, r))
            })
            .collect();
        let unresolved = self
            .log
            .unresolved()
            .map(|u| {
                u.iter()
                    .filter(|t| members.contains(&t.prepared.identifier))
                    .count()
            })
            .unwrap_or(usize::MAX);
        let disabled: BTreeMap<&String, &String> = members
            .iter()
            .filter_map(|m| doc.disabled.get(m).map(|r| (m, r)))
            .collect();
        let state = if !disabled.is_empty() {
            "Disabled"
        } else if !reasons.is_empty() {
            "Degraded"
        } else if unresolved > 0 {
            "RecoveryRequired"
        } else {
            "Healthy"
        };
        let ids: Vec<u64> = constraints
            .iter()
            .filter(|c| members.contains(&c.table))
            .map(|c| c.id)
            .collect();
        let anchors: BTreeMap<&String, &Anchor> = members
            .iter()
            .filter_map(|m| doc.anchors.get(m).map(|a| (m, a)))
            .collect();
        let versions: BTreeMap<&String, u64> =
            members.iter().map(|m| (m, doc.version(m))).collect();
        Some(json!({
            "id": members.first(),
            "tables": members,
            "state": state,
            "reasons": reasons,
            "disabled": disabled,
            "constraints": ids,
            "anchors": anchors,
            "constraint_set_versions": versions,
        }))
    }

    /// `GET /metrics`.
    pub fn metrics_text(&self) -> String {
        let doc = self.registry.snapshot();
        let transient = self.transient.lock().map(|t| t.clone()).unwrap_or_default();
        let disabled = doc
            .anchors
            .keys()
            .filter(|t| doc.disabled.contains_key(*t))
            .count();
        let degraded = doc
            .anchors
            .keys()
            .filter(|t| !doc.disabled.contains_key(*t))
            .filter(|t| doc.degraded.contains_key(*t) || transient.contains_key(*t))
            .count();
        let unresolved = self.log.unresolved().map(|u| u.len()).unwrap_or(0);
        self.metrics.render(
            doc.anchors.len() - degraded - disabled,
            degraded,
            disabled,
            unresolved,
        )
    }
}

fn join(members: &BTreeSet<String>) -> String {
    members.iter().cloned().collect::<Vec<_>>().join(", ")
}
