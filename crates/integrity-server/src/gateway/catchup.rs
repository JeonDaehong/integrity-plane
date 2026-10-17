//! Catching up from the table history (RFC 0011).
//!
//! A domain table whose `main` moved past the head its indexes reflect (RFC 0008) is followed
//! snapshot by snapshot: each snapshot after the head is validated again from its data files,
//! exactly as a commit, and the certificate that validation computes must be the snapshot's own.
//! Everything validated is applied at one epoch, with the new heads, in one index transaction.
//! What cannot be proved this way is refused as before (`BYPASS_DETECTED`). Tables linked by a
//! foreign key that both moved are followed one snapshot at a time, in the order their fenced
//! commits recorded (RFC 0012).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use integrity_core::{
    CertificateInput, Digest, SUMMARY_CONSTRAINT_SET_VERSION, certificate, constraint_set_digest,
    parse_table_uuid,
};
use integrity_iceberg::{MainChange, NewSnapshot, Operation, TableMetadata, snapshot_certificate};
use integrity_index::{IndexEpoch, KeyIndex, PersistentIndex, TableHead, Writes};
use integrity_types::{ConstraintId, ConstraintSetVersion, ErrorCode, SnapshotId};
use integrity_validator::{ResolvedConstraint, Validator};

use super::Gateway;
use crate::config::ConstraintConfig;
use crate::error::ApiError;
use crate::pipeline::{self, Job, Outcome, Staged};
use crate::registry::Binding;
use crate::store::{AuditEvent, RegistryDoc};

/// A domain table whose `main` is not the snapshot its indexes reflect.
pub(super) struct Behind {
    /// Its identifier.
    pub identifier: String,
    /// Its current metadata.
    pub meta: TableMetadata,
    /// The snapshot the indexes reflect (`None`: no snapshot yet).
    pub indexed: Option<i64>,
}

/// One table validated up to its `main`, waiting to be applied.
struct Followed {
    identifier: String,
    head: TableHead,
    snapshots: usize,
    indexed: Option<i64>,
    staged: Staged,
}

impl Gateway {
    /// Brings the indexes of the `behind` tables up to their `main` (RFC 0011), or refuses the
    /// domain. Runs in the domain's queue, before the commit is validated.
    pub(super) async fn catch_up(
        &self,
        doc: &mut RegistryDoc,
        ctx: &Ctx<'_>,
        constraints: &[ConstraintConfig],
        domain: &BTreeMap<String, (String, Option<i64>)>,
        behind: Vec<Behind>,
    ) -> Result<(), ApiError> {
        if !self.catch_up.enabled {
            let b = &behind[0];
            return Err(self.refuse_head(doc, &b.identifier, &b.meta, b.indexed, None));
        }
        // Tables linked by a foreign key that both moved need the order of their commits: only
        // fenced commits record it (RFC 0011, RFC 0012).
        let moved: BTreeSet<&str> = behind.iter().map(|b| b.identifier.as_str()).collect();
        let linked = constraints.iter().find_map(|c| {
            let r = c.references.as_ref()?;
            (r.table != c.table
                && moved.contains(c.table.as_str())
                && moved.contains(r.table.as_str()))
            .then(|| (c.table.clone(), r.table.clone()))
        });
        if let Some((child, parent)) = linked {
            return match self.catch_up_in_order(doc, ctx, domain, &behind).await {
                Ok(()) => Ok(()),
                Err(Refusal::Head(why)) => {
                    let b = behind
                        .iter()
                        .find(|b| b.identifier == child)
                        .unwrap_or(&behind[0]);
                    let why = format!(
                        "{child} and {parent}, linked by a foreign key, both moved and {why}; rebuild the domain"
                    );
                    Err(self.refuse_head(doc, &b.identifier, &b.meta, b.indexed, Some(&why)))
                }
                Err(Refusal::Api(e)) => Err(e),
            };
        }

        let mut followed = Vec::new();
        for b in &behind {
            let to = b.meta.main_snapshot_id().map(|s| s.0);
            match self.follow(doc, ctx, b, b.indexed, to).await {
                Ok(f) => followed.push(f),
                Err(Refusal::Head(why)) => {
                    return Err(self.refuse_head(
                        doc,
                        &b.identifier,
                        &b.meta,
                        b.indexed,
                        Some(&why),
                    ));
                }
                Err(Refusal::Api(e)) => return Err(e),
            }
        }
        self.apply_followed(ctx.indexes, followed).await
    }

    /// Catches up tables linked by foreign keys in the order their fenced commits recorded
    /// (RFC 0012): a table advances by one snapshot when the domain state that snapshot was
    /// validated against is the current one. Each step is applied with its head before the next
    /// one is validated against it.
    async fn catch_up_in_order(
        &self,
        doc: &RegistryDoc,
        ctx: &Ctx<'_>,
        domain: &BTreeMap<String, (String, Option<i64>)>,
        behind: &[Behind],
    ) -> Result<(), Refusal> {
        let mut pending = Vec::new();
        for b in behind {
            let to = b.meta.main_snapshot_id().map(|s| s.0);
            let chain = self.chain(&b.meta, b.indexed, to)?;
            for id in &chain {
                let recorded = b
                    .meta
                    .snapshot(SnapshotId(*id))
                    .and_then(|s| s.summary.get(super::fence::SUMMARY_DOMAIN_STATE));
                if recorded.is_none() {
                    return Err(Refusal::Head(format!(
                        "snapshot {id} of {} records no domain state (not a fenced commit)",
                        b.identifier
                    )));
                }
            }
            pending.push((b, chain, 0_usize));
        }
        // The state of every table of the domain, by uuid, as the indexes reflect it.
        let mut state: BTreeMap<String, Option<i64>> = domain
            .iter()
            .map(|(ident, (uuid, main))| {
                let at = behind
                    .iter()
                    .find(|b| b.identifier == *ident)
                    .map_or(*main, |b| b.indexed);
                (uuid.clone(), at)
            })
            .collect();
        while pending.iter().any(|(_, chain, next)| *next < chain.len()) {
            let mut advanced = false;
            for (b, chain, next) in &mut pending {
                let Some(&id) = chain.get(*next) else {
                    continue;
                };
                let uuid = b.meta.table_uuid.to_lowercase();
                let mut others = state.clone();
                others.remove(&uuid);
                let recorded = b
                    .meta
                    .snapshot(SnapshotId(id))
                    .and_then(|s| s.summary.get(super::fence::SUMMARY_DOMAIN_STATE))
                    .and_then(|v| super::fence::parse_domain_state(v));
                if recorded.as_ref() != Some(&others) {
                    continue;
                }
                let from = state.get(&uuid).copied().flatten();
                let f = self.follow(doc, ctx, b, from, Some(id)).await?;
                self.apply_followed(ctx.indexes, vec![f])
                    .await
                    .map_err(Refusal::Api)?;
                state.insert(uuid, Some(id));
                *next += 1;
                advanced = true;
            }
            if !advanced {
                return Err(Refusal::Head(
                    "no snapshot was validated against the current domain state".into(),
                ));
            }
        }
        Ok(())
    }

    /// Applies validated tables at one epoch, each with its head, in one index transaction.
    async fn apply_followed(
        &self,
        indexes: &BTreeMap<ConstraintId, PersistentIndex>,
        followed: Vec<Followed>,
    ) -> Result<(), ApiError> {
        let epoch = 1 + indexes
            .values()
            .filter_map(|i| i.epoch().ok())
            .map(|e| e.0)
            .max()
            .unwrap_or(0);
        let store = self.store.clone();
        let indexes = indexes.clone();
        let applied = tokio::task::spawn_blocking(move || {
            let mut batch: Vec<(&PersistentIndex, _, Writes<'_>)> = Vec::new();
            for f in &followed {
                for source in f.staged.sources()? {
                    let index = indexes.get(&source.constraint).ok_or_else(|| {
                        ApiError::new(
                            ErrorCode::IndexDegraded,
                            format!("no index for {}", source.constraint),
                        )
                    })?;
                    batch.push((index, source.header, source.writes));
                }
            }
            let heads: Vec<(&str, TableHead)> = followed
                .iter()
                .map(|f| (f.identifier.as_str(), f.head.clone()))
                .collect();
            store
                .apply_streamed(batch, IndexEpoch(epoch), &heads)
                .map_err(|e| {
                    ApiError::new(ErrorCode::IndexDegraded, format!("index store: {e}"))
                })?;
            drop(heads);
            Ok::<_, ApiError>(
                followed
                    .into_iter()
                    .map(|f| (f.identifier, f.indexed, f.head.snapshot, f.snapshots))
                    .collect::<Vec<_>>(),
            )
        })
        .await
        .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, format!("catch-up task: {e}")))??;

        for (identifier, from, to, snapshots) in applied {
            self.metrics
                .catch_up_snapshots
                .fetch_add(snapshots as u64, Ordering::Relaxed);
            let mut event = AuditEvent::new("INDEX_CAUGHT_UP", Some(&identifier));
            event.base_snapshot = from;
            event.result_snapshot = to;
            event.detail = Some(format!("{snapshots} snapshot(s) validated again"));
            self.audit(event);
        }
        Ok(())
    }

    /// The snapshots after `from` up to `to`, oldest first; `to` must descend from `from`.
    fn chain(
        &self,
        meta: &TableMetadata,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<Vec<i64>, Refusal> {
        let mut ids = Vec::new();
        let mut at = to;
        while at != from {
            let Some(id) = at else {
                return Err(Refusal::Head(
                    "main does not descend from it (rolled back or replaced)".into(),
                ));
            };
            if ids.len() >= self.catch_up.max_snapshots {
                return Err(Refusal::Head(format!(
                    "more than {} snapshots behind",
                    self.catch_up.max_snapshots
                )));
            }
            let snap = meta.snapshot(SnapshotId(id)).ok_or_else(|| {
                Refusal::Head(format!(
                    "snapshot {id} is not in the table metadata (expired)"
                ))
            })?;
            ids.push(id);
            at = snap.parent_snapshot_id;
        }
        ids.reverse();
        Ok(ids)
    }

    /// Validates the snapshots of one table after `from` up to `to`, oldest first, as one commit.
    async fn follow(
        &self,
        doc: &RegistryDoc,
        ctx: &Ctx<'_>,
        b: &Behind,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<Followed, Refusal> {
        let meta = &b.meta;
        let to = to.ok_or_else(|| Refusal::Head("main has no snapshot".into()))?;
        let steps: Vec<_> = self
            .chain(meta, from, Some(to))?
            .into_iter()
            .filter_map(|id| meta.snapshot(SnapshotId(id)))
            .collect();

        let version = ConstraintSetVersion(doc.version(&b.identifier));
        let mut expected = Vec::new();
        for snap in &steps {
            let id = SnapshotId(snap.snapshot_id);
            match snapshot_certificate(meta, id) {
                Ok(Some(c)) => expected.push(c),
                Ok(None) => {
                    return Err(Refusal::Head(format!(
                        "snapshot {id} was not committed through a Plane"
                    )));
                }
                Err(_) => {
                    return Err(Refusal::Head(format!(
                        "snapshot {id} has a malformed certificate"
                    )));
                }
            }
            // With signatures required, only what a key of this Plane signed is followed: a
            // certificate alone can be computed by anyone (RFC 0005, RFC 0011).
            if self.require_signatures && !signed_by_us(doc, meta, id, expected.last()) {
                return Err(Refusal::Head(format!(
                    "snapshot {id} is not signed with a key of this Plane"
                )));
            }
            let certified_under = snap
                .summary
                .get(SUMMARY_CONSTRAINT_SET_VERSION)
                .and_then(|v| v.parse::<u64>().ok());
            if certified_under != Some(version.0) {
                return Err(Refusal::Head(format!(
                    "snapshot {id} was certified under constraint set version {}, not {}",
                    certified_under.map_or_else(|| "(none)".to_owned(), |v| v.to_string()),
                    version.0
                )));
            }
        }
        let change = MainChange {
            parent: from.map(SnapshotId),
            steps: steps
                .iter()
                .map(|s| {
                    Ok(NewSnapshot {
                        parent: s.parent_snapshot_id.map(SnapshotId),
                        snapshot: SnapshotId(s.snapshot_id),
                        // A hint only; the changes are read from the manifests.
                        operation: Operation::Overwrite,
                        manifest_list: s.manifest_list.clone().ok_or_else(|| {
                            Refusal::Head(format!(
                                "snapshot {} has no manifest list",
                                s.snapshot_id
                            ))
                        })?,
                        update_index: 0,
                    })
                })
                .collect::<Result<_, Refusal>>()?,
        };

        let binding = &ctx.bindings[&b.identifier];
        let validator = Validator::new(ctx.resolved.to_vec());
        let table_id = binding.table.clone();
        let columns = validator
            .projection(&table_id)
            .into_iter()
            .map(|f| {
                binding
                    .columns
                    .get(&f)
                    .cloned()
                    .map(|t| (f, t))
                    .ok_or_else(|| {
                        Refusal::Api(ApiError::new(
                            ErrorCode::IndexDegraded,
                            format!("no type for {f}"),
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let set_digest = constraint_set_digest(version, &validator.governing(&table_id))
            .map_err(|e| Refusal::Api(ApiError::new(ErrorCode::IndexDegraded, e.to_string())))?;
        let uuid = parse_table_uuid(&meta.table_uuid)
            .map_err(|_| Refusal::Head("table uuid is not a UUID".into()))?;
        let mut previous = match from {
            None => Digest::ZERO,
            Some(h) => match snapshot_certificate(meta, SnapshotId(h)) {
                Ok(Some(c)) => c,
                // An anchor without a certificate starts the chain (RFC 0002).
                _ => Digest::ZERO,
            },
        };

        let job = Job {
            io: Arc::clone(&self.io),
            budget: self.budget,
            meta: meta.clone(),
            table: table_id,
            change,
            columns,
            validator,
            indexes: ctx.indexes.clone(),
            stats: Arc::new(pipeline::Stats::default()),
            scratch: self.store.scratch_dir().to_path_buf(),
            memory: self.scan_memory,
        };
        let outcome = tokio::task::spawn_blocking(move || pipeline::run(&job))
            .await
            .map_err(|e| {
                Refusal::Api(ApiError::new(
                    ErrorCode::IndexDegraded,
                    format!("catch-up task: {e}"),
                ))
            })?
            .map_err(Refusal::Api)?;
        let (plans, staged) = match outcome {
            Outcome::Accepted { plans, staged } => (plans, staged),
            Outcome::Rejected(details) => {
                let what: Vec<String> = details
                    .keys()
                    .map(|v| format!("{} on constraint {}", v.code, v.constraint))
                    .collect();
                return Err(Refusal::Head(format!(
                    "its data violates the constraints ({})",
                    what.join(", ")
                )));
            }
        };
        for (plan, expected) in plans.iter().zip(&expected) {
            let key_delta = plan.key_delta.map_err(|e| {
                Refusal::Api(ApiError::new(ErrorCode::IndexDegraded, e.to_string()))
            })?;
            let cert = certificate(&CertificateInput {
                table_uuid: uuid,
                snapshot: plan.step.snapshot,
                parent: plan.step.parent,
                constraint_set: set_digest,
                key_delta,
                previous,
            });
            if cert != *expected {
                return Err(Refusal::Head(format!(
                    "the certificate of snapshot {} does not match its data",
                    plan.step.snapshot
                )));
            }
            previous = cert;
        }
        Ok(Followed {
            identifier: b.identifier.clone(),
            head: TableHead {
                table_uuid: meta.table_uuid.to_lowercase(),
                snapshot: Some(to),
            },
            snapshots: steps.len(),
            indexed: from,
            staged,
        })
    }
}

/// Whether snapshot `id`'s certificate `cert` is signed with a key the registry records as this
/// Plane's (RFC 0005).
fn signed_by_us(
    doc: &RegistryDoc,
    meta: &TableMetadata,
    id: SnapshotId,
    cert: Option<&Digest>,
) -> bool {
    let (Some(cert), Ok(Some((key_id, signature)))) =
        (cert, integrity_iceberg::snapshot_signature(meta, id))
    else {
        return false;
    };
    doc.keys
        .get(&key_id)
        .and_then(|k| integrity_core::signature::public_key_from_hex(&k.public_key))
        .is_some_and(|key| integrity_core::signature::verify(&key, cert, &signature))
}

/// What validating a table again needs from the commit.
pub(super) struct Ctx<'a> {
    pub bindings: &'a BTreeMap<String, Binding>,
    pub resolved: &'a [ResolvedConstraint],
    pub indexes: &'a BTreeMap<ConstraintId, PersistentIndex>,
}

/// Why a table could not catch up.
enum Refusal {
    /// The history does not prove the change: refuse the domain (RFC 0008).
    Head(String),
    /// Something failed on the way (storage, budget): answer it as such.
    Api(ApiError),
}
