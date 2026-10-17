//! Catching up from the table history (RFC 0011).
//!
//! A domain table whose `main` moved past the head its indexes reflect (RFC 0008) is followed
//! snapshot by snapshot: each snapshot after the head is validated again from its data files,
//! exactly as a commit, and the certificate that validation computes must be the snapshot's own.
//! Everything validated is applied at one epoch, with the new heads, in one index transaction.
//! What cannot be proved this way is refused as before (`BYPASS_DETECTED`).

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
        constraints: &[ConstraintConfig],
        bindings: &BTreeMap<String, Binding>,
        resolved: &[ResolvedConstraint],
        indexes: &BTreeMap<ConstraintId, PersistentIndex>,
        behind: Vec<Behind>,
    ) -> Result<(), ApiError> {
        if !self.catch_up.enabled {
            let b = &behind[0];
            return Err(self.refuse_head(doc, &b.identifier, &b.meta, b.indexed, None));
        }
        // The order of commits to different tables is not recorded: tables linked by a foreign
        // key cannot catch up together (RFC 0011).
        let moved: BTreeSet<&str> = behind.iter().map(|b| b.identifier.as_str()).collect();
        for c in constraints {
            if let Some(r) = &c.references
                && r.table != c.table
                && moved.contains(c.table.as_str())
                && moved.contains(r.table.as_str())
            {
                let b = behind
                    .iter()
                    .find(|b| b.identifier == c.table)
                    .unwrap_or(&behind[0]);
                let why = format!(
                    "{} and {}, linked by a foreign key, both moved; rebuild the domain",
                    c.table, r.table
                );
                return Err(self.refuse_head(doc, &b.identifier, &b.meta, b.indexed, Some(&why)));
            }
        }

        let mut followed = Vec::new();
        for b in &behind {
            match self.follow(doc, bindings, resolved, indexes, b).await {
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

        // Every caught-up table at one epoch, with its head, in one transaction.
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

    /// Validates the snapshots of one table after its head, oldest first, as one commit.
    async fn follow(
        &self,
        doc: &RegistryDoc,
        bindings: &BTreeMap<String, Binding>,
        resolved: &[ResolvedConstraint],
        indexes: &BTreeMap<ConstraintId, PersistentIndex>,
        b: &Behind,
    ) -> Result<Followed, Refusal> {
        let meta = &b.meta;
        let main = meta
            .main_snapshot_id()
            .ok_or_else(|| Refusal::Head("main has no snapshot".into()))?;
        // `main` must descend from the head.
        let mut steps = Vec::new();
        let mut at = Some(main.0);
        while at != b.indexed {
            let Some(id) = at else {
                return Err(Refusal::Head(
                    "main does not descend from it (rolled back or replaced)".into(),
                ));
            };
            if steps.len() >= self.catch_up.max_snapshots {
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
            steps.push(snap);
            at = snap.parent_snapshot_id;
        }
        steps.reverse();

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
            parent: b.indexed.map(SnapshotId),
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

        let binding = &bindings[&b.identifier];
        let validator = Validator::new(resolved.to_vec());
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
        let mut previous = match b.indexed {
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
            indexes: indexes.clone(),
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
                snapshot: Some(main.0),
            },
            snapshots: steps.len(),
            indexed: b.indexed,
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

/// Why a table could not catch up.
enum Refusal {
    /// The history does not prove the change: refuse the domain (RFC 0008).
    Head(String),
    /// Something failed on the way (storage, budget): answer it as such.
    Api(ApiError),
}
