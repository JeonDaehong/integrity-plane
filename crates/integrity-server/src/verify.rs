//! Certificate chain verification (spec §18), run on a blocking thread.
//!
//! Walks `main` back from its head to where the chain starts (an anchor without a certificate, or
//! the first snapshot) and recomputes every certificate from the data files: the constraint set of
//! the version the snapshot names, the key delta of its file changes, and its parent's certificate.
//! Nothing is taken from the Plane's indexes or transaction log.

use std::collections::BTreeMap;

use integrity_core::{
    CertificateInput, Digest, SUMMARY_CONSTRAINT_SET_VERSION, certificate, constraint_set_digest,
    key_delta_digest, parse_table_uuid,
};
use integrity_iceberg::snapshot_certificate;
use integrity_iceberg::{Budgeted, FileIo, TableMetadata, commit_rows, diff_snapshots};
use integrity_types::{ConstraintSetVersion, SnapshotId};
use integrity_validator::Validator;
use serde::Serialize;

use crate::config::ConstraintConfig;
use crate::registry;

/// The verdict on one snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LinkStatus {
    /// Certificate present and recomputed identically.
    Ok,
    /// Where the chain starts: an anchor without a certificate.
    Anchor,
    /// No certificate: written without the Plane.
    Missing,
    /// A certificate that does not match the data, the constraint set or the parent's certificate.
    Mismatch,
    /// A certificate field that cannot be parsed, or a constraint set version the Plane never had.
    Malformed,
    /// Valid Iceberg the verifier cannot recompute in 0.1 (e.g. equality deletes).
    Unverifiable,
}

/// One snapshot of the chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Link {
    /// Snapshot id.
    pub snapshot: i64,
    /// Its parent.
    pub parent: Option<i64>,
    /// Its `operation`.
    pub operation: Option<String>,
    /// The verdict.
    pub status: LinkStatus,
    /// Why, when not `OK`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// `GET /v1/integrity/verify?table=…`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Report {
    /// Table identifier.
    pub table: String,
    /// Head of `main`.
    pub head: Option<i64>,
    /// Every snapshot from the chain start to the head, oldest first.
    pub chain: Vec<Link>,
    /// Oldest snapshot whose certificate is missing, mismatched or malformed: where a writer
    /// bypassed the Plane or tampered with the chain.
    pub first_broken: Option<i64>,
    /// True iff every snapshot is `OK` or `ANCHOR`.
    pub ok: bool,
}

/// What the verifier needs from the registry and the catalog.
pub struct Input {
    /// Table identifier.
    pub identifier: String,
    /// The table.
    pub meta: TableMetadata,
    /// Constraint sets by version (registry history of this table).
    pub history: BTreeMap<u64, Vec<ConstraintConfig>>,
    /// Current metadata of every other table those constraint sets name (for column types).
    pub others: BTreeMap<String, TableMetadata>,
    /// Snapshots that start a chain (current and retired anchors).
    pub anchors: Vec<Option<i64>>,
}

/// A constraint set version, resolved for this table.
struct Version {
    table: integrity_types::TableId,
    validator: Validator,
    digest: Digest,
    columns: Vec<(integrity_types::FieldId, integrity_core::LogicalType)>,
}

fn resolve_version(input: &Input, v: u64) -> Result<Version, String> {
    let configs = input
        .history
        .get(&v)
        .ok_or_else(|| format!("constraint set version {v} is unknown"))?;
    let mut bindings = BTreeMap::new();
    let mut tables: Vec<&str> = vec![input.identifier.as_str()];
    for c in configs {
        tables.push(&c.table);
        if let Some(r) = &c.references {
            tables.push(&r.table);
        }
    }
    for t in tables {
        if bindings.contains_key(t) {
            continue;
        }
        let meta = if t == input.identifier {
            &input.meta
        } else {
            input
                .others
                .get(t)
                .ok_or_else(|| format!("table {t} of constraint set {v} cannot be loaded"))?
        };
        bindings.insert(
            t.to_owned(),
            registry::bind(configs, t, meta).map_err(|e| e.message)?,
        );
    }
    let table = bindings[&input.identifier].table.clone();
    let types = bindings[&input.identifier].columns.clone();
    let resolved = registry::resolve(configs, &bindings).map_err(|e| e.message)?;
    let validator = Validator::new(resolved);
    let digest = constraint_set_digest(ConstraintSetVersion(v), &validator.governing(&table))
        .map_err(|e| e.to_string())?;
    let columns = validator
        .projection(&table)
        .into_iter()
        .map(|f| {
            types
                .get(&f)
                .cloned()
                .map(|t| (f, t))
                .ok_or_else(|| format!("no type for {f}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Version {
        table,
        validator,
        digest,
        columns,
    })
}

/// Verifies the chain of `main`.
pub fn verify(io: &(dyn FileIo + Send + Sync), input: &Input) -> Report {
    let io = Budgeted::new(io, u64::MAX);
    let meta = &input.meta;
    let head = meta.main_snapshot_id().map(|s| s.0);

    // Head back to the chain start.
    let mut ids = Vec::new();
    let mut cursor = head;
    let mut truncated = None;
    while let Some(id) = cursor {
        let Some(snap) = meta.snapshot(SnapshotId(id)) else {
            truncated = Some(id);
            break;
        };
        ids.push(id);
        let certified = !matches!(snapshot_certificate(meta, SnapshotId(id)), Ok(None));
        if !certified && input.anchors.contains(&Some(id)) {
            break;
        }
        cursor = snap.parent_snapshot_id;
    }
    ids.reverse();

    let uuid = parse_table_uuid(&meta.table_uuid);
    let mut versions: BTreeMap<u64, Result<Version, String>> = BTreeMap::new();
    let mut chain = Vec::new();
    for id in ids {
        let Some(snap) = meta.snapshot(SnapshotId(id)) else {
            continue;
        };
        let link = |status, detail: Option<String>| Link {
            snapshot: id,
            parent: snap.parent_snapshot_id,
            operation: snap.summary.get("operation").cloned(),
            status,
            detail,
        };
        let cert = match snapshot_certificate(meta, SnapshotId(id)) {
            Ok(Some(c)) => c,
            Ok(None) if input.anchors.contains(&Some(id)) => {
                chain.push(link(LinkStatus::Anchor, None));
                continue;
            }
            Ok(None) => {
                chain.push(link(
                    LinkStatus::Missing,
                    Some("no certificate: committed without the Plane".into()),
                ));
                continue;
            }
            Err(_) => {
                chain.push(link(
                    LinkStatus::Malformed,
                    Some("unreadable certificate".into()),
                ));
                continue;
            }
        };
        let Some(v) = snap
            .summary
            .get(SUMMARY_CONSTRAINT_SET_VERSION)
            .and_then(|v| v.parse::<u64>().ok())
        else {
            chain.push(link(
                LinkStatus::Malformed,
                Some("no constraint set version".into()),
            ));
            continue;
        };
        let version = match versions
            .entry(v)
            .or_insert_with(|| resolve_version(input, v))
        {
            Ok(version) => version,
            Err(e) => {
                chain.push(link(LinkStatus::Malformed, Some(e.clone())));
                continue;
            }
        };
        // The certificate covers the change from the parent: without the parent's metadata (it
        // expired, or the catalog serves only recent snapshots, as Nessie does) it cannot be
        // recomputed. That is not evidence of a bypass.
        let parent_missing = match snap.parent_snapshot_id {
            Some(p) => meta.snapshot(SnapshotId(p)).is_none(),
            None => snap.sequence_number.is_some_and(|n| n > 1),
        };
        if parent_missing {
            chain.push(link(
                LinkStatus::Unverifiable,
                Some("the catalog does not serve the parent snapshot".into()),
            ));
            continue;
        }
        let previous = match snap.parent_snapshot_id {
            None => Digest::ZERO,
            Some(p) => match meta
                .snapshot(SnapshotId(p))
                .map(|_| snapshot_certificate(meta, SnapshotId(p)))
            {
                Some(Ok(Some(c))) => c,
                // An uncertified parent is a chain root (or reported itself).
                _ => Digest::ZERO,
            },
        };
        let parent_list = snap
            .parent_snapshot_id
            .and_then(|p| meta.snapshot(SnapshotId(p)))
            .and_then(|p| p.manifest_list.clone());
        let recomputed = (|| -> Result<Digest, String> {
            let list = snap
                .manifest_list
                .as_deref()
                .ok_or("snapshot without manifest list")?;
            let changes =
                diff_snapshots(&io, parent_list.as_deref(), list).map_err(|e| e.to_string())?;
            let rows = commit_rows(
                &io,
                version.table.clone(),
                SnapshotId(id),
                &changes,
                &version.columns,
            )
            .map_err(|e| e.to_string())?;
            let deltas = version
                .validator
                .net_key_deltas(&rows)
                .map_err(|e| e.to_string())?;
            let key_delta = key_delta_digest(&deltas).map_err(|e| e.to_string())?;
            let uuid = uuid.map_err(|e| e.to_string())?;
            Ok(certificate(&CertificateInput {
                table_uuid: uuid,
                snapshot: SnapshotId(id),
                parent: snap.parent_snapshot_id.map(SnapshotId),
                constraint_set: version.digest,
                key_delta,
                previous,
            }))
        })();
        chain.push(match recomputed {
            Ok(expected) if expected == cert => link(LinkStatus::Ok, None),
            Ok(_) => link(
                LinkStatus::Mismatch,
                Some(
                    "certificate does not match the data, the constraint set or the parent".into(),
                ),
            ),
            Err(e) => link(LinkStatus::Unverifiable, Some(e)),
        });
    }
    if let Some(id) = truncated {
        chain.insert(
            0,
            Link {
                snapshot: id,
                parent: None,
                operation: None,
                status: LinkStatus::Unverifiable,
                detail: Some("snapshot expired before the chain start".into()),
            },
        );
    }
    let first_broken = chain
        .iter()
        .find(|l| {
            matches!(
                l.status,
                LinkStatus::Missing | LinkStatus::Mismatch | LinkStatus::Malformed
            )
        })
        .map(|l| l.snapshot);
    let ok = chain
        .iter()
        .all(|l| matches!(l.status, LinkStatus::Ok | LinkStatus::Anchor));
    Report {
        table: input.identifier.clone(),
        head,
        chain,
        first_broken,
        ok,
    }
}
