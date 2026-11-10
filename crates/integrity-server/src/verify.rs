//! Certificate chain verification (spec §18), run on a blocking thread.
//!
//! Walks `main` back from its head to where the chain starts (an anchor without a certificate, or
//! the first snapshot) and recomputes every certificate from the data files: the constraint set of
//! the version the snapshot names, the key delta of its file changes, and its parent's certificate.
//! Nothing is taken from the Plane's indexes or transaction log. Rows are streamed and their keys
//! sorted within [`Input::memory`], spilling to [`Input::scratch`] (as commit validation does,
//! ADR 0019 and 0025), so a snapshot's size does not bound what can be verified.

use std::collections::BTreeMap;
use std::path::PathBuf;

use integrity_core::{
    CertificateInput, Digest, SUMMARY_CONSTRAINT_SET_VERSION, certificate, constraint_set_digest,
    parse_table_uuid,
};
use integrity_iceberg::metadata::{MAIN, SnapshotRef};
use integrity_iceberg::snapshot_certificate;
use integrity_iceberg::{
    Budgeted, FileChanges, FileIo, Operation, TableMetadata, commit_rows, diff_snapshots_with,
};
use integrity_index::{IndexEpoch, KeyIndex, Overlay, PersistentIndex, PersistentStore};
use integrity_types::{ConstraintId, ConstraintSetVersion, SnapshotId};
use integrity_validator::{Decision, Validator};
use serde::Serialize;

use crate::config::ConstraintConfig;
use crate::pipeline::Streamed;
use crate::{onboard, registry};

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
    /// Valid Iceberg the verifier cannot recompute (an expired parent, an unreadable file).
    Unverifiable,
}

/// The signature of a certified snapshot (RFC 0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignatureStatus {
    /// Signed by a trusted key; signature correct.
    Valid,
    /// Not signed.
    Absent,
    /// Signed by a key the verifier does not trust.
    UnknownKey,
    /// Wrong signature, or only one of the two fields present.
    Invalid,
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
    /// Signature status of a certified snapshot (RFC 0005).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<SignatureStatus>,
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
    /// True iff every certified snapshot carries a valid signature by a trusted key.
    pub signed: bool,
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
    /// Public keys whose signatures are trusted.
    pub trusted_keys: Vec<[u8; 32]>,
    /// Unsigned snapshots and unknown keys break the chain.
    pub require_signatures: bool,
    /// Directory for sorted runs that outgrow `memory`.
    pub scratch: PathBuf,
    /// Bytes of keys kept in memory per snapshot before spilling (`limits.scan_memory`).
    pub memory: usize,
}

fn signature_status(input: &Input, snapshot: SnapshotId, cert: &Digest) -> SignatureStatus {
    match integrity_iceberg::snapshot_signature(&input.meta, snapshot) {
        Err(_) => SignatureStatus::Invalid,
        Ok(None) => SignatureStatus::Absent,
        Ok(Some((key_id, signature))) => match input
            .trusted_keys
            .iter()
            .find(|k| integrity_core::signature::key_id(k) == key_id)
        {
            None => SignatureStatus::UnknownKey,
            Some(key) if integrity_core::signature::verify(key, cert, &signature) => {
                SignatureStatus::Valid
            }
            Some(_) => SignatureStatus::Invalid,
        },
    }
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

/// The replay index of ADR 0033: the table's own PK/UNIQUE indexes, in a scratch index store
/// deleted when dropped, as of snapshot `at`, built by an onboarding scan of one snapshot and
/// moved by validating each later snapshot against it.
struct Replay {
    path: PathBuf,
    store: Option<PersistentStore>,
    indexes: BTreeMap<ConstraintId, PersistentIndex>,
    validator: Validator,
    /// The constraint set version it was built for.
    version: u64,
    /// The snapshot whose keys it holds (`None`: before the first snapshot).
    at: Option<i64>,
}

impl Drop for Replay {
    fn drop(&mut self) {
        self.indexes.clear();
        if let Some(store) = self.store.take() {
            let scratch = store.scratch_dir().to_path_buf();
            drop(store);
            let _ = std::fs::remove_dir_all(scratch);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Replay {
    /// Scans the table at snapshot `at` into a new replay index for constraint set version `v`.
    /// Only the table's own constraints are replayed: equality deletes are accepted on tables
    /// without a foreign key (ADR 0009), and FKs referencing the table only decide commits.
    fn build(
        io: &(dyn FileIo + Send + Sync),
        input: &Input,
        v: u64,
        at: Option<i64>,
    ) -> Result<Self, String> {
        let configs = input
            .history
            .get(&v)
            .ok_or_else(|| format!("constraint set version {v} is unknown"))?;
        let own: Vec<ConstraintConfig> = configs
            .iter()
            .filter(|c| c.table == input.identifier)
            .cloned()
            .collect();
        if own.iter().any(|c| c.references.is_some()) {
            return Err(
                "equality deletes on a table with a foreign key cannot be replayed (ADR 0009)"
                    .into(),
            );
        }
        // The table as of `at`.
        let mut meta = input.meta.clone();
        meta.current_snapshot_id = at;
        match at {
            Some(id) => {
                meta.refs.insert(
                    MAIN.to_owned(),
                    SnapshotRef {
                        snapshot_id: id,
                        ref_type: "branch".into(),
                    },
                );
            }
            None => {
                meta.refs.remove(MAIN);
            }
        }
        std::fs::create_dir_all(&input.scratch).map_err(|e| e.to_string())?;
        let path = input.scratch.join(format!(
            "verify-replay-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        let store = PersistentStore::open(&path).map_err(|e| format!("replay index: {e}"))?;
        let mut replay = Self {
            path,
            store: None,
            indexes: BTreeMap::new(),
            validator: Validator::new(Vec::new()),
            version: v,
            at,
        };
        let members = [onboard::Member {
            identifier: input.identifier.clone(),
            meta: meta.clone(),
        }];
        let scanned = onboard::scan(io, &store, input.memory, &own, &members, true);
        replay.store = Some(store);
        let store = replay.store.as_ref().ok_or("replay index")?;
        match scanned.map_err(|e| format!("replay index: {}", e.message))? {
            onboard::ScanOutcome::Clean(builds) => {
                store
                    .install(builds, &[])
                    .map_err(|e| format!("replay index: {e}"))?;
            }
            onboard::ScanOutcome::Violations(_) => {
                return Err(format!(
                    "snapshot {} violates the constraint set, which certified snapshots follow",
                    at.map_or_else(|| "(none)".to_owned(), |a| a.to_string())
                ));
            }
        }
        let binding = registry::bind(&own, &input.identifier, &meta).map_err(|e| e.message)?;
        let bindings = BTreeMap::from([(input.identifier.clone(), binding)]);
        let resolved = registry::resolve(&own, &bindings).map_err(|e| e.message)?;
        let mut indexes = BTreeMap::new();
        for rc in &resolved {
            if let Some(kind) = rc.index_kind() {
                let index = store
                    .index(rc.constraint.id, kind)
                    .map_err(|e| format!("replay index: {e}"))?;
                indexes.insert(rc.constraint.id, index);
            }
        }
        replay.indexes = indexes;
        replay.validator = Validator::new(resolved);
        Ok(replay)
    }

    /// Validates the snapshot's changes against the replay index as the gateway validated them,
    /// moves the index past it, and returns its key delta digest. A rejection means a certified
    /// snapshot whose data violates the constraints: `MISMATCH`.
    fn step(
        &mut self,
        io: &impl FileIo,
        changes: &FileChanges,
        version: &Version,
        snapshot: SnapshotId,
        input: &Input,
    ) -> Result<Digest, (LinkStatus, String)> {
        let unverifiable = |e: String| (LinkStatus::Unverifiable, e);
        let overlays: BTreeMap<ConstraintId, Overlay> = self
            .indexes
            .iter()
            .map(|(id, index)| (*id, Overlay::new(index as &dyn KeyIndex)))
            .collect();
        let decided = if changes.equality_deletes.is_empty() {
            let streamed = Streamed {
                validator: &self.validator,
                table: &version.table,
                snapshot,
                operation: Operation::Overwrite,
                columns: &version.columns,
                scratch: &input.scratch,
                memory: input.memory,
            };
            streamed
                .validate(io, changes, &overlays)
                .map_err(|e| unverifiable(e.message))?
                .0
        } else {
            let rows = commit_rows(
                io,
                version.table.clone(),
                snapshot,
                changes,
                &version.columns,
            )
            .map_err(|e| unverifiable(e.to_string()))?;
            self.validator
                .validate_with_details(&rows, &overlays)
                .map_err(|e| unverifiable(e.to_string()))?
        };
        let validated = match decided {
            (Decision::Accepted(validated), _) => validated,
            (Decision::Rejected(violations), _) => {
                let codes: Vec<String> = violations.iter().map(|v| format!("{v:?}")).collect();
                return Err((
                    LinkStatus::Mismatch,
                    format!(
                        "the data of this certified snapshot violates the constraints: {}",
                        codes.join(", ")
                    ),
                ));
            }
        };
        let digest = validated
            .key_delta_digest()
            .map_err(|e| unverifiable(e.to_string()))?;
        for (id, staged) in validated
            .stage(&overlays)
            .map_err(|e| unverifiable(e.to_string()))?
        {
            overlays[&id]
                .apply(staged, IndexEpoch(0))
                .map_err(|e| unverifiable(e.to_string()))?;
        }
        let mut writes = Vec::new();
        for (id, overlay) in overlays {
            writes.push((
                id,
                overlay
                    .into_staged()
                    .map_err(|e| unverifiable(e.to_string()))?,
            ));
        }
        for (id, staged) in writes {
            let index = &self.indexes[&id];
            let next = index.epoch().map_err(|e| unverifiable(e.to_string()))?.0 + 1;
            index
                .apply(staged, IndexEpoch(next))
                .map_err(|e| unverifiable(e.to_string()))?;
        }
        Ok(digest)
    }
}

/// Verifies the chain of `main`.
pub fn verify(io: &(dyn FileIo + Send + Sync), input: &Input) -> Report {
    let raw = io;
    let io = Budgeted::new(io, u64::MAX);
    // Key counts of the table's unique keys as of the last snapshot replayed (ADR 0033).
    let mut replay: Option<Replay> = None;
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
            signature: None,
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
        let recomputed = (|| -> Result<Digest, (LinkStatus, String)> {
            let unverifiable = |e: String| (LinkStatus::Unverifiable, e);
            let list = snap
                .manifest_list
                .as_deref()
                .ok_or_else(|| unverifiable("snapshot without manifest list".into()))?;
            let changes =
                diff_snapshots_with(&io, parent_list.as_deref(), list, &meta.partitioning(true))
                    .map_err(|e| unverifiable(e.to_string()))?;
            // The replay index follows the chain from where it was built (ADR 0033): it is used
            // for every later snapshot of the same constraint set version, and built at the first
            // snapshot adding equality deletes it cannot follow to.
            let follows = replay
                .as_ref()
                .is_some_and(|r| r.version == v && r.at == snap.parent_snapshot_id);
            if !follows {
                replay = None;
            }
            if !follows && !changes.equality_deletes.is_empty() {
                replay = Some(
                    Replay::build(raw, input, v, snap.parent_snapshot_id).map_err(unverifiable)?,
                );
            }
            let key_delta = match replay.as_mut() {
                Some(r) => {
                    let stepped = r.step(&io, &changes, version, SnapshotId(id), input);
                    match stepped {
                        Ok(delta) => {
                            r.at = Some(id);
                            delta
                        }
                        Err(e) => {
                            replay = None;
                            return Err(e);
                        }
                    }
                }
                None => {
                    let streamed = Streamed {
                        validator: &version.validator,
                        table: &version.table,
                        snapshot: SnapshotId(id),
                        operation: Operation::Overwrite,
                        columns: &version.columns,
                        scratch: &input.scratch,
                        memory: input.memory,
                    };
                    streamed
                        .key_delta_digest(&io, &changes)
                        .map_err(|e| unverifiable(e.message))?
                        .map_err(|e| unverifiable(e.to_string()))?
                }
            };
            let uuid = uuid.map_err(|e| unverifiable(e.to_string()))?;
            Ok(certificate(&CertificateInput {
                table_uuid: uuid,
                snapshot: SnapshotId(id),
                parent: snap.parent_snapshot_id.map(SnapshotId),
                constraint_set: version.digest,
                key_delta,
                previous,
            }))
        })();
        let mut l = match recomputed {
            Ok(expected) if expected == cert => link(LinkStatus::Ok, None),
            Ok(_) => link(
                LinkStatus::Mismatch,
                Some(
                    "certificate does not match the data, the constraint set or the parent".into(),
                ),
            ),
            Err((status, e)) => link(status, Some(e)),
        };
        let sig = signature_status(input, SnapshotId(id), &cert);
        l.signature = Some(sig);
        if l.status == LinkStatus::Ok {
            match sig {
                SignatureStatus::Invalid => {
                    l.status = LinkStatus::Malformed;
                    l.detail = Some("invalid certificate signature".into());
                }
                SignatureStatus::Absent | SignatureStatus::UnknownKey
                    if input.require_signatures =>
                {
                    l.status = LinkStatus::Malformed;
                    l.detail = Some(
                        if sig == SignatureStatus::Absent {
                            "unsigned certificate"
                        } else {
                            "certificate signed by an untrusted key"
                        }
                        .into(),
                    );
                }
                _ => {}
            }
        }
        chain.push(l);
    }
    if let Some(id) = truncated {
        chain.insert(
            0,
            Link {
                snapshot: id,
                parent: None,
                operation: None,
                signature: None,
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
    let signed = chain
        .iter()
        .filter(|l| l.signature.is_some())
        .all(|l| l.signature == Some(SignatureStatus::Valid));
    Report {
        table: input.identifier.clone(),
        head,
        chain,
        first_broken,
        ok,
        signed,
    }
}
