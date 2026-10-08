//! Phase 6 end to end on the real PyIceberg table: classification → manifest diff → key extraction
//! → validation → index apply → certificate → injection into the forwarded request, for every
//! commit, with the certificate chain carried across steps and commits.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use integrity_core::{
    CertificateInput, Constraint, ConstraintKind, Digest, EnforcementMode, KeySpec, NullsMode,
    UniqueSpec, certificate, constraint_set_digest, parse_table_uuid,
};
use integrity_iceberg::metadata::{FieldLookup, logical_type};
use integrity_iceberg::{
    Classification, CommitRequest, FileIo, ReadError, TableMetadata, check_operation,
    check_requirements, classify, commit_rows, diff_snapshots, inject_certificate,
    snapshot_certificate,
};
use integrity_index::{IndexEpoch, KeyIndex, MemoryIndex};
use integrity_types::{ConstraintId, ConstraintSetVersion, FieldId, SnapshotId, TableId};
use integrity_validator::{Decision, ResolvedConstraint, Validator};
use serde_json::{Value, json};

struct FixtureIo;

impl FileIo for FixtureIo {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        let rest = location.split_once("/warehouse/").map(|(_, r)| r).unwrap();
        let path = format!(
            "{}/tests/fixtures/table/warehouse/{rest}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(path)
            .map(Bytes::from)
            .map_err(|e| ReadError::Io(e.to_string()))
    }
}

fn metadata(file: &str) -> (TableMetadata, Value) {
    let raw: Value = serde_json::from_slice(
        &FixtureIo
            .read(&format!("x/warehouse/db/orders/metadata/{file}"))
            .unwrap(),
    )
    .unwrap();
    (serde_json::from_value(raw.clone()).unwrap(), raw)
}

fn main_commits() -> Vec<String> {
    let path = format!(
        "{}/tests/fixtures/table/commits.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let list: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    list.as_array()
        .unwrap()
        .iter()
        .filter(|c| c["branch"] == "main")
        .map(|c| c["metadata"].as_str().unwrap().to_owned())
        .collect()
}

fn request(before: &TableMetadata, after_raw: &Value) -> CommitRequest {
    let known: BTreeSet<i64> = before.snapshots.iter().map(|s| s.snapshot_id).collect();
    let mut updates: Vec<Value> = after_raw["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| !known.contains(&s["snapshot-id"].as_i64().unwrap()))
        .map(|s| json!({"action": "add-snapshot", "snapshot": s}))
        .collect();
    updates.sort_by_key(|u| u["snapshot"]["sequence-number"].as_i64());
    updates.push(json!({"action": "set-snapshot-ref", "ref-name": "main",
        "snapshot-id": after_raw["refs"]["main"]["snapshot-id"], "type": "branch"}));
    CommitRequest::from_json(json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main",
            "snapshot-id": before.main_snapshot_id().map(|s| s.0)}],
        "updates": updates
    }))
    .unwrap()
}

/// PK(id) and UNIQUE(amount) on the orders table.
fn constraints(table: &TableId) -> Vec<Constraint> {
    let c = |id, kind| Constraint {
        id: ConstraintId(id),
        table: table.clone(),
        name: format!("c{id}"),
        kind,
        mode: EnforcementMode::Enforced,
        version: ConstraintSetVersion(1),
    };
    vec![
        c(
            1,
            ConstraintKind::PrimaryKey(KeySpec {
                columns: vec![FieldId(1)],
            }),
        ),
        c(
            2,
            ConstraintKind::Unique(UniqueSpec {
                key: KeySpec {
                    columns: vec![FieldId(4)],
                },
                nulls: NullsMode::Distinct,
            }),
        ),
    ]
}

#[test]
fn real_table_commits_are_validated_applied_and_certified() {
    let files = main_commits();
    let (first, _) = metadata(&files[0]);
    let table = TableId::new(first.table_uuid.clone());
    let uuid = parse_table_uuid(&first.table_uuid).unwrap();
    let schema = first.current_schema().unwrap().clone();
    let types: BTreeMap<FieldId, _> = (1..=4)
        .map(|id| {
            let FieldLookup::TopLevel(f) = schema.lookup(FieldId(id)) else {
                panic!()
            };
            (FieldId(id), logical_type(&f.field_type))
        })
        .collect();

    let resolved: Vec<ResolvedConstraint> = constraints(&table)
        .into_iter()
        .map(|c| {
            let key = c.kind.key().unwrap().columns.clone();
            let families = key.iter().map(|f| types[f].key_family().unwrap()).collect();
            ResolvedConstraint {
                constraint: c,
                schema: Some(integrity_core::KeySchema::new(families).unwrap()),
            }
        })
        .collect();
    let validator = Validator::new(resolved);
    let version = ConstraintSetVersion(1);
    let set_digest = constraint_set_digest(version, &validator.governing(&table)).unwrap();
    let indexes: BTreeMap<ConstraintId, MemoryIndex> = [
        (
            ConstraintId(1),
            MemoryIndex::new(integrity_index::IndexKind::Unique),
        ),
        (
            ConstraintId(2),
            MemoryIndex::new(integrity_index::IndexKind::Unique),
        ),
    ]
    .into();
    let columns: Vec<_> = validator
        .projection(&table)
        .into_iter()
        .map(|f| (f, types[&f].clone()))
        .collect();
    let constrained: BTreeSet<FieldId> = columns.iter().map(|(f, _)| *f).collect();

    let mut epoch = 0;
    let mut previous_cert = Digest::ZERO; // chain root: the table was created with its constraints
    let mut certified = Vec::new();
    for pair in files.windows(2) {
        let (before, _) = metadata(&pair[0]);
        let (after, after_raw) = metadata(&pair[1]);
        let mut req = request(&before, &after_raw);
        check_requirements(&before, &req).unwrap();
        let Classification::MainChange(change) = classify(&before, &req, &constrained).unwrap()
        else {
            panic!()
        };
        if let Some(parent) = change.parent {
            // PyIceberg wrote the fixture without the Plane: no certificates in its summaries.
            assert_eq!(snapshot_certificate(&before, parent), Ok(None));
        }
        for step in &change.steps {
            let parent_list = step
                .parent
                .map(|p| after.snapshot(p).unwrap().manifest_list.clone().unwrap());
            let changes =
                diff_snapshots(&FixtureIo, parent_list.as_deref(), &step.manifest_list).unwrap();
            let rows =
                commit_rows(&FixtureIo, table.clone(), step.snapshot, &changes, &columns).unwrap();
            check_operation(step.operation, &rows).unwrap();

            let Decision::Accepted(validated) = validator.validate(&rows, &indexes).unwrap() else {
                panic!("{}: rejected", pair[1])
            };
            epoch += 1;
            for (id, staged) in validated.stage(&indexes).unwrap() {
                indexes[&id].apply(staged, IndexEpoch(epoch)).unwrap();
            }

            let cert = certificate(&CertificateInput {
                table_uuid: uuid,
                snapshot: step.snapshot,
                parent: step.parent,
                constraint_set: set_digest,
                key_delta: validated.key_delta_digest().unwrap(),
                previous: previous_cert,
            });
            inject_certificate(&mut req, step, cert, version).unwrap();
            let summary = &req.json()["updates"][step.update_index]["snapshot"]["summary"];
            assert_eq!(summary["integrity.cert"], cert.to_hex());
            assert_eq!(summary["integrity.cert-version"], "1");
            assert_eq!(summary["integrity.constraint-set-version"], "1");
            assert!(
                summary["operation"].is_string(),
                "client summary fields are kept"
            );
            certified.push((step.snapshot, cert));
            previous_cert = cert;
        }
    }

    // append, append, COW delete, whole-file delete, overwrite (2 snapshots)
    assert_eq!(certified.len(), 6);
    let distinct: BTreeSet<_> = certified.iter().map(|(_, c)| *c).collect();
    assert_eq!(distinct.len(), 6, "every snapshot has its own certificate");

    // Final index contents: PK keys {10, 11}, UNIQUE amounts {10.00, 11.00}.
    let pk = indexes[&ConstraintId(1)].entries().unwrap();
    assert_eq!(pk.len(), 2);
    assert_eq!(indexes[&ConstraintId(2)].entries().unwrap().len(), 2);

    // A summary written by the Plane is read back by the next commit.
    let (last, mut last_raw) = metadata(files.last().unwrap());
    let head = last.main_snapshot_id().unwrap();
    let (_, head_cert) = *certified.last().unwrap();
    for s in last_raw["snapshots"].as_array_mut().unwrap() {
        if s["snapshot-id"] == head.0 {
            s["summary"]["integrity.cert"] = json!(head_cert.to_hex());
            s["summary"]["integrity.cert-version"] = json!("1");
        }
    }
    let with_cert: TableMetadata = serde_json::from_value(last_raw).unwrap();
    assert_eq!(snapshot_certificate(&with_cert, head), Ok(Some(head_cert)));
    assert!(snapshot_certificate(&with_cert, SnapshotId(-42)).is_err());
}
