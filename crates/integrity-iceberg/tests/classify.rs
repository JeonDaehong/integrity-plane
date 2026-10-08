//! Spec §15 capability matrix, update-level rows: one or more tests per row. Data-level rows
//! (what the manifests add and remove) are tested with the manifest diff.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeSet;

use integrity_iceberg::{
    Classification, CommitRequest, Operation, Rejection, TableMetadata, check_requirements,
    classify,
};
use integrity_types::{ErrorCode, FieldId, SnapshotId};
use serde_json::{Value, json};

/// orders(1 id long, 2 customer_id int, 3 amount decimal(10,2), 4 note string, 5 tags list<string>)
/// main → snapshot 20 (parent 10); branch `ingest` → 10.
fn meta() -> TableMetadata {
    serde_json::from_value(json!({
        "format-version": 2,
        "table-uuid": "6c1b2d42-0000-4000-8000-000000000001",
        "location": "s3://warehouse/orders",
        "last-column-id": 6,
        "last-partition-id": 999,
        "default-spec-id": 0,
        "default-sort-order-id": 0,
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "customer_id", "required": false, "type": "int"},
            {"id": 3, "name": "amount", "required": false, "type": "decimal(10, 2)"},
            {"id": 4, "name": "note", "required": false, "type": "string"},
            {"id": 5, "name": "tags", "required": false, "type":
                {"type": "list", "element-id": 6, "element": "string", "element-required": false}}
        ]}],
        "current-snapshot-id": 20,
        "snapshots": [
            {"snapshot-id": 10, "sequence-number": 1, "timestamp-ms": 1, "manifest-list": "s3://m/10.avro",
             "summary": {"operation": "append"}},
            {"snapshot-id": 20, "parent-snapshot-id": 10, "sequence-number": 2, "timestamp-ms": 2,
             "manifest-list": "s3://m/20.avro", "summary": {"operation": "append"}}
        ],
        "refs": {"main": {"snapshot-id": 20, "type": "branch"}, "ingest": {"snapshot-id": 10, "type": "branch"}}
    }))
    .unwrap()
}

/// Fields of enforced constraints: PK(id), FK(customer_id), UNIQUE(amount).
fn constrained() -> BTreeSet<FieldId> {
    [FieldId(1), FieldId(2), FieldId(3)].into()
}

fn snapshot(id: i64, parent: i64, op: &str) -> Value {
    json!({"action": "add-snapshot", "snapshot": {
        "snapshot-id": id, "parent-snapshot-id": parent, "sequence-number": 3, "timestamp-ms": 3,
        "manifest-list": format!("s3://m/{id}.avro"), "summary": {"operation": op, "added-data-files": "1"}
    }})
}

fn set_ref(name: &str, id: i64) -> Value {
    json!({"action": "set-snapshot-ref", "ref-name": name, "snapshot-id": id, "type": "branch"})
}

fn request(updates: Vec<Value>) -> CommitRequest {
    CommitRequest::from_json(json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 20}],
        "updates": updates
    }))
    .unwrap()
}

fn run(updates: Vec<Value>) -> Result<Classification, Rejection> {
    classify(&meta(), &request(updates), &constrained())
}

fn main_change(updates: Vec<Value>) -> (Operation, i64) {
    match run(updates).unwrap() {
        Classification::MainChange(c) => {
            assert_eq!(c.parent, Some(SnapshotId(20)));
            assert_eq!(c.steps.len(), 1);
            assert_eq!(c.steps[0].parent, Some(SnapshotId(20)));
            assert_eq!(c.steps[0].update_index, 0);
            (c.steps[0].operation, c.steps[0].snapshot.0)
        }
        other => panic!("expected a main change, got {other:?}"),
    }
}

fn unsupported(updates: Vec<Value>) -> String {
    match run(updates) {
        Err(r @ Rejection::Unsupported(_)) => {
            assert_eq!(r.code(), ErrorCode::UnsupportedCommitOperation);
            r.to_string()
        }
        other => panic!("expected unsupported, got {other:?}"),
    }
}

fn schema_change(field_type: Value, extra: Value) -> Result<Classification, Rejection> {
    let mut fields = vec![
        json!({"id": 1, "name": "id", "required": true, "type": "long"}),
        json!({"id": 2, "name": "customer_id", "required": false, "type": "int"}),
        json!({"id": 3, "name": "amount", "required": false, "type": "decimal(10, 2)"}),
        json!({"id": 4, "name": "note", "required": false, "type": "string"}),
    ];
    let mut changed =
        json!({"id": 2, "name": "customer_id", "required": false, "type": field_type});
    if let (Value::Object(c), Value::Object(e)) = (&mut changed, extra) {
        c.extend(e);
    }
    fields[1] = changed;
    run(vec![
        json!({"action": "add-schema", "schema": {"schema-id": 1, "type": "struct", "fields": fields}}),
        json!({"action": "set-current-schema", "schema-id": -1}),
    ])
}

// ---------- §15 rows ----------

#[test]
fn row_append() {
    assert_eq!(
        main_change(vec![snapshot(30, 20, "append"), set_ref("main", 30)]),
        (Operation::Append, 30)
    );
}

#[test]
fn row_overwrite_copy_on_write() {
    assert_eq!(
        main_change(vec![snapshot(30, 20, "overwrite"), set_ref("main", 30)]),
        (Operation::Overwrite, 30)
    );
}

#[test]
fn row_replace_compaction() {
    assert_eq!(
        main_change(vec![snapshot(30, 20, "replace"), set_ref("main", 30)]),
        (Operation::Replace, 30)
    );
}

#[test]
fn row_delete_whole_files() {
    assert_eq!(
        main_change(vec![snapshot(30, 20, "delete"), set_ref("main", 30)]),
        (Operation::Delete, 30)
    );
}

#[test]
fn row_schema_change_touching_constrained_field() {
    // Allowed: int → long, decimal precision widening with the same scale.
    assert_eq!(
        schema_change(json!("long"), json!({})),
        Ok(Classification::PassThrough)
    );
    let widened = run(vec![
        json!({"action": "add-schema", "schema": {"schema-id": 1, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "customer_id", "required": false, "type": "int"},
            {"id": 3, "name": "amount_renamed", "required": false, "type": "decimal(18, 2)"}
        ]}}),
        json!({"action": "set-current-schema", "schema-id": 1}),
    ]);
    assert_eq!(widened, Ok(Classification::PassThrough));

    // Rejected: other type changes, scale change, removal, initial-default.
    assert!(matches!(
        schema_change(json!("string"), json!({})),
        Err(Rejection::Unsupported(_))
    ));
    assert!(matches!(
        schema_change(json!("decimal(10, 3)"), json!({})),
        Err(Rejection::Unsupported(_))
    ));
    assert!(matches!(
        schema_change(json!("int"), json!({"initial-default": 0})),
        Err(Rejection::Unsupported(_))
    ));
    let removed = run(vec![
        json!({"action": "add-schema", "schema": {"schema-id": 1, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 3, "name": "amount", "required": false, "type": "decimal(10, 2)"}
        ]}}),
        json!({"action": "set-current-schema", "schema-id": -1}),
    ]);
    assert!(matches!(removed, Err(Rejection::Unsupported(_))));
}

#[test]
fn row_schema_and_property_changes_not_touching_constrained_fields() {
    let add_column = run(vec![
        json!({"action": "add-schema", "last-column-id": 7, "schema": {"schema-id": 1, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "customer_id", "required": false, "type": "int"},
            {"id": 3, "name": "amount", "required": false, "type": "decimal(10, 2)"},
            {"id": 7, "name": "extra", "required": false, "type": "double"}
        ]}}),
        json!({"action": "set-current-schema", "schema-id": -1}),
    ]);
    assert_eq!(add_column, Ok(Classification::PassThrough));
    // Unconstrained field 4 changes type and field 5 is dropped: not our concern.
    let note = run(vec![
        json!({"action": "add-schema", "schema": {"schema-id": 1, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "customer_id", "required": false, "type": "int"},
            {"id": 3, "name": "amount", "required": false, "type": "decimal(10, 2)"},
            {"id": 4, "name": "note", "required": false, "type": "long"}
        ]}}),
        json!({"action": "set-current-schema", "schema-id": 1}),
    ]);
    assert_eq!(note, Ok(Classification::PassThrough));
}

#[test]
fn row_expiry_properties_sort_and_partition_specs() {
    for update in [
        json!({"action": "remove-snapshots", "snapshot-ids": [10]}),
        json!({"action": "set-properties", "updates": {"k": "v"}}),
        json!({"action": "remove-properties", "removals": ["k"]}),
        json!({"action": "add-sort-order", "sort-order": {"order-id": 1, "fields": []}}),
        json!({"action": "set-default-sort-order", "sort-order-id": 1}),
        json!({"action": "add-spec", "spec": {"spec-id": 1, "fields": []}}),
        json!({"action": "set-default-spec", "spec-id": 1}),
        json!({"action": "upgrade-format-version", "format-version": 3}),
    ] {
        assert_eq!(
            run(vec![update.clone()]),
            Ok(Classification::PassThrough),
            "{update}"
        );
    }
}

#[test]
fn row_commits_to_other_refs_pass_through_uncertified() {
    assert_eq!(
        run(vec![snapshot(30, 10, "append"), set_ref("ingest", 30)]),
        Ok(Classification::PassThrough)
    );
    assert_eq!(
        run(vec![set_ref("audit", 20)]),
        Ok(Classification::PassThrough)
    );
    assert_eq!(
        run(vec![
            json!({"action": "remove-snapshot-ref", "ref-name": "ingest"})
        ]),
        Ok(Classification::PassThrough)
    );
}

#[test]
fn row_moving_main_to_a_non_child_snapshot() {
    // Rollback to an existing snapshot.
    assert!(unsupported(vec![set_ref("main", 10)]).contains("existing snapshot"));
    // Fast-forward main to a branch head that already exists (cherry-pick / WAP publish).
    assert!(
        unsupported(vec![
            snapshot(30, 10, "append"),
            set_ref("ingest", 30),
            set_ref("main", 10)
        ])
        .contains("existing snapshot")
    );
}

#[test]
fn unknown_changes_are_rejected_by_name() {
    assert!(unsupported(vec![json!({"action": "rewrite-history"})]).contains("rewrite-history"));
    assert!(
        unsupported(vec![json!({"action": "assign-uuid", "uuid": "x"})]).contains("assign-uuid")
    );
    assert!(
        unsupported(vec![
            json!({"action": "remove-snapshot-ref", "ref-name": "main"})
        ])
        .contains("main")
    );
}

// ---------- further fail-closed rules ----------

#[test]
fn main_must_be_a_branch() {
    let tag =
        json!({"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 30, "type": "tag"});
    assert!(unsupported(vec![snapshot(30, 20, "append"), tag]).contains("branch"));
}

#[test]
fn several_new_snapshots_on_main_are_validated_in_order() {
    // PyIceberg's overwrite: a delete snapshot then an append snapshot, main set after each.
    let both_moves = run(vec![
        snapshot(30, 20, "delete"),
        set_ref("main", 30),
        snapshot(31, 30, "append"),
        set_ref("main", 31),
    ]);
    // Only the final move, intermediate snapshot reached through the parent link.
    let final_move = run(vec![
        snapshot(30, 20, "delete"),
        snapshot(31, 30, "append"),
        set_ref("main", 31),
    ]);
    for r in [both_moves, final_move] {
        let Classification::MainChange(c) = r.unwrap() else {
            panic!()
        };
        assert_eq!(c.parent, Some(SnapshotId(20)));
        let steps: Vec<_> = c
            .steps
            .iter()
            .map(|s| (s.parent.map(|p| p.0), s.snapshot.0, s.operation))
            .collect();
        assert_eq!(
            steps,
            vec![
                (Some(20), 30, Operation::Delete),
                (Some(30), 31, Operation::Append)
            ]
        );
    }
}

#[test]
fn main_history_must_be_one_chain_from_the_current_main() {
    // main → 30, then → 31 whose parent is 20, not 30: 30 is not on the final history.
    assert!(
        unsupported(vec![
            snapshot(30, 20, "append"),
            set_ref("main", 30),
            snapshot(31, 20, "append"),
            set_ref("main", 31),
        ])
        .contains("outside its new history")
    );
    // The chain bottoms out at an old snapshot: stale.
    let r = run(vec![
        snapshot(30, 10, "append"),
        snapshot(31, 30, "append"),
        set_ref("main", 31),
    ]);
    assert!(matches!(r, Err(Rejection::Stale(_))), "{r:?}");
}

#[test]
fn unknown_operation_and_missing_manifest_list_are_rejected() {
    assert!(
        unsupported(vec![snapshot(30, 20, "truncate"), set_ref("main", 30)]).contains("operation")
    );
    let no_list = json!({"action": "add-snapshot", "snapshot": {
        "snapshot-id": 30, "parent-snapshot-id": 20, "timestamp-ms": 3, "summary": {"operation": "append"}}});
    assert!(unsupported(vec![no_list, set_ref("main", 30)]).contains("manifest list"));
}

#[test]
fn snapshot_built_on_an_old_main_is_stale() {
    let r = run(vec![snapshot(30, 10, "append"), set_ref("main", 30)]);
    assert!(matches!(r, Err(Rejection::Stale(_))));
    assert_eq!(r.unwrap_err().code(), ErrorCode::StaleBaseSnapshot);
}

#[test]
fn first_snapshot_of_an_empty_table() {
    let empty: TableMetadata = serde_json::from_value(json!({
        "format-version": 2, "table-uuid": "u", "current-schema-id": 0, "current-snapshot-id": -1,
        "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}]
    }))
    .unwrap();
    let req = CommitRequest::from_json(json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
        "updates": [
            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "timestamp-ms": 1,
             "manifest-list": "s3://m/1.avro", "summary": {"operation": "append"}}},
            set_ref("main", 1)
        ]
    }))
    .unwrap();
    check_requirements(&empty, &req).unwrap();
    let Classification::MainChange(c) = classify(&empty, &req, &[FieldId(1)].into()).unwrap()
    else {
        panic!()
    };
    assert_eq!(c.parent, None);
    assert_eq!(
        (c.steps[0].parent, c.steps[0].snapshot),
        (None, SnapshotId(1))
    );
}

// ---------- requirements (spec §14 step 3) ----------

#[test]
fn requirements_detect_stale_bases() {
    let m = meta();
    let with = |req: Value| {
        CommitRequest::from_json(json!({"requirements": [req], "updates": []})).unwrap()
    };
    for ok in [
        json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 20}),
        json!({"type": "assert-ref-snapshot-id", "ref": "nope", "snapshot-id": null}),
        json!({"type": "assert-table-uuid", "uuid": "6c1b2d42-0000-4000-8000-000000000001"}),
        json!({"type": "assert-current-schema-id", "current-schema-id": 0}),
        json!({"type": "assert-last-assigned-field-id", "last-assigned-field-id": 6}),
        json!({"type": "assert-default-spec-id", "default-spec-id": 0}),
    ] {
        assert_eq!(check_requirements(&m, &with(ok.clone())), Ok(()), "{ok}");
    }
    for stale in [
        json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 10}),
        json!({"type": "assert-table-uuid", "uuid": "other"}),
        json!({"type": "assert-current-schema-id", "current-schema-id": 1}),
    ] {
        assert!(
            matches!(
                check_requirements(&m, &with(stale.clone())),
                Err(Rejection::Stale(_))
            ),
            "{stale}"
        );
    }
    assert!(matches!(
        check_requirements(&m, &with(json!({"type": "assert-create"}))),
        Err(Rejection::Unsupported(_))
    ));
    assert!(matches!(
        check_requirements(&m, &with(json!({"type": "assert-future-thing"}))),
        Err(Rejection::Unsupported(_))
    ));
}

#[test]
fn malformed_requests_are_errors() {
    for body in [
        json!({}),
        json!({"requirements": [], "updates": [{"no-action": 1}]}),
        json!({"requirements": [], "updates": [{"action": "add-snapshot", "snapshot": {"snapshot-id": 1}}]}),
        json!({"requirements": [{"type": "assert-ref-snapshot-id"}], "updates": []}),
    ] {
        assert!(CommitRequest::from_json(body.clone()).is_err(), "{body}");
    }
}

#[test]
fn summary_fields_are_injected_into_json_and_parsed_form() {
    let mut req = request(vec![snapshot(30, 20, "append"), set_ref("main", 30)]);
    req.set_summary_fields(
        0,
        &[
            ("integrity.cert", "ab".into()),
            ("integrity.cert-version", "1".into()),
        ],
    )
    .unwrap();
    let summary = &req.json()["updates"][0]["snapshot"]["summary"];
    assert_eq!(summary["integrity.cert"], "ab");
    assert_eq!(summary["operation"], "append", "existing fields are kept");
    assert_eq!(summary["added-data-files"], "1");
    let integrity_iceberg::Update::AddSnapshot(s) = &req.updates[0] else {
        panic!()
    };
    assert_eq!(s.summary["integrity.cert-version"], "1");
    // Only add-snapshot updates carry a summary.
    assert!(
        req.set_summary_fields(1, &[("integrity.cert", "x".into())])
            .is_err()
    );
}
