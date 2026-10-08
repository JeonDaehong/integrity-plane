//! Planner behavior (spec §13.3) and fail-closed error paths.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use integrity_core::{
    CommitRows, Constraint, ConstraintKind, Datum, EncodedKey, EnforcementMode, ForeignKeySpec,
    KeySchema, KeySpec, KeyValue, MatchMode, ReferentialAction, RowBatch, TypeFamily,
};
use integrity_index::{
    IndexDelta, IndexEpoch, IndexKind, IndexValue, KeyIndex, MemoryIndex, Result as IndexResult,
    StagedDelta,
};
use integrity_types::{
    ConstraintId, ConstraintSetVersion, ErrorCode, FieldId, SnapshotId, TableId,
};
use integrity_validator::{
    Decision, Inconsistency, ResolvedConstraint, ValidationError, Validator,
};

/// Counts keys passed to `get_many`.
struct Counting {
    inner: MemoryIndex,
    probed: AtomicUsize,
}

impl Counting {
    fn new(kind: IndexKind) -> Self {
        Self {
            inner: MemoryIndex::new(kind),
            probed: AtomicUsize::new(0),
        }
    }
}

impl KeyIndex for Counting {
    fn kind(&self) -> IndexKind {
        self.inner.kind()
    }
    fn get_many(&self, keys: &[EncodedKey]) -> IndexResult<Vec<Option<IndexValue>>> {
        self.probed.fetch_add(keys.len(), Ordering::SeqCst);
        self.inner.get_many(keys)
    }
    fn stage(&self, delta: &IndexDelta) -> IndexResult<StagedDelta> {
        self.inner.stage(delta)
    }
    fn apply(&self, staged: StagedDelta, epoch: IndexEpoch) -> IndexResult<()> {
        self.inner.apply(staged, epoch)
    }
    fn epoch(&self) -> IndexResult<IndexEpoch> {
        self.inner.epoch()
    }
}

// customer(1 id long) PK=1 ; orders(1 oid long, 2 cust int) PK=2, FK(cust)→customer PK = 3

const PK_CUSTOMER: ConstraintId = ConstraintId(1);
const PK_ORDERS: ConstraintId = ConstraintId(2);
const FK: ConstraintId = ConstraintId(3);

fn int_schema() -> KeySchema {
    KeySchema::new(vec![TypeFamily::Integer]).unwrap()
}

fn resolved(
    id: ConstraintId,
    table: &str,
    kind: ConstraintKind,
    mode: EnforcementMode,
) -> ResolvedConstraint {
    let schema = match kind {
        ConstraintKind::NotNull(_) => None,
        _ => Some(int_schema()),
    };
    ResolvedConstraint {
        constraint: Constraint {
            id,
            table: TableId::new(table),
            name: format!("c{}", id.0),
            kind,
            mode,
            version: ConstraintSetVersion(1),
        },
        schema,
    }
}

fn key1(f: i32) -> KeySpec {
    KeySpec {
        columns: vec![FieldId(f)],
    }
}

fn fixture(parent_mode: EnforcementMode) -> Vec<ResolvedConstraint> {
    vec![
        resolved(
            PK_CUSTOMER,
            "customer",
            ConstraintKind::PrimaryKey(key1(1)),
            parent_mode,
        ),
        resolved(
            PK_ORDERS,
            "orders",
            ConstraintKind::PrimaryKey(key1(1)),
            EnforcementMode::Enforced,
        ),
        resolved(
            FK,
            "orders",
            ConstraintKind::ForeignKey(ForeignKeySpec {
                child: key1(2),
                parent_table: TableId::new("customer"),
                parent_constraint: PK_CUSTOMER,
                match_mode: MatchMode::Simple,
                on_delete: ReferentialAction::Restrict,
            }),
            EnforcementMode::Enforced,
        ),
    ]
}

fn indexes() -> BTreeMap<ConstraintId, Counting> {
    [
        (PK_CUSTOMER, Counting::new(IndexKind::Unique)),
        (PK_ORDERS, Counting::new(IndexKind::Unique)),
        (FK, Counting::new(IndexKind::Reference)),
    ]
    .into()
}

fn int(v: i64) -> Datum {
    Datum::Value(KeyValue::Integer(v))
}

fn batch(cols: &[i32], rows: Vec<Vec<Datum>>) -> RowBatch {
    let mut b = RowBatch::new(cols.iter().map(|&c| FieldId(c)).collect());
    for r in rows {
        b.push(r).unwrap();
    }
    b
}

fn commit(table: &str, added: RowBatch, removed: RowBatch) -> CommitRows {
    CommitRows {
        table: TableId::new(table),
        snapshot: SnapshotId(1),
        added,
        removed,
    }
}

fn apply(v: &Validator, idx: &BTreeMap<ConstraintId, Counting>, c: &CommitRows, epoch: u64) {
    let Decision::Accepted(d) = v.validate(c, idx).unwrap() else {
        panic!("rejected")
    };
    for (id, staged) in d.stage(idx).unwrap() {
        idx[&id].apply(staged, IndexEpoch(epoch)).unwrap();
    }
}

fn customers(ids: impl IntoIterator<Item = i64>) -> CommitRows {
    commit(
        "customer",
        batch(&[1], ids.into_iter().map(|i| vec![int(i)]).collect()),
        batch(&[1], vec![]),
    )
}

#[test]
fn fk_parent_probes_are_deduplicated() {
    // 10 orders over 8 distinct customers ⇒ 8 parent probes (spec §13.3).
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let idx = indexes();
    apply(&v, &idx, &customers(0..8), 1);
    let custs = [0, 1, 2, 3, 4, 5, 6, 7, 0, 1];
    let orders = batch(
        &[1, 2],
        custs
            .iter()
            .enumerate()
            .map(|(i, &c)| vec![int(i as i64), int(c)])
            .collect(),
    );
    let before = idx[&PK_CUSTOMER].probed.load(Ordering::SeqCst);
    apply(
        &v,
        &idx,
        &commit("orders", orders, batch(&[1, 2], vec![])),
        2,
    );
    assert_eq!(idx[&PK_CUSTOMER].probed.load(Ordering::SeqCst) - before, 8);
    assert_eq!(
        idx[&FK]
            .inner
            .get_many(&[EncodedKey::encode(&int_schema(), &[Some(KeyValue::Integer(0))]).unwrap()])
            .unwrap(),
        vec![Some(IndexValue::Reference { child_count: 2 })]
    );
}

#[test]
fn projection_lists_only_constrained_columns() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    assert_eq!(
        v.projection(&TableId::new("orders")),
        vec![FieldId(1), FieldId(2)]
    );
    assert_eq!(v.projection(&TableId::new("customer")), vec![FieldId(1)]);
    assert!(v.projection(&TableId::new("unconstrained")).is_empty());
}

#[test]
fn unconstrained_table_is_accepted_without_index_changes() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let c = commit("logs", batch(&[], vec![vec![]]), batch(&[], vec![]));
    let Decision::Accepted(d) = v.validate(&c, &indexes()).unwrap() else {
        panic!()
    };
    assert_eq!(d.deltas().count(), 0);
}

// ---------- fail closed ----------

fn err(v: &Validator, idx: &BTreeMap<ConstraintId, Counting>, c: &CommitRows) -> ValidationError {
    v.validate(c, idx).unwrap_err()
}

#[test]
fn missing_projected_column_is_unsupported() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let c = commit(
        "orders",
        batch(&[1], vec![vec![int(1)]]),
        batch(&[1], vec![]),
    );
    let e = err(&v, &indexes(), &c);
    assert_eq!(e, ValidationError::MissingColumn(FieldId(2)));
    assert_eq!(e.code(), ErrorCode::UnsupportedCommitOperation);
}

#[test]
fn malformed_key_values_are_unsupported() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let opaque = commit(
        "customer",
        batch(&[1], vec![vec![Datum::Opaque]]),
        batch(&[1], vec![]),
    );
    assert_eq!(
        err(&v, &indexes(), &opaque),
        ValidationError::MalformedValue {
            field: Some(FieldId(1))
        }
    );
    let wrong_family = commit(
        "customer",
        batch(&[1], vec![vec![Datum::Value(KeyValue::String("1".into()))]]),
        batch(&[1], vec![]),
    );
    let e = err(&v, &indexes(), &wrong_family);
    assert_eq!(
        e,
        ValidationError::MalformedValue {
            field: Some(FieldId(1))
        }
    );
    assert_eq!(e.code(), ErrorCode::UnsupportedCommitOperation);
}

#[test]
fn fk_to_a_disabled_parent_is_unprovable_on_both_sides() {
    let v = Validator::new(fixture(EnforcementMode::Disabled));
    let idx = indexes();
    let child = commit(
        "orders",
        batch(&[1, 2], vec![vec![int(1), int(1)]]),
        batch(&[1, 2], vec![]),
    );
    assert_eq!(err(&v, &idx, &child), ValidationError::Unprovable(FK));
    let parent = customers([1]);
    assert_eq!(err(&v, &idx, &parent), ValidationError::Unprovable(FK));
}

#[test]
fn missing_index_degrades() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let mut idx = indexes();
    idx.remove(&PK_CUSTOMER);
    let e = err(&v, &idx, &customers([1]));
    assert_eq!(e, ValidationError::MissingIndex(PK_CUSTOMER));
    assert_eq!(e.code(), ErrorCode::IndexDegraded);
}

#[test]
fn removing_an_unindexed_key_is_inconsistent() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let c = commit(
        "customer",
        batch(&[1], vec![]),
        batch(&[1], vec![vec![int(5)]]),
    );
    let e = err(&v, &indexes(), &c);
    assert_eq!(
        e,
        ValidationError::Inconsistent(Inconsistency::RemovedKeyNotIndexed(PK_CUSTOMER))
    );
    assert_eq!(e.code(), ErrorCode::IndexDegraded);
}

#[test]
fn removed_row_with_null_primary_key_is_inconsistent() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let c = commit(
        "customer",
        batch(&[1], vec![]),
        batch(&[1], vec![vec![Datum::Null]]),
    );
    assert_eq!(
        err(&v, &indexes(), &c),
        ValidationError::Inconsistent(Inconsistency::RemovedRowViolates(PK_CUSTOMER))
    );
}

#[test]
fn staging_after_a_concurrent_change_is_stale() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let idx = indexes();
    apply(&v, &idx, &customers([1]), 1);
    // Validate an order for customer 1, then delete customer 1 before staging.
    let order = commit(
        "orders",
        batch(&[1, 2], vec![vec![int(10), int(1)]]),
        batch(&[1, 2], vec![]),
    );
    let Decision::Accepted(pending) = v.validate(&order, &idx).unwrap() else {
        panic!()
    };
    let delete = commit(
        "customer",
        batch(&[1], vec![]),
        batch(&[1], vec![vec![int(1)]]),
    );
    apply(&v, &idx, &delete, 2);
    assert_eq!(pending.stage(&idx), Err(ValidationError::StaleValidation));
}

#[test]
fn verdicts_on_the_small_fixture() {
    let v = Validator::new(fixture(EnforcementMode::Enforced));
    let idx = indexes();
    apply(&v, &idx, &customers([1]), 1);
    let order = |oid, cust| {
        commit(
            "orders",
            batch(&[1, 2], vec![vec![int(oid), cust]]),
            batch(&[1, 2], vec![]),
        )
    };
    let rejected = |pairs: &[(ConstraintId, ErrorCode)]| {
        Decision::Rejected(
            pairs
                .iter()
                .map(|&(constraint, code)| integrity_core::Violation { constraint, code })
                .collect(),
        )
    };
    assert_eq!(
        v.validate(&order(1, int(999)), &idx).unwrap(),
        rejected(&[(FK, ErrorCode::ForeignKeyViolation)])
    );
    assert!(matches!(
        v.validate(&order(1, Datum::Null), &idx).unwrap(),
        Decision::Accepted(_)
    ));
    apply(&v, &idx, &order(1, int(1)), 2);
    assert_eq!(
        v.validate(&order(1, int(1)), &idx).unwrap(),
        rejected(&[(PK_ORDERS, ErrorCode::DuplicatePrimaryKey)])
    );
    let delete = commit(
        "customer",
        batch(&[1], vec![]),
        batch(&[1], vec![vec![int(1)]]),
    );
    assert_eq!(
        v.validate(&delete, &idx).unwrap(),
        rejected(&[(FK, ErrorCode::ReferencedRowDelete)])
    );
    // Rewriting the parent row (remove + re-add) does not orphan anything.
    let rewrite = commit(
        "customer",
        batch(&[1], vec![vec![int(1)]]),
        batch(&[1], vec![vec![int(1)]]),
    );
    assert!(matches!(
        v.validate(&rewrite, &idx).unwrap(),
        Decision::Accepted(_)
    ));
}
