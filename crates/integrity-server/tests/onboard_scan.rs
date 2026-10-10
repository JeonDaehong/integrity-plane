//! The bounded-memory onboarding scan against the in-memory reference scan (ADR 0011), on real
//! Parquet and Avro files: random domains of three tables (PK, UNIQUE, NOT NULL, FK), files with
//! several row groups, and memory budgets small enough to spill many sorted runs. Both must find
//! the same violation reports, or install exactly the same index contents.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

mod support;

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicI64;

use bytes::Bytes;
use integrity_iceberg::{FileIo, ReadError, TableMetadata};
use integrity_index::{KeyIndex, PersistentStore};
use integrity_server::config::{ConstraintConfig, ReferenceConfig};
use integrity_server::onboard::{Member, ScanOutcome, scan, scan_in_memory};
use proptest::prelude::*;
use proptest::test_runner::Config;
use serde_json::json;
use support::{Dir, Files, Row, empty_table};

struct Local;

impl FileIo for Local {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        std::fs::read(location)
            .map(Bytes::from)
            .map_err(|e| ReadError::Io(format!("{location}: {e}")))
    }
}

fn files() -> Files {
    Files {
        dir: Dir::new(),
        next: AtomicI64::new(1),
        manifests_of: Mutex::new(HashMap::new()),
        rows_of: Mutex::new(HashMap::new()),
    }
}

fn constraint(
    id: u64,
    table: &str,
    kind: &str,
    columns: &[i32],
    nulls: Option<&str>,
    references: Option<(&str, u64)>,
) -> ConstraintConfig {
    ConstraintConfig {
        id,
        table: table.into(),
        name: format!("c{id}"),
        kind: kind.into(),
        columns: columns.to_vec(),
        nulls: nulls.map(str::to_owned),
        references: references.map(|(table, constraint)| ReferenceConfig {
            table: table.into(),
            constraint,
        }),
        match_mode: None,
        column_names: None,
    }
}

const TABLES: [&str; 3] = ["db.parent", "db.child", "db.other"];

/// parent: PK(id), UNIQUE(ref) · child: PK(id), FK(ref) → parent PK · other: NOT NULL(ref),
/// UNIQUE NULLS NOT DISTINCT(ref).
fn configs() -> Vec<ConstraintConfig> {
    vec![
        constraint(1, TABLES[0], "primary_key", &[1], None, None),
        constraint(2, TABLES[0], "unique", &[2], None, None),
        constraint(3, TABLES[1], "primary_key", &[1], None, None),
        constraint(
            4,
            TABLES[1],
            "foreign_key",
            &[2],
            None,
            Some((TABLES[0], 1)),
        ),
        constraint(5, TABLES[2], "not_null", &[2], None, None),
        constraint(6, TABLES[2], "unique", &[2], Some("not_distinct"), None),
    ]
}

/// A table whose `main` holds one data file per element of `files_rows`.
fn member(files: &Files, t: usize, files_rows: &[Vec<Row>], row_group: usize) -> Member {
    let mut meta = empty_table(&format!("00000000-0000-0000-0000-00000000000{t}"));
    if !files_rows.is_empty() {
        let manifests: Vec<String> = files_rows
            .iter()
            .map(|rows| files.manifest_with_row_groups(rows, Some(row_group)))
            .collect();
        let (id, list) = files.list(&manifests);
        meta["current-snapshot-id"] = json!(id);
        meta["snapshots"] =
            json!([{"snapshot-id": id, "sequence-number": 1, "manifest-list": list}]);
        meta["refs"] = json!({"main": {"snapshot-id": id, "type": "branch"}});
    }
    Member {
        identifier: TABLES[t].into(),
        meta: TableMetadata::from_json(&meta.to_string()).unwrap(),
    }
}

type Domain = Vec<Vec<Vec<Row>>>;

/// Both scans of `domain`; panics unless they agree. `true` if the domain is clean.
fn compare(domain: &Domain, row_group: usize, memory: usize) -> Result<bool, String> {
    let files = files();
    let members: Vec<Member> = domain
        .iter()
        .enumerate()
        .map(|(t, f)| member(&files, t, f, row_group))
        .collect();
    let configs = configs();
    let reference = scan_in_memory(&Local, &configs, &members, false);
    let store_dir = Dir::new();
    let store = PersistentStore::open(store_dir.0.join("indexes.redb")).unwrap();
    let bounded = scan(&Local, &store, memory, &configs, &members, false);
    let clean = match (reference, bounded) {
        (Ok(Ok(contents)), Ok(ScanOutcome::Clean(builds))) => {
            store.install(builds).unwrap();
            for (id, (kind, entries)) in contents {
                let installed = store.index(id, kind).unwrap().entries().unwrap();
                if installed != entries {
                    return Err(format!("{id:?}: {installed:?} != {entries:?}"));
                }
            }
            true
        }
        (Ok(Err(want)), Ok(ScanOutcome::Violations(got))) => {
            if got != want {
                return Err(format!("reports differ: {got:?} != {want:?}"));
            }
            // Nothing was installed.
            for c in configs.iter().filter(|c| c.kind != "not_null") {
                let kind = if c.kind == "foreign_key" {
                    integrity_index::IndexKind::Reference
                } else {
                    integrity_index::IndexKind::Unique
                };
                let index = store
                    .index(integrity_types::ConstraintId(c.id), kind)
                    .unwrap();
                if !index.entries().unwrap().is_empty() {
                    return Err(format!("c{} changed after violations", c.id));
                }
            }
            false
        }
        (Err(a), Err(b)) if a.code == b.code => false,
        (a, b) => return Err(format!("outcomes differ: {a:?} vs {:?}", b.map(|_| ()))),
    };
    // Sort runs are gone.
    let leftover = std::fs::read_dir(store.scratch_dir())
        .map(|d| d.count())
        .unwrap_or(0);
    if leftover != 0 {
        return Err(format!("{leftover} scratch entries left"));
    }
    Ok(clean)
}

/// Rows of one table: random, or tidied into valid rows (distinct ids and refs; children
/// referencing parent ids 0..4).
fn table(t: usize) -> impl Strategy<Value = Vec<Vec<Row>>> {
    let row = (0i64..40, proptest::option::weighted(0.9, 0i64..40));
    (
        any::<bool>(),
        proptest::collection::vec(proptest::collection::vec(row, 0..25), 0..4),
    )
        .prop_map(move |(tidy, files)| {
            if !tidy {
                return files;
            }
            let mut n = 0;
            files
                .into_iter()
                .map(|rows| {
                    rows.into_iter()
                        .map(|(_, r)| {
                            n += 1;
                            match t {
                                0 => (n, Some(n)),
                                1 => (n, r.map(|p| p % 5 + 1)),
                                _ => (n, Some(n)),
                            }
                        })
                        .collect()
                })
                .collect()
        })
}

fn domain() -> impl Strategy<Value = Domain> {
    (table(0), table(1), table(2)).prop_map(|(a, b, c)| vec![a, b, c])
}

proptest! {
    // Each case writes Parquet/Avro files and a redb store; fewer cases.
    #![proptest_config(Config { cases: 64, ..Config::default() })]

    #[test]
    fn the_bounded_scan_equals_the_in_memory_scan(
        domain in domain(),
        row_group in 1usize..10,
        memory in 1usize..4000,
    ) {
        if let Err(e) = compare(&domain, row_group, memory) {
            prop_assert!(false, "{}", e);
        }
    }
}

/// Guards against a vacuous property: clean domains with references and violating domains both
/// occur, with spilled runs.
#[test]
fn both_outcomes_occur_with_spills() {
    let parents: Vec<Vec<Row>> = vec![(1..=50).map(|i| (i, Some(i))).collect(), vec![]];
    let children: Vec<Vec<Row>> = vec![
        (1..=60).map(|i| (i, Some(i % 7 + 1))).collect(),
        (61..=70).map(|i| (i, None)).collect(),
    ];
    let others: Vec<Vec<Row>> = vec![(1..=30).map(|i| (i, Some(i))).collect()];
    let clean = vec![parents.clone(), children.clone(), others.clone()];
    assert_eq!(compare(&clean, 4, 100), Ok(true));

    let mut dirty_children = children;
    dirty_children[1].push((5, Some(99))); // duplicate id and a missing parent
    let dirty = vec![parents, dirty_children, others];
    assert_eq!(compare(&dirty, 3, 100), Ok(false));
}
