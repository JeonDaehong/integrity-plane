//! Apache Iceberg adapter: commit inspection, manifest diff, Parquet key extraction.
//!
//! - [`metadata`]: the subset of table metadata the Plane reads.
//! - [`request`]: REST `CommitTableRequest` parsing (and certificate field injection).
//! - [`classify`]: requirement checks and the spec §15 capability matrix at the update level.
//! - [`manifest`] / [`changes`]: manifest diff of a new `main` snapshot and the rows it adds and
//!   removes (§15 data-level rows).
//! - [`extract_rows`]: key-column extraction from Parquet data files.
//! - [`certify`]: certificates in snapshot summaries (RFC 0002).
//! - [`io`]: file access with the inline validation budget.

pub mod certify;
pub mod changes;
pub mod classify;
pub mod deletion_vector;
pub mod io;
pub mod manifest;
pub mod metadata;
mod parquet_keys;
pub mod request;

pub use certify::{inject_certificate, snapshot_certificate};
pub use changes::{
    FileChanges, InspectError, PositionDeletes, check_operation, commit_rows, diff_snapshots,
};
pub use classify::{
    Classification, MainChange, NewSnapshot, Operation, Rejection, check_requirements, classify,
};
pub use io::{Budgeted, FileIo, MemoryIo, ReadError};
pub use metadata::TableMetadata;
pub use parquet_keys::{ExtractError, extract_rows};
pub use request::{CommitRequest, MalformedRequest, Requirement, Update};
