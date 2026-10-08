//! Apache Iceberg adapter: commit inspection, manifest diff, Parquet key extraction.
//!
//! - [`metadata`]: the subset of table metadata the Plane reads.
//! - [`request`]: REST `CommitTableRequest` parsing (and certificate field injection).
//! - [`classify`]: requirement checks and the spec §15 capability matrix at the update level.
//! - [`extract_rows`]: key-column extraction from Parquet data files.

pub mod classify;
pub mod metadata;
mod parquet_keys;
pub mod request;

pub use classify::{
    Classification, MainChange, Operation, Rejection, check_requirements, classify,
};
pub use metadata::TableMetadata;
pub use parquet_keys::{ExtractError, extract_rows};
pub use request::{CommitRequest, MalformedRequest, Requirement, Update};
