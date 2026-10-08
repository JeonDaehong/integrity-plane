//! Apache Iceberg adapter: commit inspection, manifest diff, Parquet key extraction.
//!
//! Implemented so far: key-column extraction from Parquet data files ([`extract_rows`]).
//! Commit inspection and manifest diffing arrive in Phase 6.

mod parquet_keys;

pub use parquet_keys::{ExtractError, extract_rows};
