//! Identifiers, field references and stable error codes for the Open Integrity Plane.
//!
//! This crate has no dependencies and no I/O (spec §12).

mod error_code;
mod ids;

pub use error_code::{ErrorCode, UnknownErrorCode};
pub use ids::{ConstraintId, ConstraintSetVersion, FieldId, SnapshotId, TableId};
