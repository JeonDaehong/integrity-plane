//! Constraint model, key encoding, key deltas and NULL semantics of the Open Integrity Plane.
//!
//! This crate is format- and I/O-independent: it MUST NOT depend on Iceberg, object storage,
//! HTTP or async runtimes (spec §12, checked by `ci/check-layering.sh`).

pub mod constraint;
pub mod delta;
pub mod key;
pub mod logical_type;
pub mod nulls;

pub use constraint::{
    ColumnRef, Constraint, ConstraintKind, EnforcementMode, ForeignKeySpec, InvalidConstraint,
    KeySpec, MatchMode, NullsMode, ReferentialAction, RegistrationContext, UniqueSpec,
};
pub use delta::{DeltaError, KeyDelta, KeyMultiset, NetDelta};
pub use key::{
    DecodeError, EncodedKey, KEY_FORMAT_VERSION, KeyError, KeySchema, KeyValue, TypeFamily,
};
pub use logical_type::{LogicalType, UnsupportedKeyType};
pub use nulls::{KeyDisposition, KeyRole, classify};
