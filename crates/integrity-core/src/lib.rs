//! Constraint model, key encoding, key deltas and NULL semantics of the Open Integrity Plane.
//!
//! This crate is format- and I/O-independent: it MUST NOT depend on Iceberg, object storage,
//! HTTP or async runtimes (spec §12, checked by `ci/check-layering.sh`).

pub mod certificate;
pub mod constraint;
pub mod delta;
pub mod key;
pub mod logical_type;
pub mod nulls;
pub mod rows;
pub mod verdict;

pub use certificate::{
    CERT_VERSION, CertificateInput, Digest, InvalidCertificate, SUMMARY_CERT, SUMMARY_CERT_VERSION,
    SUMMARY_CONSTRAINT_SET_VERSION, certificate, constraint_set_digest, key_delta_digest,
    parse_table_uuid,
};
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
pub use rows::{ArityMismatch, CommitRows, Datum, RowBatch};
pub use verdict::Violation;
