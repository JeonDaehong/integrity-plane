//! Certificates in Iceberg snapshot summaries (spec §18, RFC 0002, ADR 0007 for Appendix B.1).

use integrity_core::{
    CERT_VERSION, Digest, InvalidCertificate, SUMMARY_CERT, SUMMARY_CERT_KEY_ID,
    SUMMARY_CERT_SIGNATURE, SUMMARY_CERT_VERSION, SUMMARY_CONSTRAINT_SET_VERSION,
};
use integrity_types::{ConstraintSetVersion, SnapshotId};

use crate::classify::NewSnapshot;
use crate::metadata::TableMetadata;
use crate::request::{CommitRequest, MalformedRequest};

/// The certificate recorded in a snapshot's summary.
///
/// `Ok(None)` when the snapshot has none (a chain root follows, or a writer bypassed the Plane:
/// the caller decides with the Plane's own log). A present but malformed certificate, or one of
/// an unknown format version, is an error.
pub fn snapshot_certificate(
    meta: &TableMetadata,
    snapshot: SnapshotId,
) -> Result<Option<Digest>, InvalidCertificate> {
    let summary = &meta.snapshot(snapshot).ok_or(InvalidCertificate)?.summary;
    match (summary.get(SUMMARY_CERT), summary.get(SUMMARY_CERT_VERSION)) {
        (None, None) => Ok(None),
        (Some(cert), Some(version)) if version == CERT_VERSION => Digest::from_hex(cert).map(Some),
        _ => Err(InvalidCertificate),
    }
}

/// Writes a step's certificate into its `add-snapshot` summary before the request is forwarded.
pub fn inject_certificate(
    request: &mut CommitRequest,
    step: &NewSnapshot,
    certificate: Digest,
    constraint_set_version: ConstraintSetVersion,
) -> Result<(), MalformedRequest> {
    request.set_summary_fields(
        step.update_index,
        &[
            (SUMMARY_CERT, certificate.to_hex()),
            (SUMMARY_CERT_VERSION, CERT_VERSION.to_owned()),
            (
                SUMMARY_CONSTRAINT_SET_VERSION,
                constraint_set_version.0.to_string(),
            ),
        ],
    )
}

/// The signature recorded in a snapshot's summary (RFC 0005): `(key id, signature hex)`.
///
/// `Ok(None)` when unsigned; one field without the other is malformed.
pub fn snapshot_signature(
    meta: &TableMetadata,
    snapshot: SnapshotId,
) -> Result<Option<(String, String)>, InvalidCertificate> {
    let summary = &meta.snapshot(snapshot).ok_or(InvalidCertificate)?.summary;
    match (
        summary.get(SUMMARY_CERT_KEY_ID),
        summary.get(SUMMARY_CERT_SIGNATURE),
    ) {
        (None, None) => Ok(None),
        (Some(k), Some(s)) => Ok(Some((k.clone(), s.clone()))),
        _ => Err(InvalidCertificate),
    }
}

/// Writes a step's certificate signature into its `add-snapshot` summary (RFC 0005).
pub fn inject_signature(
    request: &mut CommitRequest,
    step: &NewSnapshot,
    key_id: &str,
    signature: &str,
) -> Result<(), MalformedRequest> {
    request.set_summary_fields(
        step.update_index,
        &[
            (SUMMARY_CERT_KEY_ID, key_id.to_owned()),
            (SUMMARY_CERT_SIGNATURE, signature.to_owned()),
        ],
    )
}
