//! Integrity certificates, format v1 (spec §18, RFC 0002).

use std::collections::BTreeMap;
use std::fmt;

use integrity_types::{ConstraintId, ConstraintSetVersion, SnapshotId};

use crate::constraint::{
    Constraint, ConstraintKind, EnforcementMode, KeySpec, MatchMode, NullsMode, ReferentialAction,
};
use crate::delta::NetDelta;

/// `integrity.cert-version` of this format.
pub const CERT_VERSION: &str = "1";
/// Summary key of the certificate.
pub const SUMMARY_CERT: &str = "integrity.cert";
/// Summary key of the certificate format version.
pub const SUMMARY_CERT_VERSION: &str = "integrity.cert-version";
/// Summary key of the constraint set version.
pub const SUMMARY_CONSTRAINT_SET_VERSION: &str = "integrity.constraint-set-version";

/// A 32-byte BLAKE3 digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    /// The predecessor of a chain root.
    pub const ZERO: Digest = Digest([0; 32]);

    /// Lowercase hex, as stored in snapshot summaries.
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Parses 64 hex characters (either case).
    pub fn from_hex(s: &str) -> Result<Self, InvalidCertificate> {
        let bytes = s.as_bytes();
        if bytes.len() != 64 {
            return Err(InvalidCertificate);
        }
        let mut out = [0u8; 32];
        for (i, pair) in bytes.as_chunks::<2>().0.iter().enumerate() {
            let hi = hex_value(pair[0])?;
            let lo = hex_value(pair[1])?;
            out[i] = hi << 4 | lo;
        }
        Ok(Self(out))
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({})", self.to_hex())
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

fn hex_value(c: u8) -> Result<u8, InvalidCertificate> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(InvalidCertificate),
    }
}

/// A certificate, digest or table UUID that is not well-formed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCertificate;

impl fmt::Display for InvalidCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("malformed certificate input")
    }
}

impl std::error::Error for InvalidCertificate {}

/// Parses an Iceberg table UUID from its canonical text form (case-insensitive).
pub fn parse_table_uuid(s: &str) -> Result<[u8; 16], InvalidCertificate> {
    let b = s.as_bytes();
    if b.len() != 36 || [8, 13, 18, 23].iter().any(|&i| b[i] != b'-') {
        return Err(InvalidCertificate);
    }
    let hex: Vec<u8> = b.iter().copied().filter(|&c| c != b'-').collect();
    if hex.len() != 32 {
        return Err(InvalidCertificate);
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.as_chunks::<2>().0.iter().enumerate() {
        out[i] = hex_value(pair[0])? << 4 | hex_value(pair[1])?;
    }
    Ok(out)
}

struct Writer(blake3::Hasher);

impl Writer {
    fn new(domain: &str) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(domain.as_bytes());
        Self(h)
    }
    fn u8(&mut self, v: u8) {
        self.0.update(&[v]);
    }
    fn u32(&mut self, v: u32) {
        self.0.update(&v.to_be_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.update(&v.to_be_bytes());
    }
    fn i32(&mut self, v: i32) {
        self.0.update(&v.to_be_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.update(&v.to_be_bytes());
    }
    fn bytes(&mut self, v: &[u8]) {
        self.0.update(v);
    }
    fn len(&mut self, n: usize) -> Result<(), InvalidCertificate> {
        self.u32(u32::try_from(n).map_err(|_| InvalidCertificate)?);
        Ok(())
    }
    fn str(&mut self, s: &str) -> Result<(), InvalidCertificate> {
        self.len(s.len())?;
        self.bytes(s.as_bytes());
        Ok(())
    }
    fn key(&mut self, key: &KeySpec) -> Result<(), InvalidCertificate> {
        self.len(key.columns.len())?;
        for f in &key.columns {
            self.i32(f.0);
        }
        Ok(())
    }
    fn finish(self) -> Digest {
        Digest(*self.0.finalize().as_bytes())
    }
}

/// Digest of the enforced constraints governing a table (RFC 0002). Pass the constraints declared
/// on the table and the FKs referencing it; disabled ones are skipped and order does not matter.
pub fn constraint_set_digest(
    version: ConstraintSetVersion,
    constraints: &[&Constraint],
) -> Result<Digest, InvalidCertificate> {
    let mut enforced: Vec<&Constraint> = constraints
        .iter()
        .copied()
        .filter(|c| c.mode == EnforcementMode::Enforced)
        .collect();
    enforced.sort_by_key(|c| c.id);
    enforced.dedup_by_key(|c| c.id);

    let mut w = Writer::new("oip-constraints-v1");
    w.u64(version.0);
    w.len(enforced.len())?;
    for c in enforced {
        w.u64(c.id.0);
        w.str(c.table.as_str())?;
        match &c.kind {
            ConstraintKind::PrimaryKey(key) => {
                w.u8(0x01);
                w.key(key)?;
            }
            ConstraintKind::Unique(spec) => {
                w.u8(0x02);
                w.key(&spec.key)?;
                w.u8(match spec.nulls {
                    NullsMode::Distinct => 0,
                    NullsMode::NotDistinct => 1,
                });
            }
            ConstraintKind::ForeignKey(spec) => {
                w.u8(0x03);
                w.key(&spec.child)?;
                w.str(spec.parent_table.as_str())?;
                w.u64(spec.parent_constraint.0);
                w.u8(match spec.match_mode {
                    MatchMode::Simple => 0,
                    MatchMode::Full => 1,
                });
                w.u8(match spec.on_delete {
                    ReferentialAction::Restrict => 0,
                });
            }
            ConstraintKind::NotNull(field) => {
                w.u8(0x04);
                w.i32(field.0);
            }
        }
    }
    Ok(w.finish())
}

/// Digest of a snapshot's net key changes, one entry per PK/UNIQUE/FK constraint on the table
/// (RFC 0002). Include constraints with empty deltas.
pub fn key_delta_digest(
    deltas: &BTreeMap<ConstraintId, NetDelta>,
) -> Result<Digest, InvalidCertificate> {
    let mut d = KeyDeltaDigest::new(deltas.len())?;
    for (id, delta) in deltas {
        d.constraint(*id, delta.len() as u64)?;
        for (key, change) in delta.iter() {
            d.change(key.as_bytes(), change)?;
        }
    }
    d.finish()
}

/// [`key_delta_digest`] computed one change at a time, for deltas too large to hold (ADR 0025):
/// each constraint, in increasing id order, is announced with its number of changes, then its
/// changes follow in key order. Any other sequence is [`InvalidCertificate`].
pub struct KeyDeltaDigest {
    w: Writer,
    constraints_left: usize,
    changes_left: u64,
    last: Option<ConstraintId>,
}

impl fmt::Debug for KeyDeltaDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyDeltaDigest")
            .field("constraints_left", &self.constraints_left)
            .field("changes_left", &self.changes_left)
            .finish_non_exhaustive()
    }
}

impl KeyDeltaDigest {
    /// Starts the digest of a commit's deltas over `constraints` constraints.
    pub fn new(constraints: usize) -> Result<Self, InvalidCertificate> {
        let mut w = Writer::new("oip-key-delta-v1");
        w.len(constraints)?;
        Ok(Self {
            w,
            constraints_left: constraints,
            changes_left: 0,
            last: None,
        })
    }

    /// Starts the next constraint, which has `changes` changes.
    pub fn constraint(&mut self, id: ConstraintId, changes: u64) -> Result<(), InvalidCertificate> {
        if self.changes_left != 0 || self.constraints_left == 0 || self.last >= Some(id) {
            return Err(InvalidCertificate);
        }
        self.constraints_left -= 1;
        self.last = Some(id);
        self.w.u64(id.0);
        self.w
            .len(usize::try_from(changes).map_err(|_| InvalidCertificate)?)?;
        self.changes_left = changes;
        Ok(())
    }

    /// Adds the next change of the current constraint: the key's bytes and its signed change.
    pub fn change(&mut self, key: &[u8], change: i128) -> Result<(), InvalidCertificate> {
        self.changes_left = self.changes_left.checked_sub(1).ok_or(InvalidCertificate)?;
        self.w.len(key.len())?;
        self.w.bytes(key);
        self.w
            .i64(i64::try_from(change).map_err(|_| InvalidCertificate)?);
        Ok(())
    }

    /// The digest, once every announced constraint and change was added.
    pub fn finish(self) -> Result<Digest, InvalidCertificate> {
        if self.constraints_left != 0 || self.changes_left != 0 {
            return Err(InvalidCertificate);
        }
        Ok(self.w.finish())
    }
}

/// Everything a certificate commits to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateInput {
    /// The Iceberg table UUID.
    pub table_uuid: [u8; 16],
    /// The certified snapshot.
    pub snapshot: SnapshotId,
    /// Its parent, if any.
    pub parent: Option<SnapshotId>,
    /// [`constraint_set_digest`].
    pub constraint_set: Digest,
    /// [`key_delta_digest`].
    pub key_delta: Digest,
    /// The parent's certificate, or [`Digest::ZERO`] for a chain root.
    pub previous: Digest,
}

/// Computes `cert_n` (spec §18, RFC 0002).
pub fn certificate(input: &CertificateInput) -> Digest {
    let mut w = Writer::new("oip-cert-v1");
    w.bytes(&input.table_uuid);
    w.i64(input.snapshot.0);
    match input.parent {
        None => {
            w.u8(0);
            w.i64(0);
        }
        Some(p) => {
            w.u8(1);
            w.i64(p.0);
        }
    }
    w.bytes(&input.constraint_set.0);
    w.bytes(&input.key_delta.0);
    w.bytes(&input.previous.0);
    w.finish()
}
