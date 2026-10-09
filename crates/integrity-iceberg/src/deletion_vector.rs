//! Iceberg v3 deletion vectors (spec "Deletion Vectors", Puffin blob type `deletion-vector-v1`).
//!
//! A deletion vector is a blob inside a Puffin file, located by the manifest entry's
//! `content_offset` and `content_size_in_bytes`, and applying to exactly one data file
//! (`referenced_data_file`). The blob is:
//!
//! ```text
//! length: u32 big-endian      length of magic + vector
//! magic:  D1 D3 39 64
//! vector: 64-bit Roaring bitmap, portable serialization
//! crc:    u32 big-endian      CRC-32 of magic + vector
//! ```

use std::collections::BTreeSet;

use roaring::RoaringTreemap;

const MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Why a deletion vector blob is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidDeletionVector(pub String);

impl std::fmt::Display for InvalidDeletionVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid deletion vector: {}", self.0)
    }
}

impl std::error::Error for InvalidDeletionVector {}

fn invalid(m: impl Into<String>) -> InvalidDeletionVector {
    InvalidDeletionVector(m.into())
}

/// CRC-32 (IEEE 802.3, reflected, as in zlib and Java's `CRC32`).
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Decodes the blob at `offset..offset + size` of a Puffin file into deleted row positions.
pub fn read(file: &[u8], offset: i64, size: i64) -> Result<BTreeSet<i64>, InvalidDeletionVector> {
    let start = usize::try_from(offset).map_err(|_| invalid("negative offset"))?;
    let len = usize::try_from(size).map_err(|_| invalid("negative size"))?;
    let end = start
        .checked_add(len)
        .filter(|&e| e <= file.len())
        .ok_or_else(|| invalid("blob beyond the end of the file"))?;
    decode(&file[start..end])
}

/// Decodes one `deletion-vector-v1` blob.
pub fn decode(blob: &[u8]) -> Result<BTreeSet<i64>, InvalidDeletionVector> {
    if blob.len() < 12 {
        return Err(invalid("blob too short"));
    }
    let declared = u32::from_be_bytes([blob[0], blob[1], blob[2], blob[3]]) as usize;
    if declared + 8 != blob.len() {
        return Err(invalid("length field disagrees with the blob size"));
    }
    let body = &blob[4..blob.len() - 4];
    if body[..4] != MAGIC {
        return Err(invalid("bad magic"));
    }
    let tail = &blob[blob.len() - 4..];
    let crc = u32::from_be_bytes([tail[0], tail[1], tail[2], tail[3]]);
    if crc != crc32(body) {
        return Err(invalid("checksum mismatch"));
    }
    let vector = &body[4..];
    // Decoding a malformed bitmap must not take the process down.
    let bitmap = std::panic::catch_unwind(|| RoaringTreemap::deserialize_from(vector))
        .map_err(|_| invalid("malformed bitmap"))?
        .map_err(|e| invalid(format!("malformed bitmap: {e}")))?;
    if bitmap.serialized_size() != vector.len() {
        return Err(invalid("trailing bytes after the bitmap"));
    }
    bitmap
        .iter()
        .map(|p| i64::try_from(p).map_err(|_| invalid("position beyond i64")))
        .collect()
}

/// Encodes positions as a `deletion-vector-v1` blob (tests and fixtures).
pub fn encode(positions: &[u64]) -> Vec<u8> {
    let bitmap: RoaringTreemap = positions.iter().copied().collect();
    let mut body = MAGIC.to_vec();
    // Writing into a Vec cannot fail.
    let _ = bitmap.serialize_into(&mut body);
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc32(&body).to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    /// The portable 64-bit Roaring layout, written by hand from the format specification
    /// (RoaringFormatSpec): bitmap count (u64 LE); per bitmap, its high 32 bits (u32 LE) and a
    /// 32-bit Roaring bitmap: cookie 12346, container count, key and cardinality - 1 per
    /// container, offsets, then an array container of u16 values.
    #[test]
    fn decodes_the_portable_format_written_by_other_implementations() {
        let mut vector = Vec::new();
        vector.extend_from_slice(&1u64.to_le_bytes()); // one 32-bit bitmap
        vector.extend_from_slice(&0u32.to_le_bytes()); // high bits 0
        vector.extend_from_slice(&12346u32.to_le_bytes()); // SERIAL_COOKIE_NO_RUNCONTAINER
        vector.extend_from_slice(&1u32.to_le_bytes()); // one container
        vector.extend_from_slice(&0u16.to_le_bytes()); // container key
        vector.extend_from_slice(&1u16.to_le_bytes()); // cardinality - 1
        vector.extend_from_slice(&16u32.to_le_bytes()); // offset of the container data
        vector.extend_from_slice(&1u16.to_le_bytes()); // values 1 and 5
        vector.extend_from_slice(&5u16.to_le_bytes());
        let mut body = MAGIC.to_vec();
        body.extend_from_slice(&vector);
        let mut blob = (body.len() as u32).to_be_bytes().to_vec();
        blob.extend_from_slice(&body);
        blob.extend_from_slice(&crc32(&body).to_be_bytes());
        assert_eq!(decode(&blob).unwrap(), BTreeSet::from([1, 5]));
        assert_eq!(encode(&[1, 5]), blob, "and we write the same bytes");
    }

    #[test]
    fn round_trips_and_locates_blobs_in_a_file() {
        let positions = [0u64, 7, 70_000, 5_000_000_000];
        let blob = encode(&positions);
        let mut file = b"PFA1 other blobs ".to_vec();
        let offset = file.len() as i64;
        file.extend_from_slice(&blob);
        file.extend_from_slice(b" footer");
        let got = read(&file, offset, blob.len() as i64).unwrap();
        assert_eq!(
            got,
            positions.iter().map(|&p| p as i64).collect::<BTreeSet<_>>()
        );
        assert!(read(&file, offset, blob.len() as i64 + 100).is_err());
    }

    #[test]
    fn corrupted_blobs_are_refused() {
        let blob = encode(&[3, 4]);
        for i in 0..blob.len() {
            let mut bad = blob.clone();
            bad[i] ^= 0x40;
            assert!(decode(&bad).is_err(), "flip at byte {i} went unnoticed");
        }
        assert!(decode(&blob[..blob.len() - 1]).is_err());
    }
}
