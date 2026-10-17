//! The footer and page headers of client-written Parquet files, checked before the decoder sees
//! them.
//!
//! The Parquet decoder allocates what a file declares: a list's length or a schema element's child
//! count while decoding the footer, a page's uncompressed size before decompressing it, a
//! dictionary's value count before decoding it. A crafted file can declare gigabytes in a few bytes
//! and abort the process (found by `fuzz/parquet_keys`). This module walks the Thrift compact
//! structures itself and refuses declarations that cannot be true: a collection longer than its
//! remaining bytes (every element takes at least one), a schema element with more children than the
//! schema has elements, a page larger than [`MAX_PAGE_SIZE`], pages that overrun their column chunk,
//! or a dictionary with more values than its uncompressed bytes can hold.

/// Largest uncompressed page accepted (writers use about 1 MiB).
pub const MAX_PAGE_SIZE: i64 = 256 << 20;
/// Thrift nesting accepted while skipping unknown fields.
const MAX_DEPTH: usize = 16;

/// What a column chunk's values are, for the dictionary bound.
#[derive(Debug, Clone, Copy)]
pub enum Width {
    /// Bytes per value (fixed-width types; at least 1).
    Fixed(i64),
    /// Variable length: at least the 4-byte length prefix of PLAIN byte arrays.
    Variable,
    /// Booleans: one bit per value.
    Bit,
}

/// Checks every page header of `chunk` (a column chunk's bytes, dictionary page first).
pub fn check_chunk(chunk: &[u8], width: Width) -> Result<(), String> {
    let mut at = 0usize;
    while at < chunk.len() {
        let mut r = Reader {
            buf: &chunk[at..],
            pos: 0,
        };
        let h = r.page_header()?;
        let header_len = r.pos;
        if !(0..=MAX_PAGE_SIZE).contains(&h.uncompressed) {
            return Err(format!(
                "page declares {} uncompressed bytes",
                h.uncompressed
            ));
        }
        let compressed = usize::try_from(h.compressed)
            .map_err(|_| "negative compressed page size".to_owned())?;
        let end = at
            .checked_add(header_len)
            .and_then(|e| e.checked_add(compressed))
            .filter(|&e| e <= chunk.len())
            .ok_or("page runs past its column chunk")?;
        if let Some(n) = h.dictionary_values {
            let n = i64::from(n);
            let fits = match width {
                Width::Fixed(w) => n.checked_mul(w.max(1)).is_some_and(|b| b <= h.uncompressed),
                Width::Variable => n.checked_mul(4).is_some_and(|b| b <= h.uncompressed),
                Width::Bit => n <= h.uncompressed.saturating_mul(8),
            };
            if n < 0 || !fits {
                return Err(format!(
                    "dictionary declares {n} values in {} bytes",
                    h.uncompressed
                ));
            }
        }
        at = end;
    }
    Ok(())
}

/// Checks the footer (`FileMetaData`) bytes before they are decoded.
pub fn check_footer(footer: &[u8]) -> Result<(), String> {
    let mut r = Reader {
        buf: footer,
        pos: 0,
    };
    let mut children = Vec::new();
    let mut elements = 0u64;
    let mut last = 0;
    while let Some((id, ty)) = r.field(&mut last)? {
        if (id, ty) != (2, LIST) {
            r.skip_value(ty, 0)?;
            continue;
        }
        // `schema: list<SchemaElement>`; an element's field 5 is `num_children`.
        let (n, elem) = r.collection()?;
        if elem != STRUCT {
            return Err("schema is not a list of structs".into());
        }
        elements = n;
        for _ in 0..n {
            let mut inner = 0;
            while let Some((id, ty)) = r.field(&mut inner)? {
                match (id, ty) {
                    (5, I32) => children.push(i64::from(r.i32()?)),
                    _ => r.skip_value(ty, 1)?,
                }
            }
        }
    }
    match children
        .iter()
        .find(|&&c| c < 0 || c as u64 >= elements.max(1))
    {
        Some(c) => Err(format!(
            "a schema element declares {c} children among {elements} elements"
        )),
        None => Ok(()),
    }
}

struct Header {
    uncompressed: i64,
    compressed: i64,
    dictionary_values: Option<i32>,
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

const STOP: u8 = 0;
const TRUE: u8 = 1;
const FALSE: u8 = 2;
const BYTE: u8 = 3;
const I16: u8 = 4;
const I32: u8 = 5;
const I64: u8 = 6;
const DOUBLE: u8 = 7;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const SET: u8 = 10;
const MAP: u8 = 11;
const STRUCT: u8 = 12;

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, String> {
        let b = *self.buf.get(self.pos).ok_or("truncated Parquet metadata")?;
        self.pos += 1;
        Ok(b)
    }

    fn skip(&mut self, n: usize) -> Result<(), String> {
        self.pos = self
            .pos
            .checked_add(n)
            .filter(|&p| p <= self.buf.len())
            .ok_or("truncated Parquet metadata")?;
        Ok(())
    }

    fn varint(&mut self) -> Result<u64, String> {
        let mut out = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err("varint too long in Parquet metadata".into())
    }

    fn zigzag(&mut self) -> Result<i64, String> {
        let v = self.varint()?;
        Ok((v >> 1) as i64 ^ -((v & 1) as i64))
    }

    fn i32(&mut self) -> Result<i32, String> {
        i32::try_from(self.zigzag()?).map_err(|_| "i32 out of range in Parquet metadata".to_owned())
    }

    /// The next field header: `(id, type)`, `None` at the end of the struct.
    fn field(&mut self, last: &mut i16) -> Result<Option<(i16, u8)>, String> {
        let b = self.byte()?;
        if b == STOP {
            return Ok(None);
        }
        let ty = b & 0x0f;
        let delta = (b >> 4) as i16;
        let id = if delta == 0 {
            i16::try_from(self.zigzag()?).map_err(|_| "field id out of range".to_owned())?
        } else {
            last.checked_add(delta).ok_or("field id overflow")?
        };
        *last = id;
        Ok(Some((id, ty)))
    }

    /// A list or set header: `(length, element type)`. The length is at most the bytes left, as
    /// every element takes at least one.
    fn collection(&mut self) -> Result<(u64, u8), String> {
        let b = self.byte()?;
        let n = match b >> 4 {
            15 => self.varint()?,
            n => u64::from(n),
        };
        if n > (self.buf.len() - self.pos) as u64 {
            return Err(format!("a collection declares {n} elements in fewer bytes"));
        }
        Ok((n, b & 0x0f))
    }

    fn skip_value(&mut self, ty: u8, depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH {
            return Err("Parquet metadata nested too deeply".into());
        }
        match ty {
            TRUE | FALSE => Ok(()),
            BYTE => self.skip(1),
            I16 | I32 | I64 => self.varint().map(|_| ()),
            DOUBLE => self.skip(8),
            BINARY => {
                let n = usize::try_from(self.varint()?).map_err(|_| "binary too long")?;
                self.skip(n)
            }
            LIST | SET => {
                let (n, elem) = self.collection()?;
                for _ in 0..n {
                    // Booleans in collections take a byte each.
                    if elem == TRUE || elem == FALSE {
                        self.skip(1)?;
                    } else {
                        self.skip_value(elem, depth + 1)?;
                    }
                }
                Ok(())
            }
            MAP => {
                let n = self.varint()?;
                if n == 0 {
                    return Ok(());
                }
                if n.saturating_mul(2) > (self.buf.len() - self.pos) as u64 {
                    return Err(format!("a map declares {n} entries in fewer bytes"));
                }
                let kinds = self.byte()?;
                for _ in 0..n {
                    self.skip_value(kinds >> 4, depth + 1)?;
                    self.skip_value(kinds & 0x0f, depth + 1)?;
                }
                Ok(())
            }
            STRUCT => {
                let mut last = 0;
                while let Some((_, t)) = self.field(&mut last)? {
                    self.skip_value(t, depth + 1)?;
                }
                Ok(())
            }
            other => Err(format!("unknown thrift type {other} in Parquet metadata")),
        }
    }

    /// `PageHeader` (parquet.thrift): sizes (fields 2, 3) and the dictionary page header's
    /// `num_values` (field 7, its field 1).
    fn page_header(&mut self) -> Result<Header, String> {
        let (mut uncompressed, mut compressed, mut dictionary_values) = (None, None, None);
        let mut last = 0;
        while let Some((id, ty)) = self.field(&mut last)? {
            match (id, ty) {
                (2, I32) => uncompressed = Some(i64::from(self.i32()?)),
                (3, I32) => compressed = Some(i64::from(self.i32()?)),
                (7, STRUCT) => {
                    let mut inner = 0;
                    while let Some((id, ty)) = self.field(&mut inner)? {
                        match (id, ty) {
                            (1, I32) => dictionary_values = Some(self.i32()?),
                            _ => self.skip_value(ty, 1)?,
                        }
                    }
                }
                _ => self.skip_value(ty, 0)?,
            }
        }
        Ok(Header {
            uncompressed: uncompressed.ok_or("page header without uncompressed size")?,
            compressed: compressed.ok_or("page header without compressed size")?,
            dictionary_values,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zigzag(v: i64) -> Vec<u8> {
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let b = (z & 0x7f) as u8;
            z >>= 7;
            if z == 0 {
                out.push(b);
                return out;
            }
            out.push(b | 0x80);
        }
    }

    /// A page header: type, sizes, and optionally a dictionary page header with `values`.
    fn header(uncompressed: i64, compressed: i64, values: Option<i64>) -> Vec<u8> {
        let mut h = vec![0x15]; // field 1, i32: type
        h.extend(zigzag(if values.is_some() { 2 } else { 0 }));
        h.push(0x15); // field 2, i32
        h.extend(zigzag(uncompressed));
        h.push(0x15); // field 3, i32
        h.extend(zigzag(compressed));
        if let Some(n) = values {
            h.push(0x4c); // field 7 (delta 4), struct
            h.push(0x15); // field 1, i32: num_values
            h.extend(zigzag(n));
            h.push(0x15); // field 2, i32: encoding
            h.extend(zigzag(0));
            h.push(0);
        }
        h.push(0);
        h
    }

    fn page(uncompressed: i64, body: usize, values: Option<i64>) -> Vec<u8> {
        let mut p = header(uncompressed, body as i64, values);
        p.extend(vec![0u8; body]);
        p
    }

    #[test]
    fn footers_with_impossible_lengths_are_refused() {
        // FileMetaData { 1: version, 2: schema [ {5: num_children}, {} ] }
        let footer = |children: i64, declared: u8| {
            let mut f = vec![0x15];
            f.extend(zigzag(2));
            f.push(0x19); // field 2, list
            f.push((declared << 4) | STRUCT);
            f.push(0x55); // field 5, i32
            f.extend(zigzag(children));
            f.push(0);
            f.push(0); // second element: empty struct
            f.push(0);
            f
        };
        assert_eq!(check_footer(&footer(1, 2)), Ok(()));
        assert!(
            check_footer(&footer(1 << 30, 2))
                .unwrap_err()
                .contains("children")
        );
        assert!(
            check_footer(&footer(-1, 2))
                .unwrap_err()
                .contains("children")
        );
        assert!(
            check_footer(&footer(1, 14)).is_err(),
            "list longer than its bytes"
        );
        // A list whose varint length claims billions of elements.
        let mut huge = vec![0x19, 0xf0 | STRUCT];
        huge.extend([0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(check_footer(&huge).unwrap_err().contains("fewer bytes"));
    }

    #[test]
    fn honest_chunks_pass() {
        let mut chunk = page(80, 40, Some(10));
        chunk.extend(page(1000, 300, None));
        chunk.extend(page(1000, 300, None));
        assert_eq!(check_chunk(&chunk, Width::Fixed(8)), Ok(()));
        assert_eq!(
            check_chunk(&page(40, 20, Some(10)), Width::Variable),
            Ok(())
        );
        assert_eq!(check_chunk(&page(2, 2, Some(16)), Width::Bit), Ok(()));
        assert_eq!(check_chunk(&[], Width::Fixed(8)), Ok(()));
    }

    #[test]
    fn impossible_declarations_are_refused() {
        let refused = |chunk: Vec<u8>, width| check_chunk(&chunk, width).unwrap_err();
        assert!(refused(page(79, 40, Some(10)), Width::Fixed(8)).contains("dictionary declares"));
        assert!(
            refused(page(100, 10, Some(1 << 30)), Width::Fixed(8)).contains("dictionary declares")
        );
        assert!(refused(page(100, 10, Some(-1)), Width::Fixed(8)).contains("dictionary declares"));
        assert!(
            refused(page(MAX_PAGE_SIZE + 1, 10, None), Width::Fixed(8)).contains("uncompressed")
        );
        assert!(refused(page(-5, 10, None), Width::Fixed(8)).contains("uncompressed"));
        let mut overrun = header(100, 50, None);
        overrun.extend([0u8; 10]);
        assert!(refused(overrun, Width::Fixed(8)).contains("past its column chunk"));
        assert!(
            refused(header(100, 0, None)[..3].to_vec(), Width::Fixed(8))
                .contains("truncated Parquet metadata")
        );
        // Hostile nesting while skipping an unknown field: a list of lists of lists…
        let mut deep = vec![0x19]; // field 1, list
        deep.extend(std::iter::repeat_n(0x19, 64)); // one-element lists of lists
        assert!(refused(deep, Width::Fixed(8)).contains("nested too deeply"));
    }
}
