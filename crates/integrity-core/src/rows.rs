//! Projected rows of a commit: the input to validation.
//!
//! A format adapter reads only the columns the validator asks for (key columns and NOT NULL
//! columns) from the rows a commit adds and removes, and hands them over as [`CommitRows`].

use std::fmt;

use integrity_types::{FieldId, SnapshotId, TableId};

use crate::key::KeyValue;

/// One cell of a projected row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Datum {
    /// SQL NULL.
    Null,
    /// A value of a key-capable column type.
    Value(KeyValue),
    /// A non-NULL value of a type that cannot be a key (float, nested, …). Only its
    /// non-NULL-ness matters to any constraint, so the value is not carried.
    Opaque,
}

/// Rows projected onto a fixed list of columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowBatch {
    columns: Vec<FieldId>,
    rows: Vec<Vec<Datum>>,
}

/// A row whose arity differs from its batch's column list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArityMismatch {
    /// Columns in the batch.
    pub expected: usize,
    /// Cells in the row.
    pub actual: usize,
}

impl fmt::Display for ArityMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "row has {} cells but the batch has {} columns",
            self.actual, self.expected
        )
    }
}

impl std::error::Error for ArityMismatch {}

impl RowBatch {
    /// An empty batch over `columns`.
    pub fn new(columns: Vec<FieldId>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// Appends a row; cells are in column order.
    pub fn push(&mut self, row: Vec<Datum>) -> Result<(), ArityMismatch> {
        if row.len() != self.columns.len() {
            return Err(ArityMismatch {
                expected: self.columns.len(),
                actual: row.len(),
            });
        }
        self.rows.push(row);
        Ok(())
    }

    /// The projected columns.
    pub fn columns(&self) -> &[FieldId] {
        &self.columns
    }

    /// Position of `field` in the projection.
    pub fn column_index(&self, field: FieldId) -> Option<usize> {
        self.columns.iter().position(|&c| c == field)
    }

    /// The rows.
    pub fn rows(&self) -> &[Vec<Datum>] {
        &self.rows
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// `true` if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// What a single-table commit adds and removes, projected for validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRows {
    /// The table written.
    pub table: TableId,
    /// The snapshot the commit would publish.
    pub snapshot: SnapshotId,
    /// Rows added.
    pub added: RowBatch,
    /// Rows removed (read from the files the commit removes or deletes from).
    pub removed: RowBatch,
    /// Equality deletes (ADR 0009): every row that existed before the commit and whose values on
    /// the batch's columns equal one of its rows (NULL equals NULL) is removed. Rows added by the
    /// same commit are not affected.
    pub equality_deletes: Option<RowBatch>,
}

/// Which side of a commit a row is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Side {
    /// A row the commit adds.
    Added,
    /// A row the commit removes.
    Removed,
}

/// An injective byte encoding of a projected row: equal rows (as [`Datum`] values) and only equal
/// rows have equal encodings. Used to compare the rows of both sides of a commit by sorting
/// (a `replace` must not change any row). `Opaque` cells are equal to each other, as with `==`.
pub fn encode_row(row: &[Datum]) -> Result<Vec<u8>, crate::key::KeyError> {
    let mut out = Vec::new();
    for cell in row {
        match cell {
            Datum::Null => out.push(0),
            Datum::Opaque => out.push(1),
            Datum::Value(v) => {
                let schema = crate::key::KeySchema::new(vec![v.family()])?;
                let key = crate::key::EncodedKey::encode(&schema, &[Some(v.clone())])?;
                let bytes = key.as_bytes();
                out.push(2);
                out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_encoding_is_injective_on_samples() {
        let rows: Vec<Vec<Datum>> = vec![
            vec![],
            vec![Datum::Null],
            vec![Datum::Opaque],
            vec![Datum::Value(KeyValue::Integer(1))],
            vec![Datum::Value(KeyValue::Integer(256))],
            vec![Datum::Value(KeyValue::Date(1))],
            vec![Datum::Value(KeyValue::String("a".into()))],
            vec![Datum::Value(KeyValue::String("a".into())), Datum::Null],
            vec![Datum::Null, Datum::Value(KeyValue::String("a".into()))],
            vec![Datum::Value(KeyValue::String("ab".into()))],
            vec![
                Datum::Value(KeyValue::String("a".into())),
                Datum::Value(KeyValue::String("b".into())),
            ],
            vec![Datum::Value(KeyValue::Binary(b"a".to_vec()))],
            vec![Datum::Value(KeyValue::Decimal {
                unscaled: 1,
                scale: 1,
            })],
            vec![Datum::Value(KeyValue::Decimal {
                unscaled: 1,
                scale: 2,
            })],
        ];
        for a in &rows {
            for b in &rows {
                assert_eq!(
                    encode_row(a).unwrap() == encode_row(b).unwrap(),
                    a == b,
                    "{a:?} / {b:?}"
                );
            }
        }
    }
}
