//! Structured violation reports (spec §24): which constraint, how many offending keys, and up to
//! ten sample keys by column name, unless `errors.redact_keys` is set (keys can be personal data).

use std::collections::BTreeMap;

use integrity_core::{KeyValue, Violation};
use integrity_iceberg::TableMetadata;
use integrity_types::ErrorCode;
use integrity_validator::ViolationDetail;
use serde_json::{Value, json};

use crate::config::ConstraintConfig;

/// Column names by table identifier and field id, from the current schemas.
pub type ColumnNames = BTreeMap<String, BTreeMap<i32, String>>;

/// Adds the column names of `meta`'s current schema under `identifier`.
pub fn add_names(names: &mut ColumnNames, identifier: &str, meta: &TableMetadata) {
    if let Some(schema) = meta.current_schema() {
        names.insert(
            identifier.to_owned(),
            schema
                .fields
                .iter()
                .map(|f| (f.id, f.name.clone()))
                .collect(),
        );
    }
}

/// Days since 1970-01-01 as `YYYY-MM-DD` (proleptic Gregorian).
fn date(days: i64) -> String {
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

fn timestamp(nanos: i128, zone: &str) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let frac = nanos.rem_euclid(1_000_000_000);
    let days = i64::try_from(secs.div_euclid(86_400)).unwrap_or(i64::MAX / 2);
    let tod = secs.rem_euclid(86_400);
    format!(
        "{}T{:02}:{:02}:{:02}.{frac:09}{zone}",
        date(days),
        tod / 3600,
        tod % 3600 / 60,
        tod % 60
    )
}

fn decimal(unscaled: i128, scale: u8) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let sign = if unscaled < 0 { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A key value as JSON: numbers and booleans as such; decimals, dates, timestamps, binary and
/// UUIDs as strings in their usual text form.
pub fn key_json(v: &KeyValue) -> Value {
    match v {
        KeyValue::Boolean(b) => json!(b),
        KeyValue::Integer(i) => json!(i),
        KeyValue::Decimal { unscaled, scale } => json!(decimal(*unscaled, *scale)),
        KeyValue::Date(d) => json!(date(i64::from(*d))),
        KeyValue::Timestamp(n) => json!(timestamp(*n, "")),
        KeyValue::TimestampTz(n) => json!(timestamp(*n, "Z")),
        KeyValue::String(s) => json!(s),
        KeyValue::Binary(b) => json!(hex(b)),
        KeyValue::Uuid(u) => {
            let h = hex(u);
            json!(format!(
                "{}-{}-{}-{}-{}",
                &h[0..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..32]
            ))
        }
    }
}

/// One report entry per violated constraint, most severe code first.
pub fn violations(
    details: &BTreeMap<Violation, ViolationDetail>,
    configs: &[ConstraintConfig],
    names: &ColumnNames,
    redact: bool,
) -> Vec<Value> {
    let config = |id: u64| configs.iter().find(|c| c.id == id);
    let mut entries: Vec<(&Violation, &ViolationDetail)> = details.iter().collect();
    entries.sort_by_key(|(v, _)| (v.code.number(), v.constraint));
    entries
        .into_iter()
        .map(|(v, detail)| {
            let c = config(v.constraint.0);
            // The keys of a referenced-row delete are parent keys.
            let key_owner = match (v.code, c.and_then(|c| c.references.as_ref())) {
                (ErrorCode::ReferencedRowDelete, Some(r)) => config(r.constraint),
                _ => c,
            };
            let columns: Vec<String> = key_owner.map_or_else(Vec::new, |k| {
                k.columns
                    .iter()
                    .map(|f| {
                        names
                            .get(&k.table)
                            .and_then(|n| n.get(f))
                            .cloned()
                            .unwrap_or_else(|| format!("field_{f}"))
                    })
                    .collect()
            });
            let mut entry = json!({
                "code": v.code.code(),
                "constraint": c.map_or_else(|| v.constraint.to_string(), |c| c.name.clone()),
                "constraint_id": v.constraint.0,
                "table": c.map(|c| c.table.clone()),
                "violation_count": detail.count(),
            });
            if redact {
                entry["sample_keys_redacted"] = json!(true);
            } else if !detail.samples().is_empty() {
                let samples: Vec<Value> = detail
                    .samples()
                    .iter()
                    .map(|tuple| {
                        let mut obj = serde_json::Map::new();
                        for (i, value) in tuple.iter().enumerate() {
                            let name = columns
                                .get(i)
                                .cloned()
                                .unwrap_or_else(|| format!("column_{i}"));
                            obj.insert(name, value.as_ref().map_or(Value::Null, key_json));
                        }
                        Value::Object(obj)
                    })
                    .collect();
                entry["sample_keys"] = Value::Array(samples);
            }
            entry
        })
        .collect()
}

/// A short summary for the error message: `INT-005 on fk_orders_customer (12 keys)`.
pub fn summary(entries: &[Value]) -> String {
    entries
        .iter()
        .map(|e| {
            let n = e["violation_count"].as_u64().unwrap_or(0);
            format!(
                "{} on {} ({n} {})",
                e["code"].as_str().unwrap_or("?"),
                e["constraint"].as_str().unwrap_or("?"),
                if e["code"] == "INT-007" {
                    if n == 1 { "row" } else { "rows" }
                } else if n == 1 {
                    "key"
                } else {
                    "keys"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_values_render_in_their_text_form() {
        assert_eq!(key_json(&KeyValue::Integer(-7)), json!(-7));
        assert_eq!(
            key_json(&KeyValue::Decimal {
                unscaled: -1234,
                scale: 2
            }),
            json!("-12.34")
        );
        assert_eq!(
            key_json(&KeyValue::Decimal {
                unscaled: 5,
                scale: 3
            }),
            json!("0.005")
        );
        assert_eq!(key_json(&KeyValue::Date(0)), json!("1970-01-01"));
        assert_eq!(key_json(&KeyValue::Date(19_723)), json!("2024-01-01"));
        assert_eq!(key_json(&KeyValue::Date(-1)), json!("1969-12-31"));
        assert_eq!(
            key_json(&KeyValue::TimestampTz(1_500_000_000)),
            json!("1970-01-01T00:00:01.500000000Z")
        );
        assert_eq!(key_json(&KeyValue::Binary(vec![0xab, 1])), json!("ab01"));
        let mut u = [0u8; 16];
        u[15] = 1;
        assert_eq!(
            key_json(&KeyValue::Uuid(u)),
            json!("00000000-0000-0000-0000-000000000001")
        );
    }
}
