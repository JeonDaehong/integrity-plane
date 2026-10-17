//! Fenced domain commits (RFC 0012).
//!
//! A commit to an enforced table is forwarded as one multi-table transaction that also asserts
//! `main` of every other table of its domain, so the catalog refuses it (409) if any of them
//! changed after the commit was validated: several Planes can take commits for the same tables.
//! Each certified snapshot records the domain state it was validated against, the order catching
//! up replays FK-linked tables in.

use std::collections::BTreeMap;

use axum::http::{HeaderMap, HeaderValue, Method, header};
use bytes::Bytes;
use integrity_types::ErrorCode;
use serde_json::{Value, json};

use super::{Captured, Gateway, Loaded, TablePath};
use crate::error::ApiError;

/// Snapshot summary field: the other domain tables' `main` the commit was validated against.
pub(crate) const SUMMARY_DOMAIN_STATE: &str = "integrity.domain-state";

/// `uuid@snapshot,…` (`-` for no snapshot), sorted by uuid.
pub(super) fn domain_state(others: &BTreeMap<String, Option<i64>>) -> String {
    others
        .iter()
        .map(|(uuid, at)| match at {
            Some(s) => format!("{uuid}@{s}"),
            None => format!("{uuid}@-"),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Parses [`domain_state`]; `None` if malformed.
pub(super) fn parse_domain_state(text: &str) -> Option<BTreeMap<String, Option<i64>>> {
    if text.is_empty() {
        return Some(BTreeMap::new());
    }
    text.split(',')
        .map(|entry| {
            let (uuid, at) = entry.rsplit_once('@')?;
            let at = match at {
                "-" => None,
                s => Some(s.parse().ok()?),
            };
            Some((uuid.to_owned(), at))
        })
        .collect()
}

/// A table identifier as the REST protocol writes it.
fn identifier_json(identifier: &str) -> Option<Value> {
    let (ns, table) = identifier.rsplit_once('.')?;
    Some(json!({"namespace": ns.split('.').collect::<Vec<_>>(), "name": table}))
}

/// What the fenced commit asserts.
pub(super) struct Fence<'a> {
    /// The committed table.
    pub table: &'a TablePath,
    /// Its uuid and the snapshot the commit was validated on.
    pub uuid: &'a str,
    pub base: Option<i64>,
    /// Every other table of the domain: identifier → (uuid, `main` the validation used).
    pub others: &'a BTreeMap<String, (String, Option<i64>)>,
}

impl Gateway {
    /// Forwards a validated, certified commit as a multi-table transaction asserting the domain
    /// (RFC 0012). `Ok(None)`: the outcome is unknown (recovery decides it).
    pub(super) async fn forward_fenced(
        &self,
        fence: &Fence<'_>,
        headers: &HeaderMap,
        request: &Value,
    ) -> Result<Option<Captured>, ApiError> {
        let ident = |i: &str| {
            identifier_json(i).ok_or_else(|| {
                ApiError::new(ErrorCode::IndexDegraded, format!("bad identifier {i}"))
            })
        };
        let assert_main = |uuid: &str, at: Option<i64>| {
            vec![
                json!({"type": "assert-table-uuid", "uuid": uuid}),
                json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": at}),
            ]
        };
        // The client's requirements, without those on `main` and the uuid: the Plane checked them
        // and asserts the state it validated against instead.
        let mut requirements: Vec<Value> = request["requirements"]
            .as_array()
            .map(|r| {
                r.iter()
                    .filter(|q| {
                        let kind = q["type"].as_str().unwrap_or_default();
                        !(kind == "assert-table-uuid"
                            || (kind == "assert-ref-snapshot-id" && q["ref"] == "main"))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        requirements.extend(assert_main(fence.uuid, fence.base));
        let mut changes = vec![json!({
            "identifier": ident(&fence.table.identifier())?,
            "requirements": requirements,
            "updates": request["updates"].clone(),
        })];
        for (identifier, (uuid, at)) in fence.others {
            changes.push(json!({
                "identifier": ident(identifier)?,
                "requirements": assert_main(uuid, *at),
                "updates": [],
            }));
        }
        let body = serde_json::to_vec(&json!({"table-changes": changes}))
            .map_err(|e| ApiError::new(ErrorCode::IndexDegraded, e.to_string()))?;
        let path = format!("{}/transactions/commit", fence.table.base);
        let Ok(resp) = self
            .send(Method::POST, &path, headers, Bytes::from(body))
            .await
        else {
            return Ok(None);
        };
        let Ok(captured) = Captured::read(resp).await else {
            return Ok(None);
        };
        match captured.status {
            200..=299 => {}
            404 | 405 | 501 => {
                return Err(ApiError::new(
                    ErrorCode::UnsupportedCommitOperation,
                    format!(
                        "the upstream catalog does not take multi-table commits (HTTP {}), which [ha] fence = \"multi-table\" needs",
                        captured.status
                    ),
                ));
            }
            _ => return Ok(Some(captured)),
        }
        // The catalog answers a transaction without the table: load it, as the client expects.
        match self.load(&fence.table.path(), headers).await {
            Ok(Loaded::Table(_, body)) => Ok(Some(Captured {
                status: 200,
                headers: vec![(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            })),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_states_round_trip_and_sort() {
        let state: BTreeMap<String, Option<i64>> = [
            ("b-uuid".to_owned(), Some(-42)),
            ("a-uuid".to_owned(), None),
        ]
        .into_iter()
        .collect();
        let text = domain_state(&state);
        assert_eq!(text, "a-uuid@-,b-uuid@-42");
        assert_eq!(parse_domain_state(&text), Some(state));
        assert_eq!(parse_domain_state(""), Some(BTreeMap::new()));
        assert_eq!(parse_domain_state("x@y"), None);
        assert_eq!(parse_domain_state("nothing"), None);
    }
}
