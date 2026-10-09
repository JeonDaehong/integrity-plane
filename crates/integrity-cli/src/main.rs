//! `integrity`: operator CLI for the Open Integrity Plane (spec §23).
//!
//! A thin client of the gateway's integrity API. The gateway URL comes from `--url` or
//! `INTEGRITY_URL` (default `http://127.0.0.1:8181`), the API token from `--token` or
//! `INTEGRITY_TOKEN`. Audit events name `--actor` (default: `$USER` or `$USERNAME`).
//!
//! Exit codes: 0 success, 1 the request failed or `verify` found a broken chain, 2 usage error.

use std::process::ExitCode;

use serde_json::{Value, json};

const USAGE: &str = "\
usage: integrity [--url URL] [--token TOKEN] [--actor NAME] [--json] <command>

commands:
  status
  constraints list [--table NS.TABLE]
  constraints add --table NS.TABLE --name NAME --type primary_key|unique|foreign_key|not_null
                  --columns COL[,COL...] [--nulls distinct|not_distinct]
                  [--ref-table NS.TABLE --ref-constraint NAME|ID] [--match simple|full]
  constraints drop ID
  verify NS.TABLE            exit 1 if the certificate chain is broken
  rebuild CONSTRAINT_ID      rebuild the indexes of the constraint's domain
  domain NS.TABLE
  domain disable NS.TABLE --reason TEXT   forward the domain's commits unchecked until a rebuild
  txn ID
  audit [--table NS.TABLE] [--since SEQ]
";

/// One API call.
#[derive(Debug, Clone, PartialEq)]
struct Call {
    method: &'static str,
    path: String,
    body: Option<Value>,
    /// Render as a verify report.
    verify: bool,
}

/// Global options and the call.
#[derive(Debug, Clone, PartialEq)]
struct Invocation {
    url: Option<String>,
    token: Option<String>,
    actor: Option<String>,
    json: bool,
    call: Call,
}

fn get(path: impl Into<String>) -> Call {
    Call {
        method: "GET",
        path: path.into(),
        body: None,
        verify: false,
    }
}

/// Percent-encodes a query or path value.
fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `--flag value` pairs after the command words.
fn flags(args: &[String]) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let name = a
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {a}"))?;
        let value = it.next().ok_or_else(|| format!("--{name} needs a value"))?;
        out.push((name.to_owned(), value.clone()));
    }
    Ok(out)
}

fn flag<'a>(flags: &'a [(String, String)], name: &str) -> Option<&'a str> {
    flags
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

fn only(flags: &[(String, String)], allowed: &[&str]) -> Result<(), String> {
    match flags.iter().find(|(n, _)| !allowed.contains(&n.as_str())) {
        Some((n, _)) => Err(format!("unknown option --{n}")),
        None => Ok(()),
    }
}

fn query(pairs: &[(&str, Option<&str>)]) -> String {
    let parts: Vec<String> = pairs
        .iter()
        .filter_map(|(k, v)| v.map(|v| format!("{k}={}", encode(v))))
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

fn parse(args: &[String]) -> Result<Invocation, String> {
    let mut url = None;
    let mut token = None;
    let mut actor = None;
    let mut json_out = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                url = Some(args.get(i + 1).ok_or("--url needs a value")?.clone());
                i += 2;
            }
            "--token" => {
                token = Some(args.get(i + 1).ok_or("--token needs a value")?.clone());
                i += 2;
            }
            "--actor" => {
                actor = Some(args.get(i + 1).ok_or("--actor needs a value")?.clone());
                i += 2;
            }
            "--json" => {
                json_out = true;
                i += 1;
            }
            _ => break,
        }
    }
    let rest = &args[i..];
    let words: Vec<&str> = rest.iter().map(String::as_str).collect();
    let call = match words.as_slice() {
        ["status"] => get("/v1/integrity/status"),
        ["constraints", "list", ..] => {
            let f = flags(&rest[2..])?;
            only(&f, &["table"])?;
            get(format!(
                "/v1/integrity/constraints{}",
                query(&[("table", flag(&f, "table"))])
            ))
        }
        ["constraints", "add", ..] => {
            let f = flags(&rest[2..])?;
            only(
                &f,
                &[
                    "table",
                    "name",
                    "type",
                    "columns",
                    "nulls",
                    "ref-table",
                    "ref-constraint",
                    "match",
                ],
            )?;
            let required = |n: &str| flag(&f, n).ok_or_else(|| format!("--{n} is required"));
            let columns: Vec<Value> = required("columns")?
                .split(',')
                .map(|c| json!(c.trim()))
                .collect();
            let mut body = json!({
                "table": required("table")?,
                "name": required("name")?,
                "type": required("type")?,
                "columns": columns,
            });
            if let Some(n) = flag(&f, "nulls") {
                body["nulls"] = json!(n);
            }
            if let Some(m) = flag(&f, "match") {
                body["match"] = json!(m);
            }
            match (flag(&f, "ref-table"), flag(&f, "ref-constraint")) {
                (Some(t), Some(c)) => {
                    let constraint = c.parse::<u64>().map_or_else(|_| json!(c), |id| json!(id));
                    body["references"] = json!({"table": t, "constraint": constraint});
                }
                (None, None) => {}
                _ => return Err("--ref-table and --ref-constraint go together".into()),
            }
            Call {
                method: "POST",
                path: "/v1/integrity/constraints".into(),
                body: Some(body),
                verify: false,
            }
        }
        ["constraints", "drop", id] => Call {
            method: "DELETE",
            path: format!("/v1/integrity/constraints/{}", number(id)?),
            body: None,
            verify: false,
        },
        ["verify", table] => Call {
            verify: true,
            ..get(format!(
                "/v1/integrity/verify{}",
                query(&[("table", Some(table))])
            ))
        },
        ["rebuild", id] => Call {
            method: "POST",
            path: format!("/v1/integrity/indexes/{}/rebuild", number(id)?),
            body: None,
            verify: false,
        },
        ["domain", "disable", table, ..] => {
            let f = flags(&rest[3..])?;
            only(&f, &["reason"])?;
            let reason = flag(&f, "reason").ok_or("--reason is required")?;
            Call {
                method: "POST",
                path: format!("/v1/integrity/domains/{}/disable", encode(table)),
                body: Some(json!({ "reason": reason })),
                verify: false,
            }
        }
        ["domain", table] => get(format!("/v1/integrity/domains/{}", encode(table))),
        ["txn", id] => get(format!("/v1/integrity/transactions/{}", number(id)?)),
        ["audit", ..] => {
            let f = flags(&rest[1..])?;
            only(&f, &["table", "since"])?;
            if let Some(s) = flag(&f, "since") {
                number(s)?;
            }
            get(format!(
                "/v1/integrity/audit{}",
                query(&[("table", flag(&f, "table")), ("since", flag(&f, "since"))])
            ))
        }
        [] => return Err("no command".into()),
        _ => return Err(format!("unknown command: {}", words.join(" "))),
    };
    Ok(Invocation {
        url,
        token,
        actor,
        json: json_out,
        call,
    })
}

fn number(s: &str) -> Result<u64, String> {
    s.parse().map_err(|_| format!("{s} is not a number"))
}

/// A human-readable verify report, and whether the chain is intact.
fn render_verify(report: &Value) -> (String, bool) {
    let ok = report["ok"].as_bool().unwrap_or(false);
    let table = report["table"].as_str().unwrap_or("?");
    let head = report["head"]
        .as_i64()
        .map_or_else(|| "none".to_owned(), |h| h.to_string());
    let verdict = match report["first_broken"].as_i64() {
        Some(s) => format!("BROKEN at snapshot {s}"),
        None if ok => "OK".to_owned(),
        None => "INCOMPLETE".to_owned(),
    };
    let mut out = format!("{table}  head {head}  {verdict}\n");
    for link in report["chain"].as_array().into_iter().flatten() {
        let status = link["status"].as_str().unwrap_or("?");
        let snapshot = link["snapshot"].as_i64().unwrap_or_default();
        let operation = link["operation"].as_str().unwrap_or("-");
        out.push_str(&format!("  {status:<12} {snapshot:>20}  {operation}"));
        if let Some(d) = link["detail"].as_str() {
            out.push_str(&format!("  {d}"));
        }
        out.push('\n');
    }
    (out, ok)
}

fn run(inv: &Invocation) -> Result<bool, String> {
    let base = inv
        .url
        .clone()
        .or_else(|| std::env::var("INTEGRITY_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8181".to_owned());
    let token = inv
        .token
        .clone()
        .or_else(|| std::env::var("INTEGRITY_TOKEN").ok());
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .map_err(|e| e.to_string())?;
    let url = format!("{}{}", base.trim_end_matches('/'), inv.call.path);
    let mut req = match inv.call.method {
        "POST" => client.post(&url),
        "DELETE" => client.delete(&url),
        _ => client.get(&url),
    };
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let actor = inv
        .actor
        .clone()
        .or_else(|| std::env::var("USER").ok())
        .or_else(|| std::env::var("USERNAME").ok());
    if let Some(a) = actor {
        req = req.header("x-integrity-actor", a);
    }
    if let Some(body) = &inv.call.body {
        req = req.json(body);
    }
    let resp = req.send().map_err(|e| format!("{url}: {e}"))?;
    let status = resp.status();
    let text = resp.text().map_err(|e| e.to_string())?;
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
    if !status.is_success() {
        let message = body["error"]["message"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| body["error"].as_str().map(str::to_owned))
            .unwrap_or_else(|| body.to_string());
        eprintln!("integrity: {status}: {message}");
        if let Some(report) = body.get("integrity") {
            eprintln!(
                "{}",
                serde_json::to_string_pretty(report).unwrap_or_default()
            );
        }
        return Ok(false);
    }
    if inv.call.verify && !inv.json {
        let (text, ok) = render_verify(&body);
        print!("{text}");
        return Ok(ok);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&body).unwrap_or_default()
    );
    Ok(!inv.call.verify || body["ok"].as_bool().unwrap_or(false))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let inv = match parse(&args) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("integrity: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&inv) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("integrity: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(line: &str) -> Result<Invocation, String> {
        parse(
            &line
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn commands_map_to_api_calls() {
        let inv = p("--url http://gw:8181 --token t status").unwrap();
        assert_eq!(inv.url.as_deref(), Some("http://gw:8181"));
        assert_eq!(inv.token.as_deref(), Some("t"));
        assert_eq!(inv.call, get("/v1/integrity/status"));

        assert_eq!(
            p("constraints list --table db.orders").unwrap().call,
            get("/v1/integrity/constraints?table=db.orders")
        );
        assert_eq!(
            p("constraints drop 7").unwrap().call.path,
            "/v1/integrity/constraints/7"
        );
        let verify = p("verify db.a b").unwrap_err();
        assert!(verify.contains("unknown command"), "{verify}");
        let v = p("verify db.orders").unwrap().call;
        assert!(v.verify);
        assert_eq!(v.path, "/v1/integrity/verify?table=db.orders");
        assert_eq!(
            p("rebuild 3").unwrap().call.path,
            "/v1/integrity/indexes/3/rebuild"
        );
        assert_eq!(
            p("audit --table db.orders --since 10").unwrap().call.path,
            "/v1/integrity/audit?table=db.orders&since=10"
        );
        assert_eq!(
            p("domain db.orders").unwrap().call.path,
            "/v1/integrity/domains/db.orders"
        );
        let disable = p("--actor bob domain disable db.orders --reason incident").unwrap();
        assert_eq!(disable.actor.as_deref(), Some("bob"));
        assert_eq!(disable.call.method, "POST");
        assert_eq!(disable.call.path, "/v1/integrity/domains/db.orders/disable");
        assert_eq!(disable.call.body.unwrap(), json!({"reason": "incident"}));
        assert!(p("domain disable db.orders").is_err(), "reason required");
        assert_eq!(
            p("txn 12").unwrap().call.path,
            "/v1/integrity/transactions/12"
        );
    }

    #[test]
    fn constraints_add_builds_the_registration_body() {
        let call = p(
            "constraints add --table demo.orders --name fk --type foreign_key \
                      --columns customer_id --ref-table demo.customer --ref-constraint pk_customer \
                      --match full",
        )
        .unwrap()
        .call;
        assert_eq!(call.method, "POST");
        assert_eq!(
            call.body.unwrap(),
            json!({"table": "demo.orders", "name": "fk", "type": "foreign_key",
                   "columns": ["customer_id"], "match": "full",
                   "references": {"table": "demo.customer", "constraint": "pk_customer"}})
        );
        let by_id = p(
            "constraints add --table t.u --name k --type unique --columns a,b \
                       --nulls not_distinct",
        )
        .unwrap()
        .call
        .body
        .unwrap();
        assert_eq!(by_id["columns"], json!(["a", "b"]));
        assert_eq!(by_id["nulls"], "not_distinct");
        let numeric = p(
            "constraints add --table t.u --name k --type foreign_key --columns a \
                         --ref-table t.v --ref-constraint 4",
        )
        .unwrap()
        .call
        .body
        .unwrap();
        assert_eq!(numeric["references"]["constraint"], 4);
    }

    #[test]
    fn usage_errors() {
        for line in [
            "",
            "constraints add --table t.u --name k --type unique",
            "constraints add --table t.u --name k --type foreign_key --columns a --ref-table t.v",
            "constraints list --tabel x",
            "constraints drop seven",
            "audit --since x",
            "frobnicate",
        ] {
            assert!(p(line).is_err(), "{line:?} should be a usage error");
        }
    }

    #[test]
    fn verify_reports_render_and_decide_the_exit_code() {
        let broken = json!({
            "table": "demo.orders", "head": 30, "ok": false, "first_broken": 30,
            "chain": [
                {"snapshot": 10, "status": "ANCHOR", "operation": "append"},
                {"snapshot": 20, "status": "OK", "operation": "append"},
                {"snapshot": 30, "status": "MISSING", "operation": "append",
                 "detail": "no certificate: committed without the Plane"}
            ]
        });
        let (text, ok) = render_verify(&broken);
        assert!(!ok);
        assert!(text.starts_with("demo.orders  head 30  BROKEN at snapshot 30\n"));
        assert!(text.contains("MISSING"));
        assert!(text.contains("committed without the Plane"));
        let (text, ok) =
            render_verify(&json!({"table": "t.u", "head": null, "ok": true, "chain": []}));
        assert!(ok);
        assert_eq!(text, "t.u  head none  OK\n");
    }
}
