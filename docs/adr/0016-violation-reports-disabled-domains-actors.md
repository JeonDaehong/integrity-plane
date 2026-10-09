# 0016. Violation reports, disabled domains and audit actors

- Status: Accepted
- Date: 2026-10-09

## Context

Spec §24 asks for structured errors with the number of offending keys and up to ten sample keys,
redactable because keys can be personal data. Spec §22 and RFC 0003 fix the commit error body as a
plain Iceberg `ErrorModel` and point to `/v1/integrity/transactions/{txn_id}` for the full error.
Spec §19 lets an operator put a domain in `Disabled` mode, and §25 asks audit events to name an
actor.

## Decision

**Violation details come from the validation itself.** `Validator::validate_with_details` records,
for every violated `(constraint, code)`, the distinct offending keys (or rows, for NULL violations)
and the first ten as decoded tuples, in the same pass that decides. `validate` is the same call
without the details, so the verdict and its explanation cannot disagree; the differential tests
assert that the details have exactly the verdict's violations as keys.

**Where reports go.**

- The commit error body stays an `ErrorModel` (RFC 0003). Its message keeps the integrity code first
  and adds a summary and the transaction: `INT-005 FOREIGN_KEY_VIOLATION: commit rejected: INT-005
  on fk_orders_customer (11 keys); details: GET /v1/integrity/transactions/17`. No key values.
- The structured error (code, message, transaction id, and per violation: constraint, table,
  `violation_count`, `sample_keys` by column name) is stored with the `COMMIT_REJECTED` audit event
  and served by `GET /v1/integrity/transactions/{id}`. The transaction log format is unchanged.
- Onboarding and rebuild reports use the same entries.
- With `errors.redact_keys = true`, sample keys are never stored or returned
  (`sample_keys_redacted: true`); counts remain.

Key values are rendered as JSON numbers and booleans, or as strings for decimals, dates,
timestamps (ISO 8601), binary (hex) and UUIDs.

**Disabled domains.** `POST /v1/integrity/domains/{table}/disable` with a reason marks every table
of the domain disabled. Commits to a disabled domain are forwarded unchanged: not validated, not
certified, each audited as `COMMIT_UNCHECKED`. The only way back is a rebuild (or a registration
that onboards the domain), which validates all data written meanwhile and re-anchors the chain; a
rebuild that finds violations leaves the domain disabled. Disabling is an explicit, audited operator
decision; nothing disables a domain automatically.

**Actors.** Audit events carry `actor`: the request's `X-Integrity-Actor` header, else its
`User-Agent`, else `unknown`; `plane` for the Plane's own actions. The CLI sends `--actor` or the
local user name. Actors are self-declared, not authenticated.

## Consequences

- Rejected commits with many violations cost a little more (collecting keys); accepted commits
  are unaffected.
- With redaction off (the default, as in the spec), key values of rejected commits are kept in the
  audit log; `docs/threat-model.md` says so.
- Finding a transaction's structured error scans the audit log (linear; acceptable for 0.1).

## Alternatives considered

- **Extra fields in the 400 body.** Iceberg clients ignore unknown fields today, but RFC 0003 fixes
  the body, and writers' logs would carry key values.
- **A second, explaining pass after a rejection.** Two code paths that must agree; the single pass
  makes disagreement impossible.
- **Storing the report in the transaction log.** Changes the log format (RFC 0004).
