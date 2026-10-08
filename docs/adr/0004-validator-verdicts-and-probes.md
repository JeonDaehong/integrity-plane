# 0004. Validator: full violation sets, probe rules, errors vs verdicts

- Status: Accepted
- Date: 2026-10-08

## Context

Phase 3 implements PK / UNIQUE / NOT NULL / FK validation for single-table commits (spec §8, §13.3,
§14 steps 6–8). Several choices are not fixed by the spec: how much of the verdict is reported, which
keys are probed, and what happens when input or indexes contradict the invariants.

## Decision

- **Input lives in `integrity-core`** (`CommitRows`, `RowBatch`, `Datum`): the Iceberg adapter
  depends only on core (spec §12) and produces rows projected onto `Validator::projection(table)`.
- **Full violation set.** A rejection carries every violated `(constraint, code)` pair, not the
  first one found. This makes verdicts deterministic regardless of evaluation order (Principle 4)
  and lets the differential test compare verdicts exactly with the oracle.
- **Order of work.** Classify every tuple by spec §7 and count multiplicities of added keys before
  any probe (spec §8). Then one batched `get_many` per index over distinct keys (spec §13.3).
- **Probe rules**, relying on the invariant that the pre-commit state is valid and indexed:
  - PK/UNIQUE: every key whose net count changes. Rising and present ⇒ duplicate. Falling and absent
    (or falling by more than one) ⇒ inconsistency.
  - FK child: only keys whose net count **rises** need a parent; a key whose count is unchanged or
    falls was already referenced, so its parent existed, and the parent table is not written by the
    same commit. This is what keeps validation proportional to the changed key set.
  - FK parent: keys of the parent constraint whose net count falls (removed and not re-added) must
    have no entry in any enforced FK's reference index ⇒ otherwise `REFERENCED_ROW_DELETE`.
- **Errors are not verdicts.** Anything the validator cannot prove, or any contradiction of the
  invariants, is a `ValidationError`, and the caller rejects the commit:
  - `UNSUPPORTED_COMMIT_OPERATION` for input the Plane cannot interpret: a missing projected column,
    a value that does not fit its key family, an enforced FK whose parent constraint is not enforced;
  - `INDEX_DEGRADED` for index trouble: a missing index, a removed key that is not indexed, a removed
    row that violates its constraint by NULLs, an index error, or an index that moved between
    validation and staging.
- **Staleness check.** `validate` records the epoch of every index it reads or will write before
  reading; `ValidatedDeltas::stage` refuses if any of them moved. The domain queue (Phase 9) is what
  prevents interleaving; this check makes a violation of that assumption fail closed.

## Consequences

- The differential suite (`crates/integrity-validator/tests/differential.rs`) compares verdicts as
  sets, and also checks that incrementally maintained indexes equal indexes rebuilt from the final
  oracle state.
- Error bodies with `sample_keys` / `violation_count` (spec §24) are not produced yet; they need key
  samples per violation and arrive with the gateway (Phase 7).
- Rows are held in memory per commit; streaming extraction is an optimization for later phases.

## Alternatives considered

- *Stop at the first violation*: cheaper on rejection, but the reported code would depend on
  evaluation order.
- *Probe every added FK child key*: simpler, but proportional to rows rewritten rather than keys
  changed (copy-on-write rewrites would probe the parent for every unchanged row).
