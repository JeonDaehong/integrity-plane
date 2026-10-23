# 0029. Commits are matched to constraints by table UUID, not by name

- Status: Accepted
- Date: 2026-10-23

## Context

The gateway decides whether a commit needs validation from the table name in its path:
`POST …/namespaces/db/tables/orders` is validated when a constraint names `db.orders`, and
forwarded unchecked otherwise. Catalogs resolve names their own way. Lakekeeper 0.13.6 ignores
case: a commit to `…/namespaces/DB/tables/ORDERS` lands on `db.orders` (checked on 2026-10-22,
load and commit both answered 200). Any writer could skip validation by changing the case of a
name; the next commit through the Plane would see the bypass (`BYPASS_DETECTED`), after readers had
seen the rows. Other normalizations (Unicode, aliases) would have the same effect, and the Plane
cannot know every catalog's rules.

## Decision

The table a commit lands on is identified by its UUID, which the catalog reports for whatever
name it resolved:

- A commit whose path names a constrained table goes through validation as before.
- Otherwise, if any constraint is registered, the gateway loads the table at the commit's path
  (`?snapshots=refs`) and compares its UUID with the UUIDs of the constrained tables (their
  anchors, else their index heads, else loaded from the catalog for tables never anchored). A
  match is validated as that table, under its registered name; the client's path is still the one
  forwarded.
- A table that matches none is forwarded with an added requirement
  `{"type": "assert-table-uuid", "uuid": …}`: the catalog then commits only to the table that was
  checked, even if a constrained table is renamed into that name in between.
- A path that names no table (404: a create through a commit) is forwarded unchanged; the catalog
  refuses to create a name that resolves to an existing table.
- If the load fails otherwise, the commit is refused (`RECOVERY_REQUIRED`), as for constrained
  tables.

A path that names a constrained table but loads a different UUID keeps today's answer (the table
was replaced; rebuild), so dropping and re-creating a constrained table never turns its
enforcement off.

## Consequences

- Commits to tables without constraints cost one more catalog request while any constraint is
  registered (one load, plus one per constrained table that was never anchored). With no
  constraints registered, nothing changes.
- Forwarded commits to tables without constraints carry one more requirement.
- Two warehouses holding a table of the same name are still told apart only by UUID mismatch (the
  second is refused), as before.

## Alternatives considered

- **Fold names before matching** (lowercase): needs each catalog's rules (case, Unicode,
  separators) and fails open for any rule the Plane does not know.
- **Refuse names that differ from a constrained one only by case**: same problem for other
  normalizations, and breaks case-sensitive catalogs that hold both names.
- **A configuration switch** (`identify = "name"`): an operator who sets it wrongly loses
  enforcement silently; fail closed forbids that.
