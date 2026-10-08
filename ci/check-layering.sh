#!/usr/bin/env bash
# Enforces the layering rule (CONTRIBUTING.md rule 2, spec §12):
# integrity-types and integrity-core MUST NOT depend (even transitively) on
# iceberg, object_store, axum, tokio or any other I/O / runtime crate.
set -euo pipefail

FORBIDDEN='^(iceberg|iceberg-.*|object_store|axum|axum-.*|tokio|tokio-.*|hyper|hyper-.*|reqwest|tower|tower-.*|async-std|smol|mio|parquet|arrow|arrow-.*|opendal|fjall|redb|rocksdb|librocksdb-sys|sled)$'
status=0

for crate in integrity-types integrity-core; do
    deps=$(cargo tree --locked -p "$crate" -e normal,build --prefix none --format '{p}' \
        | awk '{print $1}' | sort -u)
    bad=$(printf '%s\n' "$deps" | grep -E "$FORBIDDEN" || true)
    if [ -n "$bad" ]; then
        echo "error: $crate depends on forbidden crate(s):" >&2
        printf '  %s\n' $bad >&2
        status=1
    else
        echo "ok: $crate has no forbidden dependencies"
    fi
done

exit "$status"
