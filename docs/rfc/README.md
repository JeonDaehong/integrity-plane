# RFCs

Required **before code** for any change to: constraint semantics, key encoding, certificate format,
transaction log format, or HTTP status mapping (`CONTRIBUTING.md`, spec Part II §9).

File name: `NNNN-short-title.md`. Use [`template.md`](template.md).

| # | Title | Status |
|---|---|---|
| [0001](0001-key-encoding-v1.md) | Key encoding v1 | Accepted |
| [0002](0002-certificate-format-v1.md) | Certificate format v1 | Accepted |
| [0003](0003-http-status-mapping.md) | HTTP status mapping v1 (adds INT-016) | Accepted |
| [0004](0004-transaction-log-v1.md) | Transaction log format v1 | Accepted |
| [0005](0005-signed-certificates.md) | Signed integrity certificates | Implemented |
| [0006](0006-chunked-deltas-and-log-retention.md) | Chunked staged deltas and txn log retention | Accepted |
| [0007](0007-request-limits.md) | Request limits (adds INT-017) | Accepted |
| [0008](0008-index-heads.md) | Index heads: `main` must be what the indexes reflect | Accepted |
| [0009](0009-external-signers.md) | Signing keys in Vault Transit or PKCS#11 (adds INT-018) | Accepted |
| [0010](0010-compressed-terminal-records.md) | Compressed terminal records in the transaction log | Accepted |
| [0011](0011-catch-up.md) | Catching up from the table history | Accepted |
| [0012](0012-fenced-domain-commits.md) | Fenced domain commits (several active Planes) | Accepted |
| [0013](0013-confined-file-reads.md) | Reading only the table's files | Accepted |
