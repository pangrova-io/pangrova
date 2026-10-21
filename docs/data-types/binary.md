---
title: "Binary"
description: "BLOB / VARBINARY / FIXED binary types and UUID"
---

# Binary

Binary data types for raw bytes, fixed-size encodings, and universally unique identifiers.


## BLOB / VARBINARY

| Property | Value |
|----------|-------|
| Storage | Variable-length binary data |
| Aliases | `BLOB`, `VARBINARY`, `BINARY` |
| Literal | Hex literal `X'...'` |

```sql
CREATE TABLE binaries (id BIGINT, data BLOB);
INSERT INTO binaries VALUES (1, X'DEADBEEF');
```

---

## FIXED

| Property | Value |
|----------|-------|
| Storage | Fixed-length binary data |
| Use case | UUIDs, hashes, fixed-size encodings |

---

## UUID

| Property | Value |
|----------|-------|
| Storage | 128-bit universally unique identifier |
| SQL literal | `UUID '00112233-4455-6677-8899-aabbccddeeff'` |
| Ordering | Unsigned comparison of the canonical 16 big-endian bytes |
| JDBC | `Types.OTHER`, returned as `java.util.UUID` |

`UUID` remains distinct from `VARBINARY` and `FIXED(16)`. Casts between UUID,
VARCHAR, and VARBINARY use Native conversion semantics; a binary input must have
exactly 16 bytes. Invalid lengths fail without truncation.

```sql
SELECT CAST(X'00112233445566778899AABBCCDDEEFF' AS UUID);
SELECT CAST(UUID '00112233-4455-6677-8899-aabbccddeeff' AS VARCHAR);
```

Iceberg Parquet UUID columns use the UUID annotation on a 16-byte fixed binary
payload. Native readers validate that declaration before conversion. Plain binary
columns do not acquire UUID identity from their width.

Iceberg file and manifest UUID min/max statistics from historical writers can mix
signed Java comparison with unsigned Parquet comparison. Such bounds cannot safely
exclude files for UUID value predicates. Those predicates remain in Native execution,
with canonical byte filters applied to validated Parquet readers before row-reader
creation. Null-count and non-UUID metadata pruning remain enabled. Paimon primary-key
merge evaluates value predicates after merging versions.

See the [Parquet UUID definition](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#uuid)
and the [upstream Iceberg pruning analysis](https://issues.apache.org/jira/browse/IMPALA-15476).
