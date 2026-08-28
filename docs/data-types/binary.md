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
