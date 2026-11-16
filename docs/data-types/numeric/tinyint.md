---
title: "TINYINT"
description: "8-bit signed integer: range, casts, connector mappings, and limits"
---

# TINYINT

`TINYINT` is an 8-bit signed integer. Use it for compact flags and small enumerations
where the value range is guaranteed by the application.

| Property | Value |
|----------|-------|
| Storage | 8-bit signed integer |
| Range | -128 to 127 |
| Null | Supported; SQL `NULL` is distinct from `0` |
| Alias | None |

## Literals and casts

A bare integer literal does not select `TINYINT`; the planner types literals from their
value and context. Use an explicit cast or a typed column:

```sql
SELECT CAST(127 AS TINYINT);
SELECT CAST(-128 AS TINYINT);
```

A narrowing cast such as `CAST(1000 AS TINYINT)` may lose precision or fail depending on
the execution path. Validate boundary values in the workload; use `INTEGER` or `BIGINT`
when the domain is not strictly bounded.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | 32-bit `INTEGER` | `INTEGER`; the original width is not preserved |
| Apache Paimon | `TINYINT` | `TINYINT` |
| Lance | Not declared by the managed connector | Not declared |

Creating a `TINYINT` column in Iceberg therefore widens it to `INTEGER`; a later read
returns `INTEGER`, not `TINYINT`. Paimon preserves the declared width.

## Example

```sql
CREATE TABLE feature_flags (
  flag_id TINYINT,
  enabled BOOLEAN
);

INSERT INTO feature_flags VALUES (CAST(1 AS TINYINT), TRUE);

SELECT flag_id, enabled
FROM feature_flags
WHERE flag_id > CAST(0 AS TINYINT);
```

## Related

- [Type casting](../type-casting.md) for conversion rules
- [INTEGER](integer.md) and [SMALLINT](smallint.md) for wider ranges
- [DECIMAL](decimal.md) for exact fractional values
