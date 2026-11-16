---
title: "SMALLINT"
description: "16-bit signed integer: range, casts, connector mappings, and limits"
---

# SMALLINT

`SMALLINT` is a 16-bit signed integer. It is a compact choice for counters and codes whose
upper bound is known to fit in 32767.

| Property | Value |
|----------|-------|
| Storage | 16-bit signed integer |
| Range | -32,768 to 32,767 |
| Null | Supported; SQL `NULL` is distinct from `0` |
| Alias | None |

## Literals and casts

Bare integer literals are typed by the planner; use `CAST` or a typed column to select the
width explicitly.

```sql
SELECT CAST(32767 AS SMALLINT);
SELECT CAST(-32768 AS SMALLINT);
```

Narrowing conversions such as `CAST(40000 AS SMALLINT)` may lose precision or fail;
validate boundary values and choose `INTEGER` when the domain is not strictly bounded.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | 32-bit `INTEGER` | `INTEGER`; the original width is not preserved |
| Apache Paimon | `SMALLINT` | `SMALLINT` |
| Lance | Not declared by the managed connector | Not declared |

## Example

```sql
CREATE TABLE sensor_readings (
  sensor_id SMALLINT,
  reading   DOUBLE
);

INSERT INTO sensor_readings VALUES (CAST(42 AS SMALLINT), 21.5);

SELECT sensor_id, reading
FROM sensor_readings
WHERE sensor_id BETWEEN CAST(1 AS SMALLINT) AND CAST(100 AS SMALLINT);
```

## Related

- [Type casting](../type-casting.md)
- [TINYINT](tinyint.md), [INTEGER](integer.md), and [BIGINT](bigint.md)
