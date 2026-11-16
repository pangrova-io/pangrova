---
title: "DOUBLE"
description: "64-bit IEEE 754 floating point: range, precision, mappings, and limits"
---

# DOUBLE

`DOUBLE` (alias `DOUBLE PRECISION`) is a 64-bit IEEE 754 binary floating-point type with
about 15 significant decimal digits. It is the default floating-point result type when an
expression mixes `FLOAT` and `DOUBLE`.

| Property | Value |
|----------|-------|
| Storage | 64-bit IEEE 754 binary floating point |
| Precision | About 15 decimal digits |
| Range | Approximately ±1.8 × 10^308 |
| Alias | `DOUBLE PRECISION` |
| Exact arithmetic | No; use `DECIMAL` for exact values |

## Literals and casts

```sql
SELECT CAST(3.141592653589793 AS DOUBLE);
SELECT CAST(1 AS DOUBLE) / CAST(3 AS DOUBLE);
```

Casting to `FLOAT` narrows precision and range. Casting to an integer type truncates the
fractional part; the result must still be representable in the target width.

## Comparisons and aggregation

- Equality compares binary values; values produced by different computation paths may not
  compare equal even when their decimal renderings agree.
- Aggregations over `DOUBLE` are approximate. Use `DECIMAL` for monetary amounts and other
  exact totals.
- Non-finite values (`NaN`, infinity) follow IEEE 754 in the execution layer; validate
  ordering, grouping, and serialization behavior for the specific workload.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `DOUBLE` | `DOUBLE` |
| Apache Paimon | `DOUBLE` | `DOUBLE` |
| Lance | Not declared by the managed connector | Not declared |

## Example

```sql
CREATE TABLE coordinates (
  id BIGINT,
  latitude DOUBLE,
  longitude DOUBLE PRECISION
);

INSERT INTO coordinates VALUES (1, 37.7749, 122.4194);

SELECT id, latitude, longitude
FROM coordinates
WHERE latitude BETWEEN 37.0 AND 38.0;
```

## Related

- [FLOAT](float.md) for 32-bit binary floating point
- [DECIMAL](decimal.md) for exact fixed-point values
- [Type casting](../type-casting.md)
