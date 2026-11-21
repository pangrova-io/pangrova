---
title: "FLOAT"
description: "32-bit IEEE 754 floating point: range, precision, mappings, and limits"
---

# FLOAT

`FLOAT` is a 32-bit IEEE 754 binary floating-point type. It is an approximation type:
values such as `0.1` are not represented exactly, so FLOAT is unsuitable for currency and
other exact decimal requirements.

| Property | Value |
|----------|-------|
| Storage | 32-bit IEEE 754 binary floating point |
| Precision | About 7 decimal digits |
| Range | Approximately ±3.4 × 10^38 |
| Exact arithmetic | No; use `DECIMAL` for exact values |

The planner records single-precision metadata for `FLOAT`. An expression that mixes
`FLOAT` and `DOUBLE` is evaluated as `DOUBLE`, regardless of operand order.

## Literals and casts

```sql
SELECT CAST(3.14 AS FLOAT);
SELECT CAST(1 AS FLOAT) / CAST(3 AS FLOAT);
```

Casting `DOUBLE` to `FLOAT` narrows precision. Casting an exact numeric to `FLOAT` may
produce a rounded binary value. Use `DECIMAL` when equality and exact sums matter.

## Comparisons and aggregation

- Equality on `FLOAT` is exact binary equality, not approximate-equality semantics.
- `SUM` and `AVG` over `FLOAT` accumulate in the engine's promoted type; results may differ
  from decimal arithmetic.
- `NaN` and infinity follow IEEE 754 in the execution layer; do not rely on ordering or
  grouping of non-finite values without validating the workload.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `FLOAT` | `FLOAT` |
| Apache Paimon | `FLOAT` | `FLOAT` |
| Lance | Canonical Arrow single-precision floating point | `FLOAT` retained in managed catalog metadata |

## Example

```sql
CREATE TABLE measurements (
  sensor_id BIGINT,
  temperature FLOAT
);

INSERT INTO measurements VALUES (1, CAST(21.5 AS FLOAT));

SELECT sensor_id, CAST(temperature AS DOUBLE) AS temperature_64
FROM measurements;
```

## Related

- [DOUBLE](double.md) for 64-bit binary floating point
- [DECIMAL](decimal.md) for exact fixed-point values
- [Type casting](../type-casting.md)
