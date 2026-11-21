---
title: "INT / INTEGER"
description: "32-bit signed integer: range, casts, connector mappings, and limits"
---

# INT / INTEGER

`INTEGER` (alias `INT`) is a 32-bit signed integer and the default width for most
integer-valued columns.

| Property | Value |
|----------|-------|
| Storage | 32-bit signed integer |
| Range | -2,147,483,648 to 2,147,483,647 |
| Null | Supported; SQL `NULL` is distinct from `0` |
| Alias | `INT` |

## Literals and casts

Use `CAST` when a literal or expression must have exactly 32 bits:

```sql
SELECT CAST(42 AS INTEGER);
SELECT CAST(2147483647 AS INT);
```

Widening to `BIGINT` is lossless for every `INTEGER` value. Narrowing to `SMALLINT` or
`TINYINT` is not; validate boundaries and prefer `BIGINT` for identifiers that may grow.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `INTEGER` | `INTEGER` |
| Apache Paimon | `INTEGER` | `INTEGER` |
| Lance | Canonical Arrow signed 32-bit integer | `INTEGER` retained in managed catalog metadata |

`TINYINT` and `SMALLINT` columns written to Iceberg become `INTEGER`; an Iceberg read
returns `INTEGER` for all of them.

## Example

```sql
CREATE TABLE orders (
  order_id INTEGER,
  quantity INTEGER
);

INSERT INTO orders VALUES (1042, 3);

SELECT SUM(quantity) AS units
FROM orders
WHERE order_id >= 1000;
```

## Related

- [Type casting](../type-casting.md)
- [BIGINT](bigint.md) for 64-bit identifiers
- [DECIMAL](decimal.md) for exact fixed-point arithmetic
