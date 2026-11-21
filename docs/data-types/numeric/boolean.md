---
title: "BOOLEAN"
description: "Three-valued logical type: literals, predicates, connector mappings, and null semantics"
---

# BOOLEAN

`BOOLEAN` represents `TRUE`, `FALSE`, and the SQL unknown value `NULL`. It is the result
type of comparisons and the input type of `WHERE`, `HAVING`, and conditional expressions.

| Property | Value |
|----------|-------|
| Storage | One logical bit; Arrow stores boolean values bit-packed |
| Values | `TRUE`, `FALSE`, `NULL` |
| Casts from numeric | Not supported |
| Casts from string | `CAST('true' AS BOOLEAN)` and `CAST('false' AS BOOLEAN)` |

## Three-valued logic

`NULL` is not `FALSE`. A predicate over a `NULL` operand evaluates to `NULL`, and a row is
returned by `WHERE` only when the predicate is `TRUE`.

```sql
SELECT id
FROM flags
WHERE active;          -- excludes FALSE and NULL

SELECT id
FROM flags
WHERE active IS NOT TRUE;  -- includes FALSE and NULL
```

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `BOOLEAN` | `BOOLEAN` |
| Apache Paimon | `BOOLEAN` | `BOOLEAN` |
| Lance | Canonical Arrow boolean schema | `BOOLEAN` retained in managed catalog metadata |

## Example

```sql
CREATE TABLE flags (
  id BIGINT,
  active BOOLEAN
);

INSERT INTO flags VALUES (1, TRUE), (2, FALSE), (3, NULL);

SELECT COUNT(*) AS active_rows
FROM flags
WHERE active;

SELECT COUNT(*) AS not_true_rows
FROM flags
WHERE active IS NOT TRUE;
```

## Related

- [Type casting](../type-casting.md)
- [Conditional functions](../../sql-reference/functions/conditional.md)
