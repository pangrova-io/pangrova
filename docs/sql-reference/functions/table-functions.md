---
title: "Table Functions"
description: "UNNEST, EXPLODE, POSEXPLODE, and GENERATE_SERIES table functions"
---

# Table Functions

Table functions produce rows in the FROM clause. `UNNEST` accepts one collection
directly; `EXPLODE`, `POSEXPLODE`, and `GENERATE_SERIES` use `TABLE(...)`.


## Table Functions

| Function | Description |
|----------|-------------|
| `EXPLODE(array)` | Expands an array into rows |
| `POSEXPLODE(array)` | Expands an array into rows with position index |
| `GENERATE_SERIES(start, end)` | Generates a sequence of integers |
| `UNNEST(collection)` | Expands an array or map in the FROM clause |
| `UNNEST(collection) WITH ORDINALITY` | Adds the position of each expanded row |

**Example:**

```sql
-- Explode array column
SELECT t.id, item
FROM orders t, LATERAL TABLE(EXPLODE(t.items)) AS e(item);

-- Generate series
SELECT n FROM TABLE(GENERATE_SERIES(1, 10)) AS s(n);

-- Correlated collection with explicit output names
SELECT u.value, u.position
FROM orders t CROSS JOIN
UNNEST(t.items) WITH ORDINALITY AS u(value, position);
```
