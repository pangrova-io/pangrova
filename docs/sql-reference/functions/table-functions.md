---
title: "Table Functions"
description: "UNNEST, EXPLODE, POSEXPLODE, and GENERATE_SERIES table functions"
---

# Table Functions

Table functions produce rows in the FROM clause. `UNNEST` and
`GENERATE_SERIES` accept direct calls; `EXPLODE` and `POSEXPLODE` use
`TABLE(...)`. `GENERATE_SERIES` also accepts the `TABLE(...)` form.


## Table Functions

| Function | Description |
|----------|-------------|
| `EXPLODE(array)` | Expands an array into rows |
| `POSEXPLODE(array)` | Expands an array into rows with position index |
| `GENERATE_SERIES(start, end[, step])` | Generates inclusive BIGINT values; step defaults to 1 |
| `UNNEST(collection)` | Expands an array or map in the FROM clause |
| `UNNEST(collection) WITH ORDINALITY` | Adds the position of each expanded row |

**Example:**

```sql
-- Explode array column
SELECT t.id, item
FROM orders t, LATERAL TABLE(EXPLODE(t.items)) AS e(item);

-- Generate series
SELECT n FROM TABLE(GENERATE_SERIES(1, 10)) AS s(n);

-- Equivalent direct form with descending step
SELECT n FROM GENERATE_SERIES(10, 1, -3) AS s(n);

-- Correlated collection with explicit output names
SELECT u.value, u.position
FROM orders t CROSS JOIN
UNNEST(t.items) WITH ORDINALITY AS u(value, position);
```

`GENERATE_SERIES` arguments must be constant integers in the signed 64-bit
range. Correlated `LATERAL` calls are not supported. The end value is included
when reached by the step. A step of zero is an error; a step pointing away from
the end returns no rows.
