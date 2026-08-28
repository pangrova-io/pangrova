---
title: "Table Functions"
description: "EXPLODE, POSEXPLODE, and GENERATE_SERIES table functions"
---

# Table Functions

Table functions produce rows and can be used in the FROM clause with LATERAL TABLE(...).


## Table Functions

| Function | Description |
|----------|-------------|
| `EXPLODE(array)` | Expands an array into rows |
| `POSEXPLODE(array)` | Expands an array into rows with position index |
| `GENERATE_SERIES(start, end)` | Generates a sequence of integers |

**Example:**

```sql
-- Explode array column
SELECT t.id, item
FROM orders t, LATERAL TABLE(EXPLODE(t.items)) AS e(item);

-- Generate series
SELECT n FROM TABLE(GENERATE_SERIES(1, 10)) AS s(n);
```
