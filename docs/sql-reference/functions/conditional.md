---
title: "Conditional"
description: "CASE, COALESCE, NULLIF, null and Boolean truth tests, BETWEEN, IN, LIKE"
---

# Conditional Functions

---

## CASE Expression

```sql
-- Simple CASE
CASE expr
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    [ELSE default_result]
END

-- Searched CASE
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    [ELSE default_result]
END
```

```sql
-- Simple CASE
SELECT order_id,
       CASE status
           WHEN 'completed' THEN 'Done'
           WHEN 'pending' THEN 'Waiting'
           ELSE 'Unknown'
       END AS status_label
FROM prod.analytics.orders;

-- Searched CASE
SELECT user_id, amount,
       CASE
           WHEN amount > 1000 THEN 'high'
           WHEN amount > 100 THEN 'medium'
           ELSE 'low'
       END AS tier
FROM prod.analytics.orders;
```

---

A simple CASE compares its operand with each WHEN value using SQL equality.
NULL does not match NULL in a simple CASE; use a searched `WHEN value IS NULL`
branch to handle it. Without a matching branch, CASE returns ELSE, or NULL when
ELSE is omitted.

## Boolean Truth Tests

`predicate IS [NOT] TRUE`, `predicate IS [NOT] FALSE`, and
`predicate IS [NOT] UNKNOWN` accept a Boolean operand and always return a
non-NULL Boolean. `UNKNOWN` denotes the NULL truth value.

| Operand | IS TRUE | IS FALSE | IS UNKNOWN | IS NOT TRUE | IS NOT FALSE | IS NOT UNKNOWN |
| --- | --- | --- | --- | --- | --- | --- |
| TRUE | TRUE | FALSE | FALSE | FALSE | TRUE | TRUE |
| FALSE | FALSE | TRUE | FALSE | TRUE | FALSE | TRUE |
| NULL | FALSE | FALSE | TRUE | TRUE | TRUE | FALSE |

```sql
-- Reject both false and unknown validation results.
SELECT id FROM orders WHERE (amount > 0) IS NOT TRUE;
```

Unlike `NOT (amount > 0)`, this predicate also selects rows whose amount is NULL.
Parenthesize compound operands when combining truth tests with AND or OR.

## COALESCE

```sql
COALESCE(expr1, expr2, ...) → first non-NULL value
```

```sql
SELECT COALESCE(discount_code, 'NONE') AS code FROM orders;
SELECT COALESCE(nickname, name, 'Anonymous') AS display_name FROM users;
```

---

## IS NULL / IS NOT NULL

```sql
expr IS NULL → BOOLEAN
expr IS NOT NULL → BOOLEAN
```

```sql
SELECT * FROM orders WHERE discount_code IS NOT NULL;
SELECT * FROM users WHERE email IS NULL;
```

---

## BETWEEN

```sql
expr BETWEEN low AND high → BOOLEAN
```

Inclusive range check: equivalent to `expr >= low AND expr <= high`.

```sql
SELECT * FROM orders WHERE amount BETWEEN 100.00 AND 500.00;
SELECT * FROM events WHERE event_time BETWEEN TIMESTAMP '2024-01-01' AND TIMESTAMP '2024-12-31';
```

---

## IN / NOT IN

```sql
expr IN (value1, value2, ...)     → BOOLEAN
expr NOT IN (value1, value2, ...) → BOOLEAN
expr IN (subquery)                → BOOLEAN
expr NOT IN (subquery)            → BOOLEAN
```

```sql
SELECT * FROM orders WHERE status IN ('pending', 'processing');
SELECT * FROM users WHERE region NOT IN ('test', 'staging');

-- Subquery
SELECT * FROM users WHERE user_id IN (
    SELECT user_id FROM orders WHERE amount > 1000
);
```

---

## LIKE

```sql
expr LIKE pattern → BOOLEAN
```

Pattern: `%` = any sequence, `_` = single character.

```sql
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM products WHERE sku LIKE 'PRD-____';
```

---

## EXISTS

```sql
EXISTS (subquery) → BOOLEAN
```

```sql
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.user_id AND o.amount > 1000
);
```

---

## Notes

- CASE returns NULL if no WHEN matches and no ELSE is specified
- COALESCE evaluates arguments left-to-right and returns the first non-NULL
- NULL handling: `NULL IN (...)` returns NULL, not FALSE
- NOT IN with NULL values can produce unexpected results — prefer NOT EXISTS

**See Also:** [String Functions](string.md), [Queries](../queries.md)
