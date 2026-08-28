---
title: "Window"
description: "ROW_NUMBER, RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE"
---

# Window Functions

Window functions perform calculations across a set of rows related to the current row, without collapsing rows like aggregates.

---

## Syntax

```sql
function_name(args) OVER (
    [PARTITION BY partition_expr [, ...]]
    [ORDER BY sort_expr [ASC|DESC] [NULLS {FIRST|LAST}] [, ...]]
    [frame_clause]
)
```

**Frame Clause:**

```sql
{ROWS | RANGE} BETWEEN frame_start AND frame_end

frame_start/frame_end:
    UNBOUNDED PRECEDING | n PRECEDING | CURRENT ROW | n FOLLOWING | UNBOUNDED FOLLOWING
```

---

## Ranking Functions

### ROW_NUMBER

```sql
ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) → BIGINT
```

Sequential row number within each partition (no ties).

```sql
SELECT user_id, order_id, amount,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) AS rn
FROM prod.analytics.orders;
```

### RANK

```sql
RANK() OVER (ORDER BY ...) → BIGINT
```

Rank with gaps for ties (e.g., 1, 2, 2, 4).

```sql
SELECT product_id, price,
       RANK() OVER (ORDER BY price DESC) AS price_rank
FROM prod.analytics.products;
```

### DENSE_RANK

```sql
DENSE_RANK() OVER (ORDER BY ...) → BIGINT
```

Rank without gaps for ties (e.g., 1, 2, 2, 3).

```sql
SELECT user_id, total_spent,
       DENSE_RANK() OVER (PARTITION BY region ORDER BY total_spent DESC) AS rank
FROM user_summary;
```

### NTILE

```sql
NTILE(n) OVER (ORDER BY ...) → BIGINT
```

Divides rows into `n` approximately equal buckets.

```sql
SELECT user_id, amount,
       NTILE(4) OVER (ORDER BY amount) AS quartile
FROM prod.analytics.orders;
```

---

## Value Functions

### LAG / LEAD

```sql
LAG(expr [, offset [, default]]) OVER (ORDER BY ...) → same type
LEAD(expr [, offset [, default]]) OVER (ORDER BY ...) → same type
```

Access values from previous/next rows. Default offset is 1.

```sql
SELECT order_date, amount,
       LAG(amount) OVER (ORDER BY order_date) AS prev_amount,
       LEAD(amount) OVER (ORDER BY order_date) AS next_amount,
       amount - LAG(amount) OVER (ORDER BY order_date) AS change
FROM prod.analytics.orders
WHERE user_id = 'u001';
```

### FIRST_VALUE / LAST_VALUE

```sql
FIRST_VALUE(expr) OVER (window_spec) → same type
LAST_VALUE(expr) OVER (window_spec) → same type
```

```sql
SELECT order_date, amount,
       FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY order_date) AS first_order_amount,
       LAST_VALUE(amount) OVER (
           PARTITION BY user_id ORDER BY order_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_order_amount
FROM prod.analytics.orders;
```

---

## Aggregate Window Functions

All aggregate functions can be used as window functions:

```sql
SELECT order_date, amount,
       SUM(amount) OVER (ORDER BY order_date) AS running_total,
       AVG(amount) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3,
       COUNT(*) OVER (PARTITION BY user_id) AS user_order_count
FROM prod.analytics.orders;
```

---

## Named WINDOW Clause

Define reusable window specifications:

```sql
SELECT user_id, order_date, amount,
       ROW_NUMBER() OVER w AS rn,
       SUM(amount) OVER w AS running_total
FROM prod.analytics.orders
WINDOW w AS (PARTITION BY user_id ORDER BY order_date);
```

---

## QUALIFY

Filter rows based on window function results (avoids subqueries):

```sql
-- Keep only the top-3 orders per user
SELECT user_id, order_id, amount
FROM prod.analytics.orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) <= 3;
```

---

## Notes

- Window functions are evaluated after WHERE, GROUP BY, and HAVING
- QUALIFY is evaluated after window functions
- Frame clause defaults to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` when ORDER BY is specified
- Without ORDER BY, the frame is all rows in the partition

**See Also:** [Aggregate Functions](aggregate.md), [Queries — QUALIFY](../queries.md#qualify)
