---
title: "Aggregate"
description: "COUNT, SUM, AVG, MIN, MAX, MAX_BY, MIN_BY, STDDEV, APPROX_COUNT_DISTINCT"
---

# Aggregate Functions

Aggregate functions compute a single result from a set of input values.

---

## COUNT

```sql
COUNT(*)         → BIGINT   -- Count all rows (including NULLs)
COUNT(expr)      → BIGINT   -- Count non-NULL values
COUNT(DISTINCT expr) → BIGINT  -- Count distinct non-NULL values
```

**Example:**

```sql
SELECT COUNT(*) AS total_orders,
       COUNT(DISTINCT user_id) AS unique_users,
       COUNT(discount_code) AS discounted_orders
FROM prod.analytics.orders;
```

---

## SUM

```sql
SUM(numeric_expr) → planner-derived numeric result type
```

`TINYINT`, `SMALLINT`, and `INTEGER` inputs are promoted to `BIGINT`.
The result type is fixed during planning; overflow does not dynamically switch
an integer result to `DECIMAL`.

**Example:**

```sql
SELECT user_id, SUM(amount) AS total_spent
FROM prod.analytics.orders
GROUP BY user_id;
```

---

## AVG

```sql
AVG(numeric_expr) → DOUBLE
```

**Example:**

```sql
SELECT category, AVG(price) AS avg_price
FROM prod.analytics.products
GROUP BY category;
```

---

## MIN / MAX

```sql
MIN(expr) → same as input type
MAX(expr) → same as input type
```

Works with numeric, string, date/time types.

**Example:**

```sql
SELECT MIN(order_date) AS first_order,
       MAX(order_date) AS last_order,
       MAX(amount) AS largest_order
FROM prod.analytics.orders;
```

---

## MAX_BY / MIN_BY

```sql
MAX_BY(value, key) → type of value
MIN_BY(value, key) → type of value
```

`MAX_BY` returns the value associated with the largest key; `MIN_BY` uses the
smallest key. `ARG_MAX` and `ARG_MIN` are aliases. Keys must be orderable.
NULL keys are ignored. A NULL value associated with the selected key remains
NULL; it is not replaced by another row's value. Empty input and groups whose
keys are all NULL return NULL.

Values can be complete rows, preserving fields from the same selected record:

```sql
SELECT tenant_id,
       MAX_BY(ROW(event_id, status), event_time) AS latest_event
FROM prod.analytics.events
GROUP BY tenant_id;
```

Tied keys may select any one matching record. For a deterministic tie-breaker,
use non-NULL row-key fields with a secondary field that uniquely orders ties:

```sql
SELECT MAX_BY(event_id, ROW(event_time, event_id)) AS latest_event_id
FROM prod.analytics.events
WHERE event_time IS NOT NULL AND event_id IS NOT NULL;
```

---

## STDDEV_POP / STDDEV_SAMP

```sql
STDDEV_POP(numeric_expr)  → DOUBLE  -- Population standard deviation
STDDEV_SAMP(numeric_expr) → DOUBLE  -- Sample standard deviation
```

**Example:**

```sql
SELECT category,
       STDDEV_POP(price) AS price_stddev_pop,
       STDDEV_SAMP(price) AS price_stddev_samp
FROM prod.analytics.products
GROUP BY category;
```

---

## VAR_POP / VAR_SAMP

```sql
VAR_POP(numeric_expr)  → DOUBLE  -- Population variance
VAR_SAMP(numeric_expr) → DOUBLE  -- Sample variance
```

---

## ANY_VALUE

```sql
ANY_VALUE(expr) → same as input type
```

Returns an arbitrary non-NULL value from the group. Useful when a column is functionally dependent on the GROUP BY key.

**Example:**

```sql
SELECT o.user_id, ANY_VALUE(u.name) AS user_name, COUNT(*) AS order_count
FROM prod.analytics.orders o
JOIN prod.analytics.users u ON o.user_id = u.user_id
GROUP BY o.user_id;
```

---

## COLLECT

```sql
COLLECT(expr) → ARRAY(same as input type)
```

Collects all non-NULL values into an array.

**Example:**

```sql
SELECT user_id, COLLECT(product_id) AS purchased_products
FROM prod.analytics.orders
GROUP BY user_id;
```

---

## APPROX_COUNT_DISTINCT

```sql
APPROX_COUNT_DISTINCT(expr) → BIGINT
```

Approximate distinct count using HyperLogLog. Accuracy and resource use depend
on the implementation and workload; this is not an exact count or a guarantee
of a particular speedup or per-query error bound.

**Example:**

```sql
SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_unique_users
FROM prod.analytics.events;
```

---

## Notes

- All aggregate functions ignore NULL values (except `COUNT(*)`)
- Aggregates can be used with `GROUP BY`, `HAVING`, or as window functions with `OVER()`
- `DISTINCT` modifier supported: `SUM(DISTINCT amount)`, `AVG(DISTINCT price)`

**See Also:** [Window Functions](window.md), [Queries](../queries.md)
