Pangrova > Guide > Functions > Aggregate Functions

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
SUM(numeric_expr) → same as input type (or DECIMAL for integer overflow)
```

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
SELECT user_id, ANY_VALUE(name) AS user_name, COUNT(*) AS order_count
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

Approximate distinct count using HyperLogLog. Faster than `COUNT(DISTINCT)` for large datasets with ~2% error.

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

**See Also:** [Window Functions](window-functions.md), [SQL Reference](../sql-reference.md)
