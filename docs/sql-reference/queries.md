---
title: "Queries"
description: "SELECT syntax, joins, aggregations, subqueries, CTEs, and advanced query features"
---

# Queries

Complete reference for query statements in Pangrova, with practical examples against the sample lakehouse tables.


**Example tables used throughout this guide:**

| Table | Columns |
|-------|---------|
| `prod.analytics.events` | `event_id BIGINT`, `event_type VARCHAR`, `user_id VARCHAR`, `event_time TIMESTAMP`, `amount DECIMAL(10,2)`, `payload VARCHAR` |
| `prod.analytics.users` | `user_id VARCHAR`, `name VARCHAR`, `email VARCHAR`, `region VARCHAR`, `created_at TIMESTAMP` |
| `prod.analytics.orders` | `order_id BIGINT`, `user_id VARCHAR`, `product_id VARCHAR`, `amount DECIMAL(10,2)`, `order_date DATE`, `status VARCHAR` |
| `prod.analytics.products` | `product_id VARCHAR`, `name VARCHAR`, `category VARCHAR`, `price DECIMAL(10,2)`, `description VARCHAR` |


## SELECT Syntax

```sql
SELECT [DISTINCT] select_list
FROM table_reference
[WHERE condition]
[GROUP BY expression_list]
[HAVING condition]
[ORDER BY sort_item [ASC | DESC] [, ...]]
[LIMIT count]
[OFFSET offset];
```

## Practical Examples

**Basic SELECT with WHERE:**

```sql
SELECT event_id, event_type, amount
FROM prod.analytics.events
WHERE event_type = 'purchase'
  AND amount > 100.00
  AND event_time >= TIMESTAMP '2024-01-01 00:00:00';
```

**GROUP BY with HAVING:**

```sql
SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total_spent
FROM prod.analytics.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY user_id
HAVING SUM(amount) > 500.00;
```

**ORDER BY with LIMIT and OFFSET:**

```sql
SELECT order_id, user_id, amount, order_date
FROM prod.analytics.orders
ORDER BY amount DESC, order_date ASC
LIMIT 20
OFFSET 40;
```

**SELECT DISTINCT:**

```sql
SELECT DISTINCT region
FROM prod.analytics.users;
```

**Inline VALUES table:**

```sql
SELECT t.event_id, t.amount
FROM (VALUES (1, 12.50), (2, 7.25)) AS t(event_id, amount);
```

Each row must have the same number of fields. A declared column alias list must
match that width. Pangrova preserves the `VALUES` table constructor in the
Calcite plan, including duplicate rows.


## JOIN Examples

**INNER JOIN:**

```sql
SELECT o.order_id, u.name, o.amount
FROM prod.analytics.orders o
JOIN prod.analytics.users u ON o.user_id = u.user_id
WHERE o.status = 'completed';
```

**LEFT JOIN:**

```sql
SELECT u.user_id, u.name, o.order_id, o.amount
FROM prod.analytics.users u
LEFT JOIN prod.analytics.orders o ON u.user_id = o.user_id;
```

**RIGHT JOIN:**

```sql
SELECT o.order_id, u.name, o.amount
FROM prod.analytics.orders o
RIGHT JOIN prod.analytics.users u ON o.user_id = u.user_id;
```

**FULL OUTER JOIN:**

```sql
SELECT u.user_id, u.name, e.event_id, e.event_type
FROM prod.analytics.users u
FULL OUTER JOIN prod.analytics.events e ON u.user_id = e.user_id;
```

**CROSS JOIN:**

```sql
SELECT u.name, p.name AS product_name, p.price
FROM prod.analytics.users u
CROSS JOIN prod.analytics.products p
WHERE p.category = 'featured';
```

## Aggregation Examples

```sql
SELECT
  status,
  COUNT(*) AS total_orders,
  COUNT(DISTINCT user_id) AS unique_customers,
  SUM(amount) AS revenue,
  AVG(amount) AS avg_order_value,
  MIN(amount) AS smallest_order,
  MAX(amount) AS largest_order
FROM prod.analytics.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY status;
```

## Window Functions

**ROW_NUMBER:**

```sql
SELECT user_id, order_id, amount,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) AS rn
FROM prod.analytics.orders;
```

**RANK:**

```sql
SELECT user_id, amount,
  RANK() OVER (ORDER BY amount DESC) AS revenue_rank
FROM prod.analytics.orders;
```

**DENSE_RANK:**

```sql
SELECT product_id, category, price,
  DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank
FROM prod.analytics.products;
```

**LAG:**

```sql
SELECT order_date, amount,
  LAG(amount) OVER (ORDER BY order_date) AS prev_amount
FROM prod.analytics.orders
WHERE user_id = 'u-1001';
```

**LEAD:**

```sql
SELECT order_date, amount,
  LEAD(amount) OVER (ORDER BY order_date) AS next_amount
FROM prod.analytics.orders
WHERE user_id = 'u-1001';
```

**NTILE:**

```sql
SELECT user_id, amount,
  NTILE(4) OVER (ORDER BY amount DESC) AS spending_quartile
FROM prod.analytics.orders;
```

**Window Frame — ROWS BETWEEN:**

```sql
-- Running total
SELECT order_date, amount,
  SUM(amount) OVER (
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM prod.analytics.orders;

-- 3-row moving average
SELECT order_date, amount,
  AVG(amount) OVER (
    ORDER BY order_date
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS moving_avg
FROM prod.analytics.orders;

-- Cumulative to end
SELECT order_date, amount,
  SUM(amount) OVER (
    ORDER BY order_date
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) AS remaining_total
FROM prod.analytics.orders;
```


## Subqueries

**Scalar subquery:**

```sql
SELECT user_id, name,
  (SELECT COUNT(*) FROM prod.analytics.orders o WHERE o.user_id = u.user_id) AS order_count
FROM prod.analytics.users u;
```

**IN subquery:**

```sql
SELECT user_id, name, email
FROM prod.analytics.users
WHERE user_id IN (
  SELECT DISTINCT user_id
  FROM prod.analytics.orders
  WHERE amount > 500.00
);
```

**EXISTS subquery:**

```sql
SELECT u.user_id, u.name
FROM prod.analytics.users u
WHERE EXISTS (
  SELECT 1
  FROM prod.analytics.events e
  WHERE e.user_id = u.user_id
    AND e.event_type = 'purchase'
);
```

**Correlated subquery:**

```sql
SELECT o.order_id, o.amount
FROM prod.analytics.orders o
WHERE o.amount > (
  SELECT AVG(o2.amount)
  FROM prod.analytics.orders o2
  WHERE o2.user_id = o.user_id
);
```

## Common Table Expressions (CTE)

**Single CTE:**

```sql
WITH high_value_customers AS (
  SELECT user_id, SUM(amount) AS total_spent
  FROM prod.analytics.orders
  GROUP BY user_id
  HAVING SUM(amount) > 10000.00
)
SELECT u.name, u.email, h.total_spent
FROM high_value_customers h
JOIN prod.analytics.users u ON h.user_id = u.user_id
ORDER BY h.total_spent DESC;
```

**Multiple CTEs:**

```sql
WITH monthly_revenue AS (
  SELECT
    EXTRACT(YEAR FROM order_date) AS yr,
    EXTRACT(MONTH FROM order_date) AS mo,
    SUM(amount) AS revenue
  FROM prod.analytics.orders
  GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
),
ranked_months AS (
  SELECT yr, mo, revenue,
    RANK() OVER (ORDER BY revenue DESC) AS rnk
  FROM monthly_revenue
)
SELECT yr, mo, revenue
FROM ranked_months
WHERE rnk <= 5;
```

## Set Operations

**UNION ALL:**

```sql
SELECT user_id, event_type AS activity, event_time AS activity_time
FROM prod.analytics.events
WHERE event_type = 'login'
UNION ALL
SELECT user_id, 'order' AS activity, order_date AS activity_time
FROM prod.analytics.orders;
```

**UNION (distinct):**

```sql
SELECT region FROM prod.analytics.users WHERE region IS NOT NULL
UNION
SELECT 'unknown' AS region FROM prod.analytics.users WHERE region IS NULL;
```

**INTERSECT:**

```sql
SELECT user_id FROM prod.analytics.orders WHERE status = 'completed'
INTERSECT
SELECT user_id FROM prod.analytics.events WHERE event_type = 'review';
```

**EXCEPT:**

```sql
SELECT user_id FROM prod.analytics.users
EXCEPT
SELECT DISTINCT user_id FROM prod.analytics.orders;
```


## Expressions

**Simple CASE:**

```sql
SELECT order_id, status,
  CASE status
    WHEN 'completed' THEN 'Done'
    WHEN 'pending' THEN 'In Progress'
    WHEN 'cancelled' THEN 'Cancelled'
    ELSE 'Unknown'
  END AS status_label
FROM prod.analytics.orders;
```

**Searched CASE:**

```sql
SELECT order_id, amount,
  CASE
    WHEN amount >= 1000.00 THEN 'premium'
    WHEN amount >= 100.00 THEN 'standard'
    ELSE 'small'
  END AS order_tier
FROM prod.analytics.orders;
```

**BETWEEN:**

```sql
SELECT order_id, amount
FROM prod.analytics.orders
WHERE amount BETWEEN 50.00 AND 200.00;
```

**LIKE:**

```sql
SELECT user_id, name, email
FROM prod.analytics.users
WHERE email LIKE '%@example.com';
```

**IS NULL / IS NOT NULL:**

```sql
SELECT user_id, name
FROM prod.analytics.users
WHERE region IS NULL;

SELECT user_id, name
FROM prod.analytics.users
WHERE region IS NOT NULL;
```

**NOT IN:**

```sql
SELECT user_id, name
FROM prod.analytics.users
WHERE user_id NOT IN ('u-1001', 'u-1002', 'u-1003');
```

**String concatenation (||):**

```sql
SELECT user_id, name || ' <' || email || '>' AS display_name
FROM prod.analytics.users;
```

## CAST

```sql
SELECT CAST(event_id AS VARCHAR) AS event_id_str
FROM prod.analytics.events;

SELECT CAST(amount AS BIGINT) AS amount_rounded
FROM prod.analytics.orders;

SELECT CAST('2024-06-15 10:30:00' AS TIMESTAMP) AS ts;

SELECT CAST(price AS DECIMAL) AS price_decimal
FROM prod.analytics.products;
```

## EXTRACT

```sql
SELECT
  EXTRACT(YEAR FROM event_time) AS event_year,
  EXTRACT(MONTH FROM event_time) AS event_month,
  EXTRACT(DAY FROM event_time) AS event_day,
  EXTRACT(HOUR FROM event_time) AS event_hour,
  EXTRACT(MINUTE FROM event_time) AS event_minute,
  EXTRACT(SECOND FROM event_time) AS event_second
FROM prod.analytics.events
WHERE event_id = 1001;
```

## SUBSTRING

```sql
SELECT SUBSTRING(payload FROM 1 FOR 50) AS payload_preview
FROM prod.analytics.events
WHERE event_type = 'error';
```

## ARRAY Constructor

```sql
SELECT ARRAY[1, 2, 3] AS sample_array;

SELECT ARRAY['electronics', 'clothing', 'home'] AS categories;
```

## INTERVAL Literal

```sql
SELECT event_id, event_time
FROM prod.analytics.events
WHERE event_time >= TIMESTAMP '2024-06-01 00:00:00' - INTERVAL '30' DAY;

SELECT event_id, event_time
FROM prod.analytics.events
WHERE event_time >= TIMESTAMP '2024-06-15 12:00:00' - INTERVAL '6' HOUR;
```

---

## EXPLAIN

```sql
EXPLAIN
SELECT u.name, SUM(o.amount) AS total_spent
FROM prod.analytics.orders o
JOIN prod.analytics.users u ON o.user_id = u.user_id
GROUP BY u.name
ORDER BY total_spent DESC
LIMIT 10;
```

---

## QUALIFY

Filter rows based on window function results without a subquery.

```sql
SELECT columns
FROM table
[WHERE condition]
QUALIFY window_condition;
```

**Example:**

```sql
-- Keep only the latest order per user
SELECT user_id, order_id, amount, order_date
FROM prod.analytics.orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) = 1;
```

## TABLESAMPLE

Return a random sample of rows from a table.

```sql
SELECT columns
FROM table TABLESAMPLE {BERNOULLI | SYSTEM} (percentage)
    [REPEATABLE (seed)];
```

**Example:**

```sql
-- Sample ~10% of rows (Bernoulli: row-level sampling)
SELECT * FROM prod.analytics.events
TABLESAMPLE BERNOULLI (10);

-- System-level sampling with reproducible seed
SELECT * FROM prod.analytics.events
TABLESAMPLE SYSTEM (5) REPEATABLE (42);
```

## PIVOT

Transpose rows to columns.

```sql
SELECT columns
FROM table
PIVOT (aggregate_function(value_column)
       FOR pivot_column IN (value1 [AS alias1], value2 [AS alias2], ...))
[AS alias];
```

**Example:**

```sql
-- Pivot order amounts by status
SELECT user_id, completed, pending, cancelled
FROM prod.analytics.orders
PIVOT (SUM(amount) FOR status IN ('completed', 'pending', 'cancelled'))
AS pivoted;
```

## UNPIVOT

Transpose columns to rows.

```sql
SELECT columns
FROM table
UNPIVOT [INCLUDE NULLS | EXCLUDE NULLS]
    (value_column FOR name_column IN (col1, col2, ...))
[AS alias];
```

**Example:**

```sql
-- Unpivot quarterly revenue columns to rows
SELECT product_id, quarter, revenue
FROM quarterly_sales
UNPIVOT (revenue FOR quarter IN (q1_revenue, q2_revenue, q3_revenue, q4_revenue));
```

---

## WITH RECURSIVE (Recursive CTE)

```sql
WITH RECURSIVE cte_name [(columns)] AS (
    -- Anchor member
    SELECT ...
    UNION ALL
    -- Recursive member
    SELECT ... FROM cte_name WHERE ...
)
SELECT * FROM cte_name;
```

**Example:**

```sql
-- Generate hierarchy levels
WITH RECURSIVE org_tree (id, name, manager_id, level) AS (
    SELECT id, name, manager_id, 0 AS level
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, t.level + 1
    FROM employees e
    JOIN org_tree t ON e.manager_id = t.id
)
SELECT * FROM org_tree ORDER BY level, name;
```

**Notes:**
- Supports `SEARCH DEPTH FIRST BY` and `SEARCH BREADTH FIRST BY` clauses
- Supports `CYCLE` detection clause
