---
title: SQL Reference
sidebar_position: 2
description: Complete SQL syntax reference for Pangrova
---

# SQL Reference

Complete SQL syntax reference for Pangrova. Every SQL capability supported by Pangrova is documented here with concrete examples using Iceberg lakehouse tables.

> Features marked with 🔮 **Planned** are not yet fully implemented in the current release.
>
> **Implementation note**: Most SQL features are defined in the ANTLR4 grammar (`PangrovaSql.g4`).
> However, some capabilities (vector search, AI functions) are implemented at the planner/optimizer
> layer rather than as grammar tokens — see per-section notes for details.

**Example tables used throughout this guide:**

| Table | Columns |
|-------|---------|
| `prod.analytics.events` | `event_id BIGINT`, `event_type VARCHAR`, `user_id VARCHAR`, `event_time TIMESTAMP`, `amount DECIMAL(10,2)`, `payload VARCHAR` |
| `prod.analytics.users` | `user_id VARCHAR`, `name VARCHAR`, `email VARCHAR`, `region VARCHAR`, `created_at TIMESTAMP` |
| `prod.analytics.orders` | `order_id BIGINT`, `user_id VARCHAR`, `product_id VARCHAR`, `amount DECIMAL(10,2)`, `order_date DATE`, `status VARCHAR` |
| `prod.analytics.products` | `product_id VARCHAR`, `name VARCHAR`, `category VARCHAR`, `price DECIMAL(10,2)`, `description VARCHAR` |

---

## 1. Query

### SELECT Syntax

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

### Practical Examples

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


### JOIN Examples

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

### Aggregation Examples

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

### Window Functions

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


### Subqueries

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

### Common Table Expressions (CTE)

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

### Set Operations

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


### Expressions

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

### CAST

```sql
SELECT CAST(event_id AS VARCHAR) AS event_id_str
FROM prod.analytics.events;

SELECT CAST(amount AS BIGINT) AS amount_rounded
FROM prod.analytics.orders;

SELECT CAST('2024-06-15 10:30:00' AS TIMESTAMP) AS ts;

SELECT CAST(price AS DECIMAL) AS price_decimal
FROM prod.analytics.products;
```

### EXTRACT

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

### SUBSTRING

```sql
SELECT SUBSTRING(payload FROM 1 FOR 50) AS payload_preview
FROM prod.analytics.events
WHERE event_type = 'error';
```

### ARRAY Constructor

```sql
SELECT ARRAY[1, 2, 3] AS sample_array;

SELECT ARRAY['electronics', 'clothing', 'home'] AS categories;
```

### INTERVAL Literal

```sql
SELECT event_id, event_time
FROM prod.analytics.events
WHERE event_time >= TIMESTAMP '2024-06-01 00:00:00' - INTERVAL '30' DAY;

SELECT event_id, event_time
FROM prod.analytics.events
WHERE event_time >= TIMESTAMP '2024-06-15 12:00:00' - INTERVAL '6' HOUR;
```

### Time Travel

**FOR SYSTEM_TIME AS OF (timestamp-based):**

```sql
SELECT event_id, event_type, amount
FROM prod.analytics.events
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00'
WHERE event_type = 'purchase';
```

**FOR SYSTEM_VERSION AS OF (snapshot ID):**

```sql
SELECT order_id, amount, status
FROM prod.analytics.orders
FOR SYSTEM_VERSION AS OF 4827391056;
```

**FOR VERSION AS OF (alternative syntax):**

```sql
SELECT *
FROM prod.analytics.products
FOR VERSION AS OF 7293810245;
```

### EXPLAIN

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

## 2. DML

### INSERT INTO (VALUES)

```sql
INSERT INTO prod.analytics.events (event_id, event_type, user_id, event_time, amount, payload)
VALUES
  (9001, 'purchase', 'u-2001', TIMESTAMP '2024-06-15 14:30:00', 249.99, 'order confirmed'),
  (9002, 'refund', 'u-2002', TIMESTAMP '2024-06-15 15:00:00', 49.99, 'item returned'),
  (9003, 'purchase', 'u-2003', TIMESTAMP '2024-06-15 15:30:00', 899.00, 'bulk order');
```

### INSERT SELECT

```sql
INSERT INTO prod.analytics.orders (order_id, user_id, product_id, amount, order_date, status)
SELECT
  event_id,
  user_id,
  'prod-default',
  amount,
  CAST(event_time AS DATE),
  'pending'
FROM prod.analytics.events
WHERE event_type = 'purchase'
  AND event_time >= TIMESTAMP '2024-06-01 00:00:00';
```

### INSERT OVERWRITE

**Without PARTITION clause:**

```sql
INSERT OVERWRITE prod.analytics.orders (order_id, user_id, product_id, amount, order_date, status)
VALUES
  (8001, 'u-3001', 'prod-100', 150.00, DATE '2024-06-15', 'completed'),
  (8002, 'u-3002', 'prod-200', 320.00, DATE '2024-06-15', 'completed');
```

**With PARTITION clause:**

```sql
INSERT OVERWRITE prod.analytics.orders (order_id, user_id, product_id, amount, order_date, status)
PARTITION (status = 'archived')
VALUES
  (7001, 'u-4001', 'prod-50', 75.00, DATE '2024-01-10', 'archived'),
  (7002, 'u-4002', 'prod-60', 120.00, DATE '2024-01-11', 'archived');
```

### DELETE FROM

```sql
DELETE FROM prod.analytics.events
WHERE event_type = 'test'
  AND event_time < TIMESTAMP '2024-01-01 00:00:00';
```

### UPDATE

```sql
UPDATE prod.analytics.orders
SET status = 'cancelled', amount = 0.00
WHERE status = 'pending'
  AND order_date < DATE '2024-01-01';
```

### MERGE INTO

```sql
MERGE INTO prod.analytics.users AS target
USING (
  SELECT DISTINCT user_id, 'active' AS new_status
  FROM prod.analytics.orders
  WHERE order_date >= DATE '2024-06-01'
) AS source
ON target.user_id = source.user_id
WHEN MATCHED AND target.region IS NULL THEN
  UPDATE SET target.region = 'unknown'
WHEN NOT MATCHED THEN
  INSERT (user_id, name, email, region, created_at)
  VALUES (source.user_id, 'New User', 'pending@example.com', 'unknown', TIMESTAMP '2024-06-15 00:00:00')
WHEN MATCHED AND target.email LIKE '%@deleted.com' THEN
  DELETE;
```

---

## 3. Table Maintenance

### COMPACT TABLE

Merges small data files to improve read performance.

```sql
COMPACT prod.analytics.events;
```

### OPTIMIZE TABLE

Rewrites data files for optimal layout (sorting, z-ordering).

```sql
OPTIMIZE prod.analytics.orders;
```

### VACUUM TABLE

Removes expired snapshots and orphaned data files.

```sql
VACUUM prod.analytics.events;
```

---

## 4. Catalog DDL

### CREATE CATALOG

**Using connector with properties:**

```sql
CREATE CATALOG lakehouse
USING iceberg
WITH (
  'warehouse' = 's3://data-lake-prod/warehouse',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'https://catalog.example.com/api'
);
```

**With PROPERTIES:**

```sql
CREATE CATALOG staging WITH PROPERTIES (
  'warehouse' = 's3://data-lake-staging/warehouse',
  'type' = 'rest',
  'uri' = 'https://catalog-staging.example.com/api'
);
```

### DROP CATALOG

```sql
DROP CATALOG staging;
```

### ALTER CATALOG

```sql
ALTER CATALOG lakehouse SET PROPERTIES (
  'uri' = 'https://catalog-v2.example.com/api'
);
```

### SHOW CATALOGS

```sql
SHOW CATALOGS;
```

### DESCRIBE CATALOG

```sql
DESCRIBE CATALOG lakehouse;
```

---

## 5. Schema DDL

### CREATE SCHEMA

```sql
CREATE SCHEMA IF NOT EXISTS prod.analytics;

CREATE SCHEMA lakehouse.reporting;
```

### SHOW SCHEMAS

```sql
SHOW SCHEMAS;

SHOW SCHEMAS IN prod;
```

---

## 6. Table DDL

### CREATE TABLE

```sql
CREATE TABLE IF NOT EXISTS prod.analytics.events (
  event_id BIGINT NOT NULL,
  event_type VARCHAR,
  user_id VARCHAR,
  event_time TIMESTAMP,
  amount DECIMAL(10, 2),
  payload VARCHAR,
  embedding VECTOR(768, FLOAT),
  PRIMARY KEY (event_id) NOT ENFORCED
)
PARTITIONED BY (event_type)
COMMENT 'Raw analytics events from all sources'
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd'
);
```

### DROP TABLE

```sql
DROP TABLE IF EXISTS prod.analytics.events_staging;
```

### ALTER TABLE

**ADD COLUMN (with DEFAULT):**

```sql
ALTER TABLE prod.analytics.orders ADD COLUMN discount DECIMAL(10, 2) DEFAULT 0.00;
```

**DROP COLUMN:**

```sql
ALTER TABLE prod.analytics.orders DROP COLUMN discount;
```

**RENAME COLUMN:**

```sql
ALTER TABLE prod.analytics.users RENAME COLUMN name TO full_name;
```

**ALTER COLUMN TYPE:**

```sql
ALTER TABLE prod.analytics.orders ALTER COLUMN amount TYPE DECIMAL(12, 4);
```

**SET PARTITION SPEC (with function transforms):**

```sql
ALTER TABLE prod.analytics.events SET PARTITION SPEC (
  year(event_time),
  month(event_time),
  bucket(16, user_id),
  truncate(10, event_type)
);
```

### SHOW TABLES

```sql
SHOW TABLES;

SHOW TABLES IN prod.analytics;
```

### DESCRIBE TABLE

```sql
DESCRIBE TABLE prod.analytics.events;

DESCRIBE prod.analytics.orders;
```

---

## 7. Index DDL

### CREATE INDEX

```sql
CREATE INDEX IF NOT EXISTS idx_events_user
ON prod.analytics.events (user_id)
USING BLOOM_FILTER
WITH (
  'bloom.fpp' = '0.01',
  'bloom.num-items' = '1000000'
);
```

**With FORCE (rebuild even if exists):**

```sql
CREATE INDEX idx_orders_status
ON prod.analytics.orders (status)
USING BLOOM_FILTER
FORCE;
```

### DROP INDEX

```sql
DROP INDEX IF EXISTS idx_events_user ON prod.analytics.events;
```

### ALTER INDEX

```sql
ALTER INDEX idx_orders_status ON prod.analytics.orders REBUILD;
```

### SHOW INDEXES

```sql
SHOW INDEXES ON prod.analytics.events;
```

---

## 8. Clustering

### SET CLUSTERING ORDER

**With MODE INTERLEAVE:**

```sql
ALTER TABLE prod.analytics.events
SET CLUSTERING ORDER (user_id, event_type)
MODE INTERLEAVE;
```

**With MODE LINEAR:**

```sql
ALTER TABLE prod.analytics.orders
SET CLUSTERING ORDER (order_date, user_id)
MODE LINEAR;
```

### RECLUSTER TABLE

```sql
RECLUSTER TABLE prod.analytics.events;
```

**With WHERE clause (partial recluster):**

```sql
RECLUSTER TABLE prod.analytics.events
WHERE event_time >= TIMESTAMP '2024-06-01 00:00:00';
```

### SHOW CLUSTERING STATUS

```sql
SHOW CLUSTERING STATUS FOR TABLE prod.analytics.events;
```


---

## 9. Stream DDL

> **Status**: 🔮 Planned for future release

### CREATE STREAM

```sql
CREATE STREAM IF NOT EXISTS clickstream_ingest
FROM KAFKA (
  'bootstrap.servers' = 'kafka-broker-1:9092,kafka-broker-2:9092',
  'topic' = 'web.clickstream.events',
  'group.id' = 'pangrova-clickstream-consumer'
)
FORMAT 'json'
INTO prod.analytics.events
WITH (
  'checkpoint.interval' = '60000',
  'max.batch.size' = '10000'
);
```

### ALTER STREAM

**SUSPEND:**

```sql
ALTER STREAM clickstream_ingest SUSPEND;
```

**RESUME:**

```sql
ALTER STREAM clickstream_ingest RESUME;
```

**SET properties:**

```sql
ALTER STREAM clickstream_ingest SET (
  'max.batch.size' = '50000',
  'checkpoint.interval' = '30000'
);
```

### DROP STREAM

```sql
DROP STREAM IF EXISTS clickstream_ingest;
```

### SHOW STREAMS

```sql
SHOW STREAMS;
```

### DESCRIBE STREAM

```sql
DESCRIBE STREAM clickstream_ingest;
```

---

## 10. Dynamic Table

> **Status**: 🔮 Planned for future release

### CREATE DYNAMIC TABLE

**With SNAPSHOT refresh mode:**

```sql
CREATE DYNAMIC TABLE prod.analytics.daily_revenue_summary
TARGET_LAG = '15 minutes'
REFRESH_MODE = SNAPSHOT
WAREHOUSE = 'compute_wh_medium'
AS
SELECT
  CAST(order_date AS DATE) AS report_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_revenue,
  AVG(amount) AS avg_order_value
FROM prod.analytics.orders
WHERE status = 'completed'
GROUP BY CAST(order_date AS DATE);
```

**With CDC refresh mode:**

```sql
CREATE DYNAMIC TABLE prod.analytics.user_order_snapshot
TARGET_LAG = '5 minutes'
REFRESH_MODE = CDC
AS
SELECT
  u.user_id,
  u.name,
  u.region,
  COUNT(o.order_id) AS lifetime_orders,
  SUM(o.amount) AS lifetime_spend
FROM prod.analytics.users u
LEFT JOIN prod.analytics.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.name, u.region;
```

**With AUTO refresh mode:**

```sql
CREATE DYNAMIC TABLE prod.analytics.product_sales
TARGET_LAG = '10 minutes'
REFRESH_MODE = AUTO
AS
SELECT
  p.product_id,
  p.name,
  p.category,
  COUNT(o.order_id) AS units_sold,
  SUM(o.amount) AS revenue
FROM prod.analytics.products p
JOIN prod.analytics.orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name, p.category;
```

### ALTER DYNAMIC TABLE

**SUSPEND:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary SUSPEND;
```

**RESUME:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary RESUME;
```

**REFRESH (manual trigger):**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary REFRESH;
```

**SET TARGET_LAG:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary SET TARGET_LAG = '30 minutes';
```

**REDEFINE AS (change underlying query):**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary REDEFINE AS
SELECT
  CAST(order_date AS DATE) AS report_date,
  status,
  COUNT(*) AS order_count,
  SUM(amount) AS total_revenue
FROM prod.analytics.orders
GROUP BY CAST(order_date AS DATE), status;
```

### DROP DYNAMIC TABLE

```sql
DROP DYNAMIC TABLE prod.analytics.daily_revenue_summary;

-- With CASCADE to drop dependent objects
DROP DYNAMIC TABLE prod.analytics.user_order_snapshot CASCADE;
```

### SHOW DYNAMIC TABLES

```sql
SHOW DYNAMIC TABLES;
```

### DESCRIBE DYNAMIC TABLE

```sql
DESCRIBE DYNAMIC TABLE prod.analytics.daily_revenue_summary;
```

---

## 11. Stored Procedures

### CALL

```sql
-- Expire snapshots older than a timestamp
CALL prod.analytics.system.expire_snapshots('prod.analytics.events', TIMESTAMP '2024-01-01 00:00:00');

-- Rewrite data files for a table
CALL prod.analytics.system.rewrite_data_files('prod.analytics.orders');

-- Rollback to a previous snapshot
CALL prod.analytics.system.rollback_to_snapshot('prod.analytics.events', 4827391056);

-- No-argument procedure
CALL prod.analytics.system.collect_statistics();
```

---

## 12. Tenant Management

> **Status**: 🔮 Planned for future release

### GRANT TENANT

```sql
GRANT TENANT analytics_team TO engineering_role
WITH QUOTA (
  max_memory = '64GB',
  max_concurrency = 200
);
```

### REVOKE TENANT

```sql
REVOKE TENANT analytics_team FROM engineering_role;
```

---

## 13. Data Types

| Type | Description | Example |
|------|-------------|---------|
| `BOOLEAN` | True or false | `TRUE`, `FALSE` |
| `TINYINT` | 8-bit signed integer (-128 to 127) | `42` |
| `SMALLINT` | 16-bit signed integer (-32768 to 32767) | `1024` |
| `INT` / `INTEGER` | 32-bit signed integer | `100000` |
| `BIGINT` | 64-bit signed integer | `9223372036854775807` |
| `FLOAT` | 32-bit IEEE 754 floating point | `3.14` |
| `DOUBLE` | 64-bit IEEE 754 floating point | `2.718281828` |
| `DECIMAL(p, s)` / `NUMERIC(p, s)` | Fixed-point decimal with precision and scale | `DECIMAL(10, 2)` |
| `VARCHAR` / `VARCHAR(n)` / `STRING` | Variable-length character string | `'hello'` |
| `DATE` | Calendar date (year, month, day) | `DATE '2024-06-15'` |
| `TIMESTAMP` | Timestamp with microsecond precision | `TIMESTAMP '2024-06-15 10:30:00'` |
| `VECTOR(dim, element_type)` | Fixed-dimension vector for ML embeddings | `VECTOR(768, FLOAT)` |

### Type Casting Compatibility

| Source → Target | BIGINT | INT | VARCHAR | DOUBLE | DECIMAL | BOOLEAN | DATE | TIMESTAMP |
|-----------------|--------|-----|---------|--------|---------|---------|------|-----------|
| **BIGINT** | — | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **INT** | ✅ | — | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **VARCHAR** | ✅ | ✅ | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| **DOUBLE** | ✅ | ✅ | ✅ | — | ✅ | ❌ | ❌ | ❌ |
| **DECIMAL** | ✅ | ✅ | ✅ | ✅ | — | ❌ | ❌ | ❌ |
| **BOOLEAN** | ❌ | ❌ | ✅ | ❌ | ❌ | — | ❌ | ❌ |
| **DATE** | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | — | ✅ |
| **TIMESTAMP** | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | — |

---

## 14. Built-in Functions

### Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows | `SELECT COUNT(*) FROM prod.analytics.orders` |
| `COUNT(expr)` | Count non-null values | `SELECT COUNT(region) FROM prod.analytics.users` |
| `COUNT(DISTINCT expr)` | Count distinct non-null values | `SELECT COUNT(DISTINCT user_id) FROM prod.analytics.orders` |
| `SUM(expr)` | Sum of values | `SELECT SUM(amount) FROM prod.analytics.orders` |
| `AVG(expr)` | Average of values | `SELECT AVG(amount) FROM prod.analytics.orders` |
| `MIN(expr)` | Minimum value | `SELECT MIN(order_date) FROM prod.analytics.orders` |
| `MAX(expr)` | Maximum value | `SELECT MAX(amount) FROM prod.analytics.orders` |

### Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ROW_NUMBER()` | Sequential row number within partition | `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date)` |
| `RANK()` | Rank with gaps for ties | `RANK() OVER (ORDER BY amount DESC)` |
| `DENSE_RANK()` | Rank without gaps for ties | `DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC)` |
| `LAG(expr)` | Value from previous row | `LAG(amount) OVER (ORDER BY order_date)` |
| `LEAD(expr)` | Value from next row | `LEAD(amount) OVER (ORDER BY order_date)` |
| `NTILE(n)` | Divide rows into n buckets | `NTILE(4) OVER (ORDER BY amount)` |

### Scalar Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CAST(expr AS type)` | Convert expression to target type | `CAST(event_id AS VARCHAR)` |
| `EXTRACT(field FROM expr)` | Extract date/time component (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) | `EXTRACT(YEAR FROM event_time)` |
| `SUBSTRING(str FROM pos FOR len)` | Extract substring | `SUBSTRING(payload FROM 1 FOR 100)` |

### Date/Time Literals

| Literal | Description | Example |
|---------|-------------|---------|
| `DATE 'yyyy-MM-dd'` | Date literal | `DATE '2024-06-15'` |
| `TIMESTAMP 'yyyy-MM-dd HH:mm:ss'` | Timestamp literal | `TIMESTAMP '2024-06-15 10:30:00'` |
| `INTERVAL 'n' unit` | Interval literal (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) | `INTERVAL '30' DAY` |

### String Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `\|\|` | String concatenation | `name \|\| ' (' \|\| region \|\| ')'` |

---

## 15. View DDL

### CREATE VIEW

```sql
CREATE VIEW [IF NOT EXISTS] view_name AS
SELECT ...;
```

**Example:**

```sql
CREATE VIEW prod.analytics.active_users AS
SELECT user_id, name, email
FROM prod.analytics.users
WHERE created_at >= TIMESTAMP '2024-01-01 00:00:00';
```

### DROP VIEW

```sql
DROP VIEW [IF EXISTS] view_name;
```

**Example:**

```sql
DROP VIEW IF EXISTS prod.analytics.active_users;
```

### SHOW VIEWS

```sql
SHOW VIEWS [FROM | IN schema_name];
```

**Example:**

```sql
SHOW VIEWS FROM prod.analytics;
```

---

## 16. COPY INTO

Bulk load data from external files or URIs into a table.

### Syntax

```sql
COPY INTO table_name
FROM source
[FILE_FORMAT = format_name]
[PATTERN = 'glob_pattern']
[WITH (option = value [, ...])];
```

**Source** can be a URI string or a subquery.

**Examples:**

```sql
-- Load from S3 path
COPY INTO prod.analytics.events
FROM 's3://data-lake/events/2024/'
FILE_FORMAT = parquet
PATTERN = '*.parquet';

-- Load from subquery
COPY INTO prod.analytics.events
FROM (SELECT * FROM staging.raw_events WHERE event_date = DATE '2024-06-01');

-- With options
COPY INTO prod.analytics.logs
FROM 's3://bucket/logs/'
FILE_FORMAT = csv
WITH (header = true, delimiter = ',', null_value = 'NULL');
```

---

## 17. Storage Profile

Manage named storage configurations for external data access.

### CREATE STORAGE PROFILE

```sql
CREATE STORAGE PROFILE [IF NOT EXISTS] profile_name
WITH (property = value [, ...]);
```

**Example:**

```sql
CREATE STORAGE PROFILE s3_prod WITH (
    type = 's3',
    region = 'us-west-2',
    bucket = 'my-data-lake',
    access_key_secret = 'aws/prod-key'
);
```

### DROP STORAGE PROFILE

```sql
DROP STORAGE PROFILE [IF EXISTS] profile_name;
```

### SHOW STORAGE PROFILES

```sql
SHOW STORAGE PROFILES;
```

---

## 18. Branch & Tag (Data Versioning)

Pangrova supports Iceberg-style branch and tag management for data versioning and isolation.

### CREATE BRANCH

```sql
CREATE BRANCH branch_name ON table_name
    [FROM TAG tag_name]
    [FROM SNAPSHOT snapshot_id];
```

**Example:**

```sql
-- Create branch from current state
CREATE BRANCH feature_v2 ON prod.analytics.events;

-- Create branch from a specific tag
CREATE BRANCH hotfix ON prod.analytics.events FROM TAG release_v1;

-- Create branch from a specific snapshot
CREATE BRANCH rollback ON prod.analytics.events FROM SNAPSHOT 1234567890;
```

### DROP BRANCH

```sql
DROP BRANCH branch_name ON table_name;
```

### CREATE TAG

```sql
CREATE TAG tag_name ON table_name;
```

**Example:**

```sql
CREATE TAG release_v1 ON prod.analytics.events;
```

### DROP TAG

```sql
DROP TAG tag_name ON table_name;
```

### USE BRANCH

```sql
USE BRANCH branch_name ON table_name;
USE BRANCH MAIN ON table_name;  -- Switch back to main
```

### SHOW BRANCHES / SHOW TAGS

```sql
SHOW BRANCHES ON prod.analytics.events;
SHOW TAGS ON prod.analytics.events;
```

### Time Travel with Branch/Tag

```sql
-- Query a specific branch
SELECT * FROM prod.analytics.events VERSION AS OF BRANCH 'feature_v2';

-- Query a specific tag
SELECT * FROM prod.analytics.events VERSION AS OF TAG 'release_v1';

-- Query at a specific snapshot version
SELECT * FROM prod.analytics.events FOR SYSTEM_VERSION AS OF 1234567890;

-- Query at a specific timestamp
SELECT * FROM prod.analytics.events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 12:00:00';
```

---

## 19. Metadata Queries

### SHOW SNAPSHOTS

```sql
SHOW SNAPSHOTS FROM table_name;
```

Returns snapshot history including snapshot_id, timestamp, operation, and summary.

### SHOW FILES

```sql
SHOW FILES FROM table_name;
```

Returns data files in the current snapshot including path, size, record_count, and partition.

### SHOW MANIFESTS

```sql
SHOW MANIFESTS FROM table_name;
```

Returns manifest files in the current snapshot.

### SHOW PARTITIONS

```sql
SHOW PARTITIONS FROM table_name;
```

Returns partition information for the table.

**Example:**

```sql
-- View table history
SHOW SNAPSHOTS FROM prod.analytics.events;

-- Inspect data files
SHOW FILES FROM prod.analytics.orders;
```

---

## 20. Incremental Read

Read only the changes between two snapshots.

### Syntax

```sql
SELECT columns FROM table_name
CHANGES BETWEEN SNAPSHOT from_snapshot AND to_snapshot
[WHERE condition]
[ORDER BY columns]
[LIMIT n];
```

**Example:**

```sql
-- Read changes between two snapshots
SELECT event_id, event_type, amount
FROM prod.analytics.events
CHANGES BETWEEN SNAPSHOT 100 AND 200
WHERE event_type = 'purchase';
```

**Notes:**
- Returns rows added between the two snapshots
- Includes implicit `_change_type` column indicating INSERT operations
- Useful for CDC pipelines and incremental ETL

---

## 21. Session Management

### SET

```sql
SET property_name = value;
```

**Example:**

```sql
-- Set query timeout
SET pangrova.query.timeout_ms = 60000;

-- Set default catalog
SET pangrova.default_catalog = 'iceberg_catalog';
```

---

## 22. Advanced Query Features

### QUALIFY

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

### TABLESAMPLE

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

### PIVOT

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

### UNPIVOT

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

### Table Functions

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

### CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE [IF NOT EXISTS] table_name AS
SELECT ...;
```

**Example:**

```sql
CREATE TABLE prod.analytics.top_users AS
SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total_spent
FROM prod.analytics.orders
GROUP BY user_id
HAVING SUM(amount) > 10000;
```

### WITH RECURSIVE (Recursive CTE)

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

---

## Cross-References

| Topic | Guide |
|-------|-------|
| Data types reference | [Data Types](data-types.md) |
| AI/ML functions | [AI Functions](ai-functions.md) |
| Vector search | [Vector Search](vector-search.md) |
| Function reference | [Functions](functions/index.md) |
