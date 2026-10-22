---
title: "DML"
description: "INSERT, UPDATE, DELETE, MERGE, and COPY INTO statements"
---

# DML

Data manipulation statements for loading and modifying lakehouse table data.


**Example tables used throughout this guide:**

| Table | Columns |
|-------|---------|
| `prod.analytics.events` | `event_id BIGINT`, `event_type VARCHAR`, `user_id VARCHAR`, `event_time TIMESTAMP`, `amount DECIMAL(10,2)`, `payload VARCHAR` |
| `prod.analytics.users` | `user_id VARCHAR`, `name VARCHAR`, `email VARCHAR`, `region VARCHAR`, `created_at TIMESTAMP` |
| `prod.analytics.orders` | `order_id BIGINT`, `user_id VARCHAR`, `product_id VARCHAR`, `amount DECIMAL(10,2)`, `order_date DATE`, `status VARCHAR` |
| `prod.analytics.products` | `product_id VARCHAR`, `name VARCHAR`, `category VARCHAR`, `price DECIMAL(10,2)`, `description VARCHAR` |


## DML

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

The Native copy-on-write path supports one MATCHED UPDATE or DELETE action,
including its optional action predicate. FALSE or UNKNOWN predicates preserve
the original target row; row policies and column masks apply to predicate reads,
not to unassigned physical survivor values.

An unconditional MATCHED UPDATE can be combined with one unconditional NOT
MATCHED INSERT. Ordered multiple actions, conditional INSERT, and a conditional
MATCHED action combined with another action are currently rejected with
`NATIVE_UNSUPPORTED_PLAN`. They require distinct match and action predicates;
their conditions must not be silently discarded or moved into ON to cause an
incorrect INSERT. Complete ordered-action lowering remains an open capability.

```sql
MERGE INTO prod.analytics.users AS target
USING (
  SELECT DISTINCT user_id, 'active' AS new_status
  FROM prod.analytics.orders
  WHERE order_date >= DATE '2024-06-01'
) AS source
ON target.user_id = source.user_id
WHEN MATCHED AND target.region IS NULL THEN
  UPDATE SET target.region = 'unknown';
```

---

## COPY INTO

`COPY INTO table FROM (SELECT ...)` appends the query result through the same
native write and catalog commit path as `INSERT INTO table SELECT ...`. This is
a Pangrova extension for table-to-table copies; each execution appends rows.

File/URI loading is not implemented. A URI source, or file-loading options on a
subquery source, returns an unsupported-feature error before any write. Do not
use `COPY INTO` for retryable file ingestion until file discovery, authorization,
format scanning, and committed file identity tracking are available. See
[Current Capability Status](../getting-started/capability-status.md).

### Syntax

```sql
COPY INTO table_name
FROM (SELECT ...);
```

**Source** must be a subquery. URI syntax is parsed but rejected.

**Example:**

```sql
COPY INTO prod.analytics.events
FROM (SELECT * FROM staging.raw_events WHERE event_date = DATE '2024-06-01');
```
