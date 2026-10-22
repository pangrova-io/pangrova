---
title: "Streaming"
description: "Stream ingestion and dynamic table SQL"
---

# Streaming

Stream syntax and dynamic-table syntax have different maturity boundaries.

## Current Execution Status

| Surface | Current wiring |
|---|---|
| Stream DDL | Registered when the streaming bootstrap supplies its executor |
| CDC ingestion | Bootstrap creates a Coordinator-local connector page-sink writer; distributed Native ingestion remains incomplete |
| Continuous-query refresh | The default scheduler callback logs a tick; it does not execute a refresh |
| Dynamic-table DDL and lifecycle | Create/alter/drop/show/describe handlers and the durable publication store are wired when metadata publication is configured; the planner supplies a rewriter for published definitions. Continuous-query materialization remains unsupported |

Catalog publication and source offsets are not committed atomically. Do not infer
exactly-once or effectively-once behavior, complete refresh, or failure recovery from
these examples. See [Current Capability Status](../getting-started/capability-status.md)
and [Streaming Architecture](../architecture/streaming.md) for known gaps.


## Stream DDL

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

## Dynamic Table

**Status: materialization handlers are registered when durable metadata publication
is configured; continuous-query materialization is not implemented.** The following
SQL illustrates recognized syntax, not a qualified automatic-refresh service.

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
