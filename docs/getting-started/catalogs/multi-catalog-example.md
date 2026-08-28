---
title: "Multi-Catalog Example"
description: "End-to-end multi-catalog analytics walkthrough"
---

# Multi-Catalog Example

A complete example registering Iceberg and Paimon catalogs and querying across them.


## End-to-End Example: Multi-Catalog Analytics

Set up a production environment with both Iceberg and Paimon catalogs querying across data sources.

**Step 1: Register catalogs**

```sql
-- Iceberg catalog for historical analytics (S3 + Hive Metastore)
CREATE CATALOG prod_iceberg
USING iceberg
WITH (
  'type' = 'hive',
  'warehouse' = 's3://data-lake-prod/iceberg',
  'uri' = 'thrift://hive-metastore:9083'
);

-- Paimon catalog for real-time streaming data
CREATE CATALOG prod_paimon
USING paimon
WITH (
  'warehouse' = 's3://data-lake-prod/paimon'
);
```

**Step 2: Create schemas and tables**

```sql
CREATE SCHEMA prod_iceberg.analytics;
CREATE SCHEMA prod_paimon.realtime;

CREATE TABLE prod_iceberg.analytics.dim_customers (
  customer_id VARCHAR,
  name VARCHAR,
  email VARCHAR,
  region VARCHAR,
  tier VARCHAR,
  created_at TIMESTAMP
) PARTITIONED BY (region);

CREATE TABLE prod_paimon.realtime.live_orders (
  order_id BIGINT,
  customer_id VARCHAR,
  amount DECIMAL(10, 2),
  order_time TIMESTAMP,
  status VARCHAR
) PARTITIONED BY (status);
```

**Step 3: Cross-catalog join**

```sql
SELECT
  c.name,
  c.tier,
  COUNT(o.order_id) AS recent_orders,
  SUM(o.amount) AS recent_spend
FROM prod_paimon.realtime.live_orders o
JOIN prod_iceberg.analytics.dim_customers c
  ON o.customer_id = c.customer_id
WHERE o.order_time >= TIMESTAMP '2024-06-01 00:00:00'
GROUP BY c.name, c.tier
ORDER BY recent_spend DESC
LIMIT 20;
```

**Step 4: Table maintenance**

```sql
-- Compact small files in the Iceberg table
COMPACT prod_iceberg.analytics.dim_customers;

-- Clean up old snapshots
VACUUM prod_iceberg.analytics.dim_customers;

-- Check catalog status
SHOW CATALOGS;
DESCRIBE CATALOG prod_iceberg;
DESCRIBE CATALOG prod_paimon;
```
