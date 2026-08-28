---
title: "DDL"
description: "CREATE / ALTER / DROP statements for catalogs, schemas, tables, views, and storage profiles"
---

# DDL

Data definition statements for managing catalogs, schemas, tables, views, and storage profiles.


## Catalog DDL

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

## Schema DDL

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

## Table DDL

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

## View DDL

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

## Storage Profile

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

## CREATE TABLE AS SELECT (CTAS)

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
