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

The default literal must be castable to the declared column type. An
incompatible literal is rejected before the schema change is committed.
Iceberg stores the default in schema metadata; automatic read-time and
write-time default injection is not yet implemented.

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

**ADOPT (take ownership of an external table):**

```sql
ALTER TABLE prod.analytics.external_orders ADOPT;
```

ADOPT records the table as `LINKED`: it becomes governable and eligible for
restricted maintenance while remaining read-only for DML. The connector must
report a stable format identity (for example an Iceberg table UUID); ADOPT by
name only is rejected, as is a table that is already `MANAGED` or whose format
identity changed after a drop/recreate. Re-adopting an unchanged table is
idempotent. ADOPT does not modify or move the physical table.

A LINKED table starts with **no** maintenance operations enabled. Authorize
restricted maintenance at adoption time:

```sql
ALTER TABLE prod.analytics.external_orders ADOPT WITH (
  maintenance_operations = 'COMPACT,REMOVE_ORPHAN_FILES',
  retain_last = '10',
  orphan_file_age_hours = '72',
  snapshot_retention_hours = '168'
);
```

Supported properties are `maintenance_operations`, `retain_last`,
`orphan_file_age_hours`, `snapshot_retention_hours`, and
`active_writer_guard`. Unknown properties or operation names are rejected
before any control record is written. The active-writer guard cannot be
disabled for LINKED tables: maintenance aborts if an external writer commits
between the two version observations. ADOPT, UNLINK, every maintenance run,
and every external commit observed on a controlled table are written to the
durable audit trail with the table's stable `table_id`.

**UNLINK (release an adopted table):**

```sql
ALTER TABLE prod.analytics.external_orders UNLINK;
```

UNLINK removes the `LINKED` control record and its governance bindings; the
physical table and its data are untouched, and the table returns to `OBSERVED`
(read-only for Pangrova). `MANAGED` tables must be removed with `DROP TABLE`
instead. Both statements require `pangrova.table-control.enabled`.

**SET / UNSET COLUMN TAG:**

```sql
ALTER TABLE prod.analytics.customers ALTER COLUMN email SET TAG pii_type = 'PII_EMAIL';
ALTER TABLE prod.analytics.customers ALTER COLUMN email UNSET TAG pii_type;
```

The tag is persisted in the metadata tag store and bound to the table's stable
`table_id`. `TagBasedSecurityPolicy` reacts to the emitted tag event and upserts
(or removes) the derived column mask in the persistent mask store that query
execution reads, so the mask applies to subsequent statements.

The table must be `LINKED` or `MANAGED`; tagging an `OBSERVED` table fails and
requires `ADOPT` first. The column must exist. Tag changes are replayed on
coordinator restart, and `UNLINK`/`DROP TABLE` clears the table's tags, bindings,
and derived masks. Table-level `SET TAG` and `CREATE TAG` are not implemented.

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

### Semantic Views

A Semantic View definition is immutable and explicitly versioned. It is stored as
governed metadata; the Coordinator does not scan or combine source-table data while
executing these statements. Every `logicalTables[].source` value must use the
fully-qualified `catalog.schema.table` form.

```sql
CREATE SEMANTIC VIEW [IF NOT EXISTS] [catalog.][schema.]view_name
VERSION positive_integer
AS '<definition-json>';

ALTER SEMANTIC VIEW [catalog.][schema.]view_name
PUBLISH VERSION positive_integer
[EXPECTING VERSION current_active_version];

SHOW SEMANTIC VIEWS [FROM | IN [catalog.]schema];
```

The first publication omits `EXPECTING VERSION` and succeeds only when no active
version exists. Later publications must name the current active version; a stale
fence fails without changing the active pointer. `IF NOT EXISTS` is idempotent only
when the requested version already contains the same canonical definition.
`SHOW SEMANTIC VIEWS` is control-plane bounded and fails when a schema contains
more than 10,000 stored versions; use narrower schemas rather than treating it as
a data export interface.

---

## Storage Profile

Manage named storage configurations for external data access.
Creating or dropping a profile requires `MANAGE_STORAGE_PROFILE` on that
storage profile name (or an explicit `*` storage-profile grant). Table and
catalog `ALL` grants do not authorize these account-scoped changes. An
authenticated identity and a configured access-control provider are required.
`SHOW STORAGE PROFILES` returns only profiles covered by `VIEW_STORAGE_PROFILE`
or `MANAGE_STORAGE_PROFILE` on that profile name; it does not enumerate others.
Reading source object bytes requires the separate `READ_STORAGE_PROFILE` privilege.
Definitions persist in the metadata PostgreSQL authority across coordinator restarts.
`CREATE` is atomic across coordinators and never replaces an existing profile.
After `DROP` and re-creation, the new definition has a distinct incarnation;
historical revisions support exact recovery checks and never authorize a new read
after the current profile has been dropped or replaced.
Exact-name `VIEW_STORAGE_PROFILE`, `READ_STORAGE_PROFILE`, `ALL`, and `OWNER`
grants require an existing profile. `DROP` revokes them in the same metadata transaction;
`MANAGE_STORAGE_PROFILE` is a namespace grant that may be assigned before CREATE
and survives DROP. An account-wide `*` grant remains in force for future profiles.

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
    'path-style-access' = 'false'
);
```

Supported properties are `type`, `bucket`, `prefix`, `endpoint`, `region`,
`path-style-access`, and `project` for GCS. SQL profile definitions do not accept or persist credential
values. Source reading remains unavailable until the complete COPY authorization,
staging, and commit path is enabled.

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
