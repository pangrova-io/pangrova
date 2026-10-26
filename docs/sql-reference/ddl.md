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

Control records also bind the catalog incarnation and revision. Recreating a catalog
under the same name or changing its deployment revision invalidates the old control
authority, even when it points to the same physical table UUID. Reads using a
controlled cache entry, DML/DDL admission, governance and maintenance must resolve
the confirmed deployment before proceeding. ADOPT rejects an existing record from
another deployment; explicitly UNLINK a LINKED record before adopting it under the
new deployment. A new CREATE registration receives a new table ID.

Older records missing both catalog-binding fields stay blocked unless an operator
explicitly legacy-write-allowlists the table and the observer confirms its previously
recorded physical UUID and location. Existing deployment bindings are never silently
updated through this migration path.

When `pangrova.table-control.external-readonly` is enabled, destructive
`DROP TABLE` and `ALTER TABLE` operations check the current physical identity
against the MANAGED control record. A changed identity quarantines the record;
missing identity evidence rejects the operation without treating the table as
replaced. If identity lookup fails after physical CREATE, the statement fails
and records `ORPHANED_MANAGED` for reconciliation. STALE and ORPHANED_MANAGED
records need physical identity evidence matching their original registered UUID
before returning to ACTIVE. If the original identity is absent, neither a write
nor the background observer may infer ownership from the current name. Only an
explicit legacy write allowlist authorizes initial identity binding. Delayed
observations cannot overwrite a newer control revision or mark a replacement
table stale. DROP completion retires only the captured table ID and revision.
During RENAME, the durable `RENAMING` state blocks reads, writes and maintenance and
prevents periodic observations from interpreting the intermediate catalog state.
A confirmed completion refreshes the physical identity and version while keeping
the same table ID. A failed RENAME rolls ownership back only while that same registration is current
and the source still has its original physical identity. An ambiguous outcome
or a same-name replacement requires reconciliation instead of assigning ownership
by name.

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

Tags have one durable authority: `COLUMN_TAG` governance bindings keyed by stable
`table_id`, column and tag key. Query and deferred-publication policy readers derive
PII masks directly from those bindings; asynchronous events are cache/audit hints.
A missing or failed event projection cannot turn a committed classification into a raw read.

The table must be ACTIVE and `LINKED` or `MANAGED`; tagging an `OBSERVED` table
requires `ADOPT` first. The column must exist. SET and UNSET resolve its canonical
stored spelling: an exact match wins; a case-insensitive match must be unique.
The Metadata API applies the same checks as SQL, including fresh table ownership and
stable schema evidence during resolution. `pii_type` requires a declared VARCHAR
column and accepts exactly `PII_EMAIL`, `PII_PHONE`, `PII_SSN`, or `PII_NAME`.
NULL stays NULL. Email becomes `***@***`, names become `***`, and phone/SSN values
longer than four characters become `***` plus their final four characters; shorter
values are fully redacted. Classification masks apply to all roles and take precedence
above authored mask exemptions. Change the tag through `SET/UNSET TAG`.

Rename retains the stable binding. Both names are reserved and inaccessible while
physical rename has an uncertain outcome; an exception does not prove rollback.
Drop/recreate receives a new table identity, and delayed cleanup/events from the old
table cannot remove the replacement's policy. `UNLINK`/`DROP TABLE` clear the captured
table's bindings. Table-level `SET TAG` and `CREATE TAG` are not implemented.

Upgrade migrations accept historical tags only when each legacy value has a unique,
matching live stable binding. Unproven or ambiguous ownership blocks migration and
preserves the original rows. Authored masks without proven table identity fail closed
until explicitly rebound. Pending legacy renames require reconciliation before upgrade.

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

## Volume and Stage DDL

Volumes and stages name non-table file namespaces. Both surfaces persist in the
same metadata authority, but they keep separate namespaces, DDL, and privileges:
a volume and a stage may share a name in one schema without colliding.

This milestone registers and persists the definitions, and the file data plane is
available through the [file access API](../getting-started/catalogs/volumes-stages-files.md):
single-request upload up to 5 GiB, ranged reads with version pinning, recursive
listing, version-checked delete, and multipart upload. `LIST`, `REMOVE`, and manual
`ALTER STAGE ... REFRESH` are implemented. Directory query projections, SQL
`PUT`/`GET`, and `COPY INTO`/`read_files` ingestion are not implemented yet.

| Operation | Volume | Stage |
|---|---|---|
| Create managed/internal | `CREATE_VOLUME` on the schema | `CREATE_STAGE` on the schema |
| Create external | `CREATE_EXTERNAL_VOLUME` on the schema (plus location and storage profile) | `CREATE_STAGE` on the schema (plus URL and storage profile) |
| Metadata read | `READ_VOLUME` on the volume | `USAGE_STAGE` on the stage |
| Alter, rename, drop | `MANAGE_VOLUME` on the volume | `OWNER` on the stage |

`SHOW` returns only objects covered by the read privilege; `DESCRIBE` requires
the same exact-object read privilege. Creating an external space additionally
requires a dedicated creation privilege so ordinary users cannot mount arbitrary
buckets. Schema- and catalog-scoped grants cascade to volumes and stages; table
grants and grants on the other space kind never authorize a volume or stage.
Locations must be absolute URIs without whitespace, backslashes, or
`..` segments, and a physical prefix cannot be claimed by two definitions.

Dropping a space revokes exact-name grants in the same transaction; renaming
moves them with the object. Schema-scoped creation grants and `*` wildcards are
namespace grants and survive drop.

### CREATE VOLUME

```sql
CREATE [MANAGED | EXTERNAL] VOLUME [IF NOT EXISTS] [catalog.]schema.name
  [LOCATION 's3://bucket/prefix']
  [STORAGE_PROFILE 'profile']
  [COMMENT 'text'];
```

A managed volume must not declare a location. An external volume requires both
`LOCATION` and `STORAGE_PROFILE`.

### ALTER VOLUME

```sql
ALTER VOLUME name SET STORAGE_PROFILE 'profile';
ALTER VOLUME name SET COMMENT 'text';
ALTER VOLUME name SET OWNER TO user;
ALTER VOLUME name RENAME TO new_name;
```

`RENAME` cannot move a volume between schemas. Managed volumes retain their
persistent physical root when renamed. Recreating a dropped or renamed space's
former name allocates an independent root and does not expose its old files.

### DROP VOLUME

```sql
DROP VOLUME [IF EXISTS] name;
```

### SHOW / DESCRIBE VOLUMES

```sql
SHOW VOLUMES [FROM catalog.schema];
DESCRIBE VOLUME name;
```

### CREATE STAGE

```sql
CREATE [EXTERNAL] STAGE [IF NOT EXISTS] [catalog.]schema.name
  [URL = 's3://bucket/prefix']
  [STORAGE_PROFILE = 'profile']
  [DIRECTORY = (ENABLE = TRUE | FALSE)]
  [COMMENT = 'text'];
```

An internal stage must not declare a URL. An external stage requires `URL` and
`STORAGE_PROFILE`. Only stages accept a directory projection.

### ALTER STAGE

```sql
ALTER STAGE name SET URL = 's3://bucket/prefix';
ALTER STAGE name SET DIRECTORY = (ENABLE = TRUE | FALSE);
ALTER STAGE name SET OWNER TO user;
ALTER STAGE name RENAME TO new_name;
ALTER STAGE name REFRESH [SUBPATH = 'prefix/'];
```

Renaming an internal stage preserves its physical root and files. A new stage
using the former name receives an independent root, including after DROP.

`REFRESH` requires an enabled directory, ownership, `USAGE_STAGE`, `READ_STAGE`,
and a configured file plane. Workers list real object metadata; the coordinator
persists pages privately and atomically publishes only a complete scan. A failed
or superseded scan leaves the published index intact. `SUBPATH` replaces only the
named directory and its descendants; adjacent prefixes are preserved. This does
not yet enable the Native `DIRECTORY(@stage)` query projection or automatic
updates when files are uploaded. See the [refresh contract](../getting-started/catalogs/volumes-stages-files.md#directory-refresh).

### DROP STAGE

```sql
DROP STAGE [IF EXISTS] name;
```

### SHOW / DESCRIBE STAGES

```sql
SHOW STAGES [IN catalog.schema];
DESCRIBE STAGE name;
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
