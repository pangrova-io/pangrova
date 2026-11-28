---
title: "Versioning"
description: "Time travel, branches, tags, snapshot metadata, and incremental reads"
---

# Versioning

Data versioning statements: time travel queries, branch and tag management, snapshot inspection, and incremental reads.


**Example tables used throughout this guide:**

| Table | Columns |
|-------|---------|
| `prod.analytics.events` | `event_id BIGINT`, `event_type VARCHAR`, `user_id VARCHAR`, `event_time TIMESTAMP`, `amount DECIMAL(10,2)`, `payload VARCHAR` |
| `prod.analytics.users` | `user_id VARCHAR`, `name VARCHAR`, `email VARCHAR`, `region VARCHAR`, `created_at TIMESTAMP` |
| `prod.analytics.orders` | `order_id BIGINT`, `user_id VARCHAR`, `product_id VARCHAR`, `amount DECIMAL(10,2)`, `order_date DATE`, `status VARCHAR` |
| `prod.analytics.products` | `product_id VARCHAR`, `name VARCHAR`, `category VARCHAR`, `price DECIMAL(10,2)`, `description VARCHAR` |


## Time Travel

**FOR SYSTEM_TIME AS OF (currently a numeric snapshot identifier):**

Despite the clause name, the current resolver does not perform wall-clock timestamp
lookup. Supply an actual snapshot identifier from the table's snapshot metadata;
the numeric IDs below are illustrative.

```sql
SELECT event_id, event_type, amount
FROM prod.analytics.events
FOR SYSTEM_TIME AS OF 123456789
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

---

## Branch & Tag (Data Versioning)

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

The Coordinator checks the authenticated user's table SELECT privilege, tenant visibility and the selected branch head. Each subsequent query resolves that branch to an exact snapshot. An explicit time-travel clause overrides the session selection on that table. Iceberg supports named branch reads. Paimon rejects branch selection because its snapshots are scoped to a branch.

The JDBC connection and CLI client retain `branch_selection` from the successful terminal state of `USE BRANCH`. REST clients poll for the terminal state, then send the returned branch name in `session_properties` under `branch.routing.<table>` for subsequent requests. MAIN returns a null branch and removes that entry. Independent connections retain their own selections. The route applies to reads, including query sources within writes.


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

-- SYSTEM_TIME currently resolves a numeric snapshot identifier, not a timestamp.
SELECT * FROM prod.analytics.events FOR SYSTEM_TIME AS OF 123456789;
```

For Paimon tables, `VERSION AS OF TAG` resolves an existing Paimon tag to its
fixed snapshot before planning the scan. An unknown tag fails the query; it
does not read the current table state. Paimon branch references are not
supported by this connector.

---

## Metadata Queries

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

`SHOW PARTITIONS` is rewritten to the authorized `$partitions` system-table scan.
`SHOW SNAPSHOTS`, `SHOW FILES`, and `SHOW MANIFESTS` use the authorized
system-table scan path for supported connectors.

**Example:**

```sql
-- View table history
SHOW SNAPSHOTS FROM prod.analytics.events;

-- Inspect data files
SHOW FILES FROM prod.analytics.orders;
```

---

## Incremental Read

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
- Reads appended rows after the lower snapshot through the upper snapshot, using the upper snapshot's schema.
- Iceberg requires a retained append-only ancestry path. Direct Paimon queries require append commits on a table without a primary key.
- Equal endpoints return an empty result for the selected schema. Unsupported sources and unavailable endpoints fail.
- `SELECT *` includes `_change_type` with value `INSERT`. Explicit projections can select or filter on this column.
- The source must be a physical table without a user column named `_change_type` or an active session branch route.
