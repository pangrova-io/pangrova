---
title: "Iceberg"
description: "Apache Iceberg connector configuration and feature support"
---

# Iceberg

Configure the Iceberg connector and review supported table features.


## Iceberg Connector

Apache Iceberg provides ACID transactions, schema evolution, and time travel on data lakes.

### Catalog Configuration

```yaml
# config/catalog/my-iceberg.yml
connector: iceberg

properties:
  type: hadoop          # hadoop, hive, rest, jdbc
  warehouse: "s3://my-bucket/iceberg-warehouse"
  # uri: "thrift://hive-metastore:9083"  # Required when type=hive
```

### SQL Registration

```sql
CREATE CATALOG my_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 's3://my-bucket/iceberg-warehouse'
);
```

### Catalog Types

| Type | Description | Required Properties |
|------|-------------|-------------------|
| `hadoop` | File-system based catalog (default) | `warehouse` |
| `hive` | Hive Metastore backed catalog | `warehouse`, `uri` |
| `rest` | REST catalog (e.g., Polaris, Unity) | `uri` |
| `jdbc` | JDBC backed catalog | `uri`, `jdbc.user`, `jdbc.password` |

### Supported Iceberg Features

| Feature | Status | Notes |
|---------|--------|-------|
| Schema evolution | Supported | ADD/DROP/RENAME COLUMN |
| Iceberg v3 `unknown` | Supported for optional top-level columns | Reads return NULL, writes omit the physical Parquet column, and ALTER COLUMN TYPE can introduce a concrete type. Non-NULL writes are rejected. |
| Partition evolution | Supported | SET PARTITION SPEC |
| Time travel | Supported | FOR SYSTEM_TIME AS OF / FOR SYSTEM_VERSION AS OF |
| MERGE INTO | Supported | Row-level deletes via position delete files |
| INSERT OVERWRITE | Supported | Dynamic and static overwrite |
| COMPACT / OPTIMIZE | Supported | Small file compaction |
| VACUUM | Supported | Snapshot expiration + orphan cleanup |
| Hidden partitioning | Supported | year/month/day/hour/bucket/truncate transforms |
| Metadata tables | Partially supported | `$snapshots`, `$history`, `$refs`, `$manifests`, and supported projections of `$files`/`$entries`; `$partitions` remains pending. |


### Snapshot metadata

`SELECT * FROM catalog.schema.table$snapshots` and
`SHOW SNAPSHOTS FROM catalog.schema.table` expose retained Iceberg snapshots.
Expired snapshots are absent; an empty table returns no rows.

| Column | Type | Meaning |
|--------|------|---------|
| `committed_at` | `TIMESTAMP WITH TIME ZONE` | Commit instant, with microsecond precision |
| `snapshot_id` | `BIGINT` | Snapshot identifier |
| `parent_id` | `BIGINT` | Parent snapshot, or NULL |
| `operation` | `VARCHAR` | Commit operation |
| `manifest_list` | `VARCHAR` | Manifest-list location, or NULL for legacy embedded manifests |
| `summary` | `MAP<VARCHAR, VARCHAR>` | Snapshot summary, excluding the separate `operation` field |

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM my_iceberg.analytics.orders$snapshots
ORDER BY committed_at DESC, snapshot_id DESC;
```

A missing or empty JSON summary produces NULL. A summary containing only an
operation produces an empty map. Snapshot timestamps are read as exact instants,
without floating-point conversion.
