---
title: "Maintenance"
description: "COMPACT, OPTIMIZE, VACUUM, clustering, and stored procedures"
---

# Maintenance

Table maintenance statements that keep lakehouse tables fast and compact.


## Table Maintenance

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

## Clustering

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

## Stored Procedures

### CALL

Execute catalog maintenance procedures with
`CALL catalog.procedure_name(arg0, arg1, ...)`. Arguments are positional; the
first part of the name must be a registered catalog.

```sql
-- Compact data files (Iceberg)
CALL prod_iceberg.compact('analytics.events');

-- Expire snapshots: keep the last 5 and drop snapshots older than the cutoff (epoch ms)
CALL prod_iceberg.expire_snapshots('analytics.events', 5, 1704067200000);

-- Rollback to a previous snapshot
CALL prod_iceberg.rollback_to_snapshot('analytics.events', 4827391056);

-- Warm the table cache (engine-level, available for any catalog)
CALL prod_iceberg.warmup_table('analytics.events');
```

**Iceberg procedures:**

| Procedure | Arguments | Description |
|-----------|-----------|-------------|
| `compact` | `table_name` | Rewrite data files and apply deletes |
| `expire_snapshots` | `table_name [, retain_last] [, older_than_millis]` | Expire old snapshots |
| `remove_orphan_files` | `table_name [, older_than_millis]` | Remove unreferenced files from the table location |
| `rewrite_manifests` | `table_name` | Compact small manifests |
| `rollback_to_snapshot` | `table_name, snapshot_id` | Rollback to a snapshot |
| `set_current_snapshot` | `table_name, snapshot_id` | Fast-forward or set a branch to a snapshot |
| `cherrypick_snapshot` | `table_name, snapshot_id` | Apply a staged snapshot as a new commit |

**Paimon procedures:**

| Procedure | Description |
|-----------|-------------|
| `compact` | Compact table files |
| `expire_snapshots` | Expire old snapshots |
| `create_tag` / `delete_tag` | Manage snapshot tags |
| `rollback_to` | Rollback to a snapshot or tag |
| `create_branch` / `delete_branch` / `merge_branch` | Manage branches |
| `repair` | Repair table metadata |
| `rewrite_file_index` | Rewrite file indexes |
| `remove_orphan_files` | Remove unreferenced files |
| `reset_consumer` | Reset a consumer ID |
| `rescale_bucket` | Rescale bucket count |
