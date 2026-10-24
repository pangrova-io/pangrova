---
title: "Maintenance"
description: "COMPACT, OPTIMIZE, VACUUM, ANALYZE, clustering, and stored procedures"
---

# Maintenance

Maintenance syntax and catalog procedures have different execution paths.

## Current Execution Status

| Surface | Default behavior |
|---|---|
| `COMPACT` / `OPTIMIZE` / `VACUUM` | Registered and executed through `ControlledTableMaintenance`: ACTIVE LINKED/MANAGED records only, policy-checked, under a distributed lease |
| Automatic scan | Maintenance leader only; scans ACTIVE LINKED/MANAGED records, runs due policy operations, records `AUTOMATIC` history |
| `ANALYZE` | Collects connector statistics for ACTIVE LINKED/MANAGED tables and persists them against the stable table ID and observed version |
| Catalog-qualified `CALL` | Wired to the selected connector's procedure implementation |
| Clustering / `RECLUSTER` | Parser and task/executor components exist; no default production task registration found |

**A successful standalone maintenance response does not prove that maintenance ran.**
Every statement either executes a policy-authorized connector operation or fails
explicitly. OBSERVED tables are rejected before the connector is touched, LINKED
tables run only operations enabled by their policy, and OBSERVED/LINKED tables
cannot be written or dropped through Pangrova. Procedure availability and
semantics depend on the connector, authenticated context, and storage configuration.

See [Current Capability Status](../getting-started/capability-status.md).

## Automatic Maintenance

The maintenance leader runs a periodic scan over **controlled tables only**. The
scan asks the table control registry for ACTIVE LINKED/MANAGED records and
includes a table only when its policy enables at least one operation. OBSERVED
tables have no control record, so they are never scanned and never maintained.

For each due table the leader runs every policy-authorized operation that is due,
in this order:

1. `COMPACT` — compaction / small-file rewrite
2. `EXPIRE_SNAPSHOTS` — retention window from the policy
3. `REMOVE_ORPHAN_FILES` — orphan cutoff from the policy

Due-ness is measured from the last terminal run (success, failure, or skip) for
that operation, so a failed destructive operation is not retried on every scan.
MANAGED tables inherit the ownership defaults (all three operations); LINKED
tables run only what ADOPT explicitly authorized.

Automatic runs use the same `ControlledTableMaintenance` path as a manual
statement: distributed lease keyed by `table_id`, identity fence, double version
observation for LINKED tables, and a durable `table_control_maintenance_run`
entry. That entry carries `mode = AUTOMATIC` and actor `system:maintenance`.
When an external writer is detected between observations the run is recorded as
`SKIPPED` and re-evaluated on the next cycle; other failures propagate to the
scheduler and are logged without being masked as success.

`expire_snapshots` arguments are format-specific. Iceberg receives the retain
count plus a time horizon computed from `snapshot_retention`; Paimon receives the
retain window it validates. A format with no defined argument mapping fails
instead of sending one connector's semantics to another.

The scan starts when maintenance leadership is acquired and stops when
leadership is lost. A coordinator that is not the maintenance leader never runs
automatic maintenance.


## Table Maintenance

### COMPACT TABLE

Runs compaction through the connector after ownership, policy, identity, and
version checks. For LINKED tables the version is observed twice; an external
writer that commits in between aborts the run.

```sql
COMPACT prod.analytics.events;
```

### OPTIMIZE TABLE

Registered as `MaintenanceOperation.OPTIMIZE` under the same ownership, policy,
lease, and version checks as `COMPACT`. Sorting and z-ordering are not established
by this handler.

```sql
OPTIMIZE prod.analytics.orders;
```

### VACUUM TABLE

Runs orphan-file cleanup using the policy's orphan-file age. It does not expire
snapshots; use an explicitly authorized connector procedure for snapshot expiration.

```sql
VACUUM prod.analytics.events;
```

### ANALYZE TABLE

Collects connector statistics and stores them against the controlled table's
stable ID and the version token observed during collection. The planner uses the
stored row only while the control record still carries that token; after a
Pangrova or external commit, live connector statistics are used instead.

`ANALYZE` requires an ACTIVE LINKED or MANAGED control record. OBSERVED tables are
rejected: without a stable identity, collected statistics could not be tied to the
table they describe.

```sql
ANALYZE TABLE prod.analytics.events;
ANALYZE TABLE prod.analytics.events COMPUTE STATISTICS;
```

The statement returns one summary row: `table`, `stats_version`, `version_token`,
`row_count`, `data_size`, and `source`. If the connector cannot report a version
token, or reports no usable statistics, the statement fails instead of storing a
value that would be immediately stale or fabricated as zero.

---

## Clustering

**Status: syntax and partial implementation only.** The examples below are not
executable support claims for the default Coordinator. Task registration and a
qualified maintenance path are required before using this lifecycle.

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
| `expire_snapshots` | `table_name [, retain_last] [, older_than_millis] [, dry_run]` | Expire old snapshots; `retain_last` defaults to 10 |
| `remove_orphan_files` | `table_name [, older_than_millis] [, dry_run]` | Remove unreferenced files; the cutoff must be at least 24 hours old |
| `rewrite_manifests` | `table_name` | Compact small manifests |
| `rollback_to_snapshot` | `table_name, snapshot_id` | Rollback to a snapshot |
| `set_current_snapshot` | `table_name, snapshot_id` | Fast-forward or set a branch to a snapshot |
| `cherrypick_snapshot` | `table_name, snapshot_id` | Apply a staged snapshot as a new commit |

**Paimon procedures:**

| Procedure | Arguments | Description |
|-----------|-----------|-------------|
| `compact` | `table_name` | Compact table files |
| `expire_snapshots` | `table_name [, retain_max] [, retain_min] [, dry_run]` | Expire old snapshots; defaults keep 30 snapshots with a floor of 10 |
| `create_tag` / `delete_tag` | `table_name, tag_name, ...` | Manage snapshot tags |
| `rollback_to` | `table_name, tag_name` | Rollback to a tag |
| `create_branch` / `delete_branch` | `table_name, branch_name` | Manage branches |
| `remove_orphan_files` | `table_name [, older_than_ms] [, dry_run]` | Remove unreferenced files; the cutoff must be at least 24 hours old |

Destructive procedures are conservative by default and accept `dry_run = true`
to report what would be removed without committing anything. Procedures that
are not implemented for a connector fail with an unsupported-operation error
instead of reporting a no-op success; `merge_branch`, `repair`,
`rewrite_file_index`, `reset_consumer`, and `rescale_bucket` are currently
unsupported on Paimon.
