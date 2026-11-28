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
| Catalog-qualified `CALL` | Enforces ownership, maintenance policy, identity and a distributed lease before connector execution |
| Clustering / `RECLUSTER` | Bounded Iceberg v2 rewrite through the controlled maintenance path |

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

Maintenance requires matching physical identity evidence before and after the
connector operation. A same-name replacement quarantines the old control record
even if its reported version is unchanged. Rejection before the connector runs
is `SKIPPED`; a failed identity check after execution begins is `FAILED`.

`expire_snapshots` arguments are format-specific. Iceberg receives the retain
count plus a time horizon computed from `snapshot_retention`; Paimon receives the
retain window it validates. A format with no defined argument mapping fails
instead of sending one connector's semantics to another.

The scan starts when maintenance leadership is acquired and stops when
leadership is lost. A coordinator that is not the maintenance leader never runs
automatic maintenance.


## Table Maintenance

Iceberg compaction validates its rewrite against the snapshot used for planning.
A concurrent delete that affects a file selected for rewrite invalidates the attempt.

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

`ANALYZE` checks physical identity and captures the control revision before
collection, then verifies both again before publishing. A concurrent control
change or same-name replacement rejects the collected result; it cannot roll
the registry back to an older version. Missing version evidence before collection
also rejects the request.

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

Clustering requires `ALTER` authorization on the target table. Mutating
statements require an ACTIVE MANAGED table with `RECLUSTER` allowed by its
maintenance policy. Pangrova supports the bounded Iceberg connector rewrite
for catalogs with verified metadata compare-and-swap, including JDBC and
local Hadoop catalogs.

A rewrite operates on the current Iceberg v2 snapshot and sorts each
partition separately. It accepts up to 1,024 Parquet files, 100,000 rows
and 64 MiB of file bytes. Input files must use the current partition spec
and have no attached row deletes.

### SET CLUSTERING ORDER

The table metadata records the column field IDs and clustering mode.
`LINEAR` uses ascending order with nulls last. `INTERLEAVE` is the default
and bit-interleaves two to eight top-level integer, floating-point, boolean
or string columns. String keys contribute their first eight UTF-8 bytes.

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

The SQL call waits for an authenticated Worker to stage replacement files and
for the Coordinator to publish the result under the table maintenance lease. The
Coordinator and Workers must share the configured internal authentication secret.
The Worker uses the exact admitted catalog and table metadata revisions. The
catalog and data paths must be accessible from the Coordinator and Workers.
If a Worker is unavailable before returning its staging receipt, another
eligible Worker can stage the same revision under a fresh attempt in the same
durable run. The Coordinator publishes only the winning files in one snapshot.
Files from interrupted attempts remain eligible for ordinary Iceberg orphan cleanup.
The response includes the durable task ID and snapshot identifier. An empty
table records a successful run without publishing a snapshot.

```sql
RECLUSTER TABLE prod.analytics.events;
```

**Planned partition filtering:**

The task component rejects a `WHERE` predicate until it can bind and publish
a constrained connector rewrite.

```sql
RECLUSTER TABLE prod.analytics.events
WHERE event_time >= TIMESTAMP '2024-06-01 00:00:00';
```

### SHOW CLUSTERING STATUS

The caller needs `ALTER` on the table because this statement can reconcile an
interrupted rewrite.

The result reports the current order and the number of current files
covered by it. The latest rewrite's durable task ID and task state appear
in `task_id` and `task_state`. When a prior Coordinator left a running
record, a later request can reconcile the exact catalog snapshot marker.
If the catalog still points to the original metadata revision, recovery
can fence that revision and record a failed unpublished attempt. An
unresolved catalog outcome keeps the run pending and blocks another
clustering mutation.

```sql
SHOW CLUSTERING STATUS FOR TABLE prod.analytics.events;
```

---

## Stored Procedures

### CALL

Execute catalog maintenance procedures with
`CALL catalog.procedure_name(arg0, arg1, ...)`. Arguments are positional; the
first part of the name must be a registered catalog. Mutating procedures require
ALTER permission on the actual table and use the same control-plane checks as
COMPACT/VACUUM/OPTIMIZE. OBSERVED tables are rejected; LINKED tables require an
explicit maintenance policy. Retention arguments may preserve more history than
the policy, but cannot shorten its retention count or time windows. Unknown or
unmodeled procedures are rejected before reaching the connector. Warmup remains
a read operation and requires SELECT permission.

The table argument is a nonempty string literal in `schema.table` form. The
catalog comes from the procedure prefix; admission and execution resolve the
same fully identified table. `warmup_table` also accepts separate schema and
table arguments. These two arguments preserve literal dots and case within each component.
Warmup accepts exactly one or two nonempty string literals; extra arguments are rejected
before authorization or execution. The exact catalog prefix and bound target are shared
by admission and execution.

```sql
-- Compact data files (Iceberg)
CALL prod_iceberg.compact('analytics.events');

-- Expire snapshots within the table policy (retain at least 10 by default)
CALL prod_iceberg.expire_snapshots('analytics.events', 10, 1704067200000);

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

**Paimon procedures:**

| Procedure | Arguments | Description |
|-----------|-----------|-------------|
| `compact` | `table_name` | Compact table files |
| `expire_snapshots` | `table_name [, retain_max] [, retain_min] [, dry_run]` | Expire old snapshots; defaults come from the table policy (10 snapshots by default) |
| `remove_orphan_files` | `table_name [, older_than_ms] [, dry_run]` | Remove unreferenced files; the cutoff must be at least 24 hours old |

Snapshot rollback, cherry-pick, branch and tag mutation through CALL have no
maintenance-policy contract and are rejected. Use the dedicated authorized SQL
DDL where supported. Connector implementation availability alone does not grant
permission to invoke a procedure.

Paimon connector discovery lists `compact`, `expire_snapshots`,
`remove_orphan_files`, `create_tag`, `delete_tag`, `rollback_to`, `create_branch`
and `delete_branch`. This list describes implemented connector operations;
the SQL `CALL` admission rules above still apply. The unsupported Paimon
operations listed below are excluded from discovery as well as rejected on
direct connector invocation.

Each maintenance attempt has a unique lease holder, including concurrent requests
on the same coordinator. The lease is renewed during execution and checked before
the connector is invoked and before success is recorded. Lease loss before
execution skips the operation; loss after execution starts records a failed run.
Such a failure does not prove that the format commit rolled back: inspect the
table state before retrying. External engines remain outside Pangrova's lease,
so format-level concurrency checks and the LINKED version checks still apply.

Destructive procedures are conservative by default and accept `dry_run = true`
to report what would be removed without committing anything. Procedures that
are not implemented for a connector fail with an unsupported-operation error
instead of reporting a no-op success; `merge_branch`, `repair`,
`rewrite_file_index`, `reset_consumer`, and `rescale_bucket` are currently
unsupported on Paimon.
