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
| Dynamic-table DDL and lifecycle | Create/alter/drop/show/describe handlers are served by the unified materialization kernel when durable metadata publication is configured; refreshes execute Native CTAS and publish immutable generations. Continuous-query materialization remains unsupported |
| Materialized-view DDL and lifecycle | The same kernel serves create/alter/refresh/drop/show/describe; published views are eligible for transparent query rewrite |
| Materialization observability | `SHOW MATERIALIZATIONS`, refresh history, dependencies, and GC history project the durable kernel read model per tenant |

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
FORMAT 'debezium-json'
INTO prod.analytics.events
WITH (
  'batch_size' = '10000',
  'max_consecutive_failures' = '10'
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
  'batch_size' = '50000',
  'max_consecutive_failures' = '5'
);
```

The current pipeline reads `batch_size`, `max_consecutive_failures`,
`watermark_delay`, `late_data_policy`, and `allowed_lateness` from stream
properties:

| Property | Values | Default | Meaning |
| --- | --- | --- | --- |
| `batch_size` | positive integer | 1000 | Maximum records per poll batch |
| `max_consecutive_failures` | positive integer | 10 | Consecutive failures before the stream enters FAILED |
| `watermark_delay` | duration (`PT10S`, `10s`, `500ms`, `5m`, `1h`, `2d`, or bare milliseconds) | `10s` | Delay applied when advancing the per-table watermark |
| `late_data_policy` | `update` or `drop` | `update` | How records older than the watermark are handled |
| `allowed_lateness` | duration as above | `0s` | How far behind the watermark a record may be before it is late |

Invalid property values fail the pipeline at startup (the stream enters the
FAILED state) instead of silently falling back to a default. A RUNNING stream
restarts with a new ownership epoch when properties change. Each source batch
has its own durable checkpoint; the former `checkpoint.interval` example did
not configure a timer. The bootstrap registers `debezium-json`, `canal-json`,
and `maxwell-json` converters.

DDL persists the requested state on the accepting coordinator. Only the elected
maintenance coordinator starts pipelines; it reconciles changes accepted by other
coordinators every five seconds. This interval is not a stop/publication guarantee
for a catalog commit already in progress. Use the full target name in `INTO` so
the stored catalog and schema identify the intended table.

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

**Status: Dynamic Table DDL and refresh are served by the unified materialization
kernel when durable metadata publication is configured.** Every published
generation is immutable; refresh reserves a durable attempt, executes through
the Native execution path, and publishes through the kernel head. Names must be
unquoted `catalog.schema.table` identifiers. `CREATE ... WAREHOUSE` and
`CREATE ... WITH (...)` are rejected until a materialization policy exists, and
continuous-query materialization is not implemented.

### CREATE DYNAMIC TABLE

`TARGET_LAG` is optional. Omitting it creates a manual object: the kernel
persists a disabled schedule and only `REFRESH` (or
`ALTER ... REFRESH`) advances the published head. A positive lag such as
`'15 minutes'` creates a durable active schedule; `PT0S` is rejected because a
zero lag would mean "always running", which the scheduler does not offer.

`REFRESH_MODE` selects the refresh contract:

| Mode | Kernel mode | Behavior |
| --- | --- | --- |
| omitted, `SNAPSHOT`, or `FULL` | `FULL` | Every refresh recomputes the full definition. Creation bootstraps a full generation. |
| `AUTO` or `INCREMENTAL` | `INCREMENTAL` | Every refresh must apply a proven change set. The definition must be eligible and the source connector must prove a supported mode (append-only or partition replacement). Creation bootstraps the base full generation. |
| `CDC` | — | Not implemented; the statement is rejected. |

An incremental object never falls back to a full recomputation. If the
definition or connector cannot prove an incremental change set, creation and
refresh fail closed with `MATERIALIZATION_INCREMENTAL_UNSUPPORTED`; switch to
`REFRESH_MODE = FULL` to recompute fully.

A definition that reads another Dynamic Table or Materialized View is always
full-only. Each publication replaces the upstream physical table, so no change
proof can span the base generation and the newly pinned upstream table; such a
definition is rejected at creation when it requests `AUTO` or `INCREMENTAL`.

`INCREMENTAL_STRATEGY` pins how an incremental object applies changes:

| Value | Behavior |
| --- | --- |
| omitted or `AUTO` | The kernel selects the first registered strategy the planned definition supports, preferring partition replacement for partition-aligned sources. |
| `APPEND_ONLY` | Pins append-only merge. The definition must be a single-source projection/filter whose connector can prove an append-only change set. |
| `PARTITION_REPLACE` | Pins partition replacement. The source connector must expose identity partitions and the definition must project every partition column directly. A single-table grouping aggregate is accepted when every partition column is a grouping key. |
| `AFFECTED_KEY` | Parsed and mapped to the kernel strategy, but rejected until the deployment registers it. |
| `CDC` | Rejected. |

`PARTITION_REPLACE` replaces only the partitions whose data or delete files
changed in the interval, including deletes and file rewrites that an append-only
proof cannot cover. It copies the unchanged base partitions and recomputes the
definition for changed partitions, so the definition must expose each
partition column as a directly derived output column. A partition column that
is projected through an expression, or an aggregate that does not group by a
partition column, makes the object ineligible at creation. When the column is
not exposed, pin `INCREMENTAL_STRATEGY = APPEND_ONLY` if the connector can
prove append-only intervals.

`INCREMENTAL_STRATEGY` is only valid with `REFRESH_MODE = AUTO` or
`INCREMENTAL`; a full-refresh object that carries a strategy is rejected at
planning time. A pinned strategy is never silently replaced by another one.

**With SNAPSHOT refresh mode (explicit full refresh):**

```sql
CREATE DYNAMIC TABLE prod.analytics.daily_revenue_summary
TARGET_LAG = '15 minutes'
REFRESH_MODE = SNAPSHOT
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

**With AUTO refresh mode (append-only incremental):**

```sql
CREATE DYNAMIC TABLE prod.analytics.completed_orders
TARGET_LAG = '10 minutes'
REFRESH_MODE = AUTO
AS
SELECT order_id, user_id, amount
FROM prod.analytics.orders
WHERE status = 'completed';
```

**With a pinned incremental strategy:**

```sql
CREATE DYNAMIC TABLE prod.analytics.completed_orders
TARGET_LAG = '10 minutes'
REFRESH_MODE = INCREMENTAL
INCREMENTAL_STRATEGY = APPEND_ONLY
AS
SELECT order_id, user_id, amount
FROM prod.analytics.orders
WHERE status = 'completed';
```

`AUTO` proves append-only change sets for a single-source projection/filter
definition, and partition replacement for a partition-aligned projection or
grouping aggregate. Joins, unions, windows, and connectors without a matching
proof are rejected rather than silently recomputed.

**With CDC refresh mode (not implemented):**

```sql
CREATE DYNAMIC TABLE prod.analytics.user_order_snapshot
TARGET_LAG = '5 minutes'
REFRESH_MODE = CDC
AS
SELECT u.user_id, u.name, COUNT(o.order_id) AS lifetime_orders
FROM prod.analytics.users u
LEFT JOIN prod.analytics.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.name;
```

This statement is rejected with an unsupported-operation error until CDC
materialization is implemented.

### ALTER DYNAMIC TABLE

**SUSPEND:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary SUSPEND;
```

**RESUME:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary RESUME;
```

`SUSPEND` moves an active object into the durable `SUSPENDED` state and
suspends its schedule; an explicit `REFRESH` is rejected until `RESUME`
restores the `ACTIVE` state and the schedule. The published generation stays
readable while suspended. A manual-only object has no schedule to suspend and
is left unchanged.

**REFRESH (manual trigger):**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary REFRESH;
```

**SET TARGET_LAG:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary SET TARGET_LAG = '30 minutes';
```

**SET REFRESH_MODE (rebuilds before committing the new mode):**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary SET REFRESH_MODE = INCREMENTAL;
```

Changing the mode first publishes a forced full generation under the existing
definition, then commits the new mode and strategy. If the rebuild fails, the
previous mode and published head remain in force; retrying with the same command
id applies the pending configuration without recomputing data that already
published. `SET REFRESH_MODE` requires an active object.

**RENAME TO:**

```sql
ALTER DYNAMIC TABLE prod.analytics.daily_revenue_summary
RENAME TO prod.analytics.daily_revenue_summary_v2;
```

A rename moves the logical identity only: the incarnation, definition
revisions, published generations, dependencies, and durable schedule follow the
new name, and the published head stays readable throughout. Historical events
and command receipts keep the name they were recorded under. The rename is
rejected when the target name already exists, when the object is not active or
suspended, and while live materializations depend on it — their stored
definitions still spell the old name, so redefine or drop them first.

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
```

Without `CASCADE`, the statement fails while another live materialization
consumes the target. `DROP ... CASCADE` drops every transitive dependent first,
deepest dependent first, so no live definition ever references a dropped
object. Physical generations are retained until reader pins and GC allow
collection. The dropped name keeps a durable tombstone, and creating the same
name again starts a new incarnation at definition revision 1 instead of
reusing the old identity; earlier generations stay collectable history.

### SHOW DYNAMIC TABLES

```sql
SHOW DYNAMIC TABLES;
```

### DESCRIBE DYNAMIC TABLE

```sql
DESCRIBE DYNAMIC TABLE prod.analytics.daily_revenue_summary;
```

---

## Materialized View

**Status: Materialized View DDL, refresh, and transparent query rewrite are
served by the unified materialization kernel.** A materialized view is a kernel
object with the same durable guarantees as a dynamic table: reserved attempts,
immutable published generations, reader pins, and fenced GC. The difference is
the optimization contract, not the storage path: a published view is eligible
for transparent rewrite by the planner (see
[Query Optimization](../architecture/query-optimization.md)).

Names must be unquoted `catalog.schema.table` identifiers.

### CREATE MATERIALIZED VIEW

```sql
CREATE MATERIALIZED VIEW prod.analytics.daily_revenue_mv
TARGET_LAG = '30 minutes'
REFRESH_MODE = SNAPSHOT
AS
SELECT
  CAST(order_date AS DATE) AS report_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_revenue
FROM prod.analytics.orders
WHERE status = 'completed'
GROUP BY CAST(order_date AS DATE);
```

`CREATE` publishes the first generation. `TARGET_LAG`, `REFRESH_MODE`, and
`INCREMENTAL_STRATEGY` follow the dynamic-table contract: omitting `TARGET_LAG`
means manual refresh, `SNAPSHOT`/`FULL` recomputes the full definition,
`AUTO`/`INCREMENTAL` requires a proven incremental change set and fails closed
otherwise, and `CDC` is rejected.

**Atomic replacement:**

```sql
CREATE OR REPLACE MATERIALIZED VIEW prod.analytics.daily_revenue_mv
AS SELECT ...;

CREATE MATERIALIZED VIEW IF NOT EXISTS prod.analytics.daily_revenue_mv
AS SELECT ...;
```

`OR REPLACE` stores a new definition revision and immediately forces a full
refresh. The previous published generation stays readable until the replacement
publishes, so a failed replacement never exposes partial data; retrying the same
command resumes the pending publication. Supplying `TARGET_LAG` also updates the
schedule, while omitting it keeps the existing schedule. `IF NOT EXISTS` returns
the existing published object unchanged. The two modifiers are mutually
exclusive, and `OR REPLACE` requires an existing active or suspended object of
the same kind.

### ALTER MATERIALIZED VIEW

```sql
ALTER MATERIALIZED VIEW prod.analytics.daily_revenue_mv SUSPEND;
ALTER MATERIALIZED VIEW prod.analytics.daily_revenue_mv RESUME;
ALTER MATERIALIZED VIEW prod.analytics.daily_revenue_mv REFRESH;
ALTER MATERIALIZED VIEW prod.analytics.daily_revenue_mv SET TARGET_LAG = '10 minutes';
ALTER MATERIALIZED VIEW prod.analytics.daily_revenue_mv SET REFRESH_MODE = INCREMENTAL;
ALTER MATERIALIZED VIEW prod.analytics.daily_revenue_mv
  RENAME TO prod.analytics.daily_revenue_mv_v2;
```

`SET REFRESH_MODE` and `RENAME TO` follow the dynamic-table semantics: the mode
change publishes a forced full generation before the new configuration is
committed, and a rename preserves definitions, generations, and the schedule
while rejecting live dependents. `SUSPEND` and `RESUME` also follow the
dynamic-table contract: a suspended view keeps serving its published head but
rejects explicit refreshes until resumed.

Apart from `CREATE OR REPLACE`, `ALTER` manages scheduling and refresh only; it
never changes the stored definition. A definition change requires `CREATE OR
REPLACE`, which records a new revision and publishes it atomically.

### REFRESH MATERIALIZED VIEW

```sql
REFRESH MATERIALIZED VIEW prod.analytics.daily_revenue_mv;
```

Equivalent to `ALTER ... REFRESH`; both reserve a manual attempt.

### DROP MATERIALIZED VIEW

```sql
DROP MATERIALIZED VIEW prod.analytics.daily_revenue_mv;
DROP MATERIALIZED VIEW prod.analytics.daily_revenue_mv CASCADE;
DROP MATERIALIZED VIEW IF EXISTS prod.analytics.daily_revenue_mv;
```

`CASCADE` has the same dependency-aware semantics as for dynamic tables.
`IF EXISTS` makes a missing target a successful no-op.

### SHOW MATERIALIZED VIEWS

```sql
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS LIKE 'prod.analytics.daily_%';
```

`LIKE` matches the full object name with SQL wildcards: `%` matches any
sequence of characters and `_` matches exactly one character.

### DESCRIBE MATERIALIZED VIEW

```sql
DESCRIBE MATERIALIZED VIEW prod.analytics.daily_revenue_mv;
```

---

## Materialization Observability

These read-only surfaces project the durable kernel state for both dynamic
tables and materialized views. Every statement is tenant-scoped and served from
metadata; no physical generation is opened and no definition is recompiled.

```sql
SHOW MATERIALIZATIONS;
SHOW MATERIALIZATION REFRESH HISTORY;
SHOW MATERIALIZATION REFRESH HISTORY FOR prod.analytics.daily_revenue_mv;
SHOW MATERIALIZATION DEPENDENCIES;
SHOW MATERIALIZATION DEPENDENCIES FOR prod.analytics.daily_revenue_mv;
SHOW MATERIALIZATION GC HISTORY;
SHOW MATERIALIZATION GC HISTORY FOR prod.analytics.daily_revenue_mv;
```

| Statement | Columns |
| --- | --- |
| `SHOW MATERIALIZATIONS` | `name`, `kind`, `state`, `revision`, `definition_revision`, `published_generation`, `definer`, `security_mode`, `refresh_mode`, `incremental_strategy`, `created_at`, `updated_at` |
| `SHOW MATERIALIZATION REFRESH HISTORY` | `attempt_id`, `object`, `refresh_kind`, `trigger`, `state`, `error_code`, `definition_revision`, `requesting`, `definer`, `created_at`, `updated_at` |
| `SHOW MATERIALIZATION DEPENDENCIES` | `downstream`, `downstream_definition_revision`, `upstream`, `kind` |
| `SHOW MATERIALIZATION GC HISTORY` | `object`, `generation`, `decision`, `state`, `evidence`, `created_at`, `updated_at` |

Refresh history and GC history return the most recent 1000 rows per tenant (or
per scoped object).

## Downstream Refresh

Dependencies are recorded from each definition. When a refresh publishes a new
generation, every dependent with an active, unleased schedule has its
`next_due_at` advanced to the publication instant inside the same metadata
transaction. The scheduler still claims that dependent under its own lease, so
downstream execution never happens inside the upstream transaction and a slow
dependent cannot extend the upstream commit. A manual dependent schedule is not
advanced; refresh it explicitly.
