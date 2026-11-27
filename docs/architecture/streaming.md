---
title: "Streaming & CDC"
description: "Current CDC wiring and incomplete streaming and refresh lifecycles"
sidebar_label: "Streaming & CDC"
---

# Streaming & CDC

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Current CDC boundary.** A COMBINED Coordinator dispatches admitted CDC ranges
> to Workers. A MAINTENANCE Coordinator stages the same ranges locally. Both roles
> journal the write and acknowledge source offsets after catalog publication.
> Sections 2.4–2.5 describe the execution and recovery paths. See
> [distributed writes](./distributed-writes.md#worker-cdc-publication-and-recovery)
> for the remaining process-level acceptance criteria.

> **Summary.** CDC ingestion, incremental queries, streaming writes, and watermark tracking.

> **Code ownership.** `pangrova-streaming` owns
> `api.{aggregation,cdc,checkpoint,commit,source,watermark}` and `core.*`
> (including `store/memory` and `coordinator`). CDC format and writer providers
> live under `org.pangrova.streaming.{format,writer}`. JDBC offset and
> watermark persistence lives in
> `pangrova-persistence/pangrova-persistence-streaming-postgresql`
> (`org.pangrova.persistence.streaming.postgresql`). The Coordinator constructs
> the streaming bootstrap; it does not own a second registration, scheduler or
> offset/watermark store.

The JDBC offset batch store owns one connection and commits the complete batch
only after all writes succeed. SQL exceptions, runtime exceptions, and Errors
trigger rollback; a rollback exception is suppressed on the original failure.
The store closes its connection without restoring auto-commit in a failure path,
which could otherwise commit pending offsets. This is local database atomicity;
it does not make source acknowledgement and lake publication one transaction.

## 0. Product Positioning

The Pangrova streaming module is a **declarative incremental data transformation** engine.
Stream task registration depends on the streaming role. Incremental continuous-query
refresh and watermark-gated materialization remain open.
Dynamic-table, continuous-query, and materialized-view DDL, refresh, scheduling,
recovery, and GC are served by the unified materialization kernel when durable
metadata publication is configured: refreshes claim a durable lease, execute Native
CTAS, and publish an immutable generation through CAS; a restart reconciles reserved
attempts from the write-commit journal, and GC is fenced by reader pins.
`CREATE CONTINUOUS QUERY ... REFRESH EVERY <interval>` creates a dynamic table whose
target lag is the declared interval; refresh is interval-driven and full, not
watermark-gated. That lifecycle does not establish exactly-once CDC output or
incremental continuous-query materialization.

**Reading scope:** the diagrams, SQL/API examples, SPI sketches, and connector writer
sketches below describe design or component contracts unless explicitly identified
as current bootstrap behavior. They do not establish a working distributed ingestion,
refresh, maintenance, or recovery lifecycle. Use
[Current Capability Status](../getting-started/capability-status.md) for default wiring.

**It is NOT a general-purpose stream processing engine.**

Pangrova does not compete with Apache Flink, Spark Structured Streaming, or Kafka Streams.
Those systems provide arbitrary stateful stream processing with complex event processing,
windowed joins, and user-defined operators. Pangrova's streaming module has a narrower,
more focused scope: keeping lake tables fresh via incremental data ingestion.

### Scope Definition

#### What Pangrova Streaming IS

| Capability | Description |
|---|---|
| CDC Ingestion | Kafka feeds bounded Worker JVM tasks with both roles enabled, or the local connector writer with only the streaming role. Catalog publication precedes source acknowledgement |
| Incremental ETL | Component/design scope; arbitrary transformation execution is not established |
| Materialized View Refresh | Served by the unified materialization kernel alongside dynamic tables: durable schedule, reserved attempts, Native execution, immutable publication |
| Table Maintenance | Leader-gated maintenance and standalone SQL use the configured maintenance services. See [maintenance SQL](../sql-reference/maintenance.md) |
| Watermark Tracking | Tracking components; does not establish a functioning refresh lifecycle |

#### What Pangrova Streaming is NOT

| Anti-Pattern | Use Instead |
|---|---|
| Arbitrary stateful stream processing | Apache Flink |
| Complex event processing (CEP) | Apache Flink CEP |
| Windowed stream joins | Apache Flink / Spark Structured Streaming |
| User-defined stream operators | Apache Flink / Kafka Streams |
| Exactly-once barrier-based checkpointing | Apache Flink (Chandy-Lamport) |
| Backpressure-driven DAG execution | Apache Flink / Akka Streams |
| Stream-table duality with changelog topics | Kafka Streams / ksqlDB |

### Architecture Principles

1. **Declarative Over Imperative** — Users declare WHAT they want (a table that stays fresh from a CDC source), not HOW to process streams.
2. **Table-Centric, Not Stream-Centric** — The primary abstraction is the table, not the stream. Streams are an implementation detail for keeping tables fresh.
3. **At-Least-Once with Idempotent Writes** — Duplicates eliminated through primary key deduplication at the storage layer (Iceberg equality deletes, Paimon LSM merge).
4. **Batch-Oriented Internals** — CDC processing uses mini-batch semantics (poll N records, write batch, commit offsets).

### Comparison with Stream Processing Engines

| Dimension | Pangrova Streaming | Apache Flink | Spark Structured Streaming |
|---|---|---|---|
| Primary abstraction | Table (kept fresh) | DataStream / Table | DataFrame (micro-batch) |
| Processing model | Mini-batch CDC ingest | Record-at-a-time | Micro-batch |
| State management | Offset + watermark only | Arbitrary keyed state | Arbitrary state |
| Delivery guarantee | No qualified exactly-once/effectively-once guarantee | Depends on source/sink and checkpoint configuration | Depends on source/sink and checkpoint configuration |
| Windowing | Refresh interval only | TUMBLE/HOP/SESSION/CUMULATE | Window operations |
| Joins | Not supported in stream | Stream-stream, stream-table | Stream-stream, stream-static |
| User operators | No (declarative only) | Yes (ProcessFunction) | Yes (mapGroupsWithState) |
| Deployment | Embedded in coordinator | Standalone cluster | Spark cluster |

### Migration Notes (v0.2.0)

| Old Name | New Name | Rationale |
|---|---|---|
| Former `StreamSourceReader` | `CdcSourceReader` | Clarifies this reads CDC events, not arbitrary streams |
| StreamWriteSink (old name) | `CdcWriteSink` | Clarifies this writes CDC data, not arbitrary stream output |
| StreamWriteResult (old name) | `CdcWriteResult` | Consistent with CdcWriteSink |
| Former `StreamingWriter` | Former `IncrementalWriter` (both retired) | Clarifies incremental batch buffering, not stream processing |
| Former `StreamingWriterConfig` | Former `IncrementalWriterConfig` (both retired) | Consistent with IncrementalWriter |

Removed APIs: CheckpointCoordinator and the streaming-local CheckpointStore —
Flink-style barrier checkpointing is not needed for CDC offset-based recovery.

## 1. Unified Batch-Streaming Architecture Overview

![Streaming and CDC pipeline with Worker ingestion and the streaming-only local path](../../website/static/img/diagrams/streaming-pipeline.svg)

---

## 2. CDC Data Ingestion

Both roles use `CdcRangeIngestor` to stage the admitted source ranges. Execution
runs in Worker JVMs for COMBINED and in the Coordinator JVM for MAINTENANCE.
Each publication cycle admits up to 1,000 offsets per partition. The journal
records only those ranges, and acknowledgement leaves later offsets for
subsequent cycles.

### 2.4 CDC vs SQL Write: Transaction Orchestration Comparison

The production relational DML path uses Native Workers to produce fragments for
one Coordinator-owned, fenced catalog publication. It does not use a Coordinator
row writer for `INSERT VALUES` or other relational DML.

The leader-gated runtime owns execution and publication in both roles.

```text
StreamingBootstrap
  -> JournaledCdcRuntime (leader-gated)
  -> CdcTaskPlanner (bounded topic/partition ranges)
  -> COMBINED: Worker CDC_INGEST tasks
  -> MAINTENANCE: local range ingestion
  -> BoundCdcPageSinkWriter (connector fragment staging)
  -> WriteCommitJournal intent -> connector publication -> source-offset ack
```

The Coordinator routes bounded ingest only to Workers whose current
registration advertises CDC, subject to tenant placement and resource limits.
The shared membership store persists the capability for the registered Worker
incarnation so Coordinator failover preserves eligibility.
Both roles bind the persisted stream creator and durable execution owner before staging.
MAINTENANCE claims a new execution identity directly through the durable lease acquisition
transaction. Recovery can reuse the exact live lease held by the same process.
During MAINTENANCE staging, each poll and private-file batch checks interruption, the task
deadline, and the locally observed maintenance term. The runtime rechecks PostgreSQL ownership
and stream revision before accepting the staged fragments into the journal, again before catalog
publication, and before acknowledging offsets.
COMBINED also binds the selected Worker incarnation before dispatch. Dispatch, publication, and
acknowledgement check the active control and maintenance terms against the
same stream revision. Pending publication records block another batch for
that checkpoint generation. Once publication is durably confirmed, restart
can restore missing offsets from the exact ranges in the journal.
PostgreSQL checks the running stream revision in the same transaction that
advances these offsets. Replayed acknowledgements preserve the greatest
committed offset for each partition. The journaled runtime discovers running streams
and resolves their checkpoints through the write journal and offset store. Discovery reads the
exact checkpoint generation, source ranges, and journal identity. Fenced replay loads the write
binding and sink fragments when it claims a pending intent. A new checkpoint generation starts
from its own offsets.

Each source pass observes the partition inventory and its exclusive end offsets once.
Admission and task planning use the same observation. After the runtime reconciles a stream
and observes that its source is caught up, later passes with identical source offsets in
the same maintenance term and stream revision skip checkpoint and committed-offset reads.
Changed source offsets, a new stream revision or term, and failures invalidate the idle
observation. New work reloads the durable checkpoint and offsets before admission.

Both paths use the same write journal and connector publication context.
Independent process tests exercise Paimon recovery when a Worker exits after
staging and when a MAINTENANCE Coordinator restarts after catalog publication
while the journal is still PENDING. The other crash windows and equivalent
Iceberg cases remain open. See
[distributed writes](distributed-writes.md#worker-cdc-publication-and-recovery).

The elected Coordinator reconciles running stream definitions every five seconds.
Source work runs on a separate executor from lease renewal. DDL changes advance the
durable stream epoch, and a batch from a superseded epoch fails its next authoritative
boundary check. A renewal error clears the local maintenance epoch, which cancels local
staging at its next poll or batch boundary. A new backend term is activated only after
local demotion. Atomic publication and recovery still use the connector authority.

### 2.5 CDC Write Semantics

**Current status: correctness and recovery qualification remain open.**
Catalog publication and source-offset persistence are separate operations. Existing
primary-key semantics do not prove that replay, partial publication, delete/insert
phases, or ambiguous outcomes are safe across both Iceberg and Paimon.

The journal records tenant, source ranges, table identity, base snapshot, attempt
and ownership fence before publishing a snapshot. Paimon stages UPDATE before
and after images as native row kinds in one merge. Iceberg stages one ordered
range against the allocated snapshot. It resolves each full before-image to a
live position or an earlier staged row, then emits one fragment containing
position delete files and surviving after-images. The Coordinator publishes
one fenced RowDelta. Recovery uses the original fingerprint and rejects a
legacy mutation fragment without typed CDC evidence.
Position delete files contain the file path and physical row number. Native
scans with positional deletes apply them before predicate and runtime filters
can change row positions.
See [Worker CDC publication and recovery](distributed-writes.md#worker-cdc-publication-and-recovery).

### 2.9 TableMaintenance

This interface describes maintenance operations, not their statement wiring. The
default bootstrap injects `ControlledTableMaintenance` into `MaintenanceExecutor`;
mutating catalog-qualified `CALL` operations share the same policy and lease checks. See
[Maintenance SQL](../sql-reference/maintenance.md).

A unified table maintenance interface supporting compaction, orphan cleanup, and snapshot expiration.

```java
/**
 * Table Maintenance SPI - unified interface for table maintenance operations.
 * All operations are async and non-blocking.
 * Triggered by MaintenanceScheduler based on time interval (not threshold).
 */
public interface TableMaintenance {

    /**
     * Execute compaction asynchronously.
     * Merges small files and equality delete files into larger data files.
     */
    CompletableFuture<CompactionResult> compactAsync(String tableName);

    /**
     * Clean orphan files asynchronously.
     * Removes files not referenced by any snapshot.
     */
    CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String tableName, Duration olderThan);

    /**
     * Expire old snapshots asynchronously.
     */
    CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String tableName, Duration olderThan);

    /**
     * Get current maintenance status for a table.
     */
    MaintenanceStatus getMaintenanceStatus(String tableName);
}




public record MaintenanceStatus(
    String tableName,
    int smallFilesCount,
    int totalFilesCount,
    long totalSizeBytes,
    int snapshotCount,
    int equalityDeleteFiles,
    long equalityDeleteBytes,
    long orphanFilesEstimate,
    Instant lastCompactionTime,
    Instant lastOrphanCleanupTime,
    Instant lastSnapshotExpireTime
) {}
```

### 2.10 Maintenance Scheduler (Unified Maintenance Scheduling)

**Design principle**:
- Unify the three trigger mechanisms into a single priority model to eliminate ambiguity
- Clarify trigger priority: threshold trigger > interval trigger > scheduled scan trigger
- Global concurrency control + queue + backpressure to ensure maintenance load is predictable

### Unified Trigger Model

The three trigger mechanisms are ranked by priority from high to low. When multiple conditions are met simultaneously, the higher-priority trigger executes first and lower-priority triggers are suppressed (only one maintenance task is produced for the same table at the same time).

| Priority | Trigger Type | Source | Condition | Behavior |
|---|---|---|---|---|
| **1 (Highest)** | Threshold Trigger | Scan-discovered | `equalityDeleteFiles >= maxEqualityDeleteFiles` | Trigger immediately, bypass interval check |
| **2** | Interval Trigger | Write signal | Time since last maintenance `>= compactionInterval` | Trigger on next write completion |
| **3 (Lowest)** | Scheduled Scan Trigger | Periodic scan | `smallFilesCount >= minFilesToCompact` AND time since last `>= compactionInterval` | Trigger during periodic scan cycle |

**Precedence Rules**:
- **Threshold > Interval**: Threshold trigger fires immediately regardless of interval timer. If a threshold trigger and an interval trigger both qualify for the same table, only the threshold-triggered task executes (higher priority subsumes lower).
- **Interval > Scan**: If an interval trigger already fired for a table within the current scan cycle, the scan trigger skips that table.
- **Deduplication**: Each table maintains an `inFlightTask` flag. If a maintenance task is already running or queued for a table, new triggers for the same table are dropped.

### Automatic Cycles for Controlled Tables

Production scheduling is registry-driven. `ControlledTableSource` lists only
ACTIVE LINKED/MANAGED records whose policy enables an operation; OBSERVED tables
are absent because they have no persisted record. `CoordinatorTableMaintenance`
reports due-ness from the last terminal run per operation and executes
`COMPACT`, `EXPIRE_SNAPSHOTS`, and `REMOVE_ORPHAN_FILES` through the same
ownership/policy/lease/version checks as a manual statement.

The periodic scan is leader-gated: `ModuleIntegrationBootstrap` starts it in
`onLeaderElected()` and stops it in `onLeadershipLost()`. Non-leader coordinators
therefore never run the policy cycle. A write-triggered task remains a
compaction of the data just written; only `SCHEDULED_SCAN` tasks run the full
retention cycle. Every automatic run persists `mode = AUTOMATIC` and actor
`system:maintenance`; an active external writer aborts the run as `SKIPPED`, and
other failures surface to the scheduler instead of being reported as success.
Format-specific `expire_snapshots` arguments are selected from the control
record's format.

### Concurrency Control and Backpressure

| Parameter | Default | Description |
|---|---|---|
| `maxConcurrentTasks` | 4 | Global max concurrent maintenance tasks across all tables |
| `taskQueueCapacity` | 16 | Max queued tasks waiting for execution |
| `taskTimeout` | 30m | Max duration per maintenance task before forced cancellation |

**Backpressure Behavior**:
1. **Running tasks < maxConcurrentTasks**: New task executes immediately.
2. **Running tasks = maxConcurrentTasks, queue not full**: New task enters the priority queue (ordered by trigger priority, then by wait time).
3. **Running tasks = maxConcurrentTasks, queue full**: Drop the lowest priority task in the queue. If the new task has higher priority than the dropped task, enqueue the new task; otherwise, drop the new task itself.
4. **All slots busy + queue full + new task is lowest priority**: Log warning and discard the new task. Metric `maintenance.tasks.dropped` incremented.

![/**](../../website/static/img/diagrams/generated/streaming-10-1d154be9af.svg)

---

## 3. Incremental Query

### 3.1 Incremental Query Architecture

![Incremental Query Architecture](../../website/static/img/diagrams/generated/streaming-11-87b42b7b70.svg)

### 3.2 Incremental Query SQL

```sql
-- Iceberg incremental query (snapshot-based)
SELECT * FROM iceberg.db.orders
FOR SYSTEM_VERSION BETWEEN 100 AND 105;

-- Iceberg incremental query (time-based)
SELECT * FROM iceberg.db.orders
FOR SYSTEM_TIME BETWEEN '2024-01-01 00:00:00' AND '2024-01-02 00:00:00';

-- Paimon Changelog read
SELECT * FROM paimon.db.orders$changelog
WHERE _commit_time > '2024-01-01 00:00:00';

-- Incremental read (read only newly added data)
SELECT * FROM iceberg.db.orders
INCREMENTAL SINCE SNAPSHOT 100;

-- Streaming consumption (continuous read)
SELECT * FROM iceberg.db.orders
STREAMING START FROM SNAPSHOT 100;
```

### 3.3 Incremental Query Implementation

```java
/**
 * Incremental scan planner
 */
public class IncrementalScanPlanner {

    /**
     * Generate the incremental scan plan
     */
    public IncrementalScanPlan plan(IncrementalScanRequest request) {
        Table table = catalog.loadTable(request.getTableIdentifier());

        // Get the start and end snapshots
        Snapshot fromSnapshot = table.snapshot(request.getFromSnapshotId());
        Snapshot toSnapshot = request.getToSnapshotId() != null
            ? table.snapshot(request.getToSnapshotId())
            : table.currentSnapshot();

        // Collect incremental files
        Set<DataFile> addedFiles = new HashSet<>();
        Set<DeleteFile> addedDeletes = new HashSet<>();

        // Traverse the snapshot chain
        Snapshot current = toSnapshot;
        while (current != null && current.snapshotId() != fromSnapshot.snapshotId()) {
            for (DataFile file : current.addedDataFiles(table.io())) {
                addedFiles.add(file);
            }
            for (DeleteFile file : current.addedDeleteFiles(table.io())) {
                addedDeletes.add(file);
            }
            current = table.snapshot(current.parentId());
        }

        return IncrementalScanPlan.builder()
            .dataFiles(addedFiles)
            .deleteFiles(addedDeletes)
            .fromSnapshot(fromSnapshot)
            .toSnapshot(toSnapshot)
            .build();
    }
}

/**
 * Changelog reader
 */
public class ChangelogReader {

    /**
     * Read the Changelog (returns records with operation type)
     */
    public Iterator<ChangelogRecord> readChangelog(Table table, long fromSnapshot) {
        IncrementalScanPlan plan = planner.plan(
            IncrementalScanRequest.builder()
                .tableIdentifier(table.name())
                .fromSnapshotId(fromSnapshot)
                .build());

        return new ChangelogIterator(plan);
    }

    /**
     * Changelog record
     */
    public static class ChangelogRecord {
        private final ChangeType changeType;  // INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE
        private final GenericRecord record;
        private final long snapshotId;
        private final Instant commitTime;
    }
}
```

---

## 4. Streaming Ingestion

Use stream DDL for Kafka ingestion. Use relational SQL DML to append query results
or values through the Worker connector write path. See [Streaming SQL](../sql-reference/streaming.md)
and [DML](../sql-reference/dml.md).

## 5. Watermark Management

### 5.1 Watermark Concept

![Watermark Management](../../website/static/img/diagrams/generated/streaming-16-46c1b624f0.svg)

### 5.2 Table-Level Watermark

```sql
-- Set the table's Watermark column
ALTER TABLE iceberg.db.events
SET TBLPROPERTIES ('watermark.column' = 'event_time', 'watermark.delay' = '5 minutes');

-- Query the current Watermark
SELECT watermark FROM system.table_watermarks WHERE table_name = 'iceberg.db.events';

-- Watermark-based window query
SELECT
    TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
    COUNT(*) as event_count
FROM iceberg.db.events
WHERE event_time <= CURRENT_WATERMARK('iceberg.db.events')
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR);
```

### 5.3 Watermark Tracker

```java
/**
 * Watermark tracker
 */
public class WatermarkTracker {

    private final Map<String, Instant> tableWatermarks = new ConcurrentHashMap<>();
    private final WatermarkStore store;

    /**
     * Update the Watermark
     */
    public void updateWatermark(String tableName, Instant watermark) {
        Instant current = tableWatermarks.get(tableName);

        // The Watermark can only advance, not go backward
        if (current == null || watermark.isAfter(current)) {
            tableWatermarks.put(tableName, watermark);
            store.persist(tableName, watermark);
        }
    }

    /**
     * Infer the Watermark from data
     */
    public Instant inferWatermark(String tableName, List<Record> records,
                                   WatermarkConfig config) {
        // Find the maximum event time
        Instant maxEventTime = records.stream()
            .map(r -> r.getTimestamp(config.getWatermarkColumn()))
            .max(Instant::compareTo)
            .orElse(Instant.MIN);

        // Subtract the lateness tolerance
        return maxEventTime.minus(config.getWatermarkDelay());
    }

    /**
     * Get the current Watermark
     */
    public Instant getWatermark(String tableName) {
        return tableWatermarks.getOrDefault(tableName, Instant.MIN);
    }
}
```

---

## 6. Upsert Support

### 6.1 Upsert Modes

![Upsert Modes](../../website/static/img/diagrams/generated/streaming-19-fa582e3445.svg)

### 6.2 Upsert SQL

```sql
-- Create a table that supports Upsert
CREATE TABLE iceberg.db.users (
    user_id BIGINT,
    name STRING,
    email STRING,
    updated_at TIMESTAMP
) USING ICEBERG
TBLPROPERTIES (
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read'
);

-- Upsert operation (MERGE INTO)
MERGE INTO iceberg.db.users t
USING (SELECT * FROM staging.users) s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Simplified Upsert syntax
UPSERT INTO iceberg.db.users
SELECT * FROM staging.users;

-- Paimon table (native primary key update support)
CREATE TABLE paimon.db.users (
    user_id BIGINT PRIMARY KEY,
    name STRING,
    email STRING,
    updated_at TIMESTAMP
);

-- A plain INSERT is enough to perform an Upsert
INSERT INTO paimon.db.users VALUES (1, 'Alice', 'alice@example.com', NOW());
INSERT INTO paimon.db.users VALUES (1, 'Alice Updated', 'alice2@example.com', NOW());
-- The second row updates the first
```

### 6.3 Upsert Implementation

```java
/**
 * Upsert writer
 */
public class UpsertWriter {

    /**
     * Perform the Upsert
     */
    public void upsert(Table table, List<Record> records, List<String> primaryKeys) {
        String deleteMode = table.properties().getOrDefault(
            "write.delete.mode", "merge-on-read");

        if ("copy-on-write".equals(deleteMode)) {
            upsertCOW(table, records, primaryKeys);
        } else {
            upsertMOR(table, records, primaryKeys);
        }
    }

    /**
     * COW-mode Upsert
     */
    private void upsertCOW(Table table, List<Record> records, List<String> primaryKeys) {
        // 1. Find the affected files
        Set<DataFile> affectedFiles = findAffectedFiles(table, records, primaryKeys);

        // 2. Read data from the affected files
        Map<Object, Record> existingData = readExistingData(affectedFiles, primaryKeys);

        // 3. Merge old and new data
        for (Record record : records) {
            Object key = extractKey(record, primaryKeys);
            existingData.put(key, record);  // Overwrite or add
        }

        // 4. Rewrite the files
        RewriteFiles rewrite = table.newRewrite();
        for (DataFile oldFile : affectedFiles) {
            rewrite.deleteFile(oldFile);
        }
        DataFile newFile = writeDataFile(existingData.values());
        rewrite.addFile(newFile);
        rewrite.commit();
    }

    /**
     * MOR-mode Upsert
     */
    private void upsertMOR(Table table, List<Record> records, List<String> primaryKeys) {
        // 1. Write the new data file
        DataFile dataFile = writeDataFile(records);

        // 2. Write the Equality Delete file (delete the old version)
        DeleteFile deleteFile = writeEqualityDeleteFile(table, records, primaryKeys);

        // 3. Commit
        RowDelta rowDelta = table.newRowDelta();
        rowDelta.addRows(dataFile);
        rowDelta.addDeletes(deleteFile);
        rowDelta.commit();
    }
}
```

---

## 7. Configuration Summary

```yaml
# streaming-config.yaml
streaming:
  # CDC configuration
  cdc:
    enabled: true
    deployment-mode: embedded  # embedded | standalone
    consumer:
      bootstrap-servers: kafka:9092
      group-id: pangrova-cdc-consumer
      auto-offset-reset: earliest
    debezium:
      format: json
      include-schema: false

  # Unified write configuration (shared by CDC and client writes)
  writer:
    batch-size: 1000
    delete-strategy: hybrid  # position-first for same-batch, equality for cross-batch

  # Asynchronous maintenance scheduling configuration (unified priority model)
  maintenance:
    # Interval triggers (Priority 2: write-triggered)
    compaction-interval: 60m       # Trigger compaction every 60 minutes
    orphan-cleanup-interval: 360m  # Trigger orphan cleanup every 6 hours
    snapshot-expiration-interval: 720m  # Trigger snapshot expiration every 12 hours

    # Scan intervals (Priority 3: periodic scan)
    compaction-scan-interval: 60m
    orphan-cleanup-scan-interval: 24h

    # Thresholds (Priority 1: immediate trigger)
    max-equality-delete-files: 10  # equality delete files >= 10 → immediate compaction
    min-files-to-compact: 5        # small files >= 5 → scan-triggered compaction

    # Retention
    orphan-file-age: 3d            # Clean up orphan files older than 3 days
    snapshot-retention: 7d         # Retain snapshots within the last 7 days

    # Concurrency and backpressure
    threads: 4                         # task execution threads (executor = threads+1 to avoid deadlock)
    max-concurrent-tasks: 4        # Global max concurrent maintenance tasks (must be <= threads)
    task-queue-capacity: 16        # Wait queue capacity; backpressure triggered when full
    task-timeout: 30m              # Single-task timeout

  # Streaming ingestion configuration
  ingestion:
    buffer:
      max-bytes: 128MB
      max-rows: 1000000
      max-time: 1m
    commit:
      interval: 1m
      retry-times: 3

  # Incremental query configuration
  incremental:
    default-lookback: 24h
    max-snapshots-per-query: 100

  # Watermark configuration
  watermark:
    default-delay: 5m
    update-interval: 10s

  # Upsert configuration
  upsert:
    default-mode: merge-on-read  # copy-on-write, merge-on-read
    equality-delete-enabled: true
```

---



## 8. Observability Integration

This section defines the metrics and tracing integration points for the streaming and CDC layer. For the full observability design, see [observability.md](./observability.md).

### 8.1 Metrics

The Coordinator passes its configured metrics provider to each scheduled CDC loop. Instruments use a `stream` dimension. Both direct writes and checkpointed two-phase publication emit the following series.

| Metric | Source component | Measurement |
|--------|------------------|-------------|
| `pangrova.streaming.cdc.records.consumed` | Source poll | Records returned in a fresh poll. Retrying the retained batch does not increment the counter. |
| `pangrova.streaming.cdc.bytes.consumed` | Source poll | Sum of raw key and value lengths, in bytes. |
| `pangrova.streaming.cdc.batches.processed` | Source acknowledgement | Batches whose next offsets were acknowledged successfully, including checkpointed replays. |
| `pangrova.streaming.cdc.source.poll.duration` | Source reader | Duration of each poll attempt in seconds, including empty polls. |
| `pangrova.streaming.cdc.write.duration` | Sink | Duration of the direct write or two-phase stage attempt in seconds. |
| `pangrova.streaming.cdc.commit.duration` | Source reader | Duration of each offset acknowledgement attempt in seconds. |
| `pangrova.streaming.cdc.failures` | Pipeline | Recovery and cycle failures observed while running. |
| `pangrova.streaming.cdc.consumer.lag` | Kafka reader | Last sampled sum of assigned partition offset distances. Collection omits the point until all assigned distances are available. |
| `pangrova.streaming.cdc.pipeline.uptime` | Pipeline executor | Elapsed seconds since this loop started. The loop unregisters both gauges on exit. |

If a watermark update fails after the source acknowledges a batch, the running loop retries the watermark update before its next poll. The source acknowledgement and the watermark store have independent commits. A process loss between them can leave the persisted watermark behind the acknowledged offsets.

Maintenance design measures are listed separately.

| Metric | Source component | Measurement |
|--------|------------------|-------------|
| `pangrova.maintenance.tasks.submitted` | MaintenanceScheduler | Maintenance tasks submitted |
| `pangrova.maintenance.tasks.completed` | MaintenanceScheduler | Maintenance tasks completed |
| `pangrova.maintenance.compaction.bytes` | CompactionTask | Bytes compacted |

### 8.2 Tracing Spans

- `cdc.consume` — CDC batch consumption (SERVER span)
  - `cdc.write` — connector sink staging (INTERNAL)
  - `cdc.commit` — Transaction commit (INTERNAL)
- `maintenance.compact` — Compaction task (SERVER span)
- `maintenance.expire_snapshots` — Snapshot expiration (SERVER span)

### 8.3 Log Events

- CDC consumer start/stop events (INFO)
- Write batch completion with record count and latency (INFO)
- Maintenance task lifecycle events (INFO)
- Deduplication events for at-least-once delivery (DEBUG)

See [14 §7](./observability.md) for structured logging format.



> **CDC execution boundary.** A Coordinator hosting both roles dispatches bounded
> CDC tasks to Worker JVMs. A streaming-only Coordinator stages those ranges locally. Kafka supplies partition inventory and
> ZooKeeper remains an optional coordination backend. Catalog publication precedes
> source acknowledgement, with a failure window between the two operations. See
> [Current Capability Status](../getting-started/capability-status.md).


> **TableMaintenance Authoritative Definition**: The authoritative async interface is defined in §2.9 of this document. All references in [00 §5.1](./overview.md) and [02 §13.2](./query-execution.md) have been updated to match the async signatures here. Method names use `*Async` suffix with `CompletableFuture<*>` return types.


> **Watermark State Machine (§5)**:
>
> | State | Trigger | Next State | Action |
> |-------|---------|------------|--------|
> | INITIALIZING | First event received | ACTIVE | Set initial watermark from event timestamp |
> | ACTIVE | Event within allowed lateness | ACTIVE | Advance watermark |
> | ACTIVE | No events for `idle-timeout` | IDLE | Emit idle watermark, pause advancement |
> | IDLE | New event received | ACTIVE | Resume watermark advancement |
> | ACTIVE | Watermark regression detected | STALE | Log warning, hold current watermark |
> | STALE | Newer event received | ACTIVE | Resume normal advancement |
> | ANY | Unrecoverable error | FAILED | Emit error metric, stop tracker |
