---
title: "Streaming & CDC"
sidebar_label: "Streaming & CDC"
---

# Pangrova — Streaming & CDC

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Current CDC boundary.** Sections 2.4–2.7 retain historical `TableWriter`
> design sketches, not the running write path. The running pipeline uses
> `CdcPipelineLoop` → `CdcTwoPhaseCommitter` → `ConnectorPageSinkCdcWriter` on the
> Coordinator. The source offset and Iceberg/Paimon catalog do not share an
> atomic transaction. `COMMITTING` is now quarantined on ambiguous failure;
> Paimon rejects the context-free CDC commit, and UPDATE phases are not one
> snapshot. See [distributed writes](./distributed-writes.md#cdc-repair-contract-proposed-not-implemented)
> for the required durable Native design and its acceptance criteria. No
> exactly-once/effectively-once production guarantee is currently claimed.

> **Summary.** CDC ingestion, incremental queries, streaming writes, and watermark tracking.

## 0. Product Positioning

The Pangrova streaming module is a **declarative incremental data transformation** engine.
It provides materialized view refresh and CDC-driven incremental ETL capabilities.

**It is NOT a general-purpose stream processing engine.**

Pangrova does not compete with Apache Flink, Spark Structured Streaming, or Kafka Streams.
Those systems provide arbitrary stateful stream processing with complex event processing,
windowed joins, and user-defined operators. Pangrova's streaming module has a narrower,
more focused scope: keeping lake tables fresh via incremental data ingestion.

### Scope Definition

#### What Pangrova Streaming IS

| Capability | Description |
|---|---|
| CDC Ingestion | Consume Debezium/Maxwell/Canal change events from Kafka, write to Iceberg/Paimon tables |
| Incremental ETL | Apply schema-aligned transformations during CDC ingestion (column mapping, type coercion) |
| Materialized View Refresh | Watermark-driven incremental refresh of continuous queries |
| Table Maintenance | Compaction, snapshot expiration, orphan cleanup triggered by write activity |
| Watermark Tracking | Per-table event-time watermark for freshness monitoring and refresh triggering |

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
| Delivery guarantee | At-least-once + dedup | Exactly-once (barriers) | Exactly-once (WAL) |
| Windowing | Refresh interval only | TUMBLE/HOP/SESSION/CUMULATE | Window operations |
| Joins | Not supported in stream | Stream-stream, stream-table | Stream-stream, stream-static |
| User operators | No (declarative only) | Yes (ProcessFunction) | Yes (mapGroupsWithState) |
| Deployment | Embedded in coordinator | Standalone cluster | Spark cluster |

### Migration Notes (v0.2.0)

| Old Name | New Name | Rationale |
|---|---|---|
| `StreamSourceReader` | `CdcSourceReader` | Clarifies this reads CDC events, not arbitrary streams |
| StreamWriteSink (old name) | `CdcWriteSink` | Clarifies this writes CDC data, not arbitrary stream output |
| StreamWriteResult (old name) | `CdcWriteResult` | Consistent with CdcWriteSink |
| `StreamingWriter` | `IncrementalWriter` | Clarifies incremental batch buffering, not stream processing |
| `StreamingWriterConfig` | `IncrementalWriterConfig` | Consistent with IncrementalWriter |

Removed APIs: CheckpointCoordinator and the streaming-local CheckpointStore —
Flink-style barrier checkpointing is not needed for CDC offset-based recovery.

## 1. Unified Batch-Streaming Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Streaming Architecture                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Capability matrix:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Capability         │ Note                        │ Implementation      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  CDC Ingestion      │ Capture database changes in real time│ Debezium/Flink CDC  │   │
│  │  Incremental Query  │ Incremental reads based on Changelog│ Iceberg/Paimon native│   │
│  │  Streaming Ingestion│ Low-latency writes          │ Mini-batch writes   │   │
│  │  Watermark          │ Event-time semantics        │ Table-level Watermark│   │
│  │  Upsert             │ Primary key update          │ MOR/COW modes       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. CDC Data Ingestion

### 2.1 CDC Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           CDC Ingestion Architecture                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Data flow:                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────────────┐   │   │
│  │  │  MySQL  │     │Debezium │     │  Kafka  │     │     Pangrova      │   │   │
│  │  │PostgreSQL│ ──► │ Connector│ ──► │         │ ──► │  CDC Consumer  │   │   │
│  │  │  Oracle │     │         │     │         │     │                 │   │   │
│  │  └─────────┘     └─────────┘     └─────────┘     └────────┬────────┘   │   │
│  │                                                            │            │   │
│  │                                                            ▼            │   │
│  │                                                   ┌─────────────────┐   │   │
│  │                                                   │  Iceberg/Paimon │   │   │
│  │                                                   │     Table       │   │   │
│  │                                                   └─────────────────┘   │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  CDC message format (Debezium):                                                  │
│  {                                                                               │
│    "before": {...},     // Data before change (UPDATE/DELETE)                   │
│    "after": {...},      // Data after change (INSERT/UPDATE)                    │
│    "op": "c|u|d|r",     // Operation type: create/update/delete/read            │
│    "ts_ms": 1234567890, // Event timestamp                                      │
│    "source": {...}      // Source information                                   │
│  }                                                                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Unified Write Architecture

**Design principle**: CDC writes and client writes share the same underlying write component (`TableWriter`); CDC merely adds a parsing layer on top of the write component.

Note: `TableWriter` runs in the Worker-side Java write path. For SQL writes, the Coordinator orchestrates the transaction and schedules write Tasks, while Workers perform the actual data-file writes and report results. The CDC Consumer, as a write service, can be deployed on a Worker or on a dedicated ingestion node, but reuses the same `TableWriter` SPI and transaction semantics.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Unified Write Architecture                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐     ┌─────────────────┐                                    │
│  │  CDC Consumer   │     │  Client Write   │                                    │
│  │  (Debezium parse) │     │  (SQL INSERT)   │                                    │
│  └────────┬────────┘     └────────┬────────┘                                    │
│           │                       │                                              │
│           │ List<CdcRecord>       │ List<Record>                                │
│           └───────────┬───────────┘                                              │
│                       ▼                                                          │
│           ┌───────────────────────┐                                              │
│           │   TableWriter SPI     │  ← Unified write interface                   │
│           │   (Iceberg/Paimon)    │                                              │
│           └───────────┬───────────┘                                              │
│                       │                                                          │
│                       │ Send signal after write() completes                      │
│                       ▼                                                          │
│           ┌───────────────────────┐                                              │
│           │ MaintenanceScheduler  │  ← Unified priority trigger model            │
│           │ (threshold>interval>  │    (threshold > interval > scan)            │
│           │  scan priority)       │                                              │
│           └───────────┬───────────┘                                              │
│                       │                                                          │
│                       ▼                                                          │
│           ┌───────────────────────┐                                              │
│           │ TableMaintenanceSpi   │  ← Unified maintenance interface             │
│           │ (compaction/orphan)   │                                              │
│           └───────────────────────┘                                              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 CDC Consumer

The CDC Consumer is only responsible for parsing Debezium messages, then delegates the actual write to `TableWriter`.

```java
/**
 * CDC Consumer - consumes Debezium messages from Kafka.
 * Only responsible for parsing, delegates actual write to TableWriter.
 */
public class CdcConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final TableWriter writer;  // Unified TableWriter SPI
    private final CdcConfig config;
    private final ObjectMapper objectMapper;
    private volatile boolean running = true;

    public CdcConsumer(CdcConfig config, TableWriter writer) {
        this.config = config;
        this.writer = writer;
        this.objectMapper = new ObjectMapper();
        this.consumer = createConsumer(config);
    }

    /**
     * Consume CDC messages and delegate to TableWriter
     */
    public void consume() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) continue;

            Map<String, List<CdcRecord>> byTable = groupByTable(records);

            for (Map.Entry<String, List<CdcRecord>> entry : byTable.entrySet()) {
                String tableName = entry.getKey();
                List<CdcRecord> cdcRecords = entry.getValue();

                // Convert to unified WriteRecord and delegate to TableWriter
                List<WriteRecord> writeRecords = cdcRecords.stream()
                    .map(WriteRecord::fromCdc)
                    .toList();

                writer.write(tableName, writeRecords, WriteMode.UPSERT,
                    new WriteContext(WriteContext.TransactionMode.SELF_MANAGED, Optional.empty()));
            }

            consumer.commitSync();
        }
    }

    /**
     * Parse Debezium message
     */
    CdcRecord parseDebeziumMessage(String json) {
        JsonNode node = objectMapper.readTree(json);

        CdcOperation op = switch (node.get("op").asText()) {
            case "c", "r" -> CdcOperation.INSERT;
            case "u" -> CdcOperation.UPDATE;
            case "d" -> CdcOperation.DELETE;
            default -> throw new IllegalArgumentException("Unknown op");
        };

        JsonNode after = node.get("after");
        JsonNode before = node.get("before");
        long tsMs = node.get("ts_ms").asLong();

        return CdcRecord.builder()
            .operation(op)
            .before(before != null && !before.isNull() ? parseRow(before) : null)
            .after(after != null && !after.isNull() ? parseRow(after) : null)
            .eventTime(Instant.ofEpochMilli(tsMs))
            .build();
    }
}
```

### 2.4 CDC vs SQL Write: Transaction Orchestration Comparison

> **See also:** [query-execution.md](./query-execution.md) — distributed transaction orchestration for SQL writes

CDC writes and SQL writes share the same `TableWriter SPI`, but their transaction orchestration paths are completely different. This section clarifies the differences between the two to avoid confusion during implementation.

#### Orchestration Path Comparison

| Dimension | SQL Write (INSERT/MERGE) | CDC Write (Kafka Consumer) |
|---|---|---|
| Orchestrator | Coordinator (distributed transaction) | CdcConsumer (single-point transaction) |
| Transaction scope | Parallel writes across multiple Workers → Coordinator coordinates Commit | Single Consumer thread → direct Commit |
| Resource management | Goes through the Coordinator's ResourceQueue quota check | **Does not** go through the Coordinator; requires independent quota checking (see below) |
| Memory management | Task-level memory quota, allocated by the Java MemoryArbitrator | Consumer process-level memory, managed by the JVM heap |
| Failure handling | Coordinator reschedules to another Worker | Consumer restarts and recovers from the Kafka offset |
| Concurrency control | Coordinator controls the number of parallel Writer Tasks | The number of Kafka partitions determines parallelism |

#### CDC Write Resource Management

The CDC Consumer bypasses the Coordinator and writes directly, so it needs an independent resource management mechanism:

```
CDC Consumer Resource Check Flow:
  1. On startup, the Consumer registers with the Coordinator (POST /v1/cdc/register)
     → The Coordinator allocates CDC-specific resource quota (memory, concurrent write count)
  2. Before each write(), check the local quota:
     → Current write concurrency < max_concurrent_writes
     → Accumulated uncommitted data volume < max_pending_bytes
  3. When the quota is exceeded, pause consumption (consumer.pause()) and resume after writes complete
  4. Multi-tenant scenario: the CDC Consumer is bound to a tenant_id, and quota is deducted from the tenant quota
     (see multi-tenancy.md §4.1 QuotaChecker)
```



**`POST /v1/cdc/register` API Specification**:

Request:
```json
{
  "consumer_id": "cdc-consumer-001",
  "tenant_id": "tenant-abc",
  "target_tables": ["iceberg.db.orders", "iceberg.db.users"],
  "requested_quota": {
    "max_concurrent_writes": 4,
    "max_pending_bytes": 268435456
  }
}
```

Response (200 OK):
```json
{
  "stream_id": "cdc-stream-xyz",
  "allocated_quota": {
    "max_concurrent_writes": 4,
    "max_pending_bytes": 268435456
  },
  "heartbeat_interval_ms": 30000,
  "lease_ttl_ms": 120000
}
```

Error Responses:
| HTTP Status | Error Code | Condition |
|---|---|---|
| 400 | `CDC_PARSE_ERROR` (1401) | Invalid request body |
| 403 | `TENANT_ACCESS_DENIED` (1603) | Tenant not authorized for target tables |
| 429 | `CDC_QUOTA_EXCEEDED` (1404) | Tenant CDC quota exhausted |
| 503 | `RESOURCE_EXHAUSTED` (2001) | Coordinator cannot allocate requested resources |

**Heartbeat / Lease Renewal**: After registration, the Consumer sends periodic heartbeats (`POST /v1/cdc/{streamId}/heartbeat`) at `heartbeat_interval_ms`. If no heartbeat is received within `lease_ttl_ms`, the Coordinator reclaims the allocated quota and marks the stream as EXPIRED. The Consumer must re-register to resume.

**Behavioral differences of the TableWriter SPI in the two modes**:

| SPI Method | SQL Write Mode | CDC Write Mode |
|---|---|---|
| `write()` / `writeBatch()` | Produces write fragments; the coordinator owns the final atomic catalog commit | Writes data files **and** commits internally (single-point transaction) |
| `WriteMode` | `APPEND` (INSERT) or `OVERWRITE` | `UPSERT` (CDC default) |
| `WriteContext.transactionMode` | `COORDINATOR_MANAGED` | `SELF_MANAGED` |
| Transaction commit | Coordinator collects worker fragments, journals commit intent, and performs the atomic catalog commit | `TableWriter.write()` internally calls `table.newAppend().commit()` |

> **Design Decision**: TableWriter SPI accepts a `WriteContext` parameter (see [§2.6](#26-table-writer-spi) for authoritative definition) to distinguish the two modes. `WriteContext.transactionMode` is `COORDINATOR_MANAGED` (SQL) or `SELF_MANAGED` (CDC). In `COORDINATOR_MANAGED` mode, `write()`/`writeBatch()` only produces a DataFile list without committing; in `SELF_MANAGED` mode, `write()` completes the full transaction commit internally.

### 2.5 CDC Write Semantics

> **Guarantee Level: at-least-once with idempotent writes**

The CDC Consumer uses a **write-then-commit-offset** model: it first completes the data write and commits the transaction, then commits the Kafka offset. This ensures data is not lost, but under failure scenarios it may produce duplicate consumption, which is eliminated through primary-key idempotent deduplication.

#### Offset Commit Point

The offset commit happens after the data-write transaction succeeds (i.e., after `TableWriter.write()` returns), rather than before or as an atomic commit:

```
TableWriter.write() commit success  →  consumer.commitSync()
         ▲                                      ▲
         │                                      │
    Data is durable                    Offset is durable
```

This means:
- If `write()` succeeds but `commitSync()` fails, the Consumer will re-consume the already-written messages after restart
- If `write()` fails, the offset is not committed, and the Consumer automatically retries after restart

#### Idempotency Strategy

On write, deduplication is performed by `(primary_key, event_timestamp)`, ensuring duplicate consumption does not produce duplicate data:

| Table Format | Dedup Mechanism | Description |
|---|---|---|
| Iceberg | Equality Delete + Insert | First write an equality delete (by primary key + event timestamp), then write the insert; duplicate writes produce identical delete + insert, and results are consistent after compaction |
| Paimon | Native Changelog Dedup | The Paimon LSM Tree natively deduplicates by primary key; duplicate writes of the same key are merged automatically |

#### Write-Commit Lifecycle

```
1. consumer.poll()           → Get message batch from Kafka
2. parseDebeziumMessage()    → Convert to List<CdcRecord>
3. WriteRecord.fromCdc()     → Convert to List<WriteRecord>
4. TableWriter.write()       → Write data files + commit transaction
5. consumer.commitSync()     → Commit Kafka offset
```

If step 4 succeeds but step 5 fails: after restart, the Consumer re-consumes from the last committed offset, and primary-key idempotent deduplication guarantees eventual data consistency.

#### Failure Recovery Flow

**Consumer Restart Behavior**: After the CdcConsumer process crashes or is killed, the Kafka consumer group triggers a rebalance, and the new instance resumes consumption from the last committed offset. Because the offset is committed after the data write, two failure windows exist:

| Failure Point | Symptom | Recovery Behavior |
|---|---|---|
| Before `TableWriter.write()` | Data not written, offset not committed | Consumer restarts, re-polls same batch, normal retry |
| During `TableWriter.write()` (partial) | Iceberg/Paimon transaction uncommitted | Transaction rollback on timeout; consumer restarts, re-polls and retries full batch |
| After `TableWriter.write()`, before `commitSync()` | Data written and committed, offset not committed | Consumer restarts, re-consumes already-written messages; **duplicate detection** eliminates duplicates |
| After `commitSync()` | Both durable | No recovery needed |

**Duplicate Detection**: Duplicate records produced by duplicate consumption are eliminated via `(primary_key, event_timestamp)` deduplication:

- **Iceberg**: Duplicate writes produce identical equality delete + insert pairs. After compaction merges them, multiple writes of the same `(pk, event_ts)` are equivalent to a single write.
- **Paimon**: The LSM Tree automatically merges by primary key, keeping only the latest version for duplicate writes of the same key.

**Guarantee Level**: at-least-once delivery + idempotent writes = **effectively-once** semantics. It does not provide exactly-once (because the Kafka offset and the Iceberg/Paimon transaction cannot be committed atomically), but achieves an equivalent effect through idempotent deduplication.

#### CDC Write-Commit-Offset Sequence Diagram

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as CdcConsumer
    participant P as Parser
    participant W as TableWriter
    participant S as Storage<br/>(Iceberg/Paimon)

    Note over K,S: === Normal Flow ===

    C->>K: poll()
    K-->>C: ConsumerRecords batch
    C->>P: parseDebeziumMessage()
    P-->>C: List&lt;CdcRecord&gt;
    C->>C: WriteRecord.fromCdc()
    C->>W: write(tableName, records, UPSERT)
    W->>S: Write data files
    W->>S: Commit transaction
    S-->>W: Commit success
    W-->>C: WriteResult
    C->>K: commitSync()
    K-->>C: Offset committed
    Note over K,S: ✅ Both data and offset durable

    Note over K,S: === Failure: crash after write, before commitSync ===

    C->>K: poll()
    K-->>C: ConsumerRecords batch
    C->>P: parseDebeziumMessage()
    P-->>C: List&lt;CdcRecord&gt;
    C->>W: write(tableName, records, UPSERT)
    W->>S: Write data files
    W->>S: Commit transaction
    S-->>W: Commit success
    W-->>C: WriteResult
    Note over C: ❌ Consumer crashes before commitSync()

    Note over K,S: === Recovery: restart and dedup ===

    C->>K: poll() (from last committed offset)
    K-->>C: Same ConsumerRecords batch (re-delivered)
    C->>P: parseDebeziumMessage()
    P-->>C: List&lt;CdcRecord&gt; (duplicates)
    C->>W: write(tableName, records, UPSERT)
    W->>S: Write data files (dedup by primary_key + event_timestamp)
    W->>S: Commit transaction
    S-->>W: Commit success (duplicates eliminated)
    W-->>C: WriteResult
    C->>K: commitSync()
    K-->>C: Offset committed
    Note over K,S: ✅ Recovered: data consistent via idempotent dedup
```

### 2.6 Table Writer SPI

> **This section is the authoritative definition of the unified write SPI.**
> All other documents (overview.md §5.1, query-execution.md §6, connectors.md §5.1) reference this section.

Unified write interface shared by CDC ingestion and SQL writes. TableWriter is the single upper-layer entry point for all write operations. Internally it delegates to `ConnectorPageSinkProvider` (defined in [04 §1.2](./connectors.md)) for format-specific persistence — see [connectors.md §5.1](./connectors.md) for the delegation model.



```java
/**
 * Table Writer SPI - unified interface for all write paths.
 * Shared by CDC ingestion (record-oriented) and SQL writes (batch-oriented).
 *
 * This is the AUTHORITATIVE definition. query-execution.md §6 DataWriter is
 * DEPRECATED — use TableWriter for all new write path implementations.
 *
 * After each write()/writeBatch(), notifies MaintenanceScheduler to check
 * if maintenance should be triggered based on time interval.
 */
public interface TableWriter extends AutoCloseable {

    /**
     * Write records to the table (CDC path — record-oriented).
     *
     * @param tableName  fully qualified table name
     * @param records    list of WriteRecord (CDC or client records)
     * @param writeMode  APPEND, UPSERT, or OVERWRITE
     * @param context    write context specifying transaction mode and metadata
     * @return write result containing data file list and statistics
     */
    WriteResult write(String tableName, List<WriteRecord> records,
                      WriteMode writeMode, WriteContext context);

    /**
     * Write an Arrow batch to the table (SQL path — batch-oriented).
     * Accepts VectorSchemaRoot directly from the native execution engine,
     * avoiding expensive columnar-to-row conversion.
     *
     * Resolves IC-WR-004: bridges native VectorSchemaRoot output with
     * the TableWriter input interface for zero-conversion SQL writes.
     *
     * @param tableName  fully qualified table name
     * @param batch      Arrow VectorSchemaRoot produced by native Velox engine
     * @param writeMode  APPEND or OVERWRITE (UPSERT not supported for batch path)
     * @param context    write context specifying transaction mode and metadata
     * @return write result containing data file list and statistics
     */
    WriteResult writeBatch(String tableName, VectorSchemaRoot batch,
                           WriteMode writeMode, WriteContext context);

    TableFormat format();
}

/**
 * Write context — distinguishes transaction orchestration modes.
 * Resolves IC-WR-008: provides transaction mode so TableWriter implementations
 * can decide whether to commit internally or defer to Coordinator.
 *
 * @param transactionMode  COORDINATOR_MANAGED (SQL) or SELF_MANAGED (CDC)
 * @param sessionContext   optional session metadata for multi-tenant isolation
 */
public record WriteContext(
    TransactionMode transactionMode,
    Optional<SessionContext> sessionContext
) {
    public enum TransactionMode {
        /** SQL write path: Coordinator orchestrates commit across multiple Workers. */
        COORDINATOR_MANAGED,
        /** CDC write path: TableWriter commits transaction internally per batch. */
        SELF_MANAGED
    }
}
```

**Write path selection:**

| Caller | Method | WriteContext.transactionMode | Behavior |
|--------|--------|-----------------------------|----------|
| SQL INSERT (Worker) | `writeBatch(VectorSchemaRoot)` | `COORDINATOR_MANAGED` | Writes data files only; does NOT commit. Coordinator collects DataFile list and commits atomically. |
| CDC Consumer | `write(List<WriteRecord>)` | `SELF_MANAGED` | Writes data files AND commits transaction internally. |

```java
/**
 * Unified write record - abstracts CDC and client records.
 */
public record WriteRecord(
    Map<String, Object> data,
    WriteOperation operation,
    Instant eventTime
) {
    public enum WriteOperation { INSERT, UPDATE, DELETE }

    // Convert from CdcRecord (used by CDC Consumer)
    public static WriteRecord fromCdc(CdcRecord cdc) {
        return new WriteRecord(
            cdc.operation() == CdcOperation.DELETE ? cdc.before() : cdc.after(),
            switch (cdc.operation()) {
                case INSERT -> WriteOperation.INSERT;
                case UPDATE -> WriteOperation.UPDATE;
                case DELETE -> WriteOperation.DELETE;
            },
            cdc.eventTime()
        );
    }

    // Convert from client Record (used by SQL writes)
    public static WriteRecord fromClient(Map<String, Object> data, WriteOperation op) {
        return new WriteRecord(data, op, Instant.now());
    }
}

/**
 * Unified write record - abstracts CDC and client records
 */
public record WriteRecord(
    Map<String, Object> data,
    WriteOperation operation,
    Instant eventTime
) {
    public enum WriteOperation { INSERT, UPDATE, DELETE }

    // Convert from CdcRecord (used by CDC Consumer)
    public static WriteRecord fromCdc(CdcRecord cdc) {
        return new WriteRecord(
            cdc.operation() == CdcOperation.DELETE ? cdc.before() : cdc.after(),
            switch (cdc.operation()) {
                case INSERT -> WriteOperation.INSERT;
                case UPDATE -> WriteOperation.UPDATE;
                case DELETE -> WriteOperation.DELETE;
            },
            cdc.eventTime()
        );
    }

    // Convert from client Record (used by SQL writes)
    public static WriteRecord fromClient(Map<String, Object> data, WriteOperation op) {
        return new WriteRecord(data, op, Instant.now());
    }
}
```

### 2.7 Iceberg Table Writer (Hybrid Delete Strategy)

```java
/**
 * Iceberg Table Writer with hybrid delete strategy.
 *
 * Hybrid Delete Strategy:
 * - Same-batch updates: Position Delete (same file, query engine friendly)
 * - Cross-batch updates: Equality Delete (needs async compaction)
 */
public class IcebergTableWriter implements TableWriter {

    private final Catalog catalog;
    private final MaintenanceScheduler scheduler;

    @Override
    public WriteResult write(String tableName, List<WriteRecord> records,
                             WriteMode writeMode, WriteContext context) {
        Table table = catalog.loadTable(TableIdentifier.parse(tableName));
        List<String> primaryKeys = getPrimaryKeys(table);

        // Group by primary key to detect same-batch updates
        Map<Object, List<WriteRecord>> byKey = groupByPrimaryKey(records, primaryKeys);

        List<Record> inserts = new ArrayList<>();
        List<PositionDelete> positionDeletes = new ArrayList<>();  // Same-batch
        List<Record> equalityDeletes = new ArrayList<>();          // Cross-batch

        for (var entry : byKey.entrySet()) {
            List<WriteRecord> keyRecords = entry.getValue();

            if (keyRecords.size() > 1) {
                // Multiple updates to same key in same batch → Position Delete
                processSameBatchUpdates(table, keyRecords, inserts, positionDeletes);
            } else {
                WriteRecord record = keyRecords.get(0);
                switch (record.operation()) {
                    case INSERT -> inserts.add(toIcebergRecord(record.data(), table.schema()));
                    case UPDATE -> {
                        equalityDeletes.add(toIcebergRecord(record.data(), table.schema()));
                        inserts.add(toIcebergRecord(record.data(), table.schema()));
                    }
                    case DELETE -> equalityDeletes.add(toIcebergRecord(record.data(), table.schema()));
                }
            }
        }

        WriteResult result = commitWithHybridDeletes(table, inserts, positionDeletes,
                                                      equalityDeletes, primaryKeys);

        // Notify scheduler to check if maintenance should be triggered
        scheduler.onWriteComplete(tableName, result);

        return result;
    }
}
```

### 2.8 Paimon Table Writer

```java
/**
 * Paimon Table Writer with native changelog support.
 * Paimon handles changelog natively, no need for hybrid delete strategy.
 */
public class PaimonTableWriter implements TableWriter {

    private final ConnectorManager connectorManager;
    private final MaintenanceScheduler scheduler;

    @Override
    public WriteResult write(String tableName, List<WriteRecord> records,
                             WriteMode writeMode, WriteContext context) {
        FileStoreTable table = loadPaimonTable(tableName);

        var write = table.newWrite(UUID.randomUUID().toString());
        var commit = table.newCommit(UUID.randomUUID().toString());

        for (WriteRecord record : records) {
            RowKind rowKind = switch (record.operation()) {
                case INSERT -> RowKind.INSERT;
                case UPDATE -> RowKind.UPDATE_AFTER;
                case DELETE -> RowKind.DELETE;
            };

            var row = toPaimonRow(record.data(), rowKind);
            write.write(row);
        }

        var messages = write.prepareCommit(true, 0);
        commit.commit(0, messages);

        WriteResult result = new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());

        // Notify scheduler to check if maintenance should be triggered
        scheduler.onWriteComplete(tableName, result);

        return result;
    }
}

---

### 2.9 TableMaintenanceSpi

A unified table maintenance interface supporting compaction, orphan cleanup, and snapshot expiration.

```java
/**
 * Table Maintenance SPI - unified interface for table maintenance operations.
 * All operations are async and non-blocking.
 * Triggered by MaintenanceScheduler based on time interval (not threshold).
 */
public interface TableMaintenanceSpi {

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

```java
/**
 * Unified Maintenance Scheduler - single priority-based trigger model.
 *
 * Trigger priority (high to low):
 * 1. Threshold trigger (scan-discovered): equality delete files >= maxEqualityDeleteFiles → immediate
 * 2. Interval trigger (write signal): time since last maintenance >= compactionInterval → trigger
 * 3. Scheduled scan trigger: smallFiles >= minFilesToCompact AND interval elapsed → trigger
 *
 * Concurrency: global maxConcurrentTasks with bounded priority queue and backpressure.
 */
public class MaintenanceScheduler implements AutoCloseable {

    private final TableMaintenanceSpi maintenance;
    private final MetadataService metadataService;
    private final MaintenanceConfig config;
    private final ScheduledExecutorService scanScheduler;
    private final ExecutorService taskExecutor;
    private final ConcurrentHashMap<String, TableMaintenanceState> tableStates;
    private final Semaphore concurrencyLimiter;  // permits = maxConcurrentTasks
    private final PriorityBlockingQueue<MaintenanceTask> taskQueue;

    private final CacheEvictionNotifier cacheEvictionNotifier;  // nullable; see 09 §8.1

    //

    /**
     * Start the scheduler - enables periodic table scanning.
     */
    public void start() {
        // Periodic scan: evaluate all tables with unified priority model
        scanScheduler.scheduleAtFixedRate(
            this::scanAllTablesWithPriority,
            config.compactionScanInterval().toMinutes(),
            config.compactionScanInterval().toMinutes(),
            TimeUnit.MINUTES);

        // Periodic scan for orphan cleanup (lower frequency)
        scanScheduler.scheduleAtFixedRate(
            this::scanAndTriggerOrphanCleanup,
            config.orphanCleanupScanInterval().toHours(),
            config.orphanCleanupScanInterval().toHours(),
            TimeUnit.HOURS);

        // Queue consumer: drains taskQueue respecting concurrency limit
        taskExecutor.submit(this::processTaskQueue);
    }

    /**
     * Called by TableWriter after each write() completes.
     * Evaluates interval trigger (Priority 2).
     */
    public void onWriteComplete(String tableName, WriteResult result) {
        TableMaintenanceState state = tableStates.computeIfAbsent(
            tableName, k -> new TableMaintenanceState());

        // Skip if task already in-flight for this table
        if (state.hasInFlightTask()) return;

        Instant now = Instant.now();
        if (shouldTriggerByInterval(state.lastCompactionTime, config.compactionInterval(), now)) {
            enqueueWithBackpressure(new MaintenanceTask(tableName, TriggerPriority.INTERVAL, now));
        }
    }

    /**
     * Scan all tables and evaluate triggers with unified priority.
     * Priority 1 (threshold) checked first, then Priority 3 (scan).
     */
    private void scanAllTablesWithPriority() {
        for (String tableName : metadataService.listAllTables()) {
            if (!isCompactionEnabled(tableName)) continue;

            TableMaintenanceState state = tableStates.computeIfAbsent(
                tableName, k -> new TableMaintenanceState());
            if (state.hasInFlightTask()) continue;

            MaintenanceStatus status = maintenance.getMaintenanceStatus(tableName);
            Instant now = Instant.now();

            // Priority 1: Threshold trigger — immediate, bypass interval check
            if (status.equalityDeleteFiles() >= config.maxEqualityDeleteFiles()) {
                enqueueWithBackpressure(new MaintenanceTask(tableName, TriggerPriority.THRESHOLD, now));
                continue;  // Highest priority wins, skip lower checks
            }

            // Priority 3: Scan trigger — requires both threshold AND interval
            if (status.smallFilesCount() >= config.minFilesToCompact()
                    && shouldTriggerByInterval(state.lastCompactionTime, config.compactionInterval(), now)) {
                enqueueWithBackpressure(new MaintenanceTask(tableName, TriggerPriority.SCHEDULED_SCAN, now));
            }
        }
    }

    /**
     * Enqueue task with backpressure: drop lowest priority when queue is full.
     *
     *
     */
    private void enqueueWithBackpressure(MaintenanceTask task) {
        if (taskQueue.size() < config.taskQueueCapacity()) {
            taskQueue.offer(task);
        } else {
            // Queue full: find the lowest priority (highest ordinal) task in queue
            MaintenanceTask lowestPriority = null;
            for (MaintenanceTask queued : taskQueue) {
                if (lowestPriority == null || queued.priority().compareTo(lowestPriority.priority()) > 0) {
                    lowestPriority = queued;
                }
            }
            if (lowestPriority != null && task.priority().compareTo(lowestPriority.priority()) < 0) {
                taskQueue.remove(lowestPriority);  // Remove lowest priority
                taskQueue.offer(task);              // Add higher priority task
            } else {
                // New task is lowest priority — drop it
                metrics.increment("maintenance.tasks.dropped");
            }
        }
    }

    /**
     * Process task queue respecting global concurrency limit.
     *
     * Design:
     * - Blocks on taskQueue.take() until a task is available
     * - Acquires concurrency permit before execution (blocks if all slots busy)
     * - Handles task timeout via Future.get(taskTimeout)
     * - On task completion: updates table state, releases permit
     * - On task failure: logs error, increments failure metric, releases permit
     * - On task timeout: cancels task, marks as failed, releases permit
     *
     * Backpressure integration:
     * - enqueueWithBackpressure() controls queue admission (see above)
     * - concurrencyLimiter controls execution admission
     * - Together they form a two-stage backpressure: queue capacity + execution slots
     */
    private void processTaskQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Block until a task is available (priority-ordered: THRESHOLD > INTERVAL > SCAN)
                MaintenanceTask task = taskQueue.take();

                // Block until a concurrency slot is available
                concurrencyLimiter.acquire();

                TableMaintenanceState state = tableStates.computeIfAbsent(
                    task.tableName(), k -> new TableMaintenanceState());

                // Double-check: skip if another task is already in-flight for this table
                if (!state.trySetInFlight(true)) {
                    concurrencyLimiter.release();
                    continue;
                }

                taskExecutor.submit(() -> {
                    try {
                        Future<CompactionResult> future =
                            maintenance.compactAsync(task.tableName()).toCompletableFuture();

                        // Enforce per-task timeout
                        CompactionResult result = future.get(
                            config.taskTimeout().toMillis(), TimeUnit.MILLISECONDS);

                        // Update table state on success
                        state.setLastCompactionTime(Instant.now());
                        metrics.increment("maintenance.tasks.completed");
                        metrics.record("maintenance.compaction.duration",
                            Duration.between(task.createdAt(), Instant.now()).toMillis());

                        // Notify cache layer if files were rewritten (snapshot may have changed)
                        if (cacheEvictionNotifier != null && !result.rewrittenFileIds().isEmpty()) {
                            cacheEvictionNotifier.notifyCompactionCompleted(
                                task.tableName(), result.rewrittenFileIds());
                        }

                    } catch (TimeoutException e) {
                        // Task exceeded taskTimeout — force cancel
                        log.warn("Maintenance task timed out for table {}, trigger={}",
                                 task.tableName(), task.priority());
                        metrics.increment("maintenance.tasks.timeout");

                    } catch (ExecutionException e) {
                        log.error("Maintenance task failed for table {}: {}",
                                  task.tableName(), e.getCause().getMessage());
                        metrics.increment("maintenance.tasks.failed");

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                    } finally {
                        state.trySetInFlight(false);
                        concurrencyLimiter.release();
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private boolean shouldTriggerByInterval(Instant lastTime, Duration interval, Instant now) {
        if (lastTime == null) return true;
        return Duration.between(lastTime, now).compareTo(interval) >= 0;
    }
}

/**
 * Trigger priority enum — used for queue ordering and backpressure decisions.
 */
public enum TriggerPriority {
    THRESHOLD,       // Priority 1 (highest) — equality delete threshold exceeded
    INTERVAL,        // Priority 2 — time interval elapsed after write
    SCHEDULED_SCAN   // Priority 3 (lowest) — periodic scan discovered small files
}

public record MaintenanceTask(
    String tableName,
    TriggerPriority priority,
    Instant createdAt
) implements Comparable<MaintenanceTask> {
    @Override
    public int compareTo(MaintenanceTask other) {
        // Lower ordinal = higher priority = should be dequeued first
        return this.priority.compareTo(other.priority);
    }
}

public record MaintenanceConfig(
    // Interval-based triggers (write-triggered, Priority 2)
    Duration compactionInterval,       // Default: 60 minutes
    Duration orphanCleanupInterval,    // Default: 360 minutes (6 hours)
    Duration snapshotExpirationInterval, // Default: 720 minutes (12 hours)

    // Scan intervals (scan-triggered, Priority 3)
    Duration compactionScanInterval,   // Default: 60 minutes
    Duration orphanCleanupScanInterval, // Default: 24 hours

    // Thresholds (threshold-triggered, Priority 1)
    int minFilesToCompact,             // Default: 5 (scan trigger threshold)
    int maxEqualityDeleteFiles,        // Default: 10 (immediate trigger threshold)

    // Retention settings
    Duration orphanFileAge,            // Default: 3 days
    Duration snapshotRetention,        // Default: 7 days
    int snapshotRetainCount,           // Default: 100

    // Concurrency and backpressure
    int threads,                       // Default: 4 (task execution threads; executor sized as threads+1)
    int maxConcurrentTasks,            // Default: 4 (global concurrent limit, must be <= threads)
    int taskQueueCapacity,             // Default: 16 (bounded queue size)
    Duration taskTimeout               // Default: 30 minutes (per-task timeout)
) {
    public static MaintenanceConfig defaults() {
        return new MaintenanceConfig(
            Duration.ofMinutes(60),
            Duration.ofMinutes(360),
            Duration.ofMinutes(720),
            Duration.ofMinutes(60),
            Duration.ofHours(24),
            5,
            10,
            Duration.ofDays(3),
            Duration.ofDays(7),
            100,
            4,   // threads: task execution threads (executor = threads+1)
            4,
            16,
            Duration.ofMinutes(30)
        );
    }
}
```

---

## 3. Incremental Query

### 3.1 Incremental Query Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Incremental Query Architecture                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Incremental read modes:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Mode               │ Note                        │ Applicable scenario │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Snapshot Diff      │ Difference between two snapshots│ Batch incremental sync│   │
│  │  Changelog Scan     │ Read the Changelog stream  │ Streaming consumption│   │
│  │  Append Only        │ Read only newly added data  │ Append scenarios    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Iceberg incremental read:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Snapshot 100 ──────────────────────────────────► Snapshot 105         │   │
│  │       │                                                │                │   │
│  │       │  New files: data-001.parquet, data-002.parquet│                │   │
│  │       │  Deleted files: delete-001.parquet             │                │   │
│  │       │                                                │                │   │
│  │       └────────────────────────────────────────────────┘                │   │
│  │                                                                          │   │
│  │  Incremental result = new-file data - rows matched by delete files        │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

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

### 4.1 Mini-Batch Writes

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Streaming Ingestion Architecture                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Mini-Batch write flow:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Data Stream ──► Buffer ──► Batch Writer ──► Commit                     │   │
│  │                    │                            │                        │   │
│  │                    │ Trigger conditions:        │                        │   │
│  │                    │ • Data volume >= 128MB     │                        │   │
│  │                    │ • Time >= 1 minute         │                        │   │
│  │                    │ • Row count >= 1,000,000   │                        │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Write latency vs. file count trade-off:                                         │
│  • Low latency (seconds) → many small files → frequent Compaction needed        │
│  • High latency (minutes) → fewer large files → less Compaction pressure        │
│  • Recommended: 1-minute Mini-Batch + background automatic Compaction           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Streaming Ingestion API

```java
/**
 * Streaming writer
 *
 *
 */
public class StreamingWriter implements AutoCloseable {

    private final String tableName;
    private final TableWriter tableWriter;  // Delegates to unified TableWriter SPI (§2.6)
    private final StreamingWriterConfig config;
    private final List<Record> buffer = new ArrayList<>();
    private final ScheduledExecutorService scheduler;

    private long bufferBytes = 0;
    private Instant lastFlushTime = Instant.now();

    public StreamingWriter(String tableName, TableWriter tableWriter, StreamingWriterConfig config) {
        this.tableName = tableName;
        this.tableWriter = tableWriter;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Write a single record
     */
    public void write(Record record) {
        synchronized (buffer) {
            buffer.add(record);
            bufferBytes += estimateSize(record);

            if (shouldFlush()) {
                flush();
            }
        }
    }

    /**
     * Batch write
     */
    public void writeBatch(List<Record> records) {
        synchronized (buffer) {
            buffer.addAll(records);
            bufferBytes += records.stream().mapToLong(this::estimateSize).sum();

            if (shouldFlush()) {
                flush();
            }
        }
    }

    private boolean shouldFlush() {
        return bufferBytes >= config.getMaxBufferBytes() ||
               buffer.size() >= config.getMaxBufferRows() ||
               Duration.between(lastFlushTime, Instant.now()).compareTo(config.getMaxBufferTime()) > 0;
    }

    /**
     * Flush buffer via TableWriter SPI (not direct Iceberg API).
     * Uses SELF_MANAGED transaction mode — TableWriter commits internally.
     * MaintenanceScheduler is notified by TableWriter.write() automatically.
     */
    public void flush() {
        List<Record> toWrite;
        synchronized (buffer) {
            if (buffer.isEmpty()) return;
            toWrite = new ArrayList<>(buffer);
            buffer.clear();
            bufferBytes = 0;
            lastFlushTime = Instant.now();
        }

        // Delegate to TableWriter SPI — unified write path
        List<WriteRecord> writeRecords = toWrite.stream()
            .map(r -> WriteRecord.fromClient(r.toMap(), WriteRecord.WriteOperation.INSERT))
            .toList();

        tableWriter.write(tableName, writeRecords, WriteMode.APPEND,
            new WriteContext(WriteContext.TransactionMode.SELF_MANAGED, Optional.empty()));

        log.info("Flushed {} records to {} via TableWriter", toWrite.size(), tableName);
    }

    @Override
    public void close() {
        flush();  // Flush remaining data before closing
        scheduler.shutdown();
    }
}
```

---

## 5. Watermark Management

### 5.1 Watermark Concept

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Watermark Management                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Watermark definition:                                                           │
│  • Indicates "all data with event time <= Watermark has arrived"                │
│  • Used to handle out-of-order data and trigger window computation              │
│                                                                                  │
│  Timeline example:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Event Time:    10:00  10:01  10:02  10:03  10:04  10:05               │   │
│  │                   │      │      │      │      │      │                 │   │
│  │  Data Arrival:   ─●─────●──────●──────●──────●──────●─────────────────│   │
│  │                   │      │      │      │      │      │                 │   │
│  │  Watermark:      ─────────────────────●──────────────────────────────│   │
│  │                                    10:02                               │   │
│  │                                                                        │   │
│  │  Meaning: all data before 10:02 has arrived, so the 10:00-10:02 window computation can be triggered│   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

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

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Upsert Modes                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Mode comparison:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Mode    │ Write method          │ Read method       │ Applicable scenario│   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  COW     │ Read-merge-rewrite    │ Direct read       │ Read-heavy, write-light│   │
│  │  MOR     │ Append + Delete File │ Merge on read     │ Write-heavy, read-light│   │
│  │  Paimon  │ LSM Tree             │ Merge on read     │ High-frequency updates│   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Iceberg MOR (Merge-On-Read):                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Write:                                                                  │   │
│  │  1. New data written to Data File                                       │   │
│  │  2. Deletes/updates written to Delete File (Position Delete or Equality Delete)│   │
│  │                                                                          │   │
│  │  Read:                                                                   │   │
│  │  1. Read Data File                                                      │   │
│  │  2. Apply Delete File filtering                                         │   │
│  │  3. Return the merged result                                            │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

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

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.cdc.records.consumed` | CdcConsumer | CDC records consumed |
| `pangrova.cdc.records.written` | TableWriter | Records written via CDC path |
| `pangrova.cdc.consumer.lag` | CdcConsumer | Kafka consumer lag |
| `pangrova.cdc.commit.latency` | TableWriter | Write + commit latency |
| `pangrova.maintenance.tasks.submitted` | MaintenanceScheduler | Maintenance tasks submitted |
| `pangrova.maintenance.tasks.completed` | MaintenanceScheduler | Maintenance tasks completed |
| `pangrova.maintenance.compaction.bytes` | CompactionTask | Bytes compacted |

### 8.2 Tracing Spans

- `cdc.consume` — CDC batch consumption (SERVER span)
  - `cdc.write` — TableWriter write operation (INTERNAL)
  - `cdc.commit` — Transaction commit (INTERNAL)
- `maintenance.compact` — Compaction task (SERVER span)
- `maintenance.expire_snapshots` — Snapshot expiration (SERVER span)

### 8.3 Log Events

- CDC consumer start/stop events (INFO)
- Write batch completion with record count and latency (INFO)
- Maintenance task lifecycle events (INFO)
- Deduplication events for at-least-once delivery (DEBUG)

See [14 §7](./observability.md) for structured logging format.



> **Write Interface Hierarchy**: `TableWriter` (§2.6) is the unified upper-layer write entry point. It internally delegates to `ConnectorPageSinkProvider` ([04 §1.2](./connectors.md)) for format-specific file persistence. See [04 §5.1](./connectors.md) for the complete delegation model diagram. `DataWriter` (02 §6) is deprecated — all write paths should use `TableWriter`.


> **CDC Consumer Deployment**: CDC Consumer runs as an embedded component within Worker processes (default) or as standalone processes. The deployment model is defined here in §3. For cluster-level deployment topology, see [distributed.md §2](./distributed.md). CDC Consumer instances register with ZooKeeper for partition assignment coordination.


> **TableMaintenanceSpi Authoritative Definition**: The authoritative async interface is defined in §2.9 of this document. All references in [00 §5.1](./overview.md) and [02 §13.2](./query-execution.md) have been updated to match the async signatures here. Method names use `*Async` suffix with `CompletableFuture<*>` return types.


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
