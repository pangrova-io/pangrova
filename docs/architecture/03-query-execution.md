---
title: Query Execution
sidebar_label: Query Execution
---

# Pangrova — Query Execution

> **Summary.** The end-to-end query lifecycle — parse, optimize, Substrait conversion, native execution — plus the REST API, the exchange protocol, and the write path.

## 1. End-to-End Query Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           End-to-End Query Flow                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Client                                                                          │
│    │ SQL: SELECT * FROM iceberg.db.orders WHERE date > '2024-01-01'             │
│    ▼                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         Coordinator                                      │    │
│  │                                                                          │    │
│  │  2. SQL Parser (ANTLR4) → AST                                           │    │
│  │  3. Semantic Analyzer → bind Catalog/Schema/Table, type checking        │    │
│  │  4. Calcite Optimizer → logical optimization + physical optimization    │    │
│  │  5. Plan Fragmenter → generate distributed exec plan, split into Stages │    │
│  │  6. Substrait Converter → Calcite RelNode → Substrait Plan              │    │
│  │  7. Split Manager → call Connector SPI, generate Splits                 │    │
│  │  8. Task Scheduler → assign Splits to Workers                           │    │
│  │                                                                          │    │
│  └──────────────────────────────┬──────────────────────────────────────────┘    │
│                                 │ HTTP: TaskDescriptor (Substrait + Splits)     │
│                                 ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        Output Worker (Result)                            │    │
│  │                                                                          │    │
│  │  10. Task Manager → receive task, validate, enqueue                     │    │
│  │      │ JNI Call                                                         │    │
│  │      ▼                                                                  │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │  │  Native Layer (C++)                                              │   │    │
│  │  │  11. Substrait Parser → Protobuf → Velox PlanNode               │   │    │
│  │  │  12. Velox Task → create Task, build Pipeline, register Splits │   │    │
│  │  │  13. Pipeline Execution → vectorized execution (Batch 8192 rows)│   │    │
│  │  │  14. Native I/O → Parquet/ORC Reader, S3/HDFS FileSystem        │   │    │
│  │  └─────────────────────────────────────────────────────────────────┘   │    │
│  │      │ Arrow RecordBatch (via Arrow C Data Interface)                   │    │
│  │      ▼                                                                  │    │
│  │  15. Result Buffer → cache results, await pull or send via Arrow Flight│    │
│  │                                                                          │    │
│  └──────────────────────────────┬──────────────────────────────────────────┘    │
│                                 │ Arrow Flight: DoGet                            │
│                                 ▼                                                │
│  Client receives results from Output Worker                                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Note: the diagram above shows only the result path. Data from the Compute Workers is aggregated to the Output Worker via Arrow Flight Shuffle, and the Output Worker then streams the results back to the Client.

## 2. Calcite Integration

### 2.1 The Role of Calcite in Pangrova

```
SQL Query
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Pangrova SQL Parser (ANTLR4) - custom SQL extensions                     │
│  • OPTIMIZE, VACUUM, TIME TRAVEL, etc.                                  │
│  • Generate Calcite SqlNode                                             │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Calcite Validator - semantic analysis, type checking, name resolution (via PangrovaSchema) │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Calcite SqlToRelConverter - SqlNode → RelNode (logical plan)           │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Calcite Planner                                                        │
│  Phase 1: HepPlanner (logical optimization) - predicate pushdown, projection pushdown, constant folding │
│  Phase 2: VolcanoPlanner (physical optimization) - Join ordering, algorithm selection, partition pruning │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Pangrova Physical Plan - distributed plan (Stage division), Exchange insertion │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Substrait Converter - RelNode → Substrait Plan, serialized to Protobuf │
└─────────────────────────────────────────────────────────────────────────┘
```

**Substrait serialization boundary**: After the Plan Fragmenter completes Stage division on the Coordinator, the Substrait Converter serializes the Calcite RelNode subtree of each Stage into a Substrait Plan (Protobuf bytes). The serialized bytes are carried as `TaskDescriptor.substraitPlan` (base64-encoded) and dispatched to the Worker over HTTP REST. Upon receipt, the Worker passes the bytes directly to the Native layer (`TaskRequest.substrait_plan`) without re-serialization.

```
Serialization timing and location:
  Coordinator:
    Calcite RelNode → SubstraitConverter.toSubstrait(relNode) → Substrait Plan (Java object)
    → SubstraitPlan.toByteArray() → byte[] (Protobuf serialization)
    → Base64.encode(bytes) → TaskDescriptor.substraitPlan (JSON string)
  
  Worker (Java):
    TaskDescriptor.substraitPlan → Base64.decode → byte[]
    → TaskRequest.substrait_plan = bytes (passed directly, not deserialized)
    → nativeCreateTaskExecutor(taskRequestBytes) → Native layer
  
  Worker (Native C++):
    TaskRequest.substrait_plan → SubstraitParser::parse(bytes) → Velox PlanNode
```

### 2.2 Schema Hierarchy

<!-- Resolves: IC-MD-001 — Aligned PangrovaRootSchema to use MetadataService public API
     (listCatalogs/listSchemas/resolveTable) instead of non-existent getCatalogHandle().
     See 07-metadata-management.md §3.1 for MetadataService API. -->

```java
/**
 * Pangrova Root Schema - Manages all Catalogs in the Calcite schema tree.
 *
 * Uses MetadataService (07-metadata-management.md §3.1) as the unified entry point
 * for all metadata operations. Does NOT access CatalogRegistry or CatalogHandle
 * directly — MetadataService encapsulates access control, caching, and routing.
 */
public class PangrovaRootSchema extends AbstractSchema {
    private final MetadataService metadataService;
    private final SessionContext sessionContext;

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        // Lazy-load Catalog Schemas via MetadataService.listCatalogs()
        // MetadataService handles tenant filtering and access control internally
        for (String catalogName : metadataService.listCatalogs(sessionContext)) {
            subSchemas.computeIfAbsent(catalogName,
                name -> new PangrovaCatalogSchema(metadataService, sessionContext, name));
        }
        return subSchemas;
    }
}

/**
 * PangrovaCatalogSchema — Represents a single Catalog in the Calcite schema tree.
 *
 * Delegates schema/table listing to MetadataService rather than holding a
 * CatalogHandle reference directly. This ensures all metadata access goes
 * through the unified entry point with access control and caching.
 * See 07-metadata-management.md §3.1 for MetadataService API.
 */
public class PangrovaCatalogSchema extends AbstractSchema {
    private final MetadataService metadataService;
    private final SessionContext sessionContext;
    private final String catalogName;

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        Map<String, Schema> schemas = new LinkedHashMap<>();
        for (String schemaName : metadataService.listSchemas(sessionContext, catalogName)) {
            schemas.put(schemaName,
                new PangrovaSchemaSchema(metadataService, sessionContext, catalogName, schemaName));
        }
        return schemas;
    }
}

/**
 * Pangrova Table - Calcite Table implementation, bridges to Connector SPI.
 */
public class PangrovaTable extends AbstractTable implements ScannableTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        // Convert to Calcite RelDataType
    }
    
    @Override
    public Statistic getStatistic() {
        TableStatistics stats = metadata.getTableStatistics(session, tableHandle);
        return new PangrovaStatistic(stats);
    }
}
```

### 2.3 Custom Optimization Rules

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Pangrova Optimization Rules                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Logical Rules (HepPlanner):                                                    │
│  • FilterPushdownRule, ProjectPushdownRule, ConstantFoldingRule                 │
│  • SubqueryRemoveRule, JoinConditionPushdownRule, AggregateRemoveRule           │
│                                                                                  │
│  Physical Rules (VolcanoPlanner):                                               │
│  • HashJoinRule, MergeJoinRule, BroadcastJoinRule                               │
│  • HashAggRule, SortAggRule, StreamingAggRule                                   │
│                                                                                  │
│  Connector-Specific Rules:                                                      │
│  • IcebergPartitionPruningRule, IcebergTimeTravelRule                           │
│  • PaimonBucketPruningRule, IcebergDeleteFileRule                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.4 Cost Model

```java
public class PangrovaCostModel implements RelOptCostFactory {
    private static final double CPU_COST_PER_ROW = 1.0;
    private static final double IO_COST_PER_BYTE = 10.0;
    private static final double NETWORK_COST_PER_BYTE = 100.0;
    
    // TableScan cost: I/O + CPU, accounts for partition pruning
    // Join cost: distinguishes Broadcast/Hash/Merge, accounts for Shuffle network overhead
    // Aggregation cost: distinguishes Hash/Sort, accounts for memory footprint
}
```

## 3. Distributed Execution Plan

### 3.1 Stage Division Example

```
SQL: SELECT c.name, SUM(o.amount)
     FROM orders o JOIN customers c ON o.customer_id = c.id
     WHERE o.date > '2024-01-01'
     GROUP BY c.name

┌─────────────────────────────────────────────────────────────────────────────────┐
│  Stage 0 (Source): Scan Orders                                                  │
│  Worker-1/2/3: TableScan → Filter → Project → PartialAgg → LocalExchange       │
│                                    ↓ Shuffle (Hash by customer_id)              │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Stage 1 (Source): Scan Customers                                               │
│  Worker-1/2/3: TableScan                                                        │
│                                    ↓ Shuffle (Hash by id)                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Stage 2 (Join + Final Agg)                                                     │
│  Worker-1/2/3: Exchange(orders) → Exchange(customers) → HashJoin → FinalAgg    │
│                                    ↓ Gather (Single Partition)                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Stage 3 (Output) — Data Plane, executed on Output Worker                        │
│  Output Worker: Exchange (Gather) → Merge → Limit → Output to Client            │
│  NOTE: The Coordinator does not participate in data processing; the Output Stage runs on a dedicated Output Worker │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Exchange Types

| Exchange Type | Description | Use Case |
|--------------|------|----------|
| GATHER | All data aggregated to a single node | Final output, ORDER BY |
| REPARTITION | Repartition by Key Hash | JOIN, GROUP BY |
| REPLICATE | Broadcast to all nodes | Small-table Broadcast Join |
| ROUND_ROBIN | Round-robin distribution | Load balancing |

#### 3.2.1 Data Serialization Format Selection

Different data-transfer scenarios use different serialization formats to balance performance and compatibility:

| Transfer Scenario | Serialization Format | Zero-copy | Note |
|----------|-----------|--------|------|
| Native→Java within Worker | Arrow C Data Interface | ✓ | JNI passes pointers, no serialization overhead |
| Worker-to-Worker Shuffle (Arrow Flight) | Arrow IPC (Streaming) | ✗ | gRPC transport, must serialize to IPC bytes |
| Output Worker→Client (Arrow Flight) | Arrow IPC (Streaming) | ✗ | Same as above |
| RAM Cache storage | Arrow IPC (File) | ✗ | Persisted to memory/SSD, requires serialization |
| Spill to disk | Arrow IPC (File) + LZ4 | ✗ | Compressed and written to NVMe SSD |
| Shuffle S3 persistence | Arrow IPC (File) | ✗ | S3 persistence uses File format (with footer), consistent with Cache |

<!-- Resolves: IC-SF-008 — Shuffle persistence Arrow IPC format variant specified as File format -->

> **IPC Format Selection**: Network shuffle uses Arrow IPC Streaming format (no footer, sequential read). Persistence to S3 and local disk uses Arrow IPC File format (with footer for random access and schema self-description). This is consistent with the RAM Cache storage format.

Format conversion paths:
```
Native Velox Pipeline (RowVector)
  │
  ├─ Result output → nativeGetNextBatch() → Arrow C Data Interface (zero-copy) → Java
  │                                      → Arrow Flight putNext() → Arrow IPC bytes → network
  │
  ├─ Shuffle output → nativeGetShuffleBatch() → Arrow C Data Interface (zero-copy) → Java
  │                                            → Arrow Flight putNext() → Arrow IPC bytes → network
  │
  └─ Spill → Velox SpillManager → Arrow IPC File + LZ4 → NVMe SSD
```

Key performance points:
- Native↔Java transfer within a Worker uses the Arrow C Data Interface, with zero serialization overhead
- Worker-to-Worker network transfer uses the Arrow IPC Streaming format; Arrow Flight (gRPC) handles serialization automatically
- No explicit conversion is needed between the two formats: once the Java layer obtains Arrow C Data from Native, the Arrow Flight SDK internally serializes it into IPC bytes for sending

### 3.3 Output Task Interaction Protocol (the Coordinator never participates in data processing)

The Coordinator executes no data-processing operators. All Output Stages (including Gather, Merge, Limit, and Final Aggregation) run on a dedicated Output Worker. The Coordinator acts purely as a proxy, redirecting the Client to the Output Worker's Arrow Flight endpoint.

#### 3.3.1 Design Goals and Constraints

**Core principles**

| Principle | Description |
|------|------|
| Coordinator zero data processing | The Coordinator only performs scheduling and state management; it never touches any RecordBatch |
| Output Task = ordinary Task | The Output Task's Substrait Plan contains Exchange(Gather) + Final operators, but no TableScan |
| Unidirectional data flow | Compute Worker → Output Worker → Client; there is no reverse data flow |
| End-to-end backpressure propagation | Client consumes slowly → Output Worker pauses → Compute Worker pauses |
| Idempotent retries | All scheduling operations are idempotent, supporting safe retries |

**Role definitions**

| Role | Responsibilities | What it does NOT do |
|------|------|----------|
| Coordinator | Generate the Stage DAG, schedule Tasks, monitor state, fault-tolerance recovery | Does not process data, does not cache RecordBatch |
| Compute Worker | Execute TableScan + compute operators, expose Shuffle data via Arrow Flight | Is unaware of the Client's existence |
| Output Worker | Execute Exchange(Gather) + Final operators, expose a Result Endpoint to the Client | Architecturally identical to an ordinary Worker |
| Client | Submit queries, poll state, pull results | Does not interact directly with Compute Workers |

#### 3.3.2 Stage Split Example

Using `SELECT * FROM orders ORDER BY amount LIMIT 10` as an example:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Planner-generated Stage DAG                                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Stage 1 (N Compute Tasks, distributed across Worker-1..N)                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  TableScan(orders) → Filter → Partial Sort(amount) → Partial TopN(10) │   │
│  └──────────────────────────────────┬──────────────────────────────────────┘   │
│                                     │ Exchange (GATHER, single partition)       │
│                                     ▼                                          │
│  Stage 0 (1 Output Task, assigned to the Output Worker)                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Exchange Source → Final Merge Sort(amount) → Final TopN(10) → Output  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                          │
│                                     ▼                                          │
│                              Arrow Flight Stream → Client                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

A more complex example — `SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON o.cid = c.id GROUP BY c.name LIMIT 100`:

```
Stage 2: TableScan(orders) → Partial Agg(SUM) → Exchange(REPARTITION by cid)
Stage 3: TableScan(customers) → Exchange(REPARTITION by id)
Stage 1: Exchange(orders) + Exchange(customers) → HashJoin → Final Agg → Exchange(GATHER)
Stage 0: Exchange Source → TopN(100) → Output  ← Output Task
```

#### 3.3.3 REST API Definitions

> Note: this section is the authoritative specification of the Control Plane (REST/JSON). The Protobuf snippets that appear here serve only as design notes for the internal Java↔Native path or for the `.proto` definitions in `pangrova-protocol`. The final network API (Coordinator ↔ Worker) is JSON/HTTP; all Protobuf definitions should be kept in sync with the `pangrova-protocol` repository.

**Service overview**

Coordinator exposes (HTTP/REST):
- `POST /v1/query/submit` SubmitQuery
- `GET /v1/query/{queryId}/state` GetQueryState
- `POST /v1/query/{queryId}/cancel` CancelQuery

Worker exposes (HTTP/REST):
- `POST /v1/task` SubmitTask
- `DELETE /v1/task/{taskId}` CancelTask
- `POST /v1/task/{taskId}/exchange-sources` UpdateExchangeSources
- `POST /v1/task/{taskId}/dynamic-filter` InjectDynamicFilter
- `POST /v1/task/upstream-commit` NotifyUpstreamCommit
- `GET /v1/health` HealthCheck

Worker -> Coordinator (HTTP/REST):
- `POST /v1/task/status-report` ReportTaskStatus

Arrow Flight (data plane):
- Shuffle data pull + Client result pull (gRPC streaming)

**Query API (Coordinator REST)**

Submit query `POST /v1/query/submit`

Request:
```json
{
    "sessionId": "session-123",
    "sql": "SELECT * FROM orders",
    "sessionProperties": {"catalog": "iceberg"},
    "options": {
        "maxExecutionTimeMs": 1800000,
        "maxMemoryBytes": 34359738368,
        "resourceGroup": "interactive",
        "explainOnly": false
    }
}
```

Response:
```json
{
    "queryId": "query-20260206-001",
    "state": "RUNNING",
    "resultEndpoint": {
        "host": "worker-3",
        "port": 8082,
        "ticket": "query-20260206-001",
        "protocol": "arrow-flight"
    }
}
```

All query types return a `resultEndpoint`. For constant expressions and DDL/DML, the endpoint points to the Coordinator's own Flight server. For distributed queries, it points to the Output Worker.

<!-- Resolves: IC-WR-001 — INSERT statement SubmitQueryResponse semantics -->

> **INSERT/UPDATE/DELETE Response**: For DML statements, the response carries a `resultEndpoint` pointing to the Coordinator Flight server. The Client retrieves the affected-row-count result via Arrow Flight `doGet()`, same as any other query.
>
> ```json
> {
>     "queryId": "query-20260206-002",
>     "state": "FINISHED",
>     "resultEndpoint": {
>         "host": "coordinator-1",
>         "port": 8083,
>         "ticket": "query-20260206-002",
>         "protocol": "arrow-flight"
>     }
> }
> ```
>
> For long-running DML (large INSERT INTO ... SELECT), the initial response returns `state: "RUNNING"` with `resultEndpoint` pointing to the Output Worker. The Client polls `GET /v1/query/{queryId}/state` until `state: "FINISHED"`, then calls Flight `doGet()` to retrieve the final result.

Query state `GET /v1/query/{queryId}/state`

Response:
```json
{
    "queryId": "query-20260206-001",
    "state": "RUNNING",
    "resultEndpoint": {"host": "worker-3", "port": 8082, "ticket": "query-20260206-001"},
    "error": null,
    "stats": {
        "elapsedTimeMs": 2300,
        "processedRows": 1000000,
        "processedBytes": 128000000
    }
}
```

Cancel query `POST /v1/query/{queryId}/cancel`

Response:
```json
{ "queryId": "query-20260206-001", "state": "CANCELLED" }
```

**Task API (Worker REST)**

Submit Task `POST /v1/task`

<!-- Resolves: DEF-035 — SubmitTask request body explicitly linked to TaskDescriptor JSON -->
Request: The request body is a `TaskDescriptor` JSON object (see TaskDescriptor JSON schema below).

Response:
```json
{
    "accepted": true,
    "rejectReason": "",
    "resultEndpoint": {
        "host": "worker-3",
        "port": 8082,
        "ticket": "query-20260206-001",
        "protocol": "arrow-flight"
    }
}
```

<!-- Resolves: IC-RD-002 — SubmitTask rejection HTTP Status Codes -->

| Scenario | HTTP Status | `accepted` | `rejectReason` | Description |
|----------|-------------|------------|----------------|-------------|
| Task accepted | 200 OK | `true` | `""` | Normal acceptance |
| Business rejection (slot full, duplicate taskId) | 200 OK | `false` | `"no available slot"` / `"duplicate task"` | Worker rejects at application layer; Coordinator reschedules to another Worker |
| Resource exhaustion (memory pressure CRITICAL) | 503 Service Unavailable | — | — | Worker cannot accept any tasks; Coordinator removes Worker from scheduling pool temporarily |
| Invalid request (malformed JSON, unknown fields) | 400 Bad Request | — | — | Client error; Coordinator logs and fails the task |

> **Design rationale**: Business-level rejections use HTTP 200 + `accepted: false` so the Coordinator can distinguish "Worker is healthy but busy" from "Worker is unhealthy" (503). This avoids false-positive Worker health demotion in the Coordinator's Worker scoring model ([01 §10](./02-distributed-architecture.md)).

Cancel Task `DELETE /v1/task/{taskId}`

Update Exchange Sources `POST /v1/task/{taskId}/exchange-sources`

**TaskDescriptor JSON (SubmitTask request body)**

<!-- Resolves: DSC-004 — Add taskType field to TaskDescriptor JSON -->
<!-- Resolves: DSC-001 — Add stageId/partitionId documentation for Native correlation -->
<!-- Resolves: DSC-002 — Define concrete splits JSON serialization format -->
<!-- Resolves: DSC-003 — Document Java-only fields (exchangeSources, outputConfig) -->

```json
{
    "taskId": "task-1",
    "queryId": "query-20260206-001",
    "stageId": 0,
    "partitionId": 0,
    "taskType": "COMPUTE",
    "substraitPlan": "<base64-encoded Substrait Plan Protobuf bytes>",
    "splits": [
        "<base64-encoded SplitInfo Protobuf bytes>",
        "<base64-encoded SplitInfo Protobuf bytes>"
    ],
    "exchangeSources": [
        {
            "sourceTaskId": "task-2",
            "host": "worker-1",
            "transportPort": 8082,
            "partitionId": 0,
            "exchangeType": "GATHER",
            "sequenceId": 12
        }
    ],
    "resourceLimit": {
        "maxMemoryBytes": 34359738368,
        "maxThreads": 16,
        "maxSpillBytes": 1099511627776,
        "spillDirectory": "/data/spill"
    },
    "sessionProperties": {"timezone": "UTC"},
    "outputConfig": {
        "ticket": "query-20260206-001",
        "maxBufferedBytes": 33554432,
        "maxBufferedBatches": 64,
        "clientIdleTimeoutMs": 300000
    },
    "writeConfig": null
}
```

> **Field Notes**:
> - `taskType`: One of `COMPUTE`, `OUTPUT`, `WRITE`. Coordinator sets this explicitly. If absent (legacy), Worker infers: `writeConfig != null` → WRITE; `outputConfig != null` → OUTPUT; otherwise → COMPUTE.
> - `splits`: Array of base64-encoded `SplitInfo` Protobuf bytes. Each element is serialized using the same `SplitInfo` message defined in [03 §5.3](./05-native-layer.md). Worker base64-decodes each element and passes the raw bytes into `TaskRequest.splits` (repeated `SplitInfo`). This is consistent with `substraitPlan` serialization (base64-encoded Protobuf).
> - `stageId` / `partitionId`: Passed through to `TaskRequest` Protobuf for Native-layer logging and metrics correlation. See the field mapping table in [03 §5.3](./05-native-layer.md).
> - `exchangeSources` / `outputConfig`: **Java-layer only** — these fields are NOT passed to the Native `TaskRequest`. The Java layer manages Exchange connections (via `ExchangeClient`) and output buffering independently. The Native layer only receives the Substrait plan, splits, resource limits, session properties, and task metadata.

<!-- Resolves: IC-WR-002 — Write Task TaskDescriptor format definition -->

**Write Task TaskDescriptor**: For INSERT/UPDATE/DELETE statements, the `writeConfig` field is populated:

```json
{
    "taskId": "write-task-1",
    "queryId": "query-20260206-002",
    "stageId": 0,
    "partitionId": 0,
    "substraitPlan": "<base64 of SELECT part>",
    "splits": ["..."],
    "exchangeSources": [],
    "resourceLimit": { "maxMemoryBytes": 34359738368 },
    "sessionProperties": {"timezone": "UTC"},
    "outputConfig": null,
    "writeConfig": {
        "targetTable": "iceberg_prod.warehouse.fact_orders",
        "writeMode": "APPEND",
        "partitionSpec": ["order_date"],
        "transactionId": "txn-20260206-001",
        "fileFormat": "PARQUET",
        "targetFileSize": 134217728
    }
}
```

<!-- Resolves: IC-WR-003 — Write Task vs Compute Task scheduling relationship -->

**Composite Task Model**: An INSERT INTO ... SELECT statement is executed as a single Composite Task per Worker. The Worker internally runs two phases within the same Task lifecycle:

1. **Phase 1 (Native SELECT)**: Velox executes the SELECT portion via `nativeCreateTaskExecutor()` / `nativeStartTask()` / `nativeGetNextBatch()`, producing Arrow RecordBatch output.
2. **Phase 2 (Java Write)**: Java `TableWriter.writeBatch()` ([10 §2.6](./09-streaming.md)) receives each RecordBatch and writes to the target table via Iceberg/Paimon SDK.

The Coordinator identifies Write Tasks by the presence of `writeConfig` in the TaskDescriptor. The `taskType` field in the Native TaskRequest Protobuf is set to `TASK_TYPE_WRITE` for these tasks.

<!-- Resolves: IC-WR-006 — WriteResult reporting mechanism -->

**WriteResult Reporting**: When a Write Task completes (state=FINISHED), the TaskStatusReport includes a `writeResult` field:

```json
{
    "taskId": "write-task-1",
    "queryId": "query-20260206-002",
    "state": "FINISHED",
    "stats": {"outputRows": 50000, "outputBytes": 67108864},
    "writeResult": {
        "dataFiles": [
            {"path": "s3://bucket/warehouse/fact_orders/data/00001.parquet", "sizeBytes": 67108864, "rowCount": 50000, "partition": "order_date=2026-02-06"}
        ],
        "totalRows": 50000,
        "totalBytes": 67108864
    },
    "error": null
}
```

The Coordinator collects `writeResult` from all Write Tasks, then performs an atomic commit via the table format SDK (Iceberg `commitTransaction()` / Paimon commit). If any Write Task fails, the Coordinator aborts the transaction and triggers orphan file cleanup (see §6.5).

<!-- Resolves: IC-WR-007 — Multi-Worker write coordination protocol -->

**Multi-Worker Write Coordination Protocol**:

1. Coordinator dispatches Write Tasks to N Workers (one per partition range or hash bucket).
2. Each Worker executes SELECT + Write phases independently, reporting `writeResult` via `TaskStatusReport`.
3. Coordinator tracks Write Task states in `QueryStateMachine`:
   - All N tasks reach `FINISHED` → collect all `dataFiles` lists → atomic commit.
   - Any task reaches `FAILED` → abort: cancel remaining tasks, mark transaction as FAILED, trigger orphan cleanup (§6.5).
   - Coordinator crash during commit → on failover, new Coordinator reads `pending_writes` from PostgreSQL, detects incomplete transaction, triggers orphan cleanup.
4. Partial success is NOT supported — the commit is all-or-nothing (Iceberg/Paimon transaction semantics).

<!-- Resolves: IC-WR-005 — Write Path backpressure mechanism -->

**Write Path Backpressure**: The Write Path reuses the Read Path's natural Pull-mode backpressure. Java `TableWriter.writeBatch()` is a blocking call — if the Iceberg/Paimon SDK write is slow (e.g., S3 upload), the Java thread blocks, which in turn delays the next `nativeGetNextBatch()` JNI call, causing the Velox pipeline to stall naturally. No additional backpressure mechanism is needed beyond the synchronous pull loop.

**UpdateExchangeSources request body**

```json
{
    "taskId": "task-1",
    "queryId": "query-20260206-001",
    "exchangeSources": [
        {
            "sourceTaskId": "task-3",
            "host": "worker-2",
            "transportPort": 8082,
            "partitionId": 0,
            "exchangeType": "GATHER",
            "sequenceId": 13
        }
    ],
    "reason": "TASK_RESCHEDULED"
}
```

Key semantics:
- `exchangeSources` is a full list; upon receipt the Worker directly replaces its local cache
- `sequenceId` is monotonically increasing; the Worker uses it to detect stale messages (discarding updates where sequenceId <= the current value)
- After receiving an update, the Worker disconnects old connections and establishes new ones; already-pulled data must be discarded (an upstream Task reschedule means the data must be recomputed)

**TaskStatusReport (Worker -> Coordinator)**

`POST /v1/task/status-report`

Request:
```json
{
    "taskId": "task-1",
    "queryId": "query-20260206-001",
    "workerId": "worker-1",
    "state": "RUNNING",
    "stats": {"outputRows": 1024, "outputBytes": 1048576},
    "error": null,
    "reportSequence": 7,
    "timestampMs": 1707300000000
}
```

Response:
```json
{
    "taskId": "task-1",
    "command": "NONE"
}
```

#### 3.3.4 Arrow Flight Data-Plane Protocol

**Flight Ticket encoding**

All Arrow Flight interactions use a unified Ticket encoding:

```protobuf
// Internal structure of the Flight Ticket (serialized to bytes and placed in the Arrow Flight Ticket)
message FlightTicket {
    string query_id = 1;
    string task_id = 2;
    int32 output_partition = 3;         // Shuffle partition number
    TicketType type = 4;
}

enum TicketType {
    TICKET_TYPE_UNSPECIFIED = 0;
    TICKET_TYPE_SHUFFLE = 1;            // Shuffle data between Compute Workers
    TICKET_TYPE_RESULT = 2;             // Final result: Output Worker → Client
}
```

**Shuffle data pull (Output Worker → Compute Worker)**

```
Exchange pull flow after the Output Worker starts:

1. Parse TaskDescriptor.exchange_sources to obtain the Flight endpoints of all upstream Tasks
2. For each ExchangeSource, construct a FlightTicket:
   {query_id, source_task_id, output_partition=0 (GATHER), type=SHUFFLE}
3. Call Arrow Flight DoGet(ticket) to establish a streaming connection

<!-- Resolves: DSC-007 — Standardize FlightTicket partition field naming to output_partition -->
<!-- Resolves: IC-SF-003 — REPARTITION FlightTicket.output_partition semantics -->

> **REPARTITION Scenario**: In REPARTITION shuffle (§3.3.9), Stage N+1 Task-k pulls partition k from every Stage N task. The FlightTicket is constructed as:
> `{query_id, source_task_id=task-j, output_partition=k, type=SHUFFLE}`
> where `task-j` iterates over all Stage N tasks and `k` is the target partition assigned to the consuming task. In GATHER mode (single Output Worker), `output_partition` is always 0.
4. Receive the RecordBatch stream and feed it into the input queue of the Velox Exchange Operator
5. Once all upstream Sources complete, the Exchange Operator marks the input as finished
```

```
┌──────────────┐                    ┌──────────────┐
│ Output Worker│                    │Compute Worker│
│              │                    │  (Worker-1)  │
│  Exchange    │  DoGet(ticket)     │  Flight      │
│  Client  ────┼───────────────────►│  Server      │
│              │                    │              │
│              │  RecordBatch #1    │  Shuffle     │
│          ◄───┼────────────────────┤  Buffer      │
│              │  RecordBatch #2    │              │
│          ◄───┼────────────────────┤              │
│              │  ...               │              │
│          ◄───┼────────────────────┤              │
│              │  COMPLETED         │              │
│          ◄───┼────────────────────┤              │
│              │                    │              │
│  Velox Task  │                    │  Velox Task  │
│  (Gather +   │                    │  (Scan +     │
│   MergeSort +│                    │   Filter +   │
│   TopN)      │                    │   PartialAgg)│
└──────────────┘                    └──────────────┘
```

**Result pull (Client → Output Worker)**

```
Client result-pull flow:

1. The Client obtains the Output Worker address from SubmitQueryResponse.result_endpoint
2. The Client constructs a FlightTicket: {query_id, task_id="", output_partition=0, type=RESULT}
3. The Client calls GetFlightInfo(ticket) to obtain Schema information

<!-- Resolves: IC-RD-006 — FlightTicket construction rules for Client -->

> **Ticket Conversion Rule**: The `resultEndpoint.ticket` string returned by the REST API (e.g., `"query-20260206-001"`) maps directly to `FlightTicket.query_id`. The Client constructs the full FlightTicket Protobuf as: `{query_id = resultEndpoint.ticket, task_id = "", output_partition = 0, type = RESULT}`, serializes it to bytes, and passes it as the Arrow Flight `Ticket` in DoGet/GetFlightInfo calls.
4. The Client calls DoGet(ticket) to establish a streaming connection
5. The Output Worker reads RecordBatches from the Output Operator of the Velox Pipeline
6. Sends them batch by batch to the Client until all data is sent
7. Sends a COMPLETED signal, and the Client closes the connection
```

**Flight Server implementation notes**

```java
/**
 * The Arrow Flight Server on the Worker, serving both Shuffle and Result requests
 */
public class PangrovaFlightProducer extends NoOpFlightProducer {
    
    private final ExchangeManager exchangeManager;
    private final OutputBufferManager outputBufferManager;
    
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightTicket ticket = FlightTicket.parseFrom(descriptor.getCommand());
        
        switch (ticket.getType()) {
            case TICKET_TYPE_SHUFFLE:
                return getShuffleFlightInfo(ticket);
            case TICKET_TYPE_RESULT:
                return getResultFlightInfo(ticket);
            default:
                throw new FlightRuntimeException(
                    CallStatus.INVALID_ARGUMENT.withDescription("Unknown ticket type"));
        }
    }
    
    @Override
    public void getStream(CallContext context, Ticket ticket,
                          ServerStreamListener listener) {
        FlightTicket flightTicket = FlightTicket.parseFrom(ticket.getBytes());
        
        switch (flightTicket.getType()) {
            case TICKET_TYPE_SHUFFLE:
                streamShuffleData(flightTicket, listener);
                break;
            case TICKET_TYPE_RESULT:
                streamResultData(flightTicket, listener);
                break;
        }
    }
}
```

#### 3.3.5 Full Interaction Timeline

**Happy Path**

```
Timeline →

Client          Coordinator         Output Worker       Worker-1          Worker-2
  │                 │                    │                  │                 │
  │ SubmitQuery     │                    │                  │                 │
  ├────────────────►│                    │                  │                 │
  │                 │                    │                  │                 │
  │                 │ Parse + Optimize + Fragment           │                 │
  │                 │ Generate Stage DAG:                   │                 │
  │                 │   Stage 1: Scan+Filter (N tasks)     │                 │
  │                 │   Stage 0: Gather+Sort+TopN (1 task) │                 │
  │                 │                    │                  │                 │
  │                 │ ── Phase 1: Schedule Compute Tasks ── │                 │
  │                 │                    │                  │                 │
  │                 │ SubmitTask(Stage1) │                  │                 │
  │                 ├──────────────────────────────────────►│                 │
  │                 │ SubmitTask(Stage1) │                  │                 │
  │                 ├─────────────────────────────────────────────────────────►
  │                 │                    │                  │                 │
  │                 │◄─────────────────────────── Accepted  │                 │
  │                 │◄──────────────────────────────────────────── Accepted  │
  │                 │                    │                  │                 │
  │                 │ ── Phase 2: Schedule Output Task ──   │                 │
  │                 │                    │                  │                 │
  │                 │ SubmitTask(Stage0) │                  │                 │
  │                 │ (exchange_sources: │                  │                 │
  │                 │  W1:8082, W2:8082) │                  │                 │
  │                 ├───────────────────►│                  │                 │
  │                 │                    │                  │                 │
  │                 │◄── Accepted +      │                  │                 │
  │                 │    ResultEndpoint  │                  │                 │
  │                 │                    │                  │                 │
  │                 │ ── Phase 3: Return to Client ──       │                 │
  │                 │                    │                  │                 │
  │◄────────────────┤                    │                  │                 │
  │ SubmitQueryResp │                    │                  │                 │
  │ {queryId,       │                    │                  │                 │
  │  RUNNING,       │                    │                  │                 │
  │  endpoint:      │                    │                  │                 │
  │  OW:8082}       │                    │                  │                 │
  │                 │                    │                  │                 │
  │                 │ ── Phase 4: Data Flow ──              │                 │
  │                 │                    │                  │                 │
  │                 │                    │ DoGet(shuffle)   │                 │
  │                 │                    ├─────────────────►│                 │
  │                 │                    │ DoGet(shuffle)   │                 │
  │                 │                    ├──────────────────────────────────►│
  │                 │                    │                  │                 │
  │                 │                    │◄── RecordBatch   │                 │
  │                 │                    │◄───────────────────── RecordBatch │
  │                 │                    │                  │                 │
  │                 │                    │ Velox: Gather +  │                 │
  │                 │                    │ MergeSort + TopN │                 │
  │                 │                    │                  │                 │
  │ ── Phase 5: Client Fetches Results ──                  │                 │
  │                 │                    │                  │                 │
  │ DoGet(result)   │                    │                  │                 │
  ├────────────────────────────────────►│                  │                 │
  │                 │                    │                  │                 │
  │◄─────────────────────────────────────┤ RecordBatch #1  │                 │
  │◄─────────────────────────────────────┤ RecordBatch #2  │                 │
  │◄─────────────────────────────────────┤ COMPLETED       │                 │
  │                 │                    │                  │                 │
  │                 │ ── Phase 6: Cleanup ──               │                 │
  │                 │                    │                  │                 │
  │                 │◄── TaskStatus:     │                  │                 │
  │                 │    FINISHED        │                  │                 │
  │                 │◄──────────────────────── FINISHED     │                 │
  │                 │◄─────────────────────────────────────────── FINISHED   │
  │                 │                    │                  │                 │
  │ GetQueryStatus  │                    │                  │                 │
  ├────────────────►│                    │                  │                 │
  │◄────────────────┤ FINISHED + stats   │                  │                 │
  │                 │                    │                  │                 │
```

**Scheduling timing constraints**

| Constraint | Description | Rationale |
|------|------|------|
| Compute Tasks scheduled before Output Task | The Coordinator must schedule Stage 1 before Stage 0 | The Output Task needs to know the Flight endpoints of upstream Tasks |
| Return to Client only after Output Task is scheduled | SubmitQueryResponse must include resultEndpoint | The Client needs to know where to pull results |
| Client may connect to the Output Worker before data is ready | The Output Worker's Flight Server accepts connections as soon as the Task starts | DoGet blocks and waits for the first RecordBatch |
| Compute Tasks may produce data before the Output Worker connects | The Shuffle Buffer caches data, waiting for downstream to pull | Decouples upstream/downstream startup ordering |

**State machines**

```
State maintained by the Coordinator:

Query State Machine:
  QUEUED → PLANNING → SCHEDULING → RUNNING → FINISHING → FINISHED
                                      │                      ↑
                                      └──── FAILED ──────────┘ (retryable in some cases)
                                      └──── CANCELLED

Task State Machine (Coordinator's view):
  PENDING → SCHEDULED → RUNNING → FINISHED
                │          │
                └──────────┴──→ FAILED → RESCHEDULING → SCHEDULED (retry)
                                                          │
                                                          └──→ ABANDONED (retry limit exceeded)

State maintained by the Worker:

Task State Machine (Worker's view):
  RECEIVED → INITIALIZING → RUNNING → FINISHING → FINISHED
                 │              │
                 └──────────────┴──→ FAILED
                                     CANCELLED
```

State transition rules:

| Current State | Event | New State | Action |
|----------|------|--------|------|
| QUEUED | Queue timeout or insufficient resources | FAILED | Notify Client that the query failed |
| QUEUED | Client cancels | CANCELLED | Remove from queue |
| PLANNING | SQL syntax error / insufficient permissions / table does not exist | FAILED | Notify Client that the query failed |
| PLANNING | Client cancels | CANCELLED | Abort planning |
| SCHEDULING | No available Worker | FAILED | Notify Client that the query failed |
| SCHEDULING | Client cancels | CANCELLED | Cancel already-scheduled Tasks |
| SCHEDULING | All Tasks scheduled successfully | RUNNING | Begin monitoring Task states |
| RUNNING | All Tasks FINISHED | FINISHING | Wait for the Client to finish pulling |
| RUNNING | Any Task FAILED (retryable) | RUNNING | Reschedule the failed Task |
| RUNNING | Any Task FAILED (non-retryable) | FAILED | Cancel all Tasks, notify Client |
| FINISHING | Client finished pulling or timed out | FINISHED | Release all Task resources |
| RUNNING | Client cancels | CANCELLED | Cancel all Tasks |

<!-- Resolves: DEF-042 — Added QUEUED/PLANNING/SCHEDULING → FAILED/CANCELLED transitions -->

#### 3.3.6 Exception-Scenario Interaction Timelines

**Compute Task failure → reschedule**

```
A Stage 1 Task on Worker-1 fails during execution (e.g., S3 read timeout):

Worker-1        Coordinator         Output Worker       Worker-3 (new)
  │                 │                    │                  │
  │ TaskStatus:     │                    │                  │
  │ FAILED          │                    │                  │
  │ (EXTERNAL err)  │                    │                  │
  ├────────────────►│                    │                  │
  │                 │                    │                  │
  │                 │ Decision: EXTERNAL error, retryable    │
  │                 │ Select Worker-3 for reschedule        │
  │                 │                    │                  │
  │                 │ SubmitTask(Stage1, same splits)       │
  │                 ├─────────────────────────────────────►│
  │                 │                    │                  │
  │                 │◄──────────────────────────── Accepted │
  │                 │                    │                  │
  │                 │ UpdateExchangeSources                 │
  │                 │ (replace W1 → W3) │                  │
  │                 ├───────────────────►│                  │
  │                 │                    │                  │
  │                 │                    │ Disconnect from W1│
  │                 │                    │ Discard pulled data│
  │                 │                    │ Connect to W3     │
  │                 │                    │                  │
  │                 │                    │ DoGet(shuffle)   │
  │                 │                    ├─────────────────►│
  │                 │                    │◄── RecordBatch   │
  │                 │                    │                  │
```

Key decisions:
- After the Output Worker receives UpdateExchangeSources, it must discard all already-pulled data and restart
- Reason: an upstream Task reschedule means the data must be recomputed; partial results cannot be reused
- The Velox Task must reset: destroy the current Task and create a new one with the same Plan

**Output Worker crash → reschedule**

```
Output Worker crash:

Client          Coordinator         Output Worker(old)   Output Worker(new)   Worker-1
  │                 │                    │                  │                 │
  │                 │ Heartbeat timeout detected │                  │                 │
  │                 │ OW failed          ╳                  │                 │
  │                 │                                       │                 │
    │                 │ Select a new ACTIVE Worker                 │                 │
  │                 │                                       │                 │
  │                 │ SubmitTask(Stage0, same plan,         │                 │
  │                 │  same exchange_sources)               │                 │
  │                 ├─────────────────────────────────────►│                 │
  │                 │                                       │                 │
  │                 │◄──────────────────────────── Accepted │                 │
  │                 │                    + new endpoint     │                 │
  │                 │                                       │                 │
  │                 │ Update the Query's resultEndpoint          │                 │
  │                 │                                       │                 │
  │ DoGet connection dropped│                             │                 │
  │ (EOF detected)  │                                     │                 │
  │                 │                                       │                 │
  │ GetQueryStatus  │                                       │                 │
  ├────────────────►│                                       │                 │
  │◄────────────────┤ RUNNING +                             │                 │
  │                 │ new endpoint                          │                 │
  │                 │                                       │                 │
  │ DoGet(result) to new OW                                │                 │
  ├───────────────────────────────────────────────────────►│                 │
  │                 │                                       │                 │
  │◄──────────────────────────────────────────── RecordBatch│                 │
  │                 │                                       │                 │
```

Key decisions:
- Upstream Compute Tasks are unaffected and keep running (the Shuffle Buffer retains data)
- The new Output Worker re-pulls all data from the upstream Tasks
- After the Client SDK detects the endpoint change via GetQueryStatus, it reconnects automatically
- The Client receives data from the beginning (resumable transfer is not supported, because the Output Task is stateful — MergeSort/TopN require the full dataset)

<!-- Resolves: DEF-049 — Shuffle Buffer retention policy during Output Worker crash -->
**Shuffle Buffer Retention Policy**: When the Output Worker crashes, the Shuffle Buffer on Compute Workers follows these rules:

| Scenario | Compute Task State | Shuffle Buffer Behavior |
|----------|-------------------|------------------------|
| Compute Task still RUNNING | RUNNING | Buffer continues to fill; bounded queue applies backpressure if full. Compute Worker pipeline yields until new Output Worker connects and drains. |
| Compute Task FINISHED, buffer not yet drained | FINISHED | Buffer data retained until all downstream Tasks confirm consumption OR retention timeout expires (default: `shuffle.retention-timeout = 5m`). |
| Compute Task FINISHED, retention timeout expired | FINISHED | Buffer released. New Output Worker must trigger Compute Task re-scheduling via Coordinator. |

If the new Output Worker connects before the retention timeout, it resumes pulling from the existing Shuffle Buffer (data is replayed from the beginning of each partition). If the buffer has been released, the Coordinator re-schedules the affected Compute Tasks with the same splits.

**Client disconnect → timeout cleanup**

```
Client abnormal disconnect:

Client          Coordinator         Output Worker
  │                 │                    │
  │ DoGet(result)   │                    │
  ├────────────────────────────────────►│
  │◄─────────────────────────────────────┤ RecordBatch #1
  │                 │                    │
  ╳ Client crash     │                    │
  │                 │                    │
  │                 │                    │ gRPC connection drop detected
  │                 │                    │ Start client_idle_timeout timer
  │                 │                    │ (default 5min)
  │                 │                    │
  │                 │                    │ Velox Pipeline paused
  │                 │                    │ (backpressure: Output Buffer full)
  │                 │                    │
  │                 │                    │ ... after 5min ...
  │                 │                    │
  │                 │                    │ Timeout, mark Task FAILED
  │                 │ TaskStatus: FAILED │
  │                 │◄───────────────────┤
  │                 │ (RESOURCE: client  │
  │                 │  idle timeout)     │
  │                 │                    │
  │                 │ Mark Query FAILED  │
  │                 │ Cancel all Tasks   │
  │                 │                    │
```

If the Client reconnects before the timeout:

```
Client          Output Worker
  │                 │
  │ DoGet(result)   │ (new connection, same ticket)
  ├────────────────►│
  │                 │ Cancel idle timeout timer
  │                 │ Reset Output Buffer
  │                 │ Re-read from the Velox Pipeline
  │                 │ (note: data starts from the beginning, not resumable)
  │◄────────────────┤ RecordBatch #1
  │◄────────────────┤ RecordBatch #2
  │                 │
```

**Memory pressure → Spill + backpressure**

```
Output Worker memory pressure:

Coordinator         Output Worker                    Worker-1
  │                    │                                │
  │                    │ Velox MergeSort memory exceeded│
  │                    │ MemoryArbitrator triggers Spill│
  │                    │ MergeSort data written to NVMe SSD│
  │                    │                                │
  │                    │ If still insufficient after Spill:│
  │                    │ Pause Exchange pull            │
  │                    │ (Arrow Flight flow control)    │
  │                    │                                │
  │                    │                                │ Compute Worker detects
  │                    │                                │ gRPC window full
  │                    │                                │ Velox Pipeline yield
  │                    │                                │
  │ TaskStatus:        │                                │
  │ RUNNING +          │                                │
  │ high memory usage  │                                │
  │◄───────────────────┤                                │
  │                    │                                │
  │ (Coordinator logs  │                                │
  │  but does not      │                                │
  │  intervene; Velox  │                                │
  │  self-manages)     │                                │
  │                    │                                │
```

**Error classification and retry policy**

<!-- Resolves: DSC-010 — Standardized ErrorCategory naming with _ERROR suffix to match 12-resilience.md -->
<!-- Resolves: DSC-012 — Added error code range references to 12-resilience.md §3 -->

| ErrorCategory | Error Code Range | Example | Retryable | Retry policy |
|---------------|-----------------|------|--------|----------|
| USER_ERROR | 1000-1999 ([08 §3](./12-resilience.md)) | SQL type mismatch | No | Directly FAILED |
| INTERNAL_ERROR | 3000-3999 ([08 §3](./12-resilience.md)) | NullPointerException | No | Directly FAILED, log a Bug |
| RESOURCE_ERROR | 2000-2999 ([08 §3](./12-resilience.md)) | OOM (still insufficient after spill) | Conditional | Retry once after waiting for memory to be released |
| EXTERNAL_ERROR | 4000-4999 ([08 §3](./12-resilience.md)) | S3 timeout, HDFS unavailable | Yes | Exponential backoff retry, up to 3 times |
| EXCHANGE_ERROR | 5000-5999 ([08 §3](./12-resilience.md)) | Upstream task failure, connection dropped | Yes | Retry after the upstream task is rescheduled |

Retry decision flow:

```
Task FAILED
  │
  ├─ ErrorCategory = USER_ERROR/INTERNAL_ERROR → Query FAILED (no retry)
  │
  ├─ ErrorCategory = RESOURCE_ERROR
  │   ├─ Already retried >= 1 time → Query FAILED
  │   └─ Not yet retried → wait 30s → reschedule to a Worker with lower memory pressure
  │
  ├─ ErrorCategory = EXTERNAL_ERROR
  │   ├─ Already retried >= 3 times → Query FAILED
  │   └─ Not yet retried → exponential backoff (1s, 2s, 4s) → reschedule to a different Worker
  │
  └─ ErrorCategory = EXCHANGE_ERROR
      ├─ Upstream task also FAILED → wait for upstream reschedule to complete → auto-retry
      └─ Upstream task healthy → reschedule the current task
```

#### 3.3.7 Detailed Design of Exchange Data Pull

**ExchangeClient (internal component of the Output Worker)**

The ExchangeClient inside the Output Worker is responsible for pulling Shuffle data from upstream Compute Workers and feeding it to the Velox Exchange Operator.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Output Worker internal structure                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Java Layer                                                          │   │
│  │                                                                      │   │
│  │  ┌──────────────────┐    ┌──────────────────┐                       │   │
│  │  │  ExchangeClient  │    │  OutputBuffer    │                       │   │
│  │  │                  │    │                  │                       │   │
│  │  │  Per-Source:      │    │  Ring Buffer of  │                       │   │
│  │  │  ┌────────────┐  │    │  RecordBatch     │                       │   │
│  │  │  │ Flight     │  │    │                  │                       │   │
│  │  │  │ DoGet to   │  │    │  → Flight Server │                       │   │
│  │  │  │ Worker-1   │  │    │    (to Client)   │                       │   │
│  │  │  ├────────────┤  │    │                  │                       │   │
│  │  │  │ Flight     │  │    │                  │                       │   │
│  │  │  │ DoGet to   │  │    │                  │                       │   │
│  │  │  │ Worker-2   │  │    │                  │                       │   │
│  │  │  └────────────┘  │    │                  │                       │   │
│  │  └────────┬─────────┘    └────────▲─────────┘                       │   │
│  │           │                       │                                  │   │
│  └───────────┼───────────────────────┼──────────────────────────────────┘   │
│              │ JNI: addBatch()       │ JNI: Java Pull nativeGetNextBatch()  │
│  ┌───────────┼───────────────────────┼──────────────────────────────────┐   │
│  │  C++ Layer│                       │                                  │   │
│  │           ▼                       │                                  │   │
│  │  ┌──────────────────┐    ┌──────────────────┐                       │   │
│  │  │ Exchange Operator│    │ Output Operator  │                       │   │
│  │  │ (Source)         │    │ (Sink)           │                       │   │
│  │  │                  │    │                  │                       │   │
│  │  │ Input Queue      │───►│ MergeSort        │                       │   │
│  │  │ (per source)     │    │ TopN             │                       │   │
│  │  │                  │    │ Final Agg        │                       │   │
│  │  └──────────────────┘    └──────────────────┘                       │   │
│  │                                                                      │   │
│  │  Velox Task Pipeline:                                                │   │
│  │  ExchangeSource → MergeSort → TopN → OutputSink                     │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**ExchangeClient state machine**

```
                    ┌──────────┐
                    │  CREATED │
                    └────┬─────┘
                         │ start()
                         ▼
                    ┌──────────┐
              ┌─────│ RUNNING  │─────┐
              │     └────┬─────┘     │
              │          │           │
         all sources     │      error/cancel
         completed       │           │
              │          │           │
              ▼          │           ▼
         ┌──────────┐   │     ┌──────────┐
         │ DRAINING │   │     │  FAILED  │
         └────┬─────┘   │     └──────────┘
              │          │
         buffer empty    │
              │          │
              ▼          │
         ┌──────────┐   │
         │ FINISHED │   │
         └──────────┘   │
                         │
                    updateSources()
                         │
                         ▼
                    ┌──────────┐
                    │RESETTING │ → destroy current connections → rebuild connections → RUNNING
                    └──────────┘
```

**ExchangeClient state transition rules and timeout handling**

| Current state | Event | Target state | Action |
|----------|------|----------|------|
| CREATED | `start()` | RUNNING | Create a Flight DoGet connection for each ExchangeSource |
| RUNNING | RecordBatch received | RUNNING | Inject into Native via JNI `nativeAddExchangeBatch()` |
| RUNNING | All sources return COMPLETED | DRAINING | Call `nativeNoMoreExchangeSources()`, wait for the buffer to drain |
| RUNNING | A single source connection times out (`exchange.source.timeout`, default 120s) | RUNNING | Trigger the reconnection logic for that source (see below) |
| RUNNING | A single source fails to reconnect ≥ `exchange.source.max-retries` times in a row (default 3) | FAILED | Mark the task FAILED, report to the Coordinator |
| RUNNING | `cancel()` or CancelTask issued by the Coordinator | FAILED | Close all Flight connections, release resources |
| RUNNING | `updateSources()` (Coordinator reschedules an upstream task) | RESETTING | See the RESETTING state description |
| DRAINING | Native OutputQueue drained (nativeGetNextBatch returns EOF) | FINISHED | Close all connections, release resources |
| DRAINING | Timeout (`exchange.drain.timeout`, default 60s) | FAILED | Force-close, mark the task FAILED |
| RESETTING | All old connections closed + all new connections established | RUNNING | Discard buffered data that was pulled but not yet injected |
| FAILED | — | (terminal) | Unrecoverable, task reports FAILED |
| FINISHED | — | (terminal) | Completed normally |

**Timeout and reconnection logic for a single source**

```java
/**
 * Per-source connection manager within ExchangeClient.
 * Handles timeout detection and reconnection for a single upstream source.
 */
class ExchangeSourceConnection {
    
    private final ExchangeSource source;
    private final Duration sourceTimeout;     // exchange.source.timeout (default 120s)
    private final int maxRetries;             // exchange.source.max-retries (default 3)
    private volatile Instant lastDataReceived = Instant.now();
    private final AtomicInteger retryCount = new AtomicInteger(0);
    
    /**
     * Pull loop for a single source (runs in dedicated thread).
     */
    void pullLoop() {
        FlightStream stream = null;
        try {
            stream = connectToSource(source);
            
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                lastDataReceived = Instant.now();
                retryCount.set(0);  // Reset on successful data
                
                // Inject into Native Exchange Operator
                NativeRuntime.nativeAddExchangeBatch(
                    taskHandle, source.sourceId(),
                    root.getArrowArrayAddress(), root.getArrowSchemaAddress());
            }
            
            // Source completed normally
            NativeRuntime.nativeNoMoreExchangeData(taskHandle, source.sourceId());
            
        } catch (FlightRuntimeException e) {
            handleSourceError(e);
        }
    }
    
    /**
     * Timeout detection (called by watchdog timer every 10s).
     */
    void checkTimeout() {
        Duration sinceLastData = Duration.between(lastDataReceived, Instant.now());
        if (sinceLastData.compareTo(sourceTimeout) > 0) {
            // Timeout — attempt reconnection
            reconnect();
        }
    }
    
    /**
     * Reconnection logic with exponential backoff.
     */
    private void reconnect() {
        int attempt = retryCount.incrementAndGet();
        if (attempt > maxRetries) {
            // Exhausted retries — propagate failure to ExchangeClient
            exchangeClient.onSourceFailed(source.sourceId(),
                new PangrovaException(EXCHANGE_CONNECTION_LOST,
                    "Source " + source.sourceTaskId() + " unreachable after " + maxRetries + " retries"));
            return;
        }
        
        Duration backoff = Duration.ofMillis(
            (long) (1000 * Math.pow(2, attempt - 1)));  // 1s, 2s, 4s
        
        log.warn("Reconnecting to source {} (attempt {}/{}), backoff={}",
                 source.sourceTaskId(), attempt, maxRetries, backoff);
        
        scheduler.schedule(() -> {
            closeCurrentStream();
            pullLoop();  // Restart pull loop with new connection
        }, backoff.toMillis(), TimeUnit.MILLISECONDS);
    }
}
```

**RESETTING state detailed flow** (triggered when the Coordinator reschedules an upstream task):

```
Coordinator sends UpdateExchangeSources(sequenceId=N+1):
  1. ExchangeClient checks sequenceId > current value → accept the update
  2. State RUNNING → RESETTING
  3. Close all old Flight connections (graceful close, wait for in-flight RPCs to finish, timeout 5s)
  4. Discard buffer data on the Java side that was pulled but not injected into Native
     (upstream task reschedule means the data must be recomputed, old data is invalid)
  5. Call nativeResetExchangeOperator(taskHandle) to clear the Native Exchange Operator's Input Queue
  6. Create new Flight connections based on the new exchangeSources list
  7. All new connections established successfully → state RESETTING → RUNNING
  8. Any new connection fails to establish → retry (up to 3 times) → still fails → FAILED
```

**Interaction between ExchangeClient and Velox**

```
Interaction between the Java ExchangeClient and the C++ Velox Exchange Operator:

1. The Java layer pulls RecordBatch from upstream Workers via Arrow Flight DoGet
2. Calls nativeAddExchangeBatch(taskHandle, sourceId, arrowArray, arrowSchema) via JNI
3. The C++ Exchange Operator reads batches from the Input Queue and feeds the Pipeline
4. The Pipeline executes operators such as MergeSort/TopN/FinalAgg
5. The Output Operator produces result batches and puts them into the Native OutputQueue
6. The Java layer calls nativeGetNextBatch(handle, arrowArrayPtr, arrowSchemaPtr) to pull batches
7. The Java layer streams the batches to the Client via Arrow Flight
```

JNI interface definitions:

```cpp
// Java Pull (result output) — already defined in NativeRuntime in 05-native-layer.md §5.4
// The Java thread calls nativeGetNextBatch() to pull data from the OutputQueue
// Return value: > 0 row count, = 0 EOF, < 0 error

// Java → Native (Exchange data injection)
extern "C" {
    // Add a batch to the Exchange Operator's Input Queue
    JNIEXPORT void JNICALL Java_org_pangrova_worker_jni_NativeRuntime_addExchangeBatch(
        JNIEnv* env, jclass cls,
        jlong taskHandle,           // Velox Task handle
        jint sourceId,              // Exchange Source number
        jlong arrowArrayPtr,        // ArrowArray* (zero-copy)
        jlong arrowSchemaPtr        // ArrowSchema* (zero-copy)
    );
    
    // Mark a given Exchange Source as completed (no more new data)
    JNIEXPORT void JNICALL Java_org_pangrova_worker_jni_NativeRuntime_noMoreExchangeData(
        JNIEnv* env, jclass cls,
        jlong taskHandle,
        jint sourceId
    );
    
    // Mark all Exchange Sources as completed
    JNIEXPORT void JNICALL Java_org_pangrova_worker_jni_NativeRuntime_noMoreExchangeSources(
        JNIEnv* env, jclass cls,
        jlong taskHandle
    );
}
```

**Backpressure propagation chain**

```
Full backpressure propagation chain (from Client to Compute Worker):

Layer 1: Client → Output Worker (Arrow Flight)
  Client consumes slowly → gRPC flow control window fills up
  → the Output Worker Flight Server's putNext() blocks
  → OutputBuffer stops being consumed

Layer 2: OutputQueue → Java Pull Loop
  The Java layer's nativeGetNextBatch() blocks (OutputQueue empty)
  → when the OutputQueue is full, the Velox Output Operator pauses
  → the Velox Pipeline's Driver thread yields

Layer 3: Velox Pipeline → ExchangeClient
  The Velox Exchange Operator's Input Queue is full
  → addExchangeBatch() blocks (or returns false)
  → the Java ExchangeClient pauses calling Flight DoGet.next()

Layer 4: ExchangeClient → Compute Worker (Arrow Flight)
  ExchangeClient stops consuming
  → gRPC flow control window fills up
  → the Compute Worker Flight Server's putNext() blocks
  → the Compute Worker's Shuffle Buffer stops being consumed
  → the Compute Worker's Velox Pipeline pauses

Result: backpressure across the entire chain propagates from the Client to all Compute Workers
```

Backpressure recovery chain (reverse direction):

```
Client resumes consuming
  → gRPC window opens
  → OutputBuffer is consumed
  → Java continues calling nativeGetNextBatch()
  → the Native OutputQueue has space
  → the Velox Pipeline resumes
  → the Exchange Operator Input Queue has space
  → ExchangeClient resumes Flight DoGet.next()
  → gRPC window opens
  → the Compute Worker Pipeline resumes
```

| Backpressure layer | Trigger condition | Behavior |
|----------|----------|------|
| Native OutputQueue | OutputQueue full (bounded queue) | Velox Pipeline Driver yields |
| Java Pull Loop | nativeGetNextBatch() blocks (Flight putNext has not returned) | Stop pulling, Native OutputQueue backs up |
| Arrow Flight (Output → Client) | gRPC window full | Pause putNext(), Java Pull Loop blocks |
| Arrow Flight (Compute → Output) | gRPC window full | Pause upstream Pipeline output |
| Velox Pipeline | Operator output queue full | Driver thread yields |

#### 3.3.8 Coordinator Scheduler Design

**Stage scheduling order**

The Coordinator schedules in topological order of the Stage DAG: leaf Stages are scheduled first, and the Output Stage is scheduled last.

```
Stage DAG example (SELECT ... JOIN ... ORDER BY ... LIMIT):

  Stage 3 (Scan orders)  Stage 2 (Scan customers)
       │                       │
       └───────┬───────────────┘
               │
          Stage 1 (Join + Agg)
               │
          Stage 0 (Output: Gather + Sort + Limit)

Scheduling order: Stage 3 → Stage 2 → Stage 1 → Stage 0

Rules:
1. Leaf Stages (no upstream dependency) can be scheduled in parallel
2. Intermediate Stages are scheduled after all tasks of their upstream Stages are scheduled
3. The Output Stage (Stage 0) is scheduled last
4. Scheduling = dispatching TaskDescriptor to Workers, without waiting for task execution to finish
```

**Output Worker selection algorithm**

The production path is executed by `DefaultWorkerSelector`; it no longer maintains a separate warm pool or a Worker-side slot reservation API.

```java
public WorkerInfo selectWithCacheAffinity(String tenantId, List<String> splitFilePaths) {
    if (cacheAffinityAssigner == null || splitFilePaths.isEmpty()) {
        return select(tenantId);
    }
    WorkerInfo cacheHit = selectByCachedFiles(activeWorkers, splitFilePaths);
    if (cacheHit != null) {
        return cacheHit;
    }
    return firstConsistentHashPreferredWorkerOrLoadBasedFallback(tenantId, splitFilePaths);
}
```

Output Worker selection strategy:

| Priority | Condition | Note |
|--------|------|------|
| 1 | An ACTIVE Worker's heartbeat cache summary hits the split file | Reuse the real cache-hit signal, prioritize data locality |
| 2 | `CacheAffinityAssigner`'s consistent-hash preferred Worker for the split file | Keep stable placement when the cache is cold or has not been reported |
| 3 | `TaskScheduler.selectWorker`'s load-based selection | The general path when there is no file path or no cache affinity available |
| 4 | No ACTIVE Worker meets the resource threshold | Query queues or throws `NoAvailableWorkerException` |

#### 3.3.9 Multi-Stage Exchange Scenarios

Output Tasks appear not only at the final output; intermediate Stages also have Exchanges. Below is the full interaction for a multi-Stage scenario.

**Three-tier Stage example**

```
SQL: SELECT c.name, SUM(o.amount)
     FROM orders o JOIN customers c ON o.cid = c.id
     GROUP BY c.name
     ORDER BY SUM(o.amount) DESC
     LIMIT 100

Stage DAG:
  Stage 3: Scan(orders) → PartialAgg → Exchange(REPARTITION by cid)
  Stage 2: Scan(customers) → Exchange(REPARTITION by id)
  Stage 1: Exchange(orders) + Exchange(customers) → HashJoin → FinalAgg → Exchange(GATHER)
  Stage 0: Exchange(GATHER) → MergeSort(desc) → TopN(100) → Output
```

The TaskDescriptor content of each Stage:

| Stage | splits | exchange_sources | output_config |
|-------|--------|------------------|---------------|
| Stage 3 (N tasks) | Splits of the orders table | empty | empty |
| Stage 2 (N tasks) | Splits of the customers table | empty | empty |
| Stage 1 (M tasks) | empty | All task endpoints of Stage 3 + Stage 2 | empty |
| Stage 0 (1 task) | empty | All task endpoints of Stage 1 | ticket, buffer config |

**Difference between an intermediate Stage's Exchange and the Output Stage's Exchange**

| Dimension | Intermediate Stage Exchange | Output Stage Exchange |
|------|---------------------|----------------------|
| Exchange type | REPARTITION (many-to-many) | GATHER (many-to-one) |
| Downstream consumers | Multiple tasks (by partition) | 1 Output Task |
| Data routing | Hash(key) % partition_count | All sent to partition 0 |
| Shuffle Buffer | Bucketed by partition | Single bucket |
| Backpressure scope | Only affects the upstream of the corresponding partition | Affects all upstream |

**Shuffle Buffer design**

The Shuffle Buffer on the Compute Worker is responsible for buffering data produced by the Velox Pipeline, waiting for downstream tasks to pull it.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Compute Worker Shuffle Buffer                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Velox Pipeline → PartitionedOutput Operator                                │
│       │                                                                      │
│       │ Velox produces a Shuffle batch → Native ShuffleQueue[partitionId]           │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  ShuffleQueue (Native, per partition)                                │   │
│  │  The Java ShuffleDrainThread calls nativeGetShuffleBatch(partitionId)  │   │
│  │  After pulling data, sends it to the target Worker via Arrow Flight  │   │
│  │                                                                      │   │
│  │  Partition 0: [Batch, Batch, Batch, ...]                            │   │
│  │  Partition 1: [Batch, Batch, ...]                                   │   │
│  │  Partition 2: [Batch, Batch, Batch, Batch, ...]                     │   │
│  │  ...                                                                 │   │
│  │  Partition M-1: [Batch, ...]                                        │   │
│  │                                                                      │   │
│  │  Each Partition is a bounded blocking queue                          │   │
│  │  Capacity: max_shuffle_buffer_bytes / partition_count                │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                      │
│       │ Arrow Flight DoGet (by partition)                                    │
│       ▼                                                                      │
│  Downstream Worker (intermediate Stage or Output Worker)                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 3.3.10 Constant Folding & Coordinator Local Result Buffer

The following query types are handled locally by the Coordinator without dispatching to a Worker. Results are buffered in the Coordinator's local Arrow IPC buffer and served via the Coordinator's own Flight server, using the same `resultEndpoint` mechanism as distributed queries.

| Query Type | Example | Handling |
|------------|---------|----------|
| Constant expressions | `SELECT 1`, `SELECT 1+2, 'hello'` | Planner folds to `LogicalValues`, serialized to Arrow IPC, buffered in `CoordinatorResultBuffer` |
| Metadata queries | `SHOW TABLES`, `SHOW SCHEMAS`, `DESCRIBE table` | Coordinator builds `TypedRow` list, converts to Arrow IPC via `CoordinatorResultWriter` |
| System table queries | `SELECT * FROM system.runtime.nodes` | Same as metadata queries |
| DDL operations | `CREATE TABLE`, `ALTER TABLE` | Result (status row) buffered in `CoordinatorResultBuffer` |
| DML operations | `INSERT`, `UPDATE`, `DELETE` | Affected-row count buffered as Arrow IPC |

**Constant Folding in Planner**: During the `plan()` phase, `PangrovaPlanner.tryFoldConstant()` checks whether the optimized `RelNode` tree contains no `TableScan`, `TableModify`, or `Exchange` nodes. If so, the expression is evaluated via Calcite Interpreter and the result is materialized as a `LogicalValues` node. This folded result is then serialized to Arrow IPC by `CoordinatorResultWriter.toArrowIpc()` and stored in `CoordinatorResultBuffer`.

**Unified Result Retrieval**: All query types — constant, DDL, DML, and distributed — return a `resultEndpoint` in the response. The Client always retrieves results via Arrow Flight `doGet()` against the endpoint. For locally-buffered results, the endpoint points to the Coordinator's own Flight server (`CoordinatorFlightProducer`).

```json
// Constant query response (unified format)
{
  "queryId": "query-20260206-002",
  "state": "FINISHED",
  "resultEndpoint": {
    "host": "coordinator-1",
    "port": 8083,
    "ticket": "query-20260206-002",
    "protocol": "arrow-flight"
  }
}
```

```json
// DML response (unified format)
{
  "queryId": "query-20260206-003",
  "state": "FINISHED",
  "resultEndpoint": {
    "host": "coordinator-1",
    "port": 8083,
    "ticket": "query-20260206-003",
    "protocol": "arrow-flight"
  }
}
```

#### 3.3.11 Client SDK Wrapper

The Client SDK hides the Output Worker redirection details from the user:

```java
// User's perspective — no different from traditional JDBC
try (Connection conn = DriverManager.getConnection("jdbc:pangrova://coordinator:8080")) {
    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM orders LIMIT 10");
    while (rs.next()) {
        // The user is unaware that data comes from the Output Worker rather than the Coordinator
    }
}
```

```
SDK internal flow:
1. POST /v1/query/submit→ Coordinator
2. Parse response.resultEndpoint
3. Arrow Flight DoGet(resultEndpoint) → stream-construct ResultSet
4. Connection dropped → GET /v1/query/{queryId}/state → obtain new endpoint → auto-reconnect
```

#### 3.3.12 Configuration Reference

<!-- Resolves: DEF-046 — All parameters below are also registered in 21-configuration-reference.md §12.1/§12.3 -->
> **Cross-reference**: All configuration parameters defined below are registered in [21-configuration-reference.md §12](./21-configuration-reference.md) with default values, ranges, units, and hot-reload support.

```yaml
coordinator:
  scheduler:
    # Output Worker scheduling
    output-worker:
      scheduling-timeout: 5s
    
    # Task retry
    retry:
      max-retries-external: 3
      max-retries-resource: 1
      retry-backoff-initial: 1s
      retry-backoff-max: 30s
      retry-backoff-multiplier: 2.0
    
    # Stage scheduling
    stage:
      max-concurrent-stages: 16
      task-submit-timeout: 10s

worker:
  task:
    # Exchange Client
    exchange:
      max-buffer-size: 64MB
      max-concurrent-requests: 8
      request-timeout: 30s
      idle-timeout: 60s
    
    # Output Buffer
    output:
      max-buffered-bytes: 32MB
      max-buffered-batches: 64
      client-idle-timeout: 5m
    
    # Shuffle Buffer
    shuffle:
      max-buffer-size: 128MB
      partition-buffer-size: 16MB
  
  # Arrow Flight
  flight:
    port: 8082
    max-message-size: 16MB
    flow-control-window: 4MB
```

Design rationale: the Coordinator belongs to the control plane; its sole responsibility is SQL parsing, optimization, and scheduling. All data processing is pushed down to the Workers, ensuring full isolation of the control plane and data plane. The Output Worker has full Velox execution capability, is not restricted by operator type, and has production-grade fault tolerance such as Spill and memory arbitration.

## 4. Velox Pipeline Execution Model

### 4.1 Pipeline Structure

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Task                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Pipeline 0 (Source Pipeline)                                           │    │
│  │  Driver 0-3: Scan → Filter → Project → PartialAgg → LocalExchange      │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                    ↓ Local Exchange (Merge Buffers)             │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Pipeline 1 (Aggregation Pipeline)                                      │    │
│  │  Driver 0: LocalExchangeSource → FinalAggregation → PartitionedOutput  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Pipeline Parallelism Calculation

The number of Drivers (parallelism) for each Pipeline is determined by the following rules:

| Pipeline type | Driver count rule | Note |
|--------------|-------------------|------|
| Source Pipeline (with TableScan) | `min(splits_count, max_drivers_per_task)` | Each Driver processes one or more Splits |
| Exchange Source Pipeline | `min(exchange_source_count, max_drivers_per_task)` | Each Driver pulls one or more upstream Sources |
| Local Exchange Consumer | 1 (FinalAgg) or `hash_partition_count` | Depends on operator semantics |
| Output Pipeline | 1 | A single Driver outputs to the OutputQueue |

Configuration parameters:

| Parameter | Default | Range | Note |
|------|--------|---------|------|
| `coordinator.scheduler.min-drivers-per-task` | 1 | 1-64 | Minimum number of Drivers per Task |
| `coordinator.scheduler.max-drivers-per-task` | 16 | 1-256 | Maximum number of Drivers per Task, usually set to the number of Worker CPU cores |
| `worker.native.thread-pool-size` | number of CPU cores | 1-256 | Velox execution thread pool size, shared by all Tasks |

Parallelism calculation pseudocode:
```
// Computed by the Coordinator when building the TaskDescriptor
int computeDriverCount(Pipeline pipeline, TaskDescriptor task) {
    if (pipeline.hasTableScan()) {
        return clamp(task.splits.size(), minDrivers, maxDrivers);
    }
    if (pipeline.hasExchangeSource()) {
        return clamp(task.exchangeSources.size(), minDrivers, maxDrivers);
    }
    if (pipeline.isFinalAggregation()) {
        return 1;  // A single Driver does the final aggregation
    }
    return maxDrivers;  // Use the maximum parallelism by default
}
```

Note: Velox internally uses a shared thread pool (`thread-pool-size`) to schedule all Drivers of all Tasks. The Driver count determines the logical parallelism, while the actual physical parallelism is limited by the thread pool size. When the total number of Drivers > the thread pool size, Velox achieves time-division multiplexing via cooperative scheduling (yield).

### 4.2 Operator Types

| Operator type | Description | Example |
|---------|------|------|
| SourceOperator | Data source, produces data | TableScan, Exchange |
| FilterOperator | Filters data | Filter |
| ProjectOperator | Projection/expression computation | Project |
| AggregationOperator | Aggregation | HashAggregation |
| JoinOperator | Join | HashJoin, MergeJoin |
| SortOperator | Sort | OrderBy |
| LimitOperator | Limit row count | Limit, TopN |
| SinkOperator | Data output | PartitionedOutput |

## 5. Dynamic Filtering

```
Scenario: SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id
      WHERE c.country = 'US'

Problem: the orders table is very large, but only a few customer_id values match country='US'
Optimization: scan customers first, collect the matching ids, then use those ids to filter orders

┌─────────────────────────────────────────────────────────────────────────────────┐
│  Stage 1: Build Side (customers)                                                │
│  TableScan → Filter(country='US') → Collect Dynamic Filter                      │
│  → build a BloomFilter or IN-list: customer_id IN (101, 102, 105, ...)          │
│                                    ↓ Broadcast Dynamic Filter                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Stage 0: Probe Side (orders)                                                   │
│  TableScan → Apply Dynamic Filter (customer_id IN ...)                          │
│  → large amounts of data are filtered out early, reducing I/O and computation   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 6. Write Path Design

### 6.1 Read/Write Separation Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Read/Write Separation                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design principles:                                                             │
│  • Read path (C++): use Velox native execution, pursue extreme performance      │
│  • Write path (Java): implemented in Java, deeply integrated with lakehouse format SDKs │
│                                                                                  │
│  Reasons:                                                                        │
│  • Writes to lakehouse formats (Iceberg/Paimon) involve complex transactions and metadata management │
│  • The official SDKs for these formats are all implemented in Java              │
│  • The write performance bottleneck is usually I/O, not CPU                     │
│  • A Java implementation is easier to maintain and keep in sync with upstream   │
│                                                                                  │
│  Read Path (C++ Velox):                                                         │
│  Substrait Plan → Velox Plan Parser → TableScan → Filter/Project → Native I/O  │
│  Characteristics: vectorized, SIMD, zero-copy, extreme performance             │
│                                                                                  │
│  Write Path (Java):                                                             │
│  INSERT/UPDATE/DELETE → Transaction Manager → Data Writer → Metadata Committer │
│  Characteristics: full transaction support, ACID, upstream-compatible          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 INSERT Flow

```
Phase 1: Query Execution (Read Path - C++)
  1. Parse the INSERT statement
  2. Optimize the SELECT subquery
  3. Generate the Substrait Plan
  4. Dispatch to Workers for execution
  5. Velox executes the SELECT, producing Arrow RecordBatch

Phase 2: Data Writing (Write Path - Java)
  Step 1: Begin Transaction
    IcebergTransaction tx = table.newTransaction();
  
  Step 2: Write Data Files (parallel)
    Worker-1: Write partition=2024-01-01 → data-00001.parquet, data-00002.parquet
    Worker-2: Write partition=2024-01-02 → data-00003.parquet
    Worker-3: Write partition=2024-01-03 → data-00004.parquet
  
  Step 3: Collect Write Results
    Coordinator collects: file paths, sizes, row counts, partition info, column statistics
  
  Step 4: Commit Transaction (atomic operation)
    for (DataFile file : writtenFiles) {
        tx.newAppend().appendFile(file).commit();
    }
    tx.commitTransaction();  // Create a new Snapshot
```

### 6.3 Write Components

> **DEPRECATED**: The `DataWriter` interface below is superseded by the unified `TableWriter` SPI
> defined in [09-streaming.md §2.6](./09-streaming.md). New implementations MUST use `TableWriter.writeBatch(VectorSchemaRoot)`
> for the SQL write path, which provides zero-conversion Arrow-native writes with `WriteContext`
> for transaction mode control. The code below is retained for historical reference only.
>
> <!-- Resolves: SPI-C01 (deprecate DataWriter in favor of unified TableWriter) -->

```java
/**
 * @deprecated Use {@link TableWriter#writeBatch(String, VectorSchemaRoot, WriteMode, WriteContext)}
 *             from 09-streaming.md §2.6 instead. DataWriter is superseded by the unified TableWriter SPI.
 */
@Deprecated
public interface DataWriter extends Closeable {
    void write(VectorSchemaRoot batch);
    WriteResult finish();
    void abort();
}

/**
 * @deprecated Use TableWriter with writeBatch() instead.
 */
@Deprecated
public class PartitionedDataWriter implements DataWriter {
    private final Map<StructLike, DataWriter> partitionWriters;
    
    @Override
    public void write(VectorSchemaRoot batch) {
        Map<StructLike, List<Integer>> partitionedRows = partitionRows(batch);
        for (Map.Entry<StructLike, List<Integer>> entry : partitionedRows.entrySet()) {
            DataWriter writer = partitionWriters.computeIfAbsent(
                entry.getKey(), p -> createPartitionWriter(p));
            writer.write(extractRows(batch, entry.getValue()));
        }
    }
}
```

**Migration to unified TableWriter**: The SQL write path now uses `TableWriter.writeBatch(tableName, batch, writeMode, context)` where `context.transactionMode = COORDINATOR_MANAGED`. The native Velox engine produces `VectorSchemaRoot` via `nativeGetNextBatch()`, which is passed directly to `writeBatch()` with zero columnar-to-row conversion overhead. See [09-streaming.md §2.6](./09-streaming.md) for the authoritative interface definition.

### 6.4 Write Path Responsibility Matrix

| Operation | Execution layer | Language | Note |
|------|--------|------|------|
| Data file write (Parquet/ORC) | Worker | Java | Write to S3/HDFS via the Iceberg/Paimon SDK |
| Metadata write (Manifest/Snapshot) | Worker | Java | Write via the Iceberg/Paimon SDK |
| Transaction commit (Commit) | Coordinator | Java | Two-phase commit; the Coordinator coordinates each Worker's write results |
| Shuffle serialization | Worker (Native) | C++ | Serialize to Arrow format in memory; Java pulls via nativeGetShuffleBatch() |
| Shuffle network transfer | Worker (Java) | Java | Send to the target Worker via Arrow Flight |
| Read path data processing | Worker (Native) | C++ | Velox execution engine, vectorized processing |

**Key boundaries**:
- The C++ Native layer does not perform any persistence write operations (both data files and metadata files are written by the Java layer)
- Shuffle at the Native layer only does in-memory partitioning and buffering (Shuffle Partitioner); the network transfer is done by the Java layer's Arrow Flight
- Transaction commit is coordinated by the Coordinator: Workers are responsible for writing data files, and the Coordinator is responsible for the final atomic commit

### 6.5 DataFile Cleanup Mechanism After Commit Failure

In the write path, Workers write DataFiles in parallel and then the Coordinator performs an atomic Commit. Commit may fail due to conflicts, timeouts, or Coordinator failure, in which case the already-written DataFiles become orphan files.

**Commit failure scenarios**:
1. Iceberg `OptimisticLockException` (concurrent write conflict)
2. Coordinator crashes before Commit
3. Network timeout leaves the Commit status unknown

**Cleanup flow**:

| Phase | Operation | Note |
|------|------|------|
| Phase 1: Write | Each time a Worker writes a DataFile, it registers it in the `pending_files` table | Record the file list |
| Phase 2: Commit | Success → delete `pending_files` records; failure → mark the transaction as FAILED | Atomic commit |
| Phase 3: Synchronous cleanup | The Coordinator immediately attempts to delete orphan files after a Commit failure (best-effort) | Immediate cleanup |
| Phase 4: Fallback cleanup | The `OrphanFileCleanup` scheduled task cleans up remaining orphan files | See §10.1 |

The authoritative definition of the `pending_files` table schema is in §10.1.

Coordinator Commit failure handling pseudocode:

```
try {
    tx.commitTransaction();
    pendingFileStore.deleteByTransaction(transactionId);
} catch (CommitFailedException e) {
    pendingWriteStore.markFailed(transactionId);
    // Synchronously clean up DataFiles (best-effort)
    for (file : pendingFileStore.getFiles(transactionId)) {
        try { fileSystem.delete(file); }
        catch (IOException) { log.warn("Deferred to OrphanFileCleanup"); }
    }
    pendingFileStore.deleteByTransaction(transactionId);
    pendingWriteStore.delete(transactionId);
    throw e;
}
```

## 7. Global Consistency Design

### 7.1 Core Principles

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Single Source of Truth                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Principle: the Java Connector is the single source of truth for metadata      │
│  The Native layer does not cache metadata; every execution fetches the latest version from the Java layer │
│                                                                                  │
│  Metadata Flow (one-way):                                                        │
│  Iceberg Catalog → Java Connector → Substrait Plan → Native Layer              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

| Principle | Description | Implementation |
|------|------|----------|
| Single source of truth | The Java Connector is the sole source of metadata | The Native layer does not cache metadata |
| Snapshot isolation | Queries see a consistent data view | Bind the Snapshot ID at query start |
| Write atomicity | A write either fully succeeds or fully fails | Two-phase commit + cleanup mechanism |
| Write visibility | Immediately visible after commit | Active cache invalidation + broadcast |
| Lock-free concurrency | Reads and writes do not block each other | MVCC + optimistic concurrency control |

### 7.2 Versioned Snapshot Propagation

```
Step 1: Get the snapshot at query start
  VersionedTableSnapshot snapshot = connector.getMetadata(session)
      .getTableSnapshot(tableName);
  queryContext.setTableSnapshot(tableName, snapshot);
  List<Split> splits = splitManager.getSplits(snapshot, predicate);

Step 2: Snapshot info is serialized into the Substrait Plan
  PangrovaReadExtension:
    version_id, version_timestamp_ms, schema_version, arrow_schema
    data_files[], delete_files[], properties{}

Step 3: The Native layer reads based on the snapshot
  // The Native layer does not do any metadata query, it only reads data based on the passed-in SplitInfo
  // The full definition of SplitInfo is in 05-native-layer.md §5.3 (the only authoritative definition)
  // PangrovaDataSource automatically composes the read components based on SplitInfo.connector_info (oneof)
  for (SplitInfo split : plan.getSplits()) {
      // Inside PangrovaDataSource, it selects based on connector_info:
      //   iceberg_info → FormatReader + DeleteFilter chain
      //   paimon_info  → FormatReader + MergeReader
      PangrovaDataSource dataSource = new PangrovaDataSource(split);
      while (dataSource.hasNext()) {
          RecordBatch batch = dataSource.readNext();
          output(batch);
      }
  }
```

### 7.3 Snapshot Isolation Guarantee

```
Timeline:
T0        T1        T2        T3        T4        T5
│         │         │         │         │         │
│  Query  │         │  Write  │         │  Query  │
│  Start  │         │  Commit │         │  End    │
│  Snap   │         │  Snap   │         │         │
│  =100   │         │  =101   │         │         │

The query obtains Snapshot=100 at T0
Even if a new write is committed at T2 (Snapshot=101)
the query still reads only the data of Snapshot=100
```

### 7.4 Cache Invalidation Strategy

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Coordinator Cluster                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                          │
│  │Coordinator-1│    │Coordinator-2│    │Coordinator-3│                          │
│  │ L1 Cache    │    │ L1 Cache    │    │ L1 Cache    │  (1min TTL)              │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                          │
│         └──────────────────┼──────────────────┘                                  │
│                     ┌──────▼──────┐                                              │
│                     │  L2 Cache   │  (Redis Cluster, 5min TTL)                  │
│                     └──────┬──────┘                                              │
│                     ┌──────▼──────┐                                              │
│                     │   Catalog   │  (Iceberg/HMS Source)                       │
│                     └─────────────┘                                              │
└─────────────────────────────────────────────────────────────────────────────────┘

Active invalidation after a write:
  1. Invalidate the local cache
  2. Invalidate the Redis cache
  3. Broadcast an invalidation message to other Coordinators (Redis Pub/Sub)
```

## 8. Custom SQL Extensions

### 8.1 Time Travel Queries

```sql
-- Query by timestamp
SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-01'

-- Query by version number
SELECT * FROM orders FOR VERSION AS OF 12345
```

### 8.2 Table Maintenance Statements

```sql
-- Table optimization (small file compaction)
OPTIMIZE TABLE orders WHERE date < '2024-01-01' REWRITE DATA

-- Data compaction
ALTER TABLE orders COMPACT PARTITION (date='2024-01-01')

-- Clean up expired data
VACUUM TABLE orders RETAIN 168 HOURS

-- Snapshot management
ALTER TABLE orders CREATE SNAPSHOT
ALTER TABLE orders ROLLBACK TO SNAPSHOT 12345

-- Branch management (Iceberg)
ALTER TABLE orders CREATE BRANCH dev AS OF 12345
ALTER TABLE orders DROP BRANCH dev
```

## 9. Statistics Collection

```java
/**
 * Collect statistics from Iceberg metadata
 */
public TableStatistics collectFromIceberg(Table table) {
    // Collect from manifest files: rowCount, sizeInBytes
    // Column statistics: nullCounts, lowerBounds, upperBounds
}

/**
 * Run ANALYZE to collect more detailed statistics
 */
public TableStatistics analyze(Table table, List<String> columns, double sampleRatio) {
    // Sampling scan
    // Collect: distinctCount (HyperLogLog), quantiles (TDigest)
}
```

---

## 10. Data Consistency Enhancements

### 10.1 Write Failure Cleanup (Authoritative Definition)

This section is the authoritative definition of the orphan file cleanup mechanism. §6.5 describes the immediate cleanup flow when a Commit fails; this section defines the fallback scheduled cleanup mechanism.

**pending_writes / pending_files table schema** (authoritative definition):

| Table | Fields | Note |
|----|------|------|
| `pending_writes` | `transaction_id, query_id, worker_id, status, created_at` | Transaction-level record |
| `pending_files` | `transaction_id, file_path, file_size_bytes, worker_id, created_at` | File-level record |

**Write flow (with cleanup guarantees)**:
1. `Begin Transaction` → record into the `pending_writes` table
2. `Write Data Files` → for each file written, record into the `pending_files` table
3. `Commit Transaction` → on success: delete records from both tables; on failure: keep records, wait for cleanup

**OrphanFileCleanup scheduled task**:

```java
/**
 * Orphan file cleaner — scheduled fallback cleanup for residual files that §6.5 synchronous cleanup failed to remove
 */
public class OrphanFileCleanup implements Runnable {
    
    private final Duration orphanThreshold = Duration.ofHours(1);
    
    @Override
    public void run() {
        List<PendingWrite> abandoned = pendingWriteStore.findOlderThan(orphanThreshold);
        for (PendingWrite write : abandoned) {
            try {
                List<String> files = pendingFileStore.getFiles(write.getTransactionId());
                for (String file : files) {
                    fileSystem.delete(new Path(file));
                }
                pendingFileStore.delete(write.getTransactionId());
                pendingWriteStore.delete(write.getTransactionId());
            } catch (Exception e) {
                log.error("Failed to cleanup transaction: {}", write.getTransactionId(), e);
            }
        }
    }
}
```

### 10.2 Snapshot Reference Counting

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Snapshot Reference Counting                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: expiring/cleaning up snapshots may affect running queries             │
│  Solution: queries hold a snapshot reference; no cleanup while referenced        │
│                                                                                  │
│  Reference counting flow:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Query Start:                                                            │   │
│  │    snapshotRef = snapshotRefManager.acquire(tableId, snapshotId);       │   │
│  │    → snapshot_refs table: {table_id, snapshot_id, ref_count++, holders}  │   │
│  │                                                                          │   │
│  │  Query End:                                                              │   │
│  │    snapshotRef.release();                                               │   │
│  │    → snapshot_refs table: ref_count--                                    │   │
│  │    → cleanup allowed when ref_count == 0                                 │   │
│  │                                                                          │   │
│  │  Expire Snapshots:                                                       │   │
│  │    → check ref_count, skip referenced snapshots                          │   │
│  │    → log the reason for skipping                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Timeout protection:                                                             │
│  • Maximum reference hold time: 24 hours                                         │
│  • Force-released after timeout; the query may fail (acceptable)                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.3 Cross-Table Transaction Note

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Cross-Table Transaction                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design decision: cross-table transactions are not supported                    │
│                                                                                  │
│  Reasons:                                                                        │
│  1. Iceberg/Paimon do not natively support cross-table transactions             │
│  2. Distributed two-phase commit is highly complex with large performance overhead │
│  3. Most OLAP scenarios do not need cross-table transactions                    │
│                                                                                  │
│  Alternatives:                                                                   │
│  • Single-table transactions: fully supported, using Iceberg/Paimon native transactions │
│  • Application-level coordination: implement eventual consistency at the business layer │
│  • Idempotent writes: support retries, guarantee at-least-once                   │
│                                                                                  │
│  If cross-table transactions are needed in the future:                          │
│  • Introduce a Transaction Coordinator component                                 │
│  • Implement 2PC (Prepare → Commit/Rollback)                                    │
│  • Requires additional Transaction Log storage                                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Operations Support

### 11.1 Native Version Management

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Native Version Management                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: Java/Native version mismatch causes crashes or abnormal behavior      │
│  Solution: version validation + compatibility matrix                            │
│                                                                                  │
│  Version info:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  NativeVersion {                                                         │   │
│  │    major: 1,           // incompatible changes                          │   │
│  │    minor: 2,           // backward-compatible new features              │   │
│  │    patch: 3,           // bug fixes                                     │   │
│  │    protocolVersion: 1, // JNI protocol version                          │   │
│  │    buildTime: "...",                                                    │   │
│  │    gitCommit: "..."                                                     │   │
│  │  }                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Validation flow:                                                                │
│  1. When the Java layer loads the Native library, it calls nativeGetVersion()   │
│  2. Compare protocolVersion; refuse to start if mismatched                      │
│  3. Compare the major version; refuse to start if mismatched                    │
│  4. minor/patch mismatch only warns                                             │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Native version validation
 */
public class NativeVersionChecker {
    
    private static final int REQUIRED_PROTOCOL_VERSION = 1;
    private static final int REQUIRED_MAJOR_VERSION = 1;
    
    public static void checkVersion() {
        NativeVersion nativeVersion = NativeRuntime.getVersion();
        
        if (nativeVersion.getProtocolVersion() != REQUIRED_PROTOCOL_VERSION) {
            throw new IncompatibleNativeVersionException(
                "Protocol version mismatch: expected %d, got %d",
                REQUIRED_PROTOCOL_VERSION, nativeVersion.getProtocolVersion());
        }
        
        if (nativeVersion.getMajor() != REQUIRED_MAJOR_VERSION) {
            throw new IncompatibleNativeVersionException(
                "Major version mismatch: expected %d, got %d",
                REQUIRED_MAJOR_VERSION, nativeVersion.getMajor());
        }
        
        log.info("Native library loaded: version={}.{}.{}, commit={}",
            nativeVersion.getMajor(), nativeVersion.getMinor(), 
            nativeVersion.getPatch(), nativeVersion.getGitCommit());
    }
}
```

### 11.2 Rolling Upgrade

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Rolling Upgrade Procedure                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Upgrade order: Coordinator → Worker (one by one)                               │
│                                                                                  │
│  Coordinator upgrade:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Remove Coordinator-1 from the Load Balancer                          │   │
│  │  2. Wait for queries on Coordinator-1 to finish (or transfer on timeout) │   │
│  │  3. Stop Coordinator-1, upgrade, start                                   │   │
│  │  4. After the health check passes, add it back to the Load Balancer      │   │
│  │  5. Repeat 2-4 to upgrade the other Coordinators                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Worker upgrade:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Mark Worker-1 as DRAINING (does not accept new tasks)                │   │
│  │  2. Wait for tasks on Worker-1 to finish                                 │   │
│  │  3. Stop Worker-1, upgrade, start                                        │   │
│  │  4. Worker-1 re-registers with the Service Registry                      │   │
│  │  5. Repeat 1-4 to upgrade the other Workers                              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Version compatibility:                                                          │
│  • During the upgrade, old and new versions of Coordinator/Worker coexist        │
│  • A new Coordinator can schedule tasks to old Workers (protocol backward-compatible) │
│  • An old Coordinator does not schedule to new Workers (via version filtering)   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 11.3 Dynamic Configuration

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Dynamic Configuration                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Hot-reloadable configuration:                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  • Resource queue config (capacity, concurrency, timeout)                │   │
│  │  • Query limits (max memory, max execution time)                         │   │
│  │  • Log level                                                             │   │
│  │  • Sampling rate (Tracing)                                               │   │
│  │  • Feature Flags                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Non-hot-reloadable configuration (requires restart):                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  • Port number                                                           │   │
│  │  • Native library path                                                   │   │
│  │  • Memory pool size                                                       │   │
│  │  • Storage path                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Config distribution:                                                            │
│  • Storage: ZooKeeper /pangrova/config/                                            │
│  • Listen: Curator PathChildrenCache                                             │
│  • Effective: takes effect < 5s after a config change                            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Dynamic configuration manager
 */
public class DynamicConfigManager {
    
    private final CuratorFramework curator;
    private final Map<String, ConfigListener> listeners = new ConcurrentHashMap<>();
    
    public void start() {
        PathChildrenCache cache = new PathChildrenCache(curator, "/pangrova/config", true);
        cache.getListenable().addListener((client, event) -> {
            String key = ZKPaths.getNodeFromPath(event.getData().getPath());
            String value = new String(event.getData().getData());
            
            ConfigListener listener = listeners.get(key);
            if (listener != null) {
                listener.onConfigChanged(key, value);
            }
        });
        cache.start();
    }
    
    public void registerListener(String key, ConfigListener listener) {
        listeners.put(key, listener);
    }
    
    @FunctionalInterface
    public interface ConfigListener {
        void onConfigChanged(String key, String value);
    }
}
```


---

## 12. Data Lake SQL Syntax Coverage

### 12.1 Syntax Category Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Data Lake SQL Syntax Coverage                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • Full coverage of Iceberg/Paimon DDL/DML/system-operation syntax               │
│  • Unified syntax abstraction, hiding underlying data lake differences           │
│  • Support Pangrova-specific extended syntax                                        │
│                                                                                  │
│  Syntax categories:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Category        │ Syntax                            │ Support status   │   │
│  │  ────────────────────────────────────────────────────────────────────── │   │
│  │  DDL             │ CREATE/ALTER/DROP TABLE/VIEW      │ ✓ Full support   │   │
│  │  DML             │ INSERT/UPDATE/DELETE/MERGE        │ ✓ Full support   │   │
│  │  Query           │ SELECT/JOIN/UNION/SUBQUERY        │ ✓ Full support   │   │
│  │  Time Travel     │ FOR SYSTEM_TIME AS OF             │ ✓ Full support   │   │
│  │  Table Maint.    │ OPTIMIZE/VACUUM/COMPACT           │ ✓ Full support   │   │
│  │  Snapshot Mgmt   │ CREATE/ROLLBACK/EXPIRE SNAPSHOT   │ ✓ Full support   │   │
│  │  Branch/Tag      │ CREATE/DROP BRANCH/TAG            │ ✓ Iceberg        │   │
│  │  System          │ SHOW/DESCRIBE/EXPLAIN/ANALYZE     │ ✓ Full support   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```


### 12.2 DDL Syntax

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE TABLE
-- ═══════════════════════════════════════════════════════════════════════════════

-- Basic table creation
CREATE TABLE catalog.db.orders (
    id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(18,2),
    order_date DATE,
    status VARCHAR
)
USING ICEBERG                                    -- or PAIMON
PARTITIONED BY (order_date, bucket(16, customer_id))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

-- CTAS (Create Table As Select)
CREATE TABLE catalog.db.orders_2024 AS
SELECT * FROM catalog.db.orders WHERE order_date >= '2024-01-01';

-- Copy structure from another table
CREATE TABLE catalog.db.orders_backup LIKE catalog.db.orders;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALTER TABLE
-- ═══════════════════════════════════════════════════════════════════════════════

-- Column operations
ALTER TABLE catalog.db.orders ADD COLUMN region VARCHAR;
ALTER TABLE catalog.db.orders DROP COLUMN region;
ALTER TABLE catalog.db.orders RENAME COLUMN status TO order_status;
ALTER TABLE catalog.db.orders ALTER COLUMN amount TYPE DECIMAL(20,2);

-- Partition operations
ALTER TABLE catalog.db.orders ADD PARTITION FIELD bucket(8, region);
ALTER TABLE catalog.db.orders DROP PARTITION FIELD bucket(16, customer_id);

-- Property operations
ALTER TABLE catalog.db.orders SET TBLPROPERTIES ('write.format.default' = 'orc');
ALTER TABLE catalog.db.orders UNSET TBLPROPERTIES ('write.format.default');

-- Rename
ALTER TABLE catalog.db.orders RENAME TO catalog.db.orders_archive;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DROP TABLE
-- ═══════════════════════════════════════════════════════════════════════════════

DROP TABLE catalog.db.orders;
DROP TABLE IF EXISTS catalog.db.orders PURGE;    -- Also delete data files
```


### 12.3 DML Syntax

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- INSERT
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO catalog.db.orders VALUES (1, 100, 99.99, DATE '2024-01-01', 'pending');
INSERT INTO catalog.db.orders SELECT * FROM staging.orders WHERE date = '2024-01-01';
INSERT OVERWRITE catalog.db.orders PARTITION (order_date = '2024-01-01')
    SELECT * FROM staging.orders WHERE date = '2024-01-01';

-- ═══════════════════════════════════════════════════════════════════════════════
-- UPDATE / DELETE
-- ═══════════════════════════════════════════════════════════════════════════════

UPDATE catalog.db.orders SET status = 'shipped' WHERE id = 12345;
DELETE FROM catalog.db.orders WHERE order_date < '2023-01-01';

-- ═══════════════════════════════════════════════════════════════════════════════
-- MERGE (Upsert)
-- ═══════════════════════════════════════════════════════════════════════════════

MERGE INTO catalog.db.orders t
USING staging.orders_update s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.status = s.status, t.amount = s.amount
WHEN NOT MATCHED THEN INSERT (id, customer_id, amount, order_date, status)
    VALUES (s.id, s.customer_id, s.amount, s.order_date, s.status);
```

### 12.4 Time Travel Queries

```sql
-- Query historical data by timestamp
SELECT * FROM catalog.db.orders FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 10:00:00';

-- Query by snapshot ID (Iceberg)
SELECT * FROM catalog.db.orders FOR VERSION AS OF 1234567890;

-- Query by tag (Iceberg)
SELECT * FROM catalog.db.orders FOR VERSION AS OF 'release-v1.0';

-- Incremental query (changes between two snapshots)
SELECT * FROM catalog.db.orders 
    FOR SYSTEM_TIME BETWEEN TIMESTAMP '2024-01-01' AND TIMESTAMP '2024-01-02';
```


### 12.5 Table Maintenance Syntax

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- OPTIMIZE (small file compaction)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Full-table optimization
OPTIMIZE TABLE catalog.db.orders;

-- Optimize a specific partition
OPTIMIZE TABLE catalog.db.orders WHERE order_date >= '2024-01-01';

-- Specify target file size
OPTIMIZE TABLE catalog.db.orders 
    SET ('target-file-size-bytes' = '134217728');  -- 128MB

-- ═══════════════════════════════════════════════════════════════════════════════
-- VACUUM (orphan file cleanup)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Clean up orphan files (retain the last 7 days)
VACUUM TABLE catalog.db.orders RETAIN 168 HOURS;

-- Clean up and show the files that would be deleted (Dry Run)
VACUUM TABLE catalog.db.orders RETAIN 168 HOURS DRY RUN;

-- ═══════════════════════════════════════════════════════════════════════════════
-- EXPIRE SNAPSHOTS (expired snapshot cleanup)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Expire old snapshots
ALTER TABLE catalog.db.orders EXPIRE SNAPSHOTS OLDER_THAN TIMESTAMP '2024-01-01';

-- Retain the last N snapshots
ALTER TABLE catalog.db.orders EXPIRE SNAPSHOTS RETAIN LAST 10;

-- ═══════════════════════════════════════════════════════════════════════════════
-- REWRITE DATA FILES (data rewrite/sort)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Rewrite data by sort key (optimize query performance)
ALTER TABLE catalog.db.orders REWRITE DATA 
    USING SORT ORDER (order_date, customer_id);

-- Z-Order sort (multi-dimensional query optimization)
ALTER TABLE catalog.db.orders REWRITE DATA 
    USING ZORDER (customer_id, order_date);
```


### 12.6 Snapshot and Branch Management

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- SNAPSHOT management
-- ═══════════════════════════════════════════════════════════════════════════════

-- Create a snapshot (manually triggered)
ALTER TABLE catalog.db.orders CREATE SNAPSHOT;

-- Roll back to a specific snapshot
ALTER TABLE catalog.db.orders ROLLBACK TO SNAPSHOT 1234567890;

-- Roll back to a specific point in time
ALTER TABLE catalog.db.orders ROLLBACK TO TIMESTAMP '2024-01-01 10:00:00';

-- Cherry-pick a snapshot (Iceberg)
ALTER TABLE catalog.db.orders CHERRY_PICK SNAPSHOT 1234567890;

-- ═══════════════════════════════════════════════════════════════════════════════
-- BRANCH / TAG management (Iceberg)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Create a branch
ALTER TABLE catalog.db.orders CREATE BRANCH dev;
ALTER TABLE catalog.db.orders CREATE BRANCH dev AS OF VERSION 1234567890;

-- Create a tag
ALTER TABLE catalog.db.orders CREATE TAG 'release-v1.0';
ALTER TABLE catalog.db.orders CREATE TAG 'release-v1.0' AS OF VERSION 1234567890;

-- Delete a branch/tag
ALTER TABLE catalog.db.orders DROP BRANCH dev;
ALTER TABLE catalog.db.orders DROP TAG 'release-v1.0';

-- Switch branch (on write)
INSERT INTO catalog.db.orders VERSION AS OF 'dev' VALUES (...);
```

### 12.7 System Operation Syntax

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- SHOW / DESCRIBE
-- ═══════════════════════════════════════════════════════════════════════════════

SHOW CATALOGS;
SHOW DATABASES IN iceberg;
SHOW TABLES IN iceberg.db;
SHOW CREATE TABLE catalog.db.orders;
SHOW PARTITIONS catalog.db.orders;
SHOW SNAPSHOTS catalog.db.orders;                -- Snapshot history
SHOW FILES catalog.db.orders;                    -- Data file list
SHOW MANIFESTS catalog.db.orders;                -- Manifest file list

DESCRIBE catalog.db.orders;
DESCRIBE EXTENDED catalog.db.orders;             -- Includes statistics

-- ═══════════════════════════════════════════════════════════════════════════════
-- EXPLAIN / ANALYZE
-- ═══════════════════════════════════════════════════════════════════════════════

EXPLAIN SELECT * FROM catalog.db.orders WHERE order_date = '2024-01-01';
EXPLAIN ANALYZE SELECT * FROM catalog.db.orders WHERE order_date = '2024-01-01';
EXPLAIN (FORMAT JSON) SELECT * FROM catalog.db.orders;

-- Collect statistics
ANALYZE TABLE catalog.db.orders;
ANALYZE TABLE catalog.db.orders COMPUTE STATISTICS FOR COLUMNS customer_id, amount;
```


---

## 13. Table Maintenance SPI Design

### 13.1 Design Goals

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Table Maintenance SPI Design                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Core problems:                                                                  │
│  • Iceberg/Paimon both currently need orphan file cleanup and small file        │
│    compaction; Hudi is a planned extension                                      │
│  • But their respective APIs and implementation details differ                  │
│  • A unified abstraction is needed, supporting global config and auto scheduling│
│                                                                                  │
│  Design principles:                                                              │
│  1. Unified interface: define a generic TableMaintenanceSpi, hiding low-level diffs│
│  2. Configurable: support global default config + table-level overrides          │
│  3. Schedulable: support manual trigger + automatic scheduled dispatch           │
│  4. Observable: metrics like execution status, duration, cleanup volume of tasks │
│                                                                                  │
│  Maintenance operation categories:                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Operation          │ Responsibility          │ Trigger condition       │   │
│  │  ────────────────────────────────────────────────────────────────────── │   │
│  │  Compaction         │ Small file compaction   │ File count/size threshold│  │
│  │  OrphanCleanup      │ Orphan file cleanup     │ Scheduled + manual      │   │
│  │  SnapshotExpire     │ Expired snapshot cleanup│ Snapshot count/time thresh│ │
│  │  DataRewrite        │ Data re-sorting         │ Manual trigger          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```


### 13.2 TableMaintenanceSpi Interface

<!-- Resolved: SPI-C02 — Adopted async model from 09-streaming.md §2.9 as authoritative definition.
     The previous synchronous interface (6 methods, Request DTO pattern) has been removed
     to eliminate the sync/async conflict. All maintenance operations use CompletableFuture
     return types with standardized method names (compactAsync, cleanOrphansAsync,
     expireSnapshotsAsync). See 10 §2.9 for the complete Java interface. -->

The authoritative `TableMaintenanceSpi` interface is defined in [09-streaming.md §2.9](./09-streaming.md). All maintenance operations are **async and non-blocking**, returning `CompletableFuture<*Result>`. The interface provides:

| Method | Return Type | Description |
|--------|-------------|-------------|
| `compactAsync(tableName)` | `CompletableFuture<CompactionResult>` | Merge small files and equality delete files |
| `cleanOrphansAsync(tableName, olderThan)` | `CompletableFuture<OrphanCleanupResult>` | Remove files not referenced by any snapshot |
| `expireSnapshotsAsync(tableName, olderThan)` | `CompletableFuture<SnapshotExpirationResult>` | Expire old snapshots |
| `getMaintenanceStatus(tableName)` | `MaintenanceStatus` | Get current maintenance status (synchronous) |

Each implemented Connector exposes `TableMaintenanceSpi` via its own `TableMaintenanceProvider`. The scheduling layer invokes the SPI uniformly without awareness of underlying format differences (Iceberg / Paimon today; Hudi when that connector is added).

### 13.3 Request/Result Data Model

Each maintenance operation corresponds to a group of Request/Result DTOs:

| Operation | Request key fields | Result key fields |
|------|-----------------|----------------|
| Compaction | `table, partitionFilter?, targetFileSizeBytes, minFilesToCompact, maxFilesToCompact, dryRun` | `inputFilesCount, outputFilesCount, inputBytesTotal, outputBytesTotal, duration` |
| OrphanCleanup | `table, retentionPeriod, location?, dryRun, maxFilesToDelete` | `orphanFilesFound, orphanFilesDeleted, bytesReclaimed` |
| SnapshotExpire | `table, retentionPeriod, retainCount` | `expiredCount, reclaimedBytes` |
| DataRewrite | `table, sortOrder, partitionFilter?` | `rewrittenFilesCount, duration` |

### 13.4 Connector Implementation Differences

| Operation | Iceberg | Paimon | Note |
|------|---------|--------|------|
| Compaction | `RewriteDataFiles` (by Partition) | `CompactAction` (by Bucket, LSM Level) | Paimon has a Level concept |
| Orphan Cleanup | `DeleteOrphanFiles` | `OrphanFilesClean` (including Changelog files) | Both need to scan storage vs metadata |
| Snapshot Expire | `ExpireSnapshots` | `ExpireSnapshots` | Similar API |
| Data Rewrite | `RewriteDataFiles` + Sort | Not supported | Paimon relies on LSM auto-sorting |
| Manifest Rewrite | `RewriteManifests` | N/A | Iceberg-specific |

### 13.5 Scheduling and Configuration

The maintenance scheduler's complete priority model, backpressure control, and processTaskQueue() implementation are authoritatively defined in [09-streaming.md §2.9](./09-streaming.md).

Core scheduling logic:
1. Periodically scan the `MaintenanceStatus` of all tables (small file count, snapshot count, etc.)
2. Decide whether to trigger a maintenance operation by threshold (file count ≥ `minFilesToCompact`, time since last execution ≥ `interval`)
3. Trigger an additional time-interval-based check after a write completes (`onWriteComplete`)
4. Concurrency control: `Semaphore` rate limiting, `maxConcurrentCompactions` upper bound

Configuration hierarchy: global default (`pangrova.yaml`) → table-level override (`TBLPROPERTIES`). For configuration parameters see [21-configuration-reference.md §12.11](./21-configuration-reference.md).

Maintenance metrics are exposed via Prometheus: `compaction_tasks_total`, `compaction_duration`, `orphan_files_deleted`, `snapshots_expired`, etc.

---

## 14. Version Compatibility Matrix

### 14.1 Java/Native Version Compatibility

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Java/Native Version Compatibility Matrix                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Compatibility rules:                                                            │
│  • Major version: incompatible changes, must upgrade in sync                    │
│  • Minor version: backward compatible, new Java works with old Native           │
│  • Patch version: bug fixes, fully compatible                                   │
│                                                                                  │
│  Version matrix:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Java Version  │ Native Version │ Protocol │ Compatibility │ Note      │   │
│  │  ────────────────────────────────────────────────────────────────────  │   │
│  │  1.0.x         │ 1.0.x          │ v1       │ ✓ Full       │ Initial version│ │
│  │  1.1.x         │ 1.0.x          │ v1       │ ✓ Backward   │ New Java feature││
│  │  1.1.x         │ 1.1.x          │ v1       │ ✓ Full       │ Recommended combo││
│  │  1.2.x         │ 1.1.x          │ v1       │ ✓ Backward   │ New Java feature││
│  │  1.2.x         │ 1.2.x          │ v2       │ ✓ Full       │ Protocol upgrade││
│  │  2.0.x         │ 1.x.x          │ v2       │ ✗ Incompatible│ Must upgrade in sync││
│  │  2.0.x         │ 2.0.x          │ v3       │ ✓ Full       │ Major version upgrade││
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Upgrade paths:                                                                  │
│  • 1.0.x → 1.1.x: upgrade Java first, then Native (optional)                    │
│  • 1.x.x → 2.0.x: must upgrade Java and Native simultaneously                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 14.2 Protocol Version Change Log

| Protocol | Change | Java Version | Native Version |
|----------|----------|-----------|-------------|
| v1 | Initial protocol | 1.0.0+ | 1.0.0+ |
| v2 | Added Dynamic Filter support | 1.2.0+ | 1.2.0+ |
| v3 | Refactored Shuffle protocol | 2.0.0+ | 2.0.0+ |

---

## 15. Dynamic Filter Degradation Strategy

### 15.1 Degradation Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Dynamic Filter Degradation Strategy                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem scenarios:                                                              │
│  • Build side has too much data, building the Dynamic Filter times out          │
│  • Network latency causes Dynamic Filter propagation timeout                    │
│  • Probe side has started scanning, Dynamic Filter not ready                    │
│                                                                                  │
│  Degradation strategy:                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Scenario              │ Threshold         │ Degradation behavior       │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Build timeout         │ 30s (configurable)│ Skip DF, continue execution│  │
│  │  Propagation timeout   │ 10s (configurable)│ Probe side doesn't wait, scans directly││
│  │  DF too large          │ 10MB (configurable)│ Degrade to Min/Max Range  │   │
│  │  Too many Build rows   │ 1M rows (configurable)│ Use BloomFilter instead of IN││
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 15.2 Degradation Implementation

```java
/**
 * Dynamic Filter degradation controller
 */
public class DynamicFilterDegradationController {
    
    private final DynamicFilterConfig config;
    
    /**
     * Evaluate whether degradation is needed
     */
    public DegradationDecision evaluate(DynamicFilterBuildContext ctx) {
        // 1. Check build timeout
        if (ctx.getBuildDuration().compareTo(config.getBuildTimeout()) > 0) {
            return DegradationDecision.skip("Build timeout exceeded");
        }
        
        // 2. Check data volume
        if (ctx.getDistinctValues() > config.getMaxDistinctValues()) {
            // Degrade to BloomFilter
            return DegradationDecision.useBloomFilter(
                ctx.getDistinctValues(), 
                config.getBloomFilterFpp());
        }
        
        // 3. Check serialized size
        if (ctx.getEstimatedSize() > config.getMaxFilterSize()) {
            // Degrade to Min/Max Range
            return DegradationDecision.useMinMaxRange(
                ctx.getMinValue(), 
                ctx.getMaxValue());
        }
        
        return DegradationDecision.proceed();
    }
    
    /**
     * Probe side wait strategy
     */
    public WaitDecision shouldWaitForFilter(ProbeContext ctx) {
        Duration elapsed = ctx.getWaitDuration();
        
        if (elapsed.compareTo(config.getProbeWaitTimeout()) > 0) {
            // Timed out, stop waiting
            metrics.recordDynamicFilterTimeout();
            return WaitDecision.proceedWithoutFilter();
        }
        
        // Continue waiting
        return WaitDecision.waitMore(config.getProbeWaitInterval());
    }
}
```

### 15.3 Configuration Parameters

```yaml
# Dynamic Filter degradation configuration
dynamic-filter:
  enabled: true
  
  # Build phase
  build:
    timeout: 30s                          # Build timeout
    max-distinct-values: 1000000          # Max number of distinct values
    max-filter-size: 10MB                 # Max filter size
    
  # Propagation phase
  propagation:
    timeout: 10s                          # Propagation timeout
    
  # Probe side
  probe:
    wait-timeout: 10s                     # Wait-for-DF timeout
    wait-interval: 100ms                  # Wait check interval
    
  # Degradation strategy
  degradation:
    bloom-filter-fpp: 0.01                # BloomFilter false positive rate
    use-min-max-fallback: true            # Whether to degrade to Min/Max
```

---

<!-- Resolves: OPT-001 Phase 2-3 — AQE Advanced Capabilities -->

## 16. Adaptive Query Execution (AQE)

This section describes the Adaptive Query Execution framework that re-optimizes query plans at runtime using actual shuffle statistics. AQE hooks into the Coordinator's stage scheduling loop: after each shuffle stage completes, `ShuffleStatsCollector` gathers partition sizes from Workers, and `AQEOptimizer` re-evaluates the downstream stage plan before scheduling it.

For the statistics foundation that AQE depends on, see [04-query-optimization.md §Statistics Collection](./04-query-optimization.md).

### 16.1 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Coordinator                                  │
│                                                                      │
│  ┌──────────────┐    ┌────────────────┐    ┌───────────────────┐    │
│  │  Stage DAG    │───▶│  AQE Optimizer  │───▶│  Re-planned       │    │
│  │  (initial)    │    │                │    │  Stage DAG        │    │
│  └──────────────┘    └───────┬────────┘    └───────────────────┘    │
│                              │                                       │
│                   ┌──────────▼────────────┐                          │
│                   │ ShuffleStatsCollector   │                          │
│                   │ (post-shuffle stats)    │                          │
│                   └────────────────────────┘                          │
│                              ▲                                       │
│              ┌───────────────┼───────────────┐                       │
│              │               │               │                       │
│         Worker-1        Worker-2        Worker-N                     │
│     shuffle_partition_stats (via heartbeat / dedicated RPC)          │
└─────────────────────────────────────────────────────────────────────┘
```

### 16.2 Capability 1 — Dynamic Coalescing of Shuffle Partitions

When actual shuffle output is much smaller than estimated, many partitions become tiny. AQE merges adjacent small partitions into larger ones to reduce scheduling overhead.

```pseudocode
function coalesceShufflePartitions(stageResult: ShuffleStageResult):
    partitionSizes = stageResult.getPartitionSizes()   // actual bytes per partition
    targetSize     = config.aqe.targetPartitionSize     // default 64 MB
    minPartitions  = config.aqe.minPartitions           // default 1

    coalescedGroups = []
    currentGroup = []
    currentSize  = 0

    for partition in partitionSizes:
        if currentSize + partition.size > targetSize AND currentGroup.nonEmpty:
            coalescedGroups.append(currentGroup)
            currentGroup = [partition]
            currentSize  = partition.size
        else:
            currentGroup.append(partition)
            currentSize += partition.size

    if currentGroup.nonEmpty:
        coalescedGroups.append(currentGroup)

    while coalescedGroups.size < minPartitions:
        splitLargest(coalescedGroups)

    return coalescedGroups   // downstream stage reads coalesced partitions
```

### 16.3 Capability 2 — Dynamic Join Strategy Switching

After the build side of a join completes, AQE checks actual data size. If the build side is small enough, it converts a SortMergeJoin or ShuffleHashJoin into a BroadcastHashJoin, cancelling the probe-side shuffle.

```pseudocode
function maybeConvertToBroadcastJoin(joinStage: JoinStage, buildStats: ShuffleStats):
    buildSideSize      = buildStats.totalBytes
    broadcastThreshold = config.aqe.broadcastThreshold   // default 10 MB

    if buildSideSize <= broadcastThreshold:
        newPlan = convertToBroadcastJoin(joinStage)
        cancelShuffleStage(joinStage.probeSideShuffleStage)
        broadcastData(buildStats.data, joinStage.probeSideTasks)
        return newPlan

    return joinStage   // keep original plan
```

Key integration points:
- `StageScheduler.onShuffleStageComplete(stageId, ShuffleStats)` — callback that triggers re-evaluation
- `cancelShuffleStage()` — cancels already-scheduled probe-side shuffle tasks
- Broadcast uses the existing Arrow Flight data distribution path

### 16.4 Capability 3 — Dynamic Skew Join Handling

When a single partition receives disproportionate data (> 5× median AND > 256 MB), AQE splits it into sub-partitions and replicates the matching partition from the other join side.

```pseudocode
function handleSkewedJoin(joinStage: JoinStage, shuffleStats: ShuffleStats):
    medianSize    = shuffleStats.medianPartitionSize
    skewThreshold = config.aqe.skewThreshold   // default 5x median AND > 256 MB

    for partition in shuffleStats.partitions:
        if partition.size > skewThreshold * medianSize AND partition.size > 256MB:
            numSplits = ceil(partition.size / medianSize)
            subPartitions = splitPartition(partition, numSplits)

            for sub in subPartitions:
                createTask(sub, replicatedOtherSide = partition.matchingPartition)

    return updatedJoinStage
```

### 16.5 Affected SPI Interfaces

| Interface | Change | Description |
|-----------|--------|-------------|
| `StageScheduler` | New callback | `onShuffleStageComplete(stageId, ShuffleStats)` |
| `TaskRequest` Protobuf | New field | `coalesced_partition_ranges` for coalesced reads |
| Worker heartbeat | New field | `shuffle_partition_stats` reporting per-partition sizes |

### 16.6 Trade-offs

| Aspect | Without AQE | With AQE |
|--------|-------------|----------|
| Performance | Fixed plan; fragile with imperfect stats | Self-corrects; 2-5× improvement for skewed/misestimated queries |
| Complexity | Simple — plan once, execute | Medium — Coordinator tracks shuffle stats and re-plans between stages |
| Risk | High — wrong plan decisions unrecoverable | Low — AQE only improves; worst case is no change |
| Latency overhead | None | ~50-100 ms per shuffle stage boundary |
| Memory overhead | None | ~1 KB per partition × partition count (ShuffleStats in Coordinator) |

### 16.7 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | ShuffleStats Collection | Add `shuffle_partition_stats` to Worker heartbeat; Coordinator collects and logs — no plan changes |
| 2 | Dynamic Coalescing | Merge small partitions in `StageScheduler`; safest capability — only merges, never changes join strategy |
| 3 | Dynamic Join Switching | Broadcast join conversion; requires `cancelShuffleStage()` and broadcast distribution |
| 4 | Skew Handling | Partition splitting + other-side replication; most complex, requires shuffle dedup logic |

---

<!-- Resolves: DEF-AP-014 — Observability integration for query execution -->

## 17. Observability Integration

This section defines the metrics and tracing integration points for the query execution layer. For the full observability design, see [14-observability.md](./14-observability.md).

### 17.1 Metrics

The following metrics from [14 §3](./14-observability.md) are emitted by query execution components:

| Metric | Type | Source Component | Description |
|--------|------|-----------------|-------------|
| `pangrova.query.submitted_total` | counter | QueryCoordinator | Total queries submitted |
| `pangrova.query.completed_total` | counter | QueryCoordinator | Total queries completed (label: status) |
| `pangrova.query.latency_ms` | histogram | QueryCoordinator | End-to-end query latency |
| `pangrova.stage.task_count` | gauge | StageScheduler | Active tasks per stage |
| `pangrova.task.execution_time_ms` | histogram | TaskExecutor | Per-task execution time |
| `pangrova.shuffle.bytes_transferred` | counter | ShuffleManager | Shuffle data volume |
| `pangrova.dynamic_filter.rows_filtered` | counter | DynamicFilterService | Rows eliminated by dynamic filters |
| `pangrova.aqe.reoptimizations_total` | counter | AQEOptimizer | AQE re-optimization events |
| `pangrova.aqe.partitions_coalesced` | counter | AQEOptimizer | Partitions merged by coalescing |
| `pangrova.aqe.joins_converted` | counter | AQEOptimizer | Joins converted to broadcast |
| `pangrova.aqe.skew_splits` | counter | AQEOptimizer | Skewed partitions split |

### 17.2 Tracing Spans

Query execution emits the following spans (see [14 §4](./14-observability.md) for span schema):

```
query.execute                              (Coordinator, SERVER)
├── query.parse                            (Coordinator, INTERNAL)
├── query.optimize                         (Coordinator, INTERNAL)
├── query.schedule                         (Coordinator, INTERNAL)
│   └── aqe.reoptimize                     (Coordinator, INTERNAL)
├── task.submit                            (Coordinator → Worker, CLIENT)
│   └── task.execute                       (Worker, SERVER)
│       └── native.execute                 (Worker → Native, INTERNAL)
├── shuffle.write / shuffle.read           (Worker, INTERNAL)
└── query.output                           (Output Worker, INTERNAL)
```

### 17.3 Log Events

- Query lifecycle transitions: QUEUED → PLANNING → SCHEDULING → RUNNING → FINISHED/FAILED (INFO)
- AQE re-optimization decisions: coalescing, join switching, skew handling (INFO)
- Dynamic filter generation and application events (DEBUG)
- Write path commit/abort events (INFO)

See [14 §7](./14-observability.md) for structured logging format.

During the PLANNING phase, `MetadataService.resolveTable()` performs security checks (authentication, RBAC, RLS filter injection, column masking). See [15-security.md §3.2](./15-security.md) for the security integration point.

---

<!-- Resolves: OPT-010 — External Shuffle Service -->

## 18. External Shuffle Service (OPT-010)

This section introduces an optional External Shuffle Service (ESS) that decouples shuffle data lifecycle from Worker processes. ESS runs as a separate long-lived process on each node, enabling shuffle data to survive Worker crashes and dynamic scale-down events.

For the embedded shuffle architecture (current default), see §3.3.9 (Shuffle Buffer) and [05-native-layer.md §11](./05-native-layer.md) (Shuffle Partitioner). ESS is an alternative backend selected via configuration.

### 18.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           Node                                   │
│                                                                  │
│  ┌──────────────────────┐     ┌────────────────────────────────┐│
│  │ Worker Process        │     │ Shuffle Service Process        ││
│  │                       │     │ (long-lived, independent)      ││
│  │ Velox Pipeline ───────┼────▶│                                ││
│  │   (shuffle write)     │ IPC │ ShuffleStore                   ││
│  │                       │     │  ├─ MemoryTier (off-heap)      ││
│  │ Velox Pipeline ◄──────┼─────│  ├─ SSDTier (Direct I/O)      ││
│  │   (shuffle read)      │ IPC │  └─ S3Tier (async backup)      ││
│  │                       │     │                                ││
│  └──────────────────────┘     │ gRPC API:                       ││
│                                │  - writePartition()             ││
│                                │  - readPartition() → stream     ││
│                                │  - removeStage()                ││
│                                └────────────────────────────────┘│
│                                                                  │
│  Worker can crash/restart without losing shuffle data             │
└─────────────────────────────────────────────────────────────────┘
```

### 18.2 Shuffle Service API

```protobuf
service ShuffleService {
    rpc WritePartition(WritePartitionRequest) returns (WritePartitionResponse);
    rpc ReadPartition(ReadPartitionRequest) returns (stream DataChunk);
    rpc RemoveStage(RemoveStageRequest) returns (RemoveStageResponse);
    rpc GetStatus(GetStatusRequest) returns (ShuffleServiceStatus);
}

message WritePartitionRequest {
    string query_id = 1;
    int32 stage_id = 2;
    int32 partition_id = 3;
    int32 attempt_id = 4;       // for speculative execution dedup
    bytes data = 5;             // Arrow IPC serialized batch
    bool is_last = 6;           // marks end of partition
}

message ReadPartitionRequest {
    string query_id = 1;
    int32 stage_id = 2;
    int32 partition_id = 3;
}
```

### 18.3 Tiered Storage

```pseudocode
class ShuffleStore:
    memoryTier: OffHeapMemoryPool    // fast path, bounded (e.g., 16GB)
    ssdTier: DirectIOFileStore       // overflow, larger (e.g., 500GB)
    config:
        memoryThreshold = 0.8        // spill to SSD when memory > 80%

    function writePartition(req):
        key = (req.query_id, req.stage_id, req.partition_id, req.attempt_id)
        if memoryTier.usage < memoryThreshold:
            memoryTier.append(key, req.data)
        else:
            ssdTier.append(key, req.data)

    function readPartition(req):
        key = (req.query_id, req.stage_id, req.partition_id)
        if memoryTier.contains(key):
            return memoryTier.read(key)
        return ssdTier.read(key)

    function removeStage(queryId, stageId):
        memoryTier.removePrefix((queryId, stageId))
        ssdTier.removePrefix((queryId, stageId))
```

### 18.4 Pluggable Shuffle Backend

```pseudocode
interface ShuffleWriter:
    function write(partitionId: int, batch: ArrowBatch)
    function flush()

class EmbeddedShuffleWriter implements ShuffleWriter:
    // Current default — shuffle data in Worker memory (§3.3.9)
    function write(partitionId, batch):
        taskMemoryPool.allocate(batch.size)
        shuffleBuffer[partitionId].append(batch)

class ExternalShuffleWriter implements ShuffleWriter:
    // Delegates to External Shuffle Service via gRPC
    function write(partitionId, batch):
        client.writePartition(WritePartitionRequest(queryId, stageId, partitionId, batch))

function createShuffleWriter(config):
    if config.shuffleService.enabled:
        return ExternalShuffleWriter(config.shuffleService.endpoint)
    return EmbeddedShuffleWriter()
```

### 18.5 Trade-offs

| Aspect | Embedded (Current) | External (ESS) |
|--------|-------------------|-----------------|
| Performance | Zero-copy within Worker; minimal latency | IPC overhead (~50–100μs per batch via Unix domain socket or gRPC) |
| Reliability | Worker crash loses shuffle data; S3 fallback adds latency | Shuffle data survives Worker crash; immediate local recovery |
| Scalability | Shuffle competes with compute for memory; cannot scale independently | Independent memory pool; can scale shuffle storage without adding Workers |
| Complexity | Simple — single process | Medium — separate process lifecycle, deployment, monitoring |
| Auto-scaling | Dangerous — scale-down loses shuffle | Safe — shuffle data persists on node even after Worker termination |
| Resource isolation | Shared Task Memory Pool | Dedicated off-heap memory + SSD; no contention with compute |

### 18.6 Migration Path

| Step | Phase | Description |
|------|-------|-------------|
| 1 | Pluggable Interface | Refactor `pangrova-shuffle` to use `ShuffleWriter`/`ShuffleReader` interfaces. Existing embedded implementation becomes `EmbeddedShuffleWriter`. |
| 2 | Shuffle Service Process | Implement standalone `ShuffleService` with gRPC API and tiered storage (memory + SSD). Deploy alongside Workers. |
| 3 | Worker Integration | Implement `ExternalShuffleWriter`/`ExternalShuffleReader`. Config flag `pangrova.shuffle.external.enabled` to opt in. |
| 4 | Coordinator Awareness | Coordinator tracks shuffle locations via Shuffle Service (not Worker heartbeat). Enables shuffle-aware scheduling and speculative execution dedup. |

> **Cross-reference**: Stage-Level Retry ([08 §13](./12-resilience.md)) depends on ESS for shuffle data validation — ESS ensures shuffle data survives Worker crash. Speculative Execution ([01 §10](./02-distributed-architecture.md)) uses `attempt_id` in `WritePartitionRequest` for shuffle dedup. The Shuffle Partitioner ([03 §11](./05-native-layer.md)) handles data partitioning before handoff to the shuffle backend (embedded or external).
