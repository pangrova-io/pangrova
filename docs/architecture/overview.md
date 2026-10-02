---
title: "System Overview"
sidebar_label: "System Overview"
sidebar_position: 2
---

# Pangrova — System Overview

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

## 1. Vision

Pangrova is a high-performance, horizontally scalable distributed SQL query engine for the
lakehouse. It is built on the Calcite + Arrow + Velox stack and realizes a **read/write-separated**
architecture: high-performance reads in C++ and full-lifecycle transactional writes in Java.

## 2. Technology Stack

```text
SQL Query
    ↓
[ANTLR4 SQL Parser]   - custom SQL extensions
    ↓
[Calcite Optimizer]   - rule- and cost-based optimization
    ↓
[Substrait Plan]      - cross-language execution plan
    ↓
[Velox Engine]        - C++ native execution engine
```

## 3. System Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   JDBC      │  │ REST API    │  │   CLI       │  │ ODBC(planned)│       │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
└─────────┼────────────────┼────────────────┼────────────────┼────────────────┘
          │                │                │                │
          └────────────────┴────────────────┴────────────────┘
                                    │
                           ┌────────▼────────┐
                           │  Load Balancer  │
                           │  (HAProxy/K8s)  │
                           └────────┬────────┘
                                    │
┌───────────────────────────────────┼─────────────────────────────────────────┐
│                        Coordinator Cluster (Multi-Active HA)                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │  Coordinator-1  │  │  Coordinator-2  │  │  Coordinator-3  │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │SQL Parser │  │  │  │SQL Parser │  │  │  │SQL Parser │  │             │
│  │  │Calcite Opt│  │  │  │Calcite Opt│  │  │  │Calcite Opt│  │             │
│  │  │Substrait  │  │  │  │Substrait  │  │  │  │Substrait  │  │             │
│  │  │Scheduler  │  │  │  │Scheduler  │  │  │  │Scheduler  │  │             │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │             │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘             │
│           │                    │                    │                       │
│  ┌────────┴────────────────────┴────────────────────┴────────┐             │
│  │                    HA Coordination Layer                   │             │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │             │
│  │  │  JDBC Store  │  │ Redis Cluster│  │  PostgreSQL  │     │             │
│  │  │State/Journal │  │Metadata Cache│  │ Query State  │     │             │
│  │  └──────────────┘  └──────────────┘  └──────────────┘     │             │
│  └───────────────────────────────────────────────────────────┘             │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ Control Plane (HTTP/REST)
                                    │ Data Plane (Arrow Flight)
┌───────────────────────────────────┼─────────────────────────────────────────┐
│                         Worker Cluster (Auto-Scaling)                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Worker Node                                                         │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │  Java Layer (JVM)                                            │   │   │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │   │   │
│  │  │  │ HTTP Server  │  │ Task Manager │  │ Memory Mgr   │       │   │   │
│  │  │  │ (Control)    │  │              │  │              │       │   │   │
│  │  │  └──────────────┘  └──────────────┘  └──────────────┘       │   │   │
│  │  └────────────────────────────┬────────────────────────────────┘   │   │
│  │                               │ JNI Bridge (Arrow C Data Interface)│   │
│  │  ┌────────────────────────────┴────────────────────────────────┐   │   │
│  │  │  C++ Layer (Velox)                                          │   │   │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │   │   │
│  │  │  │Velox Engine  │  │ Native I/O   │  │Shuffle       │       │   │   │
│  │  │  │(Compute)     │  │ (Read/Write) │  │Partitioner   │       │   │   │
│  │  │  └──────────────┘  └──────────────┘  └──────────────┘       │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
┌───────────────────────────────────┼─────────────────────────────────────────┐
│                         Unified Storage Layer                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Connector SPI (Trino-style Plugin Architecture)                     │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │   Iceberg    │  │    Paimon    │  │    Hudi      │               │   │
│  │  │  Connector   │  │  Connector   │  │  Connector   │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Storage Backend                                                     │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │     S3       │  │    HDFS      │  │   Local FS   │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 4. Core Design Principles

### 4.1 Read/Write Separation
- **Read path (C++):** Velox native execution engine, Arrow in-memory format, zero-copy transfer.
- **Write path (Java):** full transaction support, deep integration with lakehouse table formats.

### 4.2 High Availability
- Multi-active Coordinator architecture — no single point of failure.
- Automatic Worker registration and health checking.
- Query state is persisted to support failure recovery.

### 4.3 Extensibility
- Trino-style SPI plugin architecture.
- Isolated ClassLoaders to avoid dependency conflicts.
- Modular design supporting hot-pluggable components.

### 4.4 Observability
- Unified tracing with OpenTelemetry.
- Metrics collection with Prometheus.
- Structured logging.

### 4.5 Design Decision: Complete Control-Plane / Data-Plane Separation

**Decision.** The Coordinator (control plane) never participates in data processing. All data
operations — including final result aggregation — are executed by Workers.

**Rationale.** The Coordinator is a pure Java control-plane node: no Velox execution engine, no
spill mechanism, no native memory management. Letting it process data would introduce OOM risk,
break control-plane/data-plane isolation, and make it a bottleneck under high concurrency.
Following the Presto/Trino experience, running the output stage on the Coordinator causes GC storms
and scheduling latency at production scale.

**Trade-off analysis.**

| Option | Pros | Cons |
|---|---|---|
| A: Coordinator never processes data (chosen) | Full control/data-plane isolation; Coordinator is never a bottleneck; unbounded horizontal scaling | Slightly higher latency for simple queries (mitigated by load-based Output Worker scheduling and cache-affinity routing) |
| B: Coordinator runs a lightweight output stage | Low latency for simple queries; less Worker scheduling overhead | Coordinator carries data load, needs resource isolation, becomes a bottleneck at scale |
| C: Coordinator can run any operator | Maximum flexibility | Coordinator may bottleneck; violates control/data-plane separation |

**Design of the chosen option A.**

1. **Output Worker.** The Coordinator schedules a dedicated Worker task (the Output Task) to run the
   final aggregation (Gather + Merge + Limit + Final Agg + any operator). This task pulls partial
   results from other Workers, merges them, and streams the result directly to the client via
   Arrow Flight.
2. **Coordinator as pure proxy.** The Coordinator only tells the client "fetch your results from the
   Output Worker" (it returns the Arrow Flight endpoint address) and never touches data itself.
3. **Output Worker selection.** The Coordinator uses `DefaultWorkerSelector` to pick an ACTIVE
   Worker; when split file paths are available it prefers the cache summary reported via Worker
   heartbeats, then falls back to `CacheAffinityAssigner` consistent hashing, and finally to plain
   load-based scheduling.
4. **Uniform data plane.** Constant expressions, metadata rows, retrieval
   candidates, Top-K/RRF/linear fusion, reranking and cached rowsets use a
   Worker result endpoint as well. The Coordinator may validate terminal
   metadata and publish the signed endpoint, but it does not buffer or relay
   Arrow pages.

**Data flow.**
```text
Client → Coordinator → [schedule] → Workers execute → Output Worker (Gather+Merge+Limit+Agg) → Client
                  ↓                                        ↑
       returns Arrow Flight endpoint         Client connects directly to the Output Worker
```

**Scalability impact.** The Coordinator is fully stateless (apart from scheduling metadata), so it
scales horizontally without limit. All data processing happens on Workers and uniformly benefits
from Velox vectorization + spill + memory arbitration.

**Fault-tolerance impact.** If an Output Worker dies, the Coordinator detects the failure and
reschedules to another Worker, and the client reconnects to the new endpoint. This is more robust
than option B because Workers have full fault-tolerance machinery (spill, memory arbitration, task
retry).

**Latency impact.** Simple and metadata queries incur one Worker-scheduling hop.
This is the deliberate cost of a uniform, spillable and horizontally scalable
data plane; complex queries are dominated by Native execution and exchange
cost.

**Detailed design:** see [Query Execution §3.3](./query-execution.md).

### 4.6 Canonical Protocol Mapping

> **This table is the authoritative protocol mapping for the whole Pangrova architecture. Every
> protocol reference in the docs must match it.**

| Communication Path | Protocol | Classification | Description |
|---|---|---|---|
| Client → Coordinator | HTTP (REST) | Control Plane | Query submission, status, cancellation |
| Coordinator → Worker | HTTP (REST) | Control Plane | Task dispatch, status queries |
| Worker ↔ Worker (Shuffle) | Arrow Flight (gRPC) | Data Plane | Shuffle data exchange ¹ |
| Output Worker → Client (Result) | Arrow Flight (gRPC) | Data Plane | Result streaming. The Output Worker performs final aggregation and streams directly to the client; the Coordinator only returns the endpoint address |
| Java → Native | Protobuf + Arrow C Data Interface | Internal | TaskRequest dispatch |
| Native → Java | Arrow C Data Interface + Protobuf | Internal | Java pull: `nativeGetNextBatch()` zero-copy data batches + `nativeGetTaskStats()` status |

**Protocol selection principles.**
- **Control plane:** HTTP (REST) — simple, debuggable, mature ecosystem; suited to low-frequency
  metadata/scheduling operations.
- **Data plane:** Arrow Flight (gRPC) — zero-copy columnar transfer; suited to high-throughput data
  exchange.
- **Internal (JNI):** Protobuf + Arrow C Data Interface — cross-language zero-copy with minimal
  serialization overhead.

¹ Shuffle data passes through the Arrow C Data Interface (JNI) within each Worker before the Arrow
Flight network transfer. Full path: Native Velox Output → Arrow C Data Interface (JNI) → Java
ShuffleManager → Arrow Flight (gRPC) → Remote Worker Java → Arrow C Data Interface (JNI) → Native
Velox Input.

Note: Protobuf usage is scoped to Internal/Java↔Native protocols, Shared-State-layer structured
serialization (e.g. the `HeartbeatRequest` payload carried over HTTP/REST), and the cross-process
serialization definitions in `pangrova-protocol`. The control plane is HTTP/REST + JSON and the data
plane is Arrow Flight (gRPC). This rule reduces confusion about protocol boundaries.

> **Execution protocol.** The cluster admits only the canonical
> `pangrova.protocol.v2` descriptor/capability epoch. Java and Native compare the
> complete descriptor hash and fail closed before task admission on a mismatch.
> Mixed execution epochs, per-task negotiation and v1 fallbacks are not
> supported; upgrades drain or fence old Workers before the new epoch is
> admitted. Operational sequencing is described in
> [Day-2 Operations §1](../operations/day-2.md).

---

## 5. Module Design

### 5.1 `pangrova-spi` — Core SPI Definitions

The SPI layer contains stable cross-module contracts. Query lifecycle, planning,
and worker task execution are currently implemented by concrete coordinator and
worker services; they are not public `pangrova-spi` interfaces. The snippets below
are conceptual shapes, not source declarations.

```java
// Query lifecycle
public interface QueryLifecycle {
    QueryId createQuery(SessionContext session, String sql);
    QueryState getQueryState(QueryId queryId);
    void cancelQuery(QueryId queryId);
}

// Execution plan
public interface ExecutionPlan {
    PlanFragment getRootFragment();
    List<PlanFragment> getAllFragments();
    Map<PlanNodeId, Split> getSplitAssignments();
}

// Data-source connector (simplified; full interface in Connector Design §1.2)
public interface Connector {
    ConnectorMetadata getMetadata(ConnectorSession session);
    ConnectorSplitManager getSplitManager();
    ConnectorPageSourceProvider getPageSourceProvider();
    default ConnectorPageSinkProvider getPageSinkProvider() {
        throw new UnsupportedOperationException("Write not supported");
    }
    // Transaction management (see Connector Design §1.2 for full signatures)
    default ConnectorTransactionHandle beginTransaction(IsolationLevel level, boolean readOnly) { ... }
    default void commit(ConnectorTransactionHandle handle) {}
    default void rollback(ConnectorTransactionHandle handle) {}
    default void shutdown() {}
}

// Execution engine
public interface ExecutionEngine {
    TaskHandle submitTask(TaskDescriptor descriptor);
    TaskStatus getTaskStatus(TaskHandle handle);
    void cancelTask(TaskHandle handle);
}
```

**SPI module layout.**
```text
pangrova-spi/
├── src/main/java/org/pangrova/spi/
│   ├── ai/                  # AI capability SPIs (embedding, inference, rerank)
│   ├── annotation/          # Stability annotations (@Stable, @Experimental, @Internal)
│   ├── cache/               # Distributed cache SPI
│   ├── connector/           # Connector SPI
│   ├── coordination/        # Distributed coordination SPI (registry, lease, journal)
│   ├── exception/           # SPI exception definitions
│   ├── function/            # Function SPI
│   ├── index/               # Index interfaces
│   ├── indexing/            # Index management SPI
│   ├── memory/              # Memory management SPI
│   ├── observability/       # Observability SPI
│   ├── resilience/          # Fault-tolerance SPI (checkpoint)
│   ├── resource/            # Resource management SPI
│   ├── security/            # Security SPI
│   ├── streaming/           # Streaming SPI
│   └── transport/           # Transport-layer SPI
```

**SPI signature quick reference.**

> This table summarizes the key method signatures of all public Pangrova SPIs so developers can
> locate a contract quickly. Full Javadoc lives in each definition doc.

| SPI | Key method signatures | Definition | Example implementor |
|---|---|---|---|
| `ConnectorFactory` | `create(catalogName, config, context): Connector` | [Connector §1.2](./connectors.md) | [Developer Guide §3.2](../contributing/developer-guide.md) (CSV) |
| `ConnectorMetadata` | `listTables(session, schemaName: Optional<String>): List<SchemaTableName>`; `getTableHandle(...)`; `getColumnHandles(...)`; `getTableStatistics(...)` | [Connector §1.2](./connectors.md) | [Developer Guide §3.2](../contributing/developer-guide.md) |
| `ConnectorSplitManager` | `getSplits(tx, session, table, dynamicFilter, constraint): ConnectorSplitSource` | [Connector §1.2](./connectors.md) | [Developer Guide §3.2](../contributing/developer-guide.md) |
| `ConnectorPageSourceProvider` | `createPageSource(tx, session, split, table, columns, dynamicFilter): ConnectorPageSource` | [Connector §1.2](./connectors.md) | [Developer Guide §3.2](../contributing/developer-guide.md) |
| `ConnectorPageSinkProvider` | `createPageSink(tx, session, outputTableHandle)`, `createPageSink(tx, session, insertTableHandle)` | [Connector §1.2](./connectors.md) | — (optional) |
| `TableWriter` | `write(tableName, records, writeMode, context): WriteResult`, `writeBatch(...)` | [Streaming §2.6](./streaming.md) (authoritative) | Iceberg/Paimon |
| `TableMaintenanceSpi` | `compactAsync`, `cleanOrphansAsync`, `expireSnapshotsAsync`, `getMaintenanceStatus` | [Streaming §2.9](./streaming.md) (authoritative) | Iceberg/Paimon |
| `DistributedCacheStore` | `get/put/invalidate/invalidateAll/stats/close` | `pangrova-spi` | Distributed cache plugin |
| `TracingProvider` | `startSpan(...)`, `currentSpan()`, `inject(...)`, `extract(...)` | [Observability §2.1](./observability.md) | OpenTelemetry |
| `MetricsProvider` | `createCounter/createGauge/createHistogram/createUpDownCounter` | [Observability §2.1](./observability.md) | OpenTelemetry |
| `FederatedCatalog` | `listSchemas/listTables/getTable/getTableStatistics/getChangesSince/subscribeToChanges/close` | [Metadata §3.8](./metadata.md) (authoritative) | Provider plugin; none bundled |
| `FederatedCatalogFactory` | `name(): String`, `create(properties): FederatedCatalog` | [Metadata §3.8](./metadata.md) (authoritative) | Provider plugin |
| `Authenticator` | `getName/init/authenticate/supports/isHealthy/close` | [Security §2](./security.md) | Pluggable authentication backend |
| `LoggingProvider` | `getLogger(name): Logger`, `setLevel(name, level): void` | [Observability §2.1](./observability.md) | SLF4J / Log4j2 |
| `KeyManager` | `getDataEncryptionKey/createKey/rotateKey/encryptDataKey/decryptDataKey/getKeyVersion` | [Security §7](./security.md) | Key-management backend |
| `SplitConverter` | `convert(extension, session): List<SplitInfo>` | [Connector §3.5](./connectors.md) | Per-connector |

**Native extension points** are C++ registries and Velox integrations, not Java
`ServiceLoader` SPIs. See [native execution](./native-execution.md) for the native
boundary.

**SPI discovery.** Service-provider contracts that support plugin discovery register
through `META-INF/services/{fully-qualified-interface}`. Not every SPI is discovered
this way. Connector plugins use isolated class loaders; see
[connector design](./connectors.md).

### 5.2 `pangrova-protocol` — Communication Protocol Definitions

**Control-plane protocol (HTTP/REST).**

Coordinator REST API:
- `POST /v1/query/submit` — SubmitQuery
- `GET /v1/query/{queryId}/state` — GetQueryState
- `POST /v1/query/{queryId}/cancel` — CancelQuery
- `GET /v1/query/profile/{queryId}` — GetProfile (SVG flame graph)
- `GET /v1/query/profile/{queryId}/json` — GetProfileJson
- `GET /v1/query-status` — ListQueries (paginated)
- `GET /v1/query-status/{queryId}` — GetQueryDetail
- `GET /v1/query-status/{queryId}/stages` — GetQueryStages
- `POST /v1/workers/register` — RegisterWorker
- `DELETE /v1/workers/{workerId}` — DeregisterWorker
- `POST /v1/worker/heartbeat` — Heartbeat
- `POST /v1/task/status-report` — ReportTaskStatus (Worker → Coordinator)

> **Heartbeat transport.** Worker heartbeats use HTTP REST (`POST /v1/worker/heartbeat`) for
> resource reporting (CPU, memory, task slots). ZooKeeper ephemeral nodes are used only for the
> service registry (Worker liveness detection). The `HeartbeatRequest` Protobuf message defines the
> serialization format of the REST payload. See [Resource Management §13.1](./resource-management.md)
> for heartbeat content and [Distributed Architecture §3.3](./distributed.md) for the ZK registry.

Worker REST API:
- `POST /v1/task` — SubmitTask
- `DELETE /v1/task/{taskId}` — CancelTask
- `POST /v1/task/{taskId}/dynamic-filter` — InjectDynamicFilter
- `POST /v1/task/upstream-commit` — NotifyUpstreamCommit
- `GET /v1/health` — HealthCheck

Output Worker selection does not expose a Worker-side slot reservation API. The Coordinator
dispatches the Output Task through the normal Worker task endpoint and selects candidates via
`DefaultWorkerSelector`. See [Query Execution §3.3.8](./query-execution.md) for details.

Exchange inputs are immutable, fenced bindings in `TaskRequest`. Producer retries
create new task attempts; the control plane never replaces row-bearing sources on
a running task.

CDC API (Coordinator, mounted under `StreamingResource`):
- `POST /v1/streaming/cdc/register` — RegisterCdcStream: registers a CDC stream via the
  `CdcLifecycleManager` SPI; returns the stream ID, heartbeat interval, and lease TTL
- `POST /v1/streaming/cdc/heartbeat` — renew the heartbeat for an active stream.
  Streams that miss heartbeats beyond the lease TTL are expired by the lifecycle manager
  (`expireStaleStreams`); there is no explicit deregister or status endpoint yet.

See [Streaming §2](./streaming.md) for CDC design.

Cache API (Worker) — planned, not yet implemented:
- `POST /v1/cache/expire-snapshots` — NotifySnapshotExpired: the Coordinator broadcasts expired
  snapshot IDs; the Worker marks matching cache entries as stale. The proposed cache-eviction bridge
  bridge described in [Cache Layer §8.1](./caching.md) does not exist in code yet;
  until it lands, snapshot-bound cache entries are reclaimed by the eviction policy instead.

See [Cache Layer §8.1](./caching.md) for snapshot-expiry eviction.

**Data-plane protocol (Arrow Flight).**

All Arrow Flight interactions (shuffle and result streaming) use a unified `FlightTicket` encoding
serialized into the Arrow Flight `Ticket` bytes. The authoritative Protobuf definition is in
[Query Execution §3.3.4](./query-execution.md):

```protobuf
message FlightTicket {
    string query_id = 1;
    string task_id = 2;
    int32 output_partition = 3;     // Shuffle partition number
    TicketType type = 4;
}

enum TicketType {
    TICKET_TYPE_UNSPECIFIED = 0;
    TICKET_TYPE_SHUFFLE = 1;        // Shuffle data between compute Workers
    TICKET_TYPE_RESULT = 2;         // Final result from Output Worker to client
}
```

### 5.3 `pangrova-parser` — SQL Parser

**Custom SQL extensions.**
```antlr
statement
    : query | insertStatement | updateStatement | deleteStatement
    | mergeStatement | createTableStatement | createViewStatement
    | refreshStatement      // custom: refresh materialized view
    | compactStatement      // custom: data compaction
    | optimizeStatement     // custom: table optimization
    | vacuumStatement       // custom: purge expired data
    | createCatalogStatement   // dynamic catalog registration (Metadata §6.1)
    | dropCatalogStatement     // dynamic catalog removal (Metadata §6.1)
    | showCatalogsStatement    // list catalogs (Metadata §6.1)
    | describeCatalogStatement // describe catalog (Metadata §6.1)
    | alterCatalogStatement    // alter catalog config (Metadata §6.1)
    | callStatement            // CALL system.sync_catalog(), etc. (Metadata §6.1)
    ;

// Time-travel query
tableRef
    : qualifiedName
      (FOR (SYSTEM_TIME | SYSTEM_VERSION) AS OF expression)?
      (AS? alias)?
    ;
```

### 5.4 `pangrova-planner` — Query Optimizer

**Calcite integration.**
```text
┌─────────────────────────────────────────────────────────────────┐
│                      Pangrova Planner                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   Calcite Framework                      │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │   │
│  │  │   Parser    │  │  Validator  │  │  Optimizer  │      │   │
│  │  │  (Custom)   │  │  (Calcite)  │  │  (Calcite)  │      │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│  ┌───────────────────────────┴───────────────────────────┐     │
│  │                  Custom Rules                          │     │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │     │
│  │  │ Pushdown    │  │ Partition   │  │ Statistics  │    │     │
│  │  │ Rules       │  │ Pruning     │  │ Based CBO   │    │     │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │     │
│  └───────────────────────────────────────────────────────┘     │
│                              │                                   │
│  ┌───────────────────────────┴───────────────────────────┐     │
│  │              Substrait Converter                       │     │
│  │  RelNode → Substrait Plan → Velox Executable          │     │
│  └───────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### 5.5 `pangrova-planner` Substrait Layer (`o.a.planner.substrait`)

> Post-consolidation: `pangrova-substrait` was merged into `pangrova-planner` as the
> `o.a.planner.substrait` package.

**Conversion pipeline.**
```text
Calcite RelNode
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              RelNode to Substrait Converter              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │ Rel Visitor │  │Type Mapper  │  │Func Mapper  │      │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
└─────────────────────────────────────────────────────────┘
       │
       ▼
Substrait Plan (Protobuf)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              Substrait to Velox Converter                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │Plan Visitor │  │Type Mapper  │  │Func Registry│      │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
└─────────────────────────────────────────────────────────┘
       │
       ▼
Velox PlanNode
```

---

## 6. Project Structure (25 modules)

> Consolidated: `pangrova-substrait` → `pangrova-planner`, `pangrova-tenant` → `pangrova-security`.

```text
pangrova/
├── .github/                              # GitHub configuration
│   ├── workflows/
│   │   ├── ci.yml                        # CI pipeline
│   │   ├── release.yml                   # Release pipeline
│   │   └── codeql.yml                    # Code security scanning
│   └── CODEOWNERS
├── docs/                                 # Documentation
│   ├── architecture/                     # Architecture design docs
│   ├── guide/                            # User guides
│   └── develop/                          # Deployment & operations
├── bin/                                  # Launch / operations scripts
│   ├── pangrova-standalone.sh              # Start standalone
│   ├── pangrova-coordinator.sh             # Start Coordinator
│   ├── pangrova-worker.sh                  # Start Worker
│   ├── pangrova-cli.sh                     # CLI client
│   ├── pangrova-stop.sh                    # Stop services
│   └── pangrova-health-check.sh            # Health check
│
├── pangrova-common/                        # Common utilities
├── pangrova-config/                        # Configuration management
├── pangrova-spi/                           # SPI definitions
├── pangrova-shuffle/                       # Shuffle exchange
├── pangrova-protocol/                      # Communication protocols
├── pangrova-parser/                        # SQL parser
├── pangrova-planner/                       # Query optimizer (+ substrait conversion)
├── pangrova-metadata/                      # Metadata management
├── pangrova-security/                      # Security (+ multi-tenancy)
├── pangrova-observability/                 # Observability
├── pangrova-resilience/                    # Fault tolerance
├── pangrova-resource/                      # Resource governance
├── pangrova-operations/                    # Operations
├── pangrova-governance/                    # Data governance
├── pangrova-streaming/                     # Streaming
├── pangrova-indexing/                      # Indexing
├── pangrova-transport/                     # Transport layer
├── pangrova-coordinator/                   # Coordinator service
├── pangrova-worker/                        # Worker service (Java)
├── pangrova-worker-native/                 # Worker native layer (C++)
├── pangrova-standalone/                    # Standalone single-JVM mode
├── pangrova-connector/                     # Connector parent module
│   ├── pangrova-connector-iceberg/         # Iceberg connector
│   └── pangrova-connector-paimon/          # Paimon connector
├── pangrova-client/                        # Client parent module
│   ├── pangrova-client-cli/                # CLI client
│   ├── pangrova-client-jdbc/               # JDBC driver
│   └── pangrova-client-rest/               # REST client
├── pangrova-testing/                       # Test framework
│   ├── pangrova-test-benchmark/            # Performance benchmarks
│   ├── pangrova-test-chaos/                # Chaos testing
│   ├── pangrova-test-common/               # Shared test utilities
│   ├── pangrova-test-e2e/                  # End-to-end tests
│   ├── pangrova-test-fuzzer/               # Fuzzer tests
│   └── pangrova-test-smoke/                # Smoke tests
├── pangrova-native-bundle/                 # Native packaging
│
├── pom.xml                               # Parent POM
├── README.md
├── LICENSE
└── CONTRIBUTING.md
```

---

## 7. Module Dependencies (25 modules)

> **Arrow convention:** `A → B` means "A depends on B". Arrows point toward the dependency.

```text
                                    ┌─────────────┐
                                    │ pangrova-bom  │
                                    └──────┬──────┘
                                           │
                                    ┌──────▼──────┐
                                    │pangrova-common│
                                    └──────┬──────┘
                                           │
                                    ┌──────▼──────┐
                                    │  pangrova-spi │  (+ connector base + flight contracts)
                                    └──────┬──────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
             ┌──────▼──────┐       ┌───────▼───────┐      ┌──────▼──────┐
             │pangrova-parser│       │pangrova-protocol│      │pangrova-observ│
             └──────┬──────┘       └───────┬───────┘      └──────┬──────┘
                    │                      │                      │
             ┌──────▼──────────┐           │                      │
             │ pangrova-planner  │           │                      │
             │ (+ substrait)   │           │                      │
             └──────┬──────────┘           │                      │
                    │                      │                      │
                    └──────────────────────┼──────────────────────┘
                                           │
             ┌──────▼──────┐       ┌───────▼───────────┐
             │pangrova-coord │──────►│ pangrova-worker     │
             └─────────────┘       │ (+ flight)        │
                                   └───────┬───────────┘
                                           │
                                   ┌───────▼───────┐
                                   │pangrova-worker  │
                                   │   -native     │
                                   └───────────────┘
```

> **Note.** `pangrova-coordinator` → `pangrova-worker` is a compile-scope dependency strictly scoped to
> `org.pangrova.worker.transport.*` (transport layer). The Coordinator must NOT access Worker
> internal business logic.

> **Auxiliary modules** (not shown above): `pangrova-client` (CLI, JDBC, REST; depends on
> `pangrova-protocol`), `pangrova-testing` (benchmark, chaos, e2e, fuzzer, smoke), `pangrova-native-bundle`
> (native library packaging).

---

## 8. Coding Conventions

### 8.1 Package Naming
```text
org.pangrova.{module}.{layer}.{feature}

Examples:
org.pangrova.coordinator.server.http      # Coordinator HTTP service
org.pangrova.worker.execution.operator    # Worker execution operators
org.pangrova.connector.iceberg.handle     # Iceberg handle classes
```

### 8.2 Class Naming

| Kind | Convention | Example |
|---|---|---|
| Interface | noun / adjective | `Connector`, `Closeable` |
| Implementation | prefix + interface | `IcebergConnector` |
| Abstract class | `Abstract` + noun | Abstract connector |
| Factory | noun + `Factory` | `ConnectorFactory` |
| Builder | noun + `Builder` | Query builder |
| Exception | noun + `Exception` | Query exception |
| Utility | noun + `Utils` | JSON utilities |
| Config | noun + `Config` | Coordinator config |

### 8.3 Clean-Code Structure

```java
/**
 * Example: a class following clean-code conventions.
 */
public class QueryExecutor {

    // 1. Constants first
    private static final int DEFAULT_TIMEOUT_MS = 30000;
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    // 2. Injected dependencies
    private final QueryParser parser;
    private final QueryOptimizer optimizer;
    private final ExecutionEngine engine;
    private final MetricsProvider metrics;

    // 3. Constructor
    public QueryExecutor(QueryParser parser,
                         QueryOptimizer optimizer,
                         ExecutionEngine engine,
                         MetricsProvider metrics) {
        this.parser = requireNonNull(parser, "parser is null");
        this.optimizer = requireNonNull(optimizer, "optimizer is null");
        this.engine = requireNonNull(engine, "engine is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    // 4. Public methods (ordered by importance)
    public QueryResult execute(String sql, QueryContext context) {
        validateInput(sql, context);
        try {
            Statement statement = parse(sql);
            ExecutionPlan plan = optimize(statement, context);
            return executeInternal(plan, context);
        } catch (Exception e) {
            handleError(e, context);
            throw e;
        }
    }

    // 5. Private methods (ordered by call sequence)
    private void validateInput(String sql, QueryContext context) { /* ... */ }
    private Statement parse(String sql) { /* ... */ }
    private ExecutionPlan optimize(Statement statement, QueryContext context) { /* ... */ }
    private QueryResult executeInternal(ExecutionPlan plan, QueryContext context) { /* ... */ }
    private void handleError(Exception e, QueryContext context) { /* ... */ }
}
```

---

## 9. Maven Dependency Management

### 9.1 Core Dependency Versions

```xml
<properties>
    <java.version>17</java.version>


    <antlr4.version>4.13.1</antlr4.version>
    <calcite.version>1.36.0</calcite.version>
    <avatica.version>1.25.0</avatica.version>
    <arrow.version>19.0.0</arrow.version>
    <substrait.version>0.23.0</substrait.version>
    <iceberg.version>1.11.0</iceberg.version>
    <paimon.version>1.4.1</paimon.version>
    <hadoop.version>3.3.6</hadoop.version>


    <grpc.version>1.65.0</grpc.version>
    <protobuf.version>4.33.4</protobuf.version>
    <netty.version>4.1.114.Final</netty.version>
    <guava.version>32.1.3-jre</guava.version>
    <jackson.version>2.16.0</jackson.version>
    <jetty.version>11.0.18</jetty.version>
    <jersey.version>3.0.16</jersey.version>


    <opentelemetry.version>1.32.0</opentelemetry.version>
    <micrometer.version>1.12.0</micrometer.version>
    <slf4j.version>2.0.9</slf4j.version>
    <logback.version>1.4.14</logback.version>


    <junit.version>5.10.1</junit.version>
    <assertj.version>3.24.2</assertj.version>
    <testcontainers.version>1.20.6</testcontainers.version>
    <jqwik.version>1.8.2</jqwik.version>
</properties>
```
