---
title: Native Execution Layer
sidebar_label: Native Execution Layer
---

# Pangrova — Native Execution Layer

> **Summary.** The C++ native read/compute layer: Velox integration, native I/O, and the zero-copy JNI data path.

## 1. Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Worker Node                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         Java Layer (JVM)                                 │    │
│  │                                                                          │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  │    │
│  │  │ HTTP Server  │  │Task Manager  │  │Memory Manager│  │Arrow Flight │  │    │
│  │  │ (Control)    │  │              │  │              │  │ (Data Plane)│  │    │
│  │  │• Task Submit │  │• Task Queue  │  │• JVM Heap    │  │• Shuffle    │  │    │
│  │  │• Status Query│  │• Lifecycle   │  │• Off-Heap    │  │• Result     │  │    │
│  │  │• Cancel      │  │• Pull Loop   │  │• Spill Ctrl  │  │• Broadcast  │  │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └─────────────┘  │    │
│  │                                                                          │    │
│  └──────────────────────────────────┬──────────────────────────────────────┘    │
│                                     │                                            │
│  ═══════════════════════════════════╪════════════════════════════════════════   │
│                          JNI Bridge │ (Arrow C Data Interface)                   │
│  ═══════════════════════════════════╪════════════════════════════════════════   │
│                                     │                                            │
│  ┌──────────────────────────────────┴──────────────────────────────────────┐    │
│  │                         C++ Layer (Native)                               │    │
│  │                                                                          │    │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │    │
│  │  │                    Execution Engine (Velox)                       │  │    │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │  │    │
│  │  │  │ Plan Parser │  │Task Executor│  │ Expression  │               │  │    │
│  │  │  │ (Substrait) │  │ (Pipeline)  │  │   Engine    │               │  │    │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘               │  │    │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │  │    │
│  │  │  │  Operators  │  │  Vectorized │  │    SIMD     │               │  │    │
│  │  │  │ (Scan/Join/ │  │  Processing │  │ Optimization│               │  │    │
│  │  │  │  Agg/Sort)  │  │             │  │             │               │  │    │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘               │  │    │
│  │  └──────────────────────────────────────────────────────────────────┘  │    │
│  │                                                                          │    │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │    │
│  │  │                    Memory Management                              │  │    │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │  │    │
│  │  │  │ Arrow Pool  │  │ Velox Pool  │  │ Spill Mgr   │               │  │    │
│  │  │  │ (Allocator) │  │ (MemoryPool)│  │ (NVMe SSD)  │               │  │    │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘               │  │    │
│  │  └──────────────────────────────────────────────────────────────────┘  │    │
│  │                                                                          │    │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │    │
│  │  │                    Native I/O Layer                               │  │    │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │  │    │
│  │  │  │Parquet/ORC  │  │  S3/HDFS    │  │  Shuffle    │               │  │    │
│  │  │  │  Reader     │  │ FileSystem  │  │ Serializer  │               │  │    │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘               │  │    │
│  │  └──────────────────────────────────────────────────────────────────┘  │    │
│  │                                                                          │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

> **Note**: The Native Writer and Shuffle Partitioner are responsible only for in-memory data partitioning and buffering; they do not perform persistent file writes. Persistent data file writes are handled by the Java layer through the Iceberg/Paimon SDK.

<!-- Resolves: IC-SF-007 — Renamed "Shuffle Serializer" to "Shuffle Partitioner" to reflect actual responsibility (partitioning + buffering, not serialization). Arrow IPC serialization happens in Java layer via Arrow Flight SDK. -->

## 2. Design Philosophy: Separation of Control Plane and Data Plane

Building on the core ideas of Velox, Pangrova adopts an architecture that separates the "control plane" from the "data plane":

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (Java)                                │
│                                                                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │ SQL Parser  │ →  │  Calcite    │ →  │  Substrait  │ →  │   Split     │      │
│  │  (ANTLR4)   │    │  Optimizer  │    │  Converter  │    │  Manager    │      │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘      │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Connector SPI (Java)                                  │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐              │   │
│  │  │ Metadata  │  │   Split   │  │ Predicate │  │ Statistics│              │   │
│  │  │  Provider │  │  Manager  │  │  Pushdown │  │  Provider │              │   │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
                                       │ Substrait Plan + Split Info
                                       │ (Protobuf Serialization)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Data Plane (C++ Velox)                              │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Native Connector SPI (C++)                            │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐              │   │
│  │  │DataSource │  │DataSource │  │  Native   │  │  Native   │              │   │
│  │  │  Factory  │  │  Reader   │  │  Writer   │  │  Spiller  │              │   │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Velox Execution Engine                                │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐              │   │
│  │  │ TableScan │  │  Filter   │  │  Project  │  │Aggregation│              │   │
│  │  │ Operator  │  │ Operator  │  │ Operator  │  │ Operator  │              │   │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 3. Dual-Layer Connector Architecture

### 3.1 Why a Dual-Layer Design?

| Layer | Language | Responsibilities | Rationale |
|------|------|------|------|
| Java Connector | Java | Metadata, Split planning, transactions | Deep integration with lakehouse format SDKs |
| Native Connector | C++ | Data reading, vectorized processing | Extreme performance, SIMD optimization |

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Dual-Layer Connector Architecture                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Java Connector Layer                                  │   │
│  │                                                                          │   │
│  │  Responsibilities:                                                       │   │
│  │  • Metadata management (Schema, Partition, Statistics)                   │   │
│  │  • Split planning (file listing, Row Group selection, partition pruning) │   │
│  │  • Predicate pushdown decisions (which conditions can be pushed down)    │   │
│  │  • Transaction management (Iceberg Snapshot, Paimon Changelog)           │   │
│  │  • DDL operations (Create/Alter/Drop Table)                              │   │
│  │                                                                          │   │
│  │  Output: SplitInfo (file path, offset, length, format, pushed filters)   │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                       │                                         │
│                                       │ SplitInfo (Protobuf)                   │
│                                       ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Native Connector Layer                                │   │
│  │                                                                          │   │
│  │  Responsibilities:                                                       │   │
│  │  • High-performance data reading (Parquet/ORC Native Reader)             │   │
│  │  • Vectorized decoding (SIMD accelerated)                                │   │
│  │  • Column pruning execution                                              │   │
│  │  • Predicate filtering execution (Row Group/Page level)                  │   │
│  │  • Delete File merging (Iceberg MOR)                                     │   │
│  │  • Write serialization (in-memory only, no disk write)                   │   │
│  │                                                                          │   │
│  │  Output: Arrow RecordBatch / Velox RowVector                             │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 4. Data Flow in Detail

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Query Execution Data Flow                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Coordinator                                                                     │
│       │                                                                          │
│       │ Stage DAG + Splits (Substrait Plan + Split Info)                        │
│       ▼                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Java Layer                                                              │    │
│  │       │                                                                  │    │
│  │       │ 1. Receive Task via HTTP (REST)                                 │    │
│  │       │ 2. Parse Substrait Plan (Java)                                  │    │
│  │       │ 3. Allocate Memory Budget                                       │    │
│  │       ▼                                                                  │    │
│  │  TaskManager.submitTask(SubstraitPlan, Splits, MemoryBudget)            │    │
│  │       │                                                                  │    │
│  └───────┼──────────────────────────────────────────────────────────────────┘    │
│          │                                                                       │
│          │ JNI: handle = nativeCreateTaskExecutor(taskRequestBytes)            │
│          │      nativeStartTask(handle)  // async, returns immediately          │
│          │ Control: Protobuf commands via nativeCommand(handle, proto)          │
│          ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  C++ Layer                                                               │    │
│  │       │                                                                  │    │
│  │       │ 4. Deserialize Substrait Plan                                   │    │
│  │       │ 5. Convert to Velox PlanNode                                    │    │
│  │       │ 6. Create Task with Memory Pool                                 │    │
│  │       ▼                                                                  │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │  │  Velox Task Execution                                           │   │    │
│  │  │                                                                  │   │    │
│  │  │  Pipeline 1: TableScan → Filter → Project                       │   │    │
│  │  │       │                                                          │   │    │
│  │  │       │ Read from Storage (Parquet/ORC via Native I/O)          │   │    │
│  │  │       │ Vectorized Processing (SIMD)                            │   │    │
│  │  │       ▼                                                          │   │    │
│  │  │  Pipeline 2: PartialAgg → Shuffle                               │   │    │
│  │  │       │                                                          │   │    │
│  │  │       │ Shuffle Output (via C++ Arrow Flight Server)             │   │    │
│  │  │       ▼                                                          │   │    │
│  │  │  Output: Serialized pages via C++ Flight servers                │   │    │
│  │  │                                                                  │   │    │
│  │  └─────────────────────────────────────────────────────────────────┘   │    │
│  │       │                                                                  │    │
│  └───────┼──────────────────────────────────────────────────────────────────┘    │
│          │                                                                       │
│          │ Result: C++ FlightResultServer serves pre-serialized pages           │
│          │ Shuffle: C++ FlightShuffleServer serves shuffle data                 │
│          │ Exchange: C++ PangrovaExchangeSource pulls from upstream Flight        │
│          ▼                                                                       │
│  Data Exchange (C++ Arrow Flight — no Java data-plane JNI)                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 5. Native ↔ Java Interaction Protocol

### 5.1 Protocol Design Principles

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     Native ↔ Java Interaction Protocol                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design principles:                                                             │
│  1. Pull model (data path): Java actively pulls data batches from Native        │
│  2. Synchronous Listener callback (memory arbitration): within a JNI call chain │
│     initiated by Java, Native may synchronously call back a pre-registered      │
│     Java Listener (memory-arbitration path only)                                │
│  3. Protocol-first: all cross-language interactions use Protobuf definitions.   │
│     Note: this describes the internal Java↔Native (JNI) protocol, which must    │
│     not be confused with the Control Plane (HTTP/REST); the Control Plane       │
│     interface is authoritatively REST/JSON, see Canonical Protocol Mapping.     │
│  4. Native Flight direct output: data transfer is served directly by the C++    │
│     Arrow Flight Server; Java only issues control-plane commands (start/stop    │
│     Flight Server) and does not participate in the data path                    │
│  5. Natural backpressure: Flight flow control + Velox OutputQueue capacity      │
│     limits, no extra backpressure mechanism needed                              │
│                                                                                  │
│  JNI interaction model classification:                                           │
│                                                                                  │
│  (A) Pull model — data path (batch retrieval, status, command):                │
│  • Java thread calling Native is naturally attached, zero extra overhead       │
│  • Clear exception handling: Java→Native failure → caught on the Java side     │
│  • Simple lifecycle: Java holds a Native handle (long)                         │
│  • Debug-friendly: the Pull call stack is clear, fully visible to the profiler │
│                                                                                  │
│  (B) Synchronous Listener callback — memory arbitration path (memory only):    │
│  • Use case: Velox MemoryArbitrator must synchronously request memory from Java │
│    during growCapacity()                                                        │
│  • Calling thread: always the same Java thread that initiated the JNI call      │
│    (already attached), no separate AttachCurrentThread required                 │
│  • Callback mechanism: Native calls the pre-registered Java AllocationListener  │
│    via a GlobalRef                                                              │
│  • Constraints:                                                                 │
│    (1) The Listener must be registered at init; its GlobalRef lifetime is bound │
│        to the Task                                                              │
│    (2) The callback must not block on a Java-side lock that could deadlock with │
│        the Native arbitration lock                                              │
│    (3) Memory-arbitration path only; forbidden for data transfer or other      │
│        control-plane operations                                                 │
│    (4) BlockAllocationListener batches small allocations (default 8MB block),   │
│        reducing JNI call frequency                                              │
│  • Reference implementation: Apache Gluten ListenableArbitrator +               │
│    BlockAllocationListener pattern                                              │
│                                                                                  │
│  Forbidden interaction patterns:                                                │
│  • Native standalone thread AttachCurrentThread calling back into Java          │
│    (a call chain not initiated by Java)                                         │
│  • Native performing network I/O                                                │
│  • Callback model on the data path (Pull must be used)                          │
│                                                                                  │
│  Interaction model:                                                             │
│  ┌─────────────┐                              ┌─────────────┐                   │
│  │  Java Layer │                              │ Native Layer│                   │
│  │             │  ──── Task Request ────────► │             │                   │
│  │             │       (Protobuf)             │             │                   │
│  │             │                              │             │                   │
│  │             │  ──── Pull Batch ──────────► │             │                   │
│  │             │  ◄─── Arrow C Data ───────── │             │ (repeated to EOF)│
│  │             │                              │             │                   │
│  │             │  ──── Pull Status ─────────► │             │                   │
│  │             │  ◄─── TaskStatus ──────────  │             │                   │
│  └─────────────┘                              └─────────────┘                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Pull-Based Data Flow Design

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Pull-Based Data Flow Architecture                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  End-to-end data flow (Java actively pulls):                                    │
│                                                                                  │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐       │
│  │ Storage │    │ Native  │    │  Java   │    │ Arrow   │    │ Client  │       │
│  │ (Parquet│    │ Velox   │    │ Layer   │    │ Flight  │    │         │       │
│  │  /ORC)  │    │ Pipeline│    │ (Pull)  │    │ Stream  │    │         │       │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘       │
│       │              │              │              │              │             │
│       │              │  getNext()   │              │   DoGet      │             │
│       │   Batch 1    │◄─────────── │   putNext    │◄──────────── │             │
│       ├─────────────►├────────────►├─────────────►├─────────────►│             │
│       │              │              │              │              │             │
│       │              │  getNext()   │              │              │             │
│       │   Batch 2    │◄─────────── │   putNext    │   (stream)   │             │
│       ├─────────────►├────────────►├─────────────►├─────────────►│             │
│       │              │              │              │              │             │
│       │              │  getNext()   │              │              │             │
│       │   null (EOF) │◄─────────── │  completed   │              │             │
│       │              ├────────────►├─────────────►├─────────────►│             │
│                                                                                  │
│  Key design points:                                                             │
│  1. Java layer: the Flight Server thread loops calling nativeGetNextBatch()     │
│  2. Native layer: the Velox Pipeline executes asynchronously, produced batches  │
│     go into an internal queue                                                   │
│  3. nativeGetNextBatch(): takes a batch from the queue; blocks if queue is empty │
│  4. Returning 0 signals end of data (EOF); returning < 0 signals an error       │
│  5. Backpressure: Pull mode — if Java does not pull, Native does not produce    │
│     (see below)                                                                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

<!-- Resolves: DEF-AP-006, RED-003 — Reduced duplicated backpressure chain; 02 §3.3.7 is authoritative for end-to-end chain -->

Natural Backpressure — Design Rationale:

The Pull mode architecture creates natural backpressure without any explicit signaling
mechanism. Java controls the data flow by calling `nativeGetNextBatch()` — if Java
stops pulling, the Native OutputQueue fills up, and the Velox Pipeline Driver yields.

Key advantage: zero extra mechanisms, zero JNI callbacks, zero semaphores.

> For the complete end-to-end backpressure chain (all 4 layers including cross-Worker
> propagation via ExchangeClient), see [03-query-execution.md §3.3.7](./03-query-execution.md).
```

### 5.3 Protocol Definition (Protobuf) — Overview

This section is the single authoritative definition of the Java ↔ Native (JNI) interaction protocol. All Proto messages are defined in the `pangrova-protocol` module.

```protobuf
// pangrova_native_protocol.proto
syntax = "proto3";
package pangrova.native.protocol;

option java_package = "org.pangrova.protocol.native";
option java_outer_classname = "NativeProtocol";

// ============================================================================
// Task Request: Java → Native (via nativeCreateTaskExecutor)
// ============================================================================

message TaskRequest {
    string task_id = 1;
    string query_id = 2;
    bytes substrait_plan = 3;               // Substrait Plan (binary)
    repeated SplitInfo splits = 4;
    ResourceLimit resource_limit = 5;
    map<string, string> session_properties = 6;
    TaskType task_type = 7;
    int32 stage_id = 8;                     // Stage ID for logging/metrics correlation
    reserved 9;                              // Was legacy_split_bytes — removed.
    int32 partition_id = 10;                // Partition ID for logging/metrics correlation
    WriteConfig write_config = 11;
    ShuffleConfig shuffle_config = 12;
    string tenant_id = 13;                  // Tenant identity for per-tenant governance
    string principal = 14;                  // Authenticated principal for audit
}

// Resolves: DSC-001 — stage_id and partition_id added to TaskRequest for Native-layer
// logging and metrics correlation. These fields are informational only — the Native
// execution engine does not use them for scheduling or plan decisions.

// Resolves: IC-RD-003 — Complete TaskDescriptor (JSON) → TaskRequest (Protobuf) field mapping
//
// | TaskDescriptor (JSON, 02 §3.3.3) | TaskRequest (Protobuf)     | Mapping                          |
// |-----------------------------------|----------------------------|----------------------------------|
// | taskId                            | task_id                    | Direct copy                      |
// | queryId                           | query_id                   | Direct copy                      |
// | substraitPlan (base64 string)     | substrait_plan (bytes)     | Base64 decode                    |
// | splits (base64 string array)      | splits (repeated SplitInfo)| Base64 decode each → Protobuf parse |
// | resourceLimit                     | resource_limit             | Direct field mapping             |
// | sessionProperties                 | session_properties         | Direct copy (map<string,string>) |
// | stageId                           | stage_id                   | Direct copy (for logging only)   |
// | partitionId                       | partition_id               | Direct copy (for logging only)   |
// | taskType                          | task_type                  | Enum mapping (see DEF-058 below) |
// | exchangeSources                   | — (Java-only)              | Managed by ExchangeClient        |
// | outputConfig                      | — (Java-only)              | Managed by OutputBufferManager   |
// | writeConfig                       | — (Java-only)              | Managed by Java TableWriter      |

// [DEF-057] TaskRequest contains only the fields required by the Native
// execution layer.  The following fields from the REST-level TaskDescriptor
// (03-query-execution.md §3.3.3) are managed exclusively by the Java
// TaskManager and are NOT serialized into this Protobuf message:
//
//   - exchangeSources — upstream Flight endpoints; Java layer injects
//                       exchange data via the separate JNI calls
//                       nativeAddExchangeBatch() / nativeNoMoreExchangeSources()
//   - outputConfig   — output buffering parameters (ticket, maxBufferedBytes,
//                       maxBufferedBatches, clientIdleTimeoutMs); managed by
//                       the Java OutputBufferManager, not passed to Native
//   - writeConfig    — write target table info; managed by Java TableWriter
//                       (see 09-streaming.md §2.6)
//
// This separation keeps the JNI protocol minimal.  Java TaskManager
// deserializes the full TaskDescriptor from the REST API, extracts the
// Native-relevant subset into TaskRequest, and manages the remaining
// fields independently.

// Resolves: IC-RD-001 — splits JSON serialization format
// The splits field in the REST TaskDescriptor JSON is an array of base64-encoded
// SplitInfo Protobuf bytes. Each element: SplitInfo.toByteArray() → Base64.encode().
// Worker Java layer: Base64.decode(each element) → SplitInfo.parseFrom(bytes) →
// add to TaskRequest.splits. This is consistent with substraitPlan serialization.

enum TaskType {
    TASK_TYPE_UNSPECIFIED = 0;
    TASK_TYPE_READ = 1;                     // Read-only query
    TASK_TYPE_WRITE = 2;                    // Write operation
    TASK_TYPE_SHUFFLE_READ = 3;             // Shuffle consumer
    TASK_TYPE_SHUFFLE_WRITE = 4;            // Shuffle producer
}

// Resolves: DEF-058 — TaskType inference rules from Java TaskDescriptor
// The Java Coordinator populates task_type when constructing TaskRequest from
// the TaskDescriptor JSON (02 §3.3.3). Inference rules:
//   - has splits AND no exchangeSources        → TASK_TYPE_READ
//   - has exchangeSources AND is output stage  → TASK_TYPE_SHUFFLE_READ
//   - SQL is INSERT/UPDATE/DELETE              → TASK_TYPE_WRITE
//   - SHUFFLE_WRITE is set internally by Velox PartitionedOutput operator
// The taskType field is NOT present in the REST TaskDescriptor JSON (02 §3.3.3)
// because it is a Native-layer execution semantic derived by the Java layer.

// ============================================================================
// SplitInfo: complete description of a single data split
// 
// Design principles:
// 1. Self-contained: once the Native layer has a SplitInfo it can read independently, without calling back to Java
// 2. Format-specific fields are distinguished via oneof connector_info, not passed as key info through properties
// 3. Common fields (file_path, schema, filter) are at the top level; format-specific fields live inside the oneof
// ============================================================================

// NOTE: This definition MUST match the actual proto files exactly.
// Authoritative sources:
//   Java:   pangrova-protocol/src/main/proto/pangrova/protocol/v1/task.proto
//   Native: pangrova-worker-native/proto/pangrova/native/protocol/v1/split.proto
// See docs/architecture/20-java-native-protocol.md §3 for field alignment contract.

message SplitInfo {
    string split_id = 1;
    string connector_id = 2;               // "iceberg", "paimon"
    string file_path = 3;
    FileFormat file_format = 4;
    int64 start_offset = 5;
    int64 length = 6;
    repeated ColumnMapping column_mapping = 7;  // Schema evolution column mappings
    map<string, string> partition_values = 8;
    oneof connector_info {
        IcebergSplitInfo iceberg_info = 9;
        PaimonSplitInfo paimon_info = 10;
    }
    SchemaEvolution schema_evolution = 11;
    string table_name = 12;                 // Logical table name for multi-table joins
    repeated IndexEvalHintInfo index_eval_hints = 13; // L2 index pruning hints
}

enum FileFormat {
    FILE_FORMAT_UNSPECIFIED = 0;
    FILE_FORMAT_PARQUET = 1;
    FILE_FORMAT_ORC = 2;
}

// Schema Evolution: column mapping and type promotion
// Field layout matches both Java and Native proto ColumnMapping exactly.
message ColumnMapping {
    int32 source_field_id = 1;
    int32 target_field_id = 2;
    string source_name = 3;
    string target_name = 4;
}

message TypePromotion {
    int32 field_id = 1;
    string source_type = 2;
    string target_type = 3;
    string column_name = 4;                 // Name-based matching (Velox RowType has no field IDs)
}

message SchemaEvolution {
    repeated ColumnMapping column_mappings = 1;
    repeated TypePromotion type_promotions = 2;
}

message IndexEvalHintInfo {
    string index_id = 1;
    string index_type = 2;
    string index_file_path = 3;
    int32 generation = 4;
    string predicate_json = 5;              // Predicate serialized via IndexPredicateCodec
    string data_file_path = 6;
}

// ============================================================================
// Iceberg Split Info
// 
// Covered read modes:
// - Snapshot Read (COW): snapshot_id + no delete_files
// - Snapshot Read (MOR Position Delete): snapshot_id + delete_files (POSITION)
// - Snapshot Read (MOR Equality Delete): snapshot_id + delete_files (EQUALITY)
// - Snapshot Read (MOR mixed Delete): snapshot_id + delete_files (POSITION + EQUALITY)
// - Time Travel: read_mode = ICEBERG_READ_TIME_TRAVEL, snapshot_id filled in after Java resolves it
// - Incremental Read: read_mode = ICEBERG_READ_INCREMENTAL, from/to_snapshot_id
// ============================================================================

message IcebergSplitInfo {
    IcebergReadMode read_mode = 1;
    int64 snapshot_id = 2;                  // Target Snapshot ID
    repeated DeleteFileInfo delete_files = 3;
    
    // Incremental Read
    int64 from_snapshot_id = 4;             // Incremental read start (exclusive)
    int64 to_snapshot_id = 5;               // Incremental read end (inclusive)
    
    // Iceberg V2 Row-Level Delete metadata
    int64 data_sequence_number = 6;         // Data File sequence number (used for Equality Delete filtering)
    int32 spec_id = 7;                      // Partition Spec ID
    int32 content = 8;                      // 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
}

enum IcebergReadMode {
    ICEBERG_READ_SNAPSHOT = 0;              // Default: snapshot read (COW or MOR)
    ICEBERG_READ_TIME_TRAVEL = 1;           // Time Travel (Java already resolved to snapshot_id)
    ICEBERG_READ_INCREMENTAL = 2;           // Incremental read (from_snapshot_id → to_snapshot_id)
}

message DeleteFileInfo {
    string file_path = 1;
    DeleteFileType delete_type = 2;
    repeated string equality_fields = 3;    // Matching field names for Equality Delete
    int64 record_count = 4;
    int64 data_sequence_number = 5;         // Delete File sequence number
    int64 file_size = 6;                    // Delete File size (used to choose the filtering strategy)
}

enum DeleteFileType {
    DELETE_FILE_TYPE_UNSPECIFIED = 0;
    DELETE_FILE_TYPE_POSITION = 1;          // Position Delete (file_path + pos)
    DELETE_FILE_TYPE_EQUALITY = 2;          // Equality Delete (field values)
}

// ============================================================================
// Paimon Split Info
// 
// Covered read modes:
// - Append Read: read_mode = APPEND, read directly
// - Primary Key Read (LSM Merge): read_mode = PRIMARY_KEY, lsm_levels describes files at each level
// - Changelog Read: read_mode = CHANGELOG, preserves +I/-D/+U/-U markers
// - Time Travel: snapshot_id filled in after Java resolves it
// - Incremental Read (Streaming): read_mode = INCREMENTAL, from/to_snapshot_id
// ============================================================================

message PaimonSplitInfo {
    PaimonReadMode read_mode = 1;
    int32 bucket_id = 2;
    repeated string primary_keys = 3;
    repeated LsmLevelInfo lsm_levels = 4;   // LSM per-level file info (PRIMARY_KEY/CHANGELOG modes)
    int64 snapshot_id = 5;                  // Target Snapshot ID
    
    // Incremental / Streaming Read
    int64 from_snapshot_id = 6;             // Incremental read start (exclusive)
    int64 to_snapshot_id = 7;               // Incremental read end (inclusive)
    
    // Merge Engine type (affects MergeReader behavior)
    PaimonMergeEngine merge_engine = 8;
    
    // Changelog Producer type (affects Changelog read behavior)
    PaimonChangelogProducer changelog_producer = 9;
}

enum PaimonReadMode {
    PAIMON_READ_MODE_UNSPECIFIED = 0;
    PAIMON_READ_MODE_APPEND = 1;            // Append-only table, read directly
    PAIMON_READ_MODE_PRIMARY_KEY = 2;       // Primary Key table, requires LSM Merge
    PAIMON_READ_MODE_CHANGELOG = 3;         // Changelog read (preserves +I/-D/+U/-U)
    PAIMON_READ_MODE_INCREMENTAL = 4;       // Incremental read (changes between from_snapshot → to_snapshot)
}

enum PaimonMergeEngine {
    PAIMON_MERGE_DEDUPLICATE = 0;           // Default: deduplicate, keep latest
    PAIMON_MERGE_PARTIAL_UPDATE = 1;        // Partial update: non-null fields overwrite
    PAIMON_MERGE_AGGREGATION = 2;           // Aggregation: SUM/COUNT/MAX/MIN etc.
    PAIMON_MERGE_FIRST_ROW = 3;             // Keep the first row
}

enum PaimonChangelogProducer {
    PAIMON_CHANGELOG_NONE = 0;              // No Changelog
    PAIMON_CHANGELOG_INPUT = 1;             // Input Changelog (records the input directly)
    PAIMON_CHANGELOG_LOOKUP = 2;            // Lookup Changelog (looks up old values to generate complete changes)
    PAIMON_CHANGELOG_FULL_COMPACTION = 3;   // Full Compaction Changelog
}

message LsmLevelInfo {
    int32 level = 1;                        // LSM level (0 = newest)
    repeated string file_paths = 2;
    repeated bytes key_ranges = 3;          // key range of each file (used for merge sorting)
    int64 total_record_count = 4;           // total rows at this level (used for merge strategy selection)
}

// ============================================================================
// Resource Limit
// ============================================================================

message ResourceLimit {
    int64 max_memory_bytes = 1;
    int32 max_threads = 2;
    int64 max_spill_bytes = 3;
    string spill_directory = 4;
}

// ============================================================================
// Task Status: Java Pull via nativeGetTaskStatus()
// ============================================================================

message TaskStatus {
    string task_id = 1;
    TaskState state = 2;
    TaskStats stats = 3;
    string error_message = 4;
    string error_stack_trace = 5;
    NativeError error_detail = 6;           // [DEF-114] Structured error info for NativeErrorMapper
}

// [DEF-114] NativeError provides structured error information that the Java
// NativeErrorMapper (12-resilience.md §3.3) uses for error-code-based
// classification.  When a task fails, the Native layer populates both the
// legacy string fields (error_message, error_stack_trace) for backward
// compatibility AND the structured error_detail for precise error mapping.
message NativeError {
    int32 error_code = 1;                   // Numeric code, 100-999 range (see 08 §3.2)
    string error_type = 2;                  // C++ exception type name
    string message = 3;                     // Human-readable error message
    string stack_trace = 4;                 // C++ stack trace
    string task_id = 5;                     // Originating task
    string operator_type = 6;              // Velox operator type (e.g., HashBuild, TableScan)
    int32 operator_id = 7;                 // Operator ID within the pipeline
    int64 rows_processed = 8;              // Rows processed before failure
    map<string, string> properties = 9;    // Additional context (e.g., file_path, partition)
}

enum TaskState {
    TASK_STATE_UNSPECIFIED = 0;
    TASK_STATE_PENDING = 1;
    TASK_STATE_RUNNING = 2;
    TASK_STATE_FLUSHING = 3;                // Pipeline finished executing, OutputQueue still has un-pulled data
    TASK_STATE_FINISHED = 4;
    TASK_STATE_FAILED = 5;
    TASK_STATE_CANCELLED = 6;
}

// TaskState cross-layer mapping notes:
//
// Java Worker perspective (03-query-execution.md §3.3.5):
//   RECEIVED → INITIALIZING → RUNNING → FINISHING → FINISHED / FAILED / CANCELLED
//
// Native layer TaskState mapping:
//   Java RECEIVED      —   No Native counterpart (TaskExecutor not yet created; Java-only state)
//   Java INITIALIZING  ←→  Native PENDING   (Task created, Pipeline not started)
// <!-- Resolves: DEF-059 — Java RECEIVED state has no Native counterpart -->
//   Java RUNNING       ←→  Native RUNNING   (Pipeline is executing)
//   Java FINISHING     ←→  Native FLUSHING  (Pipeline finished, OutputQueue draining)
//   Java FINISHED      ←→  Native FINISHED  (all data has been pulled by Java)
//   Java FAILED        ←→  Native FAILED
//   Java CANCELLED     ←→  Native CANCELLED
//
// FLUSHING state semantics:
// • All Drivers of the Velox Pipeline have finished execution
// • The OutputQueue / ShuffleQueue still has batches not yet pulled by Java
// • The Java layer keeps calling nativeGetNextBatch() until it returns 0 (EOF)
// • Once the OutputQueue is drained and Java receives EOF, the state transitions to FINISHED
// • In the FLUSHING state, nativeGetNextBatch() does not block (data is ready)
// • nativeCancelTask() is still valid in the FLUSHING state, discarding un-pulled data

message TaskStats {
    int64 input_rows = 1;
    int64 output_rows = 2;
    int64 input_bytes = 3;
    int64 output_bytes = 4;
    int64 cpu_time_nanos = 5;
    int64 wall_time_nanos = 6;
    int64 peak_memory_bytes = 7;
    int64 spilled_bytes = 8;
    
    // Per-operator stats
    repeated OperatorStats operator_stats = 10;
    
    // Read stats (from FormatReader + DeleteFilter + MergeReader)
    ReadStats read_stats = 11;
}

message OperatorStats {
    string operator_type = 1;
    int32 operator_id = 2;
    int64 input_rows = 3;
    int64 output_rows = 4;
    int64 cpu_time_nanos = 5;
}

message ReadStats {
    int64 row_groups_total = 1;
    int64 row_groups_skipped = 2;
    int64 pages_total = 3;
    int64 pages_skipped = 4;
    int64 bloom_filter_checks = 5;
    int64 bloom_filter_skipped = 6;
    int64 rows_read = 7;
    int64 bytes_read = 8;
    int64 delete_rows_filtered = 9;         // Rows filtered by DeleteFilter
    int64 merge_input_rows = 10;            // MergeReader input rows
    int64 merge_output_rows = 11;           // MergeReader output rows (after dedup)
    int64 io_requests = 12;
    int64 io_wait_nanos = 13;
    int64 cache_hits = 14;
    int64 cache_misses = 15;
}

// ============================================================================
// Shuffle Protocol: Java Pull via nativeGetShuffleBatch()
// ============================================================================

message ShuffleWriteRequest {
    string task_id = 1;
    int32 partition_id = 2;
    int64 sequence_number = 3;
    // Data transferred via Arrow C Data Interface (not in protobuf)
}

message ShuffleWriteResponse {
    bool success = 1;
    int64 bytes_written = 2;
    string error_message = 3;
}
```

Read mode → Proto field mapping overview:

| Read mode | connector_info | Key fields | Native component combination |
|---------|---------------|---------|----------------|
| Iceberg COW snapshot read | iceberg_info | snapshot_id, no delete_files | FormatReader |
| Iceberg MOR Position Delete | iceberg_info | snapshot_id, delete_files (POSITION) | FormatReader → PositionDeleteFilter |
| Iceberg MOR Equality Delete | iceberg_info | snapshot_id, delete_files (EQUALITY) | FormatReader → EqualityDeleteFilter |
| Iceberg MOR mixed Delete | iceberg_info | snapshot_id, delete_files (POSITION + EQUALITY) | FormatReader → PositionDeleteFilter → EqualityDeleteFilter |
| Iceberg Time Travel | iceberg_info | read_mode=TIME_TRAVEL, snapshot_id (resolved by Java) | Same as above (by snapshot type) |
| Iceberg Incremental Read | iceberg_info | read_mode=INCREMENTAL, from/to_snapshot_id | FormatReader (multiple files) |
| Paimon Append read | paimon_info | read_mode=APPEND | FormatReader |
| Paimon Primary Key (LSM) | paimon_info | read_mode=PRIMARY_KEY, lsm_levels, merge_engine | Multiple FormatReader → LsmMergeReader |
| Paimon Changelog read | paimon_info | read_mode=CHANGELOG, changelog_producer | Multiple FormatReader → ChangelogMergeReader |
| Paimon Incremental/Streaming | paimon_info | read_mode=INCREMENTAL, from/to_snapshot_id | FormatReader (incremental files) |
<!-- Resolves: DEF-069 — Clarify Paimon Time Travel does not need separate read_mode -->
| Paimon Time Travel | paimon_info | snapshot_id (resolved by Java); no dedicated read_mode — Java resolves timestamp to snapshot_id, then uses the table's default read_mode (APPEND or PRIMARY_KEY) | Same as above (by read_mode) |
| Schema Evolution (any format) | any | file_schema + column_mappings | FormatReader (read-time column mapping) |

> **C++ implementation class mapping**: the component names in the table are design-level abstractions; the corresponding actual C++ classes are:
> `FormatReader` → `ParquetFormatReader` / `OrcFormatReader`; `PositionDeleteFilter` → `RowDeleteFilter.h`; `EqualityDeleteFilter` → `DeletionVectorRowFilter.h` (instantiated via `RowDeleteFilterFactory`)

### 5.4 JNI Interface Definition (Pull Mode)

```java
// NativeRuntime.java (production class: org.pangrova.worker.jni.NativeRuntime)
package org.pangrova.worker.jni;

/**
 * Bridge between the Java and native layers (Pull mode)
 * 
 * All data flows: Java → Native (call) → Java (return value)
 * The native layer does not call back into Java and holds no references to Java objects
 */
public class NativeRuntime {
    
    static { loadNativeLibrary(); }
    
    // ========================================================================
    // Lifecycle
    // ========================================================================
    
    /** Initialize Native engine with config */
    public static native void nativeInit(String configJson);
    
    /** Shutdown Native engine, release all resources */
    public static native void nativeShutdown();
    
    // ========================================================================
    // Task Management
    // ========================================================================
    
    /**
     * Create a task executor, return handle
     * @param taskRequestBytes TaskRequest protobuf serialized bytes
     * @return executor handle (C++ pointer as long)
     */
    public static native long nativeCreateTaskExecutor(byte[] taskRequestBytes);
    
    /**
     * Start task execution (async, returns immediately)
     * Velox Pipeline starts executing in background threads
     *
     * <!-- Resolves: IC-RD-004 — nativeStartTask error handling specification -->
     * Error handling:
     *   - Substrait plan parse failure → throws IllegalArgumentException
     *   - Memory allocation failure    → throws OutOfMemoryError
     *   - Velox internal error         → throws RuntimeException (message contains Velox error code)
     *   - Invalid executor handle      → throws IllegalStateException
     * Java TaskManager catches these and maps to Pangrova ErrorCategory:
     *   IllegalArgumentException → USER_ERROR, OutOfMemoryError → RESOURCE_ERROR,
     *   RuntimeException → INTERNAL_ERROR, IllegalStateException → INTERNAL_ERROR
     */
    public static native void nativeStartTask(long executorHandle);
    
    /** Cancel a running task.
     * Idempotent: calling on FINISHED/FAILED/CANCELLED task is a no-op.
     * In FLUSHING state: discards un-pulled data and transitions to CANCELLED.
     */
    public static native void nativeCancelTask(long executorHandle);
    
    /** Release executor and all associated resources */
    public static native void nativeReleaseExecutor(long executorHandle);
    
    // ========================================================================
    // Data Pull (Result)
    // ========================================================================
    
    /**
     * Pull next result batch from Native (blocking if not ready)
     * 
     * @param executorHandle executor handle
     * @param arrowArrayPtr  pre-allocated ArrowArray memory address
     * @param arrowSchemaPtr pre-allocated ArrowSchema memory address
     * @return batch status:
     *         > 0: batch row count (data available at arrowArrayPtr/arrowSchemaPtr)
     *         = 0: no more data (EOF)
     *         < 0: error (call nativeGetTaskStatus for details)
     */
    public static native long nativeGetNextBatch(
        long executorHandle, long arrowArrayPtr, long arrowSchemaPtr);
    
    // ========================================================================
    // Data Pull (Shuffle)
    // ========================================================================
    
    /**
     * Pull next shuffle batch for a specific partition (blocking if not ready)
     * 
     * @param executorHandle executor handle
     * @param partitionId    target partition
     * @param arrowArrayPtr  pre-allocated ArrowArray memory address
     * @param arrowSchemaPtr pre-allocated ArrowSchema memory address
     * @return batch status: > 0 row count, = 0 EOF, < 0 error
     */
    public static native long nativeGetShuffleBatch(
        long executorHandle, int partitionId, 
        long arrowArrayPtr, long arrowSchemaPtr);
    
    /**
     * Get shuffle partition count for this task
     */
    public static native int nativeGetShufflePartitionCount(long executorHandle);
    
    // ========================================================================
    // Status & Stats Pull
    // ========================================================================
    
    /**
     * Pull current task status (non-blocking)
     * @return TaskStatus protobuf serialized bytes
     */
    public static native byte[] nativeGetTaskStatus(long executorHandle);
    
    /**
     * Pull current task stats (non-blocking)
     * @return TaskStats protobuf serialized bytes
     */
    public static native byte[] nativeGetTaskStats(long executorHandle);
    
    // ========================================================================
    // Trace-Aware Task Submission (see 14-observability.md §4.2)
    // ========================================================================
    
    /**
     * Create a task executor with OpenTelemetry trace context.
     * The native layer creates a child Span from the passed-in traceId/spanId,
     * enabling Java ↔ Native cross-layer trace correlation.
     * 
     * @param taskRequestBytes TaskRequest protobuf serialized bytes
     * @param traceId          W3C trace-id (32 char hex), from Span.current().getSpanContext()
     * @param parentSpanId     W3C parent span-id (16 char hex)
     * @param traceFlags       W3C trace flags byte (sampled=0x01)
     * @return executor handle (C++ pointer as long)
     * 
     * C++ side signature:
     *   TaskExecutor* nativeCreateTaskExecutorWithTrace(
     *       const uint8_t* taskRequestBytes, int32_t taskRequestLen,
     *       const char* traceId, const char* parentSpanId, uint8_t traceFlags);
     * 
     * C++ side behavior:
     *   1. Call NativeTracer::startSpanWithParent(traceId, parentSpanId, "velox.task.execute")
     *   2. Bind the Span to the TaskExecutor lifecycle
     *   3. Pipelines/Operators inside the TaskExecutor create child Spans
     *   4. All Spans are automatically end()-ed when the TaskExecutor is destroyed
     * 
     * When traceId is null or empty, this degrades to nativeCreateTaskExecutor() behavior (no trace).
     */
    public static native long nativeCreateTaskExecutorWithTrace(
        byte[] taskRequestBytes,
        String traceId,
        String parentSpanId,
        byte traceFlags);
```

**Full C++ implementation** (`pangrova_native_jni.cpp`):

```cpp
#include <jni.h>
#include "pangrova/execution/TaskExecutor.h"
#include "pangrova/observability/NativeTracer.h"
#include "pangrova/protocol/pangrova_native_protocol.pb.h"

using namespace pangrova;
using namespace pangrova::observability;

/**
 * nativeCreateTaskExecutorWithTrace — JNI entry point
 *
 * @param taskRequestBytes  TaskRequest protobuf bytes (Java byte[])
 * @param taskRequestLen    implicit from JNI GetArrayLength
 * @param traceId           W3C trace-id, 32 char hex (nullable)
 * @param parentSpanId      W3C parent span-id, 16 char hex (nullable)
 * @param traceFlags        W3C trace flags (0x01 = sampled)
 * @return                  TaskExecutor* reinterpret_cast to jlong; 0 on failure
 *
 * Error handling:
 *   - Protobuf parse failure → throws Java IllegalArgumentException
 *   - Memory allocation failure → throws Java OutOfMemoryError
 *   - Other C++ exceptions → throws Java RuntimeException with message
 */
extern "C" JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutorWithTrace(
    JNIEnv* env, jclass /*cls*/,
    jbyteArray taskRequestBytes,
    jstring traceId,
    jstring parentSpanId,
    jbyte traceFlags) {

    try {
        // 1. Parse TaskRequest from protobuf bytes
        jsize len = env->GetArrayLength(taskRequestBytes);
        jbyte* bytes = env->GetByteArrayElements(taskRequestBytes, nullptr);
        
        protocol::TaskRequest request;
        if (!request.ParseFromArray(bytes, len)) {
            env->ReleaseByteArrayElements(taskRequestBytes, bytes, JNI_ABORT);
            env->ThrowNew(
                env->FindClass("java/lang/IllegalArgumentException"),
                "Failed to parse TaskRequest protobuf");
            return 0;
        }
        env->ReleaseByteArrayElements(taskRequestBytes, bytes, JNI_ABORT);

        // 2. Create trace span (if traceId is provided and non-empty)
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> rootSpan;
        
        if (traceId != nullptr) {
            const char* traceIdStr = env->GetStringUTFChars(traceId, nullptr);
            const char* spanIdStr = parentSpanId
                ? env->GetStringUTFChars(parentSpanId, nullptr)
                : nullptr;
            
            if (traceIdStr != nullptr && strlen(traceIdStr) == 32) {
                rootSpan = NativeTracer::instance().startSpanWithParent(
                    std::string(traceIdStr),
                    spanIdStr ? std::string(spanIdStr) : "",
                    "velox.task.execute");
                
                rootSpan->SetAttribute("task.id", request.task_id());
                rootSpan->SetAttribute("query.id", request.query_id());
                rootSpan->SetAttribute("trace.flags", static_cast<int>(traceFlags));
            }
            
            env->ReleaseStringUTFChars(traceId, traceIdStr);
            if (spanIdStr) {
                env->ReleaseStringUTFChars(parentSpanId, spanIdStr);
            }
        }

        // 3. Create TaskExecutor with trace context bound
        auto executor = std::make_unique<execution::TaskExecutor>(
            request, std::move(rootSpan));
        
        // 4. Transfer ownership to Java (caller must call nativeReleaseExecutor)
        <!-- Resolves: DEF-070 — Unified naming: nativeDestroyTaskExecutor → nativeReleaseExecutor -->
        return reinterpret_cast<jlong>(executor.release());

    } catch (const std::bad_alloc& e) {
        env->ThrowNew(
            env->FindClass("java/lang/OutOfMemoryError"),
            "Native memory allocation failed");
        return 0;
    } catch (const std::exception& e) {
        env->ThrowNew(
            env->FindClass("java/lang/RuntimeException"),
            e.what());
        return 0;
    }
}
```

**Span binding in the TaskExecutor constructor** (`TaskExecutor.h`):

```cpp
namespace pangrova {
namespace execution {

class TaskExecutor {
public:
    /**
     * @param request   Parsed TaskRequest
     * @param rootSpan  Root span for this task (nullable — no trace if empty)
     */
    TaskExecutor(
        const protocol::TaskRequest& request,
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> rootSpan)
        : taskId_(request.task_id())
        , queryId_(request.query_id())
        , rootSpan_(std::move(rootSpan)) {
        
        // Parse Substrait plan → Velox PlanNode
        auto planNode = SubstraitParser::parse(request.substrait_plan());
        
        // Create Velox Task with memory pool
        auto memoryPool = createMemoryPool(request.resource_limit());
        task_ = std::make_shared<facebook::velox::exec::Task>(
            taskId_, std::move(planNode), /*destination=*/0, std::move(memoryPool));
        
        // Register splits
        for (const auto& split : request.splits()) {
            task_->addSplit(/*planNodeId=*/"0", toVeloxSplit(split));
        }
    }
    
    ~TaskExecutor() {
        // End root span on destruction — captures full task lifetime
        if (rootSpan_) {
            rootSpan_->SetAttribute("memory.peak_bytes", getPeakMemoryBytes());
            rootSpan_->SetAttribute("spill.bytes", getSpilledBytes());
            rootSpan_->SetStatus(
                state_ == TaskState::TASK_STATE_FINISHED
                    ? opentelemetry::trace::StatusCode::kOk
                    : opentelemetry::trace::StatusCode::kError);
            rootSpan_->End();
        }
    }

private:
    std::string taskId_;
    std::string queryId_;
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> rootSpan_;
    std::shared_ptr<facebook::velox::exec::Task> task_;
    TaskState state_ = TaskState::TASK_STATE_CREATED;
};

} // namespace execution
} // namespace pangrova
```
    
    // ========================================================================
    // Exchange Data Injection (Java → Native)
    // ========================================================================
    
    /**
     * Add an exchange batch to Native Exchange Operator's input queue
     * (for Output Task pulling data from upstream Compute Workers)
     *
     * <!-- Resolves: IC-RD-008 — exchangeSources↔Native Exchange Operator sourceId allocation -->
     * sourceId allocation rule: Java ExchangeClient assigns sourceId as a
     * zero-based index into the TaskDescriptor.exchangeSources array.
     * exchangeSources[0] → sourceId=0, exchangeSources[1] → sourceId=1, etc.
     * The Native Exchange Operator creates one input queue per sourceId.
     * Java ExchangeClient maintains this mapping and routes batches from
     * each upstream Flight connection to the correct sourceId.
     */
    /**
     * Inject exchange batch from upstream Worker into Native Exchange Operator.
     * Error handling:
     *   - Invalid/corrupt Arrow data → throws IllegalArgumentException
     *   - Invalid sourceId           → throws IllegalArgumentException
     *   - Task already terminated    → no-op (batch silently dropped)
     */
    public static native void nativeAddExchangeBatch(
        long executorHandle, int sourceId,
        long arrowArrayPtr, long arrowSchemaPtr);
    
    /** Mark an exchange source as completed */
    public static native void nativeNoMoreExchangeData(
        long executorHandle, int sourceId);
    
    /** Mark all exchange sources as completed */
    public static native void nativeNoMoreExchangeSources(long executorHandle);
    
    // ========================================================================
    // Dynamic Filter Injection (Java → Native)
    // ========================================================================
    
    /**
     * Inject dynamic filter into running task.
     * Error handling:
     *   - Invalid/unparseable filterBytes → throws IllegalArgumentException
     *   - Task not in RUNNING state       → no-op (filter silently ignored)
     * @param filterBytes Substrait Expression serialized bytes
     */
    public static native void nativeInjectDynamicFilter(
        long executorHandle, byte[] filterBytes);
    
    // ========================================================================
    // Memory Management (Java-Native Coordination, see 11-resource-management.md §12)
    // ========================================================================
    
    /**
     * Dynamically update task memory limit.
     * If newMaxBytes < current usage, triggers Spill. If Spill fails → throws RuntimeException.
     * Native adjusts Velox MemoryPool maxBytes; triggers Spill if new limit < current usage.
     */
    public static native void nativeUpdateMemoryLimit(
        long executorHandle, long newMaxBytes);

    // <!-- Resolves: DEF-071 — JNI error handling specs for nativeCancelTask, nativeAddExchangeBatch,
    //      nativeInjectDynamicFilter, nativeUpdateMemoryLimit added above -->
}
```

```java
// ResultPuller.java
package org.pangrova.worker.execution;

/**
 * Pull-mode result puller
 * The Java Flight Server thread repeatedly calls nativeGetNextBatch() to pull data
 */
public class ResultPuller {
    
    private final long executorHandle;
    private final BufferAllocator allocator;
    
    public ResultPuller(long executorHandle, BufferAllocator allocator) {
        this.executorHandle = executorHandle;
        this.allocator = requireNonNull(allocator, "allocator is null");
    }
    
    /**
     * Stream results to Arrow Flight client (blocking loop)
     * Called by Flight Server thread on DoGet request
     */
    public void streamTo(FlightProducer.ServerStreamListener listener) {
        try (ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
             ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
            
            while (true) {
                long rowCount = NativeRuntime.nativeGetNextBatch(
                    executorHandle,
                    arrowArray.memoryAddress(),
                    arrowSchema.memoryAddress());
                
                if (rowCount == 0) {
                    listener.completed();
                    return;
                }
                if (rowCount < 0) {
                    byte[] statusBytes = NativeRuntime.nativeGetTaskStatus(executorHandle);
                    TaskStatus status = TaskStatus.parseFrom(statusBytes);
                    throw new PangrovaException(status.getErrorMessage());
                }
                
                // Zero-copy import and stream to client
                try (VectorSchemaRoot batch = Data.importVectorSchemaRoot(
                        allocator, arrowArray, arrowSchema, null)) {
                    listener.putNext();
                }
            }
        }
    }
}
```

```java
// ShuffleDrainThread.java
package org.pangrova.worker.shuffle;

/**
 * Shuffle data drain thread (per partition).
 * Pulls data from Native Shuffle Queue via JNI and places it into a Java-side
 * ShuffleBuffer. Downstream Workers pull from this buffer via Arrow Flight DoGet
 * (Pull model — consistent with 03-query-execution.md §3.3.4).
 *
 * Resolves: IC-SF-002 — Unified to Pull model. ShuffleDrainThread does NOT call
 * putNext() directly to a downstream Flight client. Instead, it fills a local
 * ShuffleBuffer that the Flight Server streams from on DoGet requests.
 */
public class ShuffleDrainThread implements Runnable {
    
    private final long executorHandle;
    private final int partitionId;
    private final ShuffleBuffer shuffleBuffer;  // Java-side bounded buffer
    private final BufferAllocator allocator;
    
    @Override
    public void run() {
        try (ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
             ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
            
            while (true) {
                long rowCount = NativeRuntime.nativeGetShuffleBatch(
                    executorHandle, partitionId,
                    arrowArray.memoryAddress(),
                    arrowSchema.memoryAddress());
                
                if (rowCount == 0) {
                    shuffleBuffer.markComplete(partitionId);  // signal EOF
                    return;
                }
                if (rowCount < 0) {
                    // Resolves: IC-SF-001 — ShuffleDrainThread error classification
                    TaskStatus status = NativeRuntime.nativeGetTaskStatus(executorHandle);
                    if (status.getState() == TaskState.CANCELLED) {
                        shuffleBuffer.markCancelled(partitionId);
                        return;  // normal termination
                    }
                    throw new ShuffleException(
                        "Native shuffle error: " + status.getErrorMessage(),
                        status.getErrorCategory());  // RESOURCE_ERROR or INTERNAL_ERROR
                }
                
                // Zero-copy import into Java, then enqueue into ShuffleBuffer
                try (VectorSchemaRoot batch = Data.importVectorSchemaRoot(
                        allocator, arrowArray, arrowSchema, null)) {
                    shuffleBuffer.put(partitionId, batch);  // blocks if buffer full (natural backpressure)
                }
            }
        }
    }
}
```

> **Pull Model**: The Flight Server's `getStream()` handler reads from `ShuffleBuffer` when a downstream Worker calls `DoGet(FlightTicket{type=SHUFFLE})`. This is consistent with the Pull model described in [03-query-execution.md §3.3.4](./03-query-execution.md). Backpressure propagates naturally: if the downstream Worker consumes slowly, `ShuffleBuffer.put()` blocks, which causes `nativeGetShuffleBatch()` to block, which causes the Native Shuffle Queue to fill, which causes the Velox Pipeline to yield.

### 5.5 C++-side Pull Interface Implementation

```cpp
// TaskExecutor.h
#pragma once

#include <memory>
#include <string>
#include <arrow/c/bridge.h>

namespace pangrova {
namespace execution {

/**
 * Task Executor (Pull mode)
 * 
 * The Velox Pipeline executes asynchronously on background threads, and produced batches are placed into the OutputQueue.
 * Java pulls data from the OutputQueue by calling getNextBatch() via JNI.
 * 
 * Backpressure: Java stops pulling → OutputQueue fills up → Velox Driver yields
 */
class TaskExecutor {
public:
    explicit TaskExecutor(const TaskRequest& request);
    ~TaskExecutor();
    
    /** Start Velox Pipeline execution (async) */
    void start();
    
    /**
     * Pull next result batch (blocking if not ready)
     * @return row count: > 0 data ready, = 0 EOF, < 0 error
     */
    int64_t getNextBatch(ArrowArray* outArray, ArrowSchema* outSchema);
    
    /**
     * Pull next shuffle batch for partition (blocking if not ready)
     * @return row count: > 0 data ready, = 0 EOF, < 0 error
     */
    int64_t getShuffleBatch(int partitionId, 
                            ArrowArray* outArray, ArrowSchema* outSchema);
    
    int getShufflePartitionCount() const;
    
    /** Get current task status (non-blocking) */
    TaskStatus getTaskStatus() const;
    
    /** Get current task stats (non-blocking) */
    TaskStats getTaskStats() const;
    
    /** Cancel task */
    void cancel();
    
    /** Add exchange batch from Java (for Output Task) */
    void addExchangeBatch(int sourceId, ArrowArray* array, ArrowSchema* schema);
    void noMoreExchangeData(int sourceId);
    void noMoreExchangeSources();
    
    /** Inject dynamic filter */
    void injectDynamicFilter(const std::string& filterBytes);
    
    /** Dynamically update memory limit (called by Java when adjusting quota) */
    void updateMemoryLimit(int64_t newMaxBytes);

private:
    std::unique_ptr<VeloxTaskWrapper> veloxTask_;
    
    // Output queue: Velox Pipeline → getNextBatch()
    BoundedQueue<RowVectorPtr> outputQueue_;
    
    // Shuffle queues: Velox PartitionedOutput → getShuffleBatch()
    std::vector<BoundedQueue<RowVectorPtr>> shuffleQueues_;
    
    std::atomic<TaskState> state_{TaskState::TASK_STATE_PENDING};
    // Note: Protobuf-generated C++ enums use TASK_STATE_ prefix.
    // <!-- Resolves: DEF-066 — Unified to Protobuf-generated enum name with TASK_STATE_ prefix -->
    TaskStats stats_;
    std::string errorMessage_;
};

} // namespace execution
} // namespace pangrova
```

### 5.6 Memory Management Protocol

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Native Memory Management (standalone mode)                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design: native coordinates memory w/ Java via PangrovaArbitrator + Listeners     │
│                                                                                  │
│  Initialization:                                                                │
│  • Java creates NativeMemoryCoordinator (AllocationListener impl)               │
│  • AllocationListener jobject is passed in during nativeInit()                  │
│  • C++ creates PangrovaAllocationListener (GlobalRef) → BlockAllocationListener   │
│  •   → PangrovaArbitrator (custom MemoryArbitrator)                               │
│  • VeloxMemoryManager uses PangrovaArbitrator, not default SharedArbitrator       │
│                                                                                  │
│  Memory allocation coordination (synchronous Listener callback):                │
│  • Velox Operator needs memory → PangrovaArbitrator.growCapacity()                │
│  • → BlockAllocationListener batches (8MB block)                                │
│  • → PangrovaAllocationListener JNI callback to Java reserve()                    │
│  • → NativeMemoryCoordinator checks free JVM memory → grant or deny             │
│  • On denial, Native triggers spill-to-disk                                     │
│                                                                                  │
│  JVM memory pressure (Java → Native one-way signal):                            │
│  • Java GC monitor detects high pressure → NativeCommand MEMORY_PRESSURE        │
│  • Native triggers AsyncDataCache eviction / spill                              │
│                                                                                  │
│  Advantages:                                                                    │
│  • 0ms-latency memory coordination (synchronous callback, no polling)           │
│  • BlockAllocationListener batching reduces JNI call frequency                  │
│  • Caller is the same Java thread; no separate AttachCurrentThread              │
│                                                                                  │
│  Memory pool hierarchy (Native side):                                           │
│  Root Pool (max_memory_bytes)                                                   │
│    └── Query Pool                                                               │
│         └── Task Pool                                                           │
│              └── Node Pool                                                      │
│                   └── Operator Pool (LEAF, actual allocation)                   │
│                                                                                  │
│  Spill trigger:                                                                 │
│  • An Operator requests memory over the Task Pool quota                         │
│  • Velox MemoryArbitrator tries to reclaim from other Operators                 │
│  • Insufficient reclaim triggers Spill (OrderBy/HashBuild/HashAgg/Window)       │
│  • Spill directory set by TaskRequest.resource_limit.spill_directory            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.7 Protocol Version Management

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Protocol Version Management                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Version compatibility policy:                                                   │
│  • Major version: incompatible changes (field removal, semantic change)          │
│  • Minor version: backward compatible (new fields, new enum values)              │
│                                                                                  │
│  Version negotiation flow:                                                       │
│  1. Java layer passes its supported protocol version range at init               │
│  2. Native layer returns the protocol version actually used                      │
│  3. On version incompatibility, fail fast with a clear error                     │
│                                                                                  │
│  Extension mechanism:                                                            │
│  • SplitInfo.extensions: supports Connector-specific extension fields            │
│  • TaskRequest.session_properties: supports passing runtime configuration        │
│  • New fields use high numbers (20+), reserving space for core fields            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. JNI Bridge Design

### 6.1 Arrow C Data Interface

The Arrow C Data Interface is the key to zero-copy data transfer between Java and C++.

```cpp
// ArrowCDataBridge.h
#pragma once

#include <arrow/c/bridge.h>
#include <jni.h>

namespace pangrova {
namespace jni {

/**
 * Arrow C Data Interface wrapper
 * 
 * Core idea:
 * 1. The Java side allocates the memory addresses of ArrowArray and ArrowSchema
 * 2. The C++ side operates directly on these addresses, achieving zero-copy
 * 3. Memory lifecycle is managed via the release callback
 */
class ArrowCDataBridge {
public:
    // Import data from the Java ArrowArray pointer (zero-copy)
    static std::shared_ptr<arrow::RecordBatch> importRecordBatch(
        jlong arrowArrayPtr, jlong arrowSchemaPtr);
    
    // Export RecordBatch to Java (zero-copy)
    static void exportRecordBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        ArrowArray* outArray, ArrowSchema* outSchema);
    
    // Velox RowVector ↔ Arrow RecordBatch conversion
    static std::shared_ptr<arrow::RecordBatch> veloxToArrow(
        const facebook::velox::RowVectorPtr& rowVector, arrow::MemoryPool* pool);
    
    static facebook::velox::RowVectorPtr arrowToVelox(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        facebook::velox::memory::MemoryPool* pool);
};

} // namespace jni
} // namespace pangrova
```

<!-- Resolves: IC-RD-005 — Arrow C Data Interface memory ownership transfer semantics -->
**Memory Ownership Rules:**
- **Export (Native→Java)**: After `exportRecordBatch()`, the `ArrowArray.release` callback retains a `shared_ptr` reference to the underlying Arrow buffer. Java owns the `ArrowArray` struct and must call `release()` when done. The underlying memory is freed only when the last `shared_ptr` reference is released (reference-counted).
- **Import (Java→Native)**: After `importRecordBatch()`, Native takes ownership of the buffer via `shared_ptr`. Java must not access the `ArrowArray` after import. The `release` callback is invoked by the import to signal transfer.
- **Lifecycle**: Zero-copy is guaranteed only while the owning side holds the reference. Cross-boundary references must not outlive the JNI call scope unless explicitly retained.

### 6.2 JNI Bridge Main Interface (Pull mode)

```cpp
// JniBridge.h
#pragma once

#include <jni.h>
#include <memory>

namespace pangrova {
namespace jni {

/**
 * JNI bridge layer (Pull mode + Listener callback)
 * 
 * Responsibilities:
 * 1. Manage the native resource lifecycle
 * 2. Handle Java/C++ type conversion
 * 3. Exception propagation (C++ exception → Java exception)
 * 4. Thread safety (invoked by Java threads, naturally attached)
 * 
 * Data path: Pull mode — all batch/status methods are actively invoked by Java threads.
 * Memory arbitration: Listener callback — PangrovaArbitrator synchronously calls back the
 * Java AllocationListener via GlobalRef (same Java thread context, no separate Attach needed).
 */
class JniBridge {
public:
    // Lifecycle
    static void initialize(JNIEnv* env, jstring configJson);
    static void shutdown(JNIEnv* env);
    
    // Task Management
    static jlong createTaskExecutor(JNIEnv* env, jbyteArray taskRequestBytes);
    static void startTask(JNIEnv* env, jlong executorHandle);
    static void cancelTask(JNIEnv* env, jlong executorHandle);
    static void releaseExecutor(JNIEnv* env, jlong executorHandle);
    
    // Data Pull (Result)
    static jlong getNextBatch(JNIEnv* env, jlong executorHandle,
                              jlong arrowArrayPtr, jlong arrowSchemaPtr);
    
    // Data Pull (Shuffle)
    static jlong getShuffleBatch(JNIEnv* env, jlong executorHandle,
                                 jint partitionId,
                                 jlong arrowArrayPtr, jlong arrowSchemaPtr);
    static jint getShufflePartitionCount(JNIEnv* env, jlong executorHandle);
    
    // Status & Stats Pull
    static jbyteArray getTaskStatus(JNIEnv* env, jlong executorHandle);
    static jbyteArray getTaskStats(JNIEnv* env, jlong executorHandle);
    
    // Exchange Data Injection (Java → Native)
    static void addExchangeBatch(JNIEnv* env, jlong executorHandle,
                                 jint sourceId,
                                 jlong arrowArrayPtr, jlong arrowSchemaPtr);
    static void noMoreExchangeData(JNIEnv* env, jlong executorHandle, jint sourceId);
    static void noMoreExchangeSources(JNIEnv* env, jlong executorHandle);
    
    // Dynamic Filter Injection
    static void injectDynamicFilter(JNIEnv* env, jlong executorHandle,
                                    jbyteArray filterBytes);
    
    // Memory Management (Java-Native Coordination)
    static void updateMemoryLimit(JNIEnv* env, jlong executorHandle,
                                  jlong newMaxBytes);
};

} // namespace jni
} // namespace pangrova
```


## 7. Native Read Architecture

### 7.1 Architecture Overview

The Native Read Architecture uses a five-layer componentized design; each layer has a single responsibility and can be extended independently:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             Native Read Architecture                             │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Layer 5: Velox Adapter                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  PangrovaConnector → PangrovaVeloxConnector                                  │    │
│  │  Composes FormatReader + DeleteFilter + MergeReader + DynamicFilterSink  │    │
│  │  Output: Velox RowVector (consumed by the Velox Pipeline)                │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│       ▲                                                                          │
│       │                                                                          │
│  Layer 4: DynamicFilterSink                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  Receives Dynamic Filters produced at Velox runtime (from HashBuild      │    │
│  │  side); pushes down to the FormatReader layer to filter early at the     │    │
│  │  Row Group/Page level                                                    │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│       ▲                                                                          │
│       │                                                                          │
│  Layer 3: DeleteFilter / MergeReader                                             │
│  ┌────────────────────────────────────┐  ┌──────────────────────────────────┐    │
│  │  DeleteFilter SPI                  │  │  MergeReader (Paimon)            │    │
│  │  ├── PositionDeleteFilter          │  │  ├── LSM Multi-Level Merge       │    │
│  │  └── EqualityDeleteFilter          │  │  ├── Primary Key Dedup           │    │
│  │  Input: RecordBatch                │  │  └── Changelog Row Markers       │    │
│  │  Output: filtered RecordBatch      │  │  Input: List<FormatReader>       │    │
│  └────────────────────────────────────┘  │  Output: merged RecordBatch      │    │
│  ▲                                       └──────────────────────────────────┘    │
│       │                                                                          │
│  Layer 2: FormatReader SPI                                                       │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  FormatReader (pure format read, agnostic of lakehouse semantics)        │    │
│  │  ├── ParquetFormatReader (Row Group Stats + Page Index + Bloom Filter)   │    │
│  │  └── OrcFormatReader (Stripe Stats + Bloom Filter)                       │    │
│  │  Input: FileSplit + Filter                                               │    │
│  │  Output: RecordBatch                                                     │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│       ▲                                                                          │
│       │                                                                          │
│  Layer 1: I/O Scheduler                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  FileSystem (S3/HDFS/GCS/Local)                                          │    │
│  │  ├── CoalescedLoad (merge small I/O requests)                            │    │
│  │  ├── Async Prefetch (IoExecutor asynchronous prefetch)                   │    │
│  │  └── AsyncDataCache (Velox SSD Cache integration)                        │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

Component composition rules:

| Lakehouse Format | FormatReader | DeleteFilter | MergeReader | Schema Evolution | Note |
|-----------|-------------|-------------|-------------|-----------------|------|
| Iceberg COW | Parquet/ORC | None | None | column_mappings | Direct read |
| Iceberg MOR (Position Delete) | Parquet/ORC | PositionDeleteFilter | None | column_mappings | Roaring Bitmap filtering |
| Iceberg MOR (Equality Delete) | Parquet/ORC | EqualityDeleteFilter | None | column_mappings | Hash Set Anti-Join |
| Iceberg MOR (mixed Delete) | Parquet/ORC | PositionDeleteFilter + EqualityDeleteFilter | None | column_mappings | Chained composition |
| Iceberg Time Travel | Same as above (by snapshot type) | Same as above | Same as above | column_mappings | Java already resolved to snapshot_id |
| Iceberg Incremental Read | Parquet/ORC | None (incremental files have no delete) | None | column_mappings | Java expands the incremental file list |
| Paimon Append | Parquet/ORC | None | None | column_mappings | Direct read |
| Paimon Primary Key (LSM) | Parquet/ORC | None | LsmMergeReader | column_mappings | Multi-level SST merge & dedup |
| Paimon Changelog | Parquet/ORC | None | ChangelogMergeReader | column_mappings | +I/-D/+U/-U marker handling |
| Paimon Incremental | Parquet/ORC | None | ChangelogMergeReader | column_mappings | Incremental change files |
| Paimon Partial Update | Parquet/ORC | None | LsmMergeReader (PARTIAL_UPDATE) | column_mappings | Non-null field overwrite |
| Paimon Aggregation | Parquet/ORC | None | LsmMergeReader (AGGREGATION) | column_mappings | SUM/COUNT/MAX/MIN |

### 7.2 SplitInfo Protocol (reference)

The complete Proto definition of SplitInfo is in §5.3 (the single authoritative definition). This section explains how each field in §5.3 drives the five-layer component composition of §7.1.

After the native layer receives a SplitInfo, PangrovaVeloxConnector selects components according to the following rules:

```
if connector_info == iceberg_info:
    reader = FormatReaderFactory.create(file_format, file_path, ...)
    
        if column_mappings is not empty:
            reader = SchemaEvolutionWrapper(reader, column_mappings)  // read-time column mapping
    
        if delete_files is not empty:
        for df in delete_files:
            if df.delete_type == POSITION:
                reader = PositionDeleteFilter(reader, df)
            if df.delete_type == EQUALITY:
                reader = EqualityDeleteFilter(reader, df)
    
    if read_mode == INCREMENTAL:
            // The Java layer has already split incremental files between from/to_snapshot into multiple SplitInfo
            // The native layer simply reads them as ordinary Splits

elif connector_info == paimon_info:
    if read_mode == APPEND:
        reader = FormatReaderFactory.create(file_format, file_path, ...)
    
    elif read_mode == PRIMARY_KEY:
        readers = [FormatReaderFactory.create(...) for each lsm_level file]
        reader = LsmMergeReader(readers, primary_keys, merge_engine)
    
    elif read_mode == CHANGELOG:
        readers = [FormatReaderFactory.create(...) for each lsm_level file]
        reader = ChangelogMergeReader(readers, primary_keys, changelog_producer)
    
    elif read_mode == INCREMENTAL:
            // Same as CHANGELOG, but only includes incremental files between from/to_snapshot

    // All modes support DynamicFilterSink injection
dynamicFilterSink = DynamicFilterSink(reader)
```

### 7.3 FormatReader SPI

FormatReader is a pure file-format read abstraction and contains no lakehouse semantics (Delete, Merge, etc. are handled by upper-layer components).

```cpp
// FormatReader.h
#pragma once

#include <arrow/record_batch.h>
#include <arrow/compute/expression.h>
#include <memory>
#include <vector>

namespace pangrova {
namespace reader {

/**
 * Read statistics — maintained by each FormatReader instance
 */
struct ReadStats {
        int64_t rowGroupsTotal = 0;       // Total Row Groups / Stripes
        int64_t rowGroupsSkipped = 0;     // Skipped by Stats filtering
        int64_t pagesTotal = 0;           // Total Pages (Parquet only)
        int64_t pagesSkipped = 0;         // Skipped by Page Index filtering
        int64_t bloomFilterChecks = 0;    // Number of Bloom Filter checks
        int64_t bloomFilterSkipped = 0;   // Row Groups skipped by Bloom Filter
        int64_t rowsRead = 0;             // Actual rows read
        int64_t bytesRead = 0;            // Actual bytes read
        int64_t ioRequests = 0;           // Number of I/O requests
        int64_t ioWaitNanos = 0;          // I/O wait time
};

/**
 * FormatReader — pure format-read interface
 * 
 * Responsibilities:
 * - File format parsing (Parquet / ORC)
 * - Column pruning
 * - Row Group / Stripe level statistics filtering
 * - Page Index filtering (Parquet)
 * - Bloom Filter filtering
 * - Nested type reading (Struct / Map / List)
 * - Vectorized decoding
 * 
 * Not responsible for:
 * - Delete File handling (handled by DeleteFilter)
 * - LSM Merge (handled by MergeReader)
 * - Dynamic Filter (injected by DynamicFilterSink)
 */
class FormatReader {
public:
    virtual ~FormatReader() = default;
    
        /** Return the read Schema (after column pruning) */
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;
    
        /** Read the next batch; returns nullptr to indicate end */
    virtual std::shared_ptr<arrow::RecordBatch> readNext() = 0;
    
        /** Whether more data is available */
    virtual bool hasNext() const = 0;
    
    /**
         * Inject a filter expression (supports runtime appending, used for Dynamic Filter)
         * Implementations should push the filter down to Row Group / Page / Bloom Filter level
     */
    virtual void addFilter(std::shared_ptr<arrow::compute::Expression> filter) = 0;
    
        /** Get read statistics */
    virtual ReadStats getStats() const = 0;
    
        /** Close and release resources */
    virtual void close() = 0;
};

/**
 * FormatReaderFactory — factory interface
 */
class FormatReaderFactory {
public:
    virtual ~FormatReaderFactory() = default;
    virtual std::string formatName() const = 0;
    
    virtual std::unique_ptr<FormatReader> createReader(
        const std::string& filePath,
        int64_t startOffset,
        int64_t length,
        std::shared_ptr<arrow::Schema> requiredSchema,
        std::shared_ptr<arrow::MemoryPool> pool,
        const std::unordered_map<std::string, std::string>& config) = 0;
};

/**
 * FormatReaderRegistry — automatic registration
 */
class FormatReaderRegistry {
public:
    static FormatReaderRegistry& instance();
    void registerFactory(std::unique_ptr<FormatReaderFactory> factory);
    FormatReaderFactory* getFactory(const std::string& formatName);
};

#define REGISTER_FORMAT_READER_FACTORY(FactoryClass) \
    static bool _registered_##FactoryClass = []() { \
        FormatReaderRegistry::instance().registerFactory( \
            std::make_unique<FactoryClass>()); \
        return true; \
    }()

} // namespace reader
} // namespace pangrova
```

#### 7.3.1 ParquetFormatReader

Parquet format reader; filtering levels (coarse to fine):
1. Row Group Statistics — filter the entire Row Group by min/max/null_count
2. Bloom Filter — exact value existence check; skip Row Groups that do not contain the target value
3. Page Index — Column Index (min/max per page) + Offset Index; skip non-matching Pages
4. Row-level filtering — evaluate row by row after decoding (executed by the Velox Filter Operator, not in this layer)

Key config: `batchSize=8192`, `enablePageIndex=true`, `enableBloomFilter=true`, `prefetchRowGroups=2`

#### 7.3.2 OrcFormatReader

ORC format reader; filtering levels:
1. Stripe Statistics — similar to Parquet Row Group Statistics
2. Row Index (every 10000 rows) — ORC built-in row index
3. Bloom Filter — ORC native Bloom Filter support
4. SearchArgument — convert Arrow Expression to ORC SARG

Key config: `batchSize=8192`, `enableBloomFilter=true`, `enableRowIndex=true`

#### 7.3.3 Nested Type Support

Both Parquet and ORC support nested types (Struct / Map / List). FormatReader's nested-type handling strategy:

| Nested Type | Parquet Mapping | ORC Mapping | Arrow Type | Column Pruning Behavior |
|---------|-------------|---------|-----------|-----------|
| Struct | Group (REQUIRED/OPTIONAL) | Struct | `arrow::StructType` | Supports sub-field pruning (read only the needed sub-fields) |
| Map | MAP (key-value repeated group) | Map | `arrow::MapType` | Read as a whole (key + value) |
| List | LIST (repeated group) | List | `arrow::ListType` | Read as a whole |
| Nested Struct | Multi-level Group nesting | Multi-level Struct nesting | Nested `StructType` | Supports multi-level sub-field pruning |

Nested-type column pruning example:

```
Original Schema: order (Struct)
  ├── order_id (INT64)
  ├── items (List<Struct>)
  │     ├── product_id (INT64)
  │     ├── quantity (INT32)
  │     └── price (DOUBLE)
  └── shipping (Struct)
        ├── address (STRING)
        └── carrier (STRING)

Query: SELECT order.order_id, order.shipping.carrier FROM ...

After pruning, read:
  ├── order_id (INT64)          ← direct read
  └── shipping (Struct)         ← read only the carrier sub-field
        └── carrier (STRING)
```

### 7.4 DeleteFilter SPI

DeleteFilter is a component independent of FormatReader, responsible for handling lakehouse delete semantics.

```cpp
// DeleteFilter.h
#pragma once

#include <arrow/record_batch.h>
#include <roaring/roaring.hh>
#include <memory>

namespace pangrova {
namespace reader {

/**
 * DeleteFilter — delete filtering interface
 * 
 * Input: RecordBatch produced by FormatReader
 * Output: RecordBatch after filtering out deleted rows
 * 
 * Supports chained composition: batch → PositionDeleteFilter → EqualityDeleteFilter → output
 */
class DeleteFilter {
public:
    virtual ~DeleteFilter() = default;
    
        /** Initialize (load Delete File data) */
    virtual void initialize() = 0;
    
        /** Filter out deleted rows */
    virtual std::shared_ptr<arrow::RecordBatch> filter(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        int64_t startRowIndex) = 0;
    
        /** Number of deleted rows */
    virtual int64_t deletedCount() const = 0;
};
```

#### 7.4.1 PositionDeleteFilter

Position Delete Filter — Iceberg Position Delete.

Algorithm: the Delete File records `(file_path, position)` pairs; load all positions into a Roaring Bitmap for O(1) lookup.

Data flow:
1. The Java layer passes Position Delete file paths in `SplitInfo.delete_files`
2. The native layer reads the Delete File (Parquet format) and extracts the position column
3. Build a `Roaring64Map`
4. For each RecordBatch, check whether a row is deleted by `startRowIndex + offset`

#### 7.4.2 EqualityDeleteFilter

Equality Delete Filter — Iceberg Equality Delete.

Algorithm: the Delete File records the field-value combinations to delete; load and build a Hash Set, and perform an Anti-Join on each row.

<!-- Resolves: DEF-062 — Iceberg V2 data_sequence_number filtering semantics -->
**Sequence Number Pre-filtering (Iceberg V2)**: Before building the Hash Set, the filter applies sequence number semantics: only Delete Files whose `data_sequence_number >= data_file.data_sequence_number` are loaded. This ensures that Equality Deletes only apply to data written before or at the same sequence as the delete. Java layer pre-filters Delete Files when constructing `SplitInfo.delete_files`, so the Native layer can trust the provided list. However, if `SplitInfo.delete_files` contains Delete Files with lower sequence numbers (defensive check), Native skips them.

Data flow:
1. The Java layer specifies the matching fields in `DeleteFileInfo.equality_fields`
2. The native layer reads the Delete File and extracts the equality_fields columns
3. Build a Hash Set (key = xxHash of the combined equality_fields values)
4. For each RecordBatch, compute the hash row by row and check whether it is in the Delete Set

Performance optimizations:
- Small Delete File (< 10000 rows): direct Hash Set
- Large Delete File (≥ 10000 rows): Bloom Filter pre-filter + Hash Set exact match

### 7.5 MergeReader (Paimon)

MergeReader handles Paimon-specific LSM multi-level merging and Changelog semantics, independent of FormatReader and DeleteFilter.

```cpp
// MergeReader.h
#pragma once

#include "pangrova/reader/FormatReader.h"

namespace pangrova {
namespace reader {

/**
 * MergeReader — Paimon LSM / Changelog merge reading
 * 
 * Input: multiple FormatReaders (each corresponding to the files of one LSM level)
 * Output: merged and deduplicated RecordBatch
 */
class MergeReader {
public:
    virtual ~MergeReader() = default;
    virtual void initialize(std::vector<std::unique_ptr<FormatReader>> readers) = 0;
    virtual std::shared_ptr<arrow::RecordBatch> readNext() = 0;
    virtual bool hasNext() const = 0;
    virtual ReadStats getStats() const = 0;
    virtual void close() = 0;
};

} // namespace reader
} // namespace pangrova
```

Two implementations:

| Implementation | Algorithm | Output | Use Case |
|------|------|------|----------|
| `LsmMergeReader` | Multi-way merge sort (priority queue); dedup by primary key, taking the highest priority (lowest level) | Final state (after dedup) | Primary Key table read |
| `ChangelogMergeReader` | Keep all change records, output in time order, including the `_VALUE_KIND` column (+I/-D/+U/-U) | Complete change stream | CDC / Streaming read |

`LsmMergeReader` executes different merge strategies according to `PaimonMergeEngine`:
- `DEDUPLICATE`: keep the latest row
- `PARTIAL_UPDATE`: non-null field overwrite
- `AGGREGATION`: SUM/COUNT/MAX/MIN aggregation
- `FIRST_ROW`: keep the first row

### 7.6 DynamicFilterSink

DynamicFilterSink pushes the Dynamic Filter produced at Velox runtime (from the HashBuild side) down to the FormatReader layer.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Dynamic Filter Push-Down                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Velox Pipeline:                                                                 │
│                                                                                  │
│  Build Side:                                                                     │
│  TableScan(dim) → HashBuild ──── Dynamic Filter ────┐                           │
│                                                      │                           │
│  Probe Side:                                         ▼                           │
│  TableScan(fact) ← DynamicFilterSink ← addDynamicFilter()                       │
│       │                                                                          │
│       │  Filter push-down path:                                                  │
│       │  1. Velox calls PangrovaVeloxConnector::addDynamicFilter()                    │
│       │  2. PangrovaVeloxConnector forwards to DynamicFilterSink                      │
│       │  3. DynamicFilterSink calls FormatReader::addFilter()                   │
│       │  4. FormatReader applies the new Filter at next Row Group/Stripe start  │
│       │                                                                          │
│       │  Effects:                                                                │
│       │  - Row Group/Stripe Statistics filtering: skip non-matching Row Groups  │
│       │  - Bloom Filter filtering: skip Row Groups without the target value     │
│       │  - Page Index filtering (Parquet): skip non-matching Pages              │
│       ▼                                                                          │
│  Filter → Project → HashProbe → ...                                             │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```cpp
// DynamicFilterSink.h
namespace pangrova { namespace reader {

/**
 * DynamicFilterSink — receives Velox Dynamic Filters and pushes them down to FormatReader
 * 
 * Thread safety: HashBuild thread writes, TableScan thread reads (mutex-protected)
 * Conversion: Velox Filter → Arrow Expression → FormatReader::addFilter()
 */
class DynamicFilterSink {
public:
    explicit DynamicFilterSink(FormatReader* reader);
    void addFilter(facebook::velox::column_index_t outputChannel,
                   const std::shared_ptr<facebook::velox::common::Filter>& filter);
};

} }
```

### 7.7 ReadStats Unified Statistics

Statistics from all read components are aggregated via ReadStats and ultimately pulled to the Java-layer Observability system through the Java call `nativeGetTaskStats()`.

```
Statistics aggregation path:
FormatReader::getStats()  ─┐
DeleteFilter (deletedCount) ─┤
MergeReader::getStats()   ─┤→ PangrovaVeloxConnector aggregation → TaskStats → Java Pull (nativeGetTaskStats) → Java Observability
I/O Scheduler (ioStats)   ─┘
```

| Statistic | Source | Purpose |
|--------|------|------|
| rowGroupsTotal / rowGroupsSkipped | FormatReader | Evaluate Statistics filtering effectiveness |
| pagesTotal / pagesSkipped | ParquetFormatReader | Evaluate Page Index filtering effectiveness |
| bloomFilterChecks / bloomFilterSkipped | FormatReader | Evaluate Bloom Filter effectiveness |
| rowsRead / bytesRead | FormatReader | I/O throughput |
| deletedCount | DeleteFilter | Delete File impact scope |
| mergeInputRows / mergeOutputRows | MergeReader | LSM Merge amplification ratio |
| ioRequests / ioWaitNanos | I/O Scheduler | I/O latency analysis |
| cacheHits / cacheMisses | AsyncDataCache | Cache hit rate |


## 8. Velox Integration Layer

### 8.1 PangrovaConnector

```cpp
// PangrovaConnector.h
#pragma once

#include <velox/connectors/Connector.h>

namespace pangrova {
namespace velox {

/**
 * Pangrova Velox Connector
 * 
 * Responsibilities: adapt Pangrova's componentized read architecture to the Velox Connector interface
 * The Velox TableScan Operator obtains data through this Connector
 */
class PangrovaConnector : public facebook::velox::connector::Connector {
public:
    static constexpr const char* kConnectorId = "pangrova";
    
    std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
        const facebook::velox::RowTypePtr& outputType,
        const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>& tableHandle,
        const std::unordered_map<std::string, 
            std::shared_ptr<facebook::velox::connector::ColumnHandle>>& columnHandles,
        facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;
    
    std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
        facebook::velox::RowTypePtr inputType,
        std::shared_ptr<facebook::velox::connector::ConnectorInsertTableHandle> insertTableHandle,
        facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
        facebook::velox::connector::CommitStrategy commitStrategy) override;
};

} // namespace velox
} // namespace pangrova
```

### 8.2 PangrovaVeloxConnector — Component Composition Core

PangrovaVeloxConnector is the implementation of the Velox DataSource; it composes FormatReader + DeleteFilter + MergeReader + DynamicFilterSink based on SplitInfo.

```cpp
// PangrovaVeloxConnector.h
#pragma once

#include <velox/connectors/Connector.h>
#include "pangrova/reader/FormatReader.h"
#include "pangrova/reader/DeleteFilter.h"
#include "pangrova/reader/MergeReader.h"
#include "pangrova/reader/DynamicFilterSink.h"

namespace pangrova {
namespace velox {

/**
 * PangrovaVeloxConnector — Velox DataSource implementation
 * 
 * Composition strategy (automatically selected based on SplitInfo):
 * 
 * Iceberg COW:
 *   FormatReader → (direct output)
 * 
 * Iceberg MOR:
 *   FormatReader → PositionDeleteFilter → [EqualityDeleteFilter] → output
 * 
 * Paimon Primary Key:
 *   multiple FormatReaders → LsmMergeReader → output
 * 
 * Paimon Changelog:
 *   multiple FormatReaders → ChangelogMergeReader → output
 * 
 * Dynamic Filter can be injected into the FormatReader layer in all modes
 */
class PangrovaVeloxConnector : public facebook::velox::connector::DataSource {
public:
    /**
     * Add a Split — Velox calls this method to pass in a data split
     * Automatically composes read components based on the SplitInfo content
     */
    void addSplit(
        std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) override;
    
    /**
     * Read the next batch — invoked in a loop by the Velox TableScan Operator
     * Internal call chain: FormatReader → DeleteFilter → MergeReader → RowVector
     */
    std::optional<facebook::velox::RowVectorPtr> next(
        uint64_t size, facebook::velox::ContinueFuture& future) override;
    
    /**
     * Receive Dynamic Filter — called after Velox HashBuild completes
     * Forwards to DynamicFilterSink → FormatReader::addFilter()
     */
    void addDynamicFilter(
        facebook::velox::column_index_t outputChannel,
        const std::shared_ptr<facebook::velox::common::Filter>& filter) override;
    
    uint64_t getCompletedRows() override;
    uint64_t getCompletedBytes() override;

private:
    /** Create the component composition based on SplitInfo */
    void buildReadPipeline(const SplitInfo& splitInfo);
    
    /** Iceberg: create the FormatReader + DeleteFilter chain */
    void buildIcebergPipeline(const SplitInfo& splitInfo);
    
    /** Paimon: create multiple FormatReaders + MergeReader */
    void buildPaimonPipeline(const SplitInfo& splitInfo);
    
    std::unique_ptr<reader::FormatReader> formatReader_;
    std::vector<std::unique_ptr<reader::DeleteFilter>> deleteFilters_;  // chained
    std::unique_ptr<reader::MergeReader> mergeReader_;
    std::unique_ptr<reader::DynamicFilterSink> dynamicFilterSink_;
    
    reader::ReadStats aggregatedStats_;
};

} // namespace velox
} // namespace pangrova
```

### 8.3 TaskExecutor

> **Authoritative definition**: The complete Pull-mode interface of TaskExecutor is defined in §5.5 (including `getNextBatch` / `getShuffleBatch` / `getTaskStatus` / `getTaskStats` / `cancel` / `addExchangeBatch` / `injectDynamicFilter` / `updateMemoryLimit` and other complete interfaces). The Trace-enabled constructor enhancement is in §5.4.
>
> TaskExecutor wraps a Velox Task; its responsibilities include:
> - Receive the Substrait Plan (via TaskRequest protobuf) and convert it to a Velox PlanNode
> - Create the Velox Task and inject PangrovaConnector
> - Manage the Task lifecycle (start / cancel)
> - Provide Pull-mode data pulling via the OutputQueue
> - Collect execution statistics (TaskStats: inputRows, outputRows, peakMemoryBytes, spilledBytes, etc.)
>
> Key types:
> - `TaskState`: PENDING → RUNNING → FINISHED / FAILED / CANCELLED
> - `TaskStats`: inputRows, outputRows, inputBytes, outputBytes, cpuTimeNanos, wallTimeNanos, peakMemoryBytes, spilledBytes

## 9. Memory Management

> **Note**: This section presents the memory-pool hierarchy from the native side (C++ Velox). The complete memory-pool boundary definition for the Worker (including independent isolation of the RAM Cache Pool, the Java-Native coordination protocol, and the boundary contract between Cache and Query Pool) is in [11-resource-management.md §2 / §12.4](./11-resource-management.md). The native-layer Root Memory Pool corresponds to the 60 GB region managed by the Root Memory Arbitrator in 06 §2, and does not include the Reserved 4 GB (JVM Heap, Metadata Cache, System Overhead).

### 9.1 Memory Pool Hierarchy (native-side view)

<!-- [DEF-095] Updated to align with 11-resource-management.md §2 (authoritative).
     Root Pool = 60 GB (Root Memory Arbitrator budget), System Pool = 10 GB. -->
<!-- Resolves: DEF-067 — §9.1 memory pool hierarchy aligned with §9 note and 06 §2 (Root=60GB, System=10GB) -->

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Memory Pool Hierarchy                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │           Root Memory Pool (Arbitrator Budget: 60 GB)                  │  │
│  │           See 11-resource-management.md §2 for authoritative layout   │  │
│  └───────────────────────────────┬───────────────────────────────────────┘  │
│                                  │                                           │
│          ┌───────────────────────┼───────────────────────────────┐          │
│          │                       │                               │          │
│  ┌───────▼───────┐      ┌───────▼───────┐              ┌───────▼───────┐   │
│  │  Query Pool   │      │  Query Pool   │              │  System Pool  │   │
│  │  (Query-1)    │      │  (Query-2)    │              │  (Reserved)   │   │
│  │   25 GB       │      │   25 GB       │              │    10 GB      │   │
│  └───────┬───────┘      └───────┬───────┘              └───────────────┘   │
│          │                      │                                           │
│    ┌─────┼─────┐          ┌─────┼─────┐                                    │
│  ┌─▼─┐ ┌─▼─┐ ┌─▼─┐      ┌─▼─┐ ┌─▼─┐ ┌─▼─┐                                 │
│  │T1 │ │T2 │ │T3 │      │T1 │ │T2 │ │T3 │   (Task Pools: dynamic)        │
│  └───┘ └───┘ └───┘      └───┘ └───┘ └───┘                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 9.2 Memory Manager

```cpp
// MemoryManager.h
namespace pangrova {
namespace memory {

enum class MemoryPressure { NONE, LOW, MEDIUM, HIGH, CRITICAL };

/**
 * Memory Manager - integrates Velox and Arrow memory pools
 */
class MemoryManager {
public:
    struct Config {
        int64_t maxMemoryBytes = 64L * 1024 * 1024 * 1024;  // 64GB
        double lowPressureThreshold = 0.5;
        // <!-- Resolves: DEF-099 — added mediumPressureThreshold to align with 06 §3 MEDIUM(0.7, 0.85) -->
        double mediumPressureThreshold = 0.7;
        double highPressureThreshold = 0.85;
        double criticalPressureThreshold = 0.95;
        std::string spillDir = "/tmp/pangrova/spill";
    };
    
    std::shared_ptr<facebook::velox::memory::MemoryPool> createVeloxPool(
        const std::string& name, int64_t maxBytes);
    
    std::shared_ptr<arrow::MemoryPool> createArrowPool(
        const std::string& name, int64_t maxBytes);
    
    int64_t getCurrentUsage() const;
    MemoryPressure getPressureLevel() const;
    bool requestMemory(int64_t bytes);
    void releaseMemory(int64_t bytes);
    int64_t forceSpill(int64_t targetBytes);
};

/**
 * Spillable interface - spillable components implement this interface
 */
class Spillable {
public:
    virtual int64_t getCurrentMemoryUsage() const = 0;
    virtual int64_t spill(int64_t targetBytes) = 0;
    virtual int getSpillPriority() const = 0;
};

} // namespace memory
} // namespace pangrova
```

### 9.3 Spill Manager

<!-- Resolves: DEF-098 — SpillManager authoritative definition is in 11-resource-management.md §6 -->

The complete `SpillManager` C++ class (including `SpillConfig`, `SpillFileInfo`, `spillBatches()`, `getCurrentSpillDiskUsage()`, and full implementation details) is defined in [11-resource-management.md §6](./11-resource-management.md). This section provides a summary of the interface used by Native operators.

```cpp
// SpillManager.h — summary interface (see 06 §6 for full definition)
// Return type: SpillFileInfo (not SpillFile) — aligned with 06 §6
class SpillManager {
public:
    SpillFileInfo spill(const std::string& taskId, 
                        const std::shared_ptr<arrow::RecordBatch>& batch);
    SpillFileInfo spillBatches(const std::string& taskId,
                               const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
    
    std::shared_ptr<arrow::RecordBatchReader> readSpillFile(const SpillFileInfo& fileInfo);
    void deleteSpillFile(const SpillFileInfo& fileInfo);
    void cleanupTask(const std::string& taskId);
    
    int64_t getTotalSpilledBytes() const;
    int64_t getTotalSpilledFiles() const;
    int64_t getCurrentSpillDiskUsage() const;
};
```


## 10. I/O Scheduler

### 10.1 Architecture Overview

The I/O Scheduler sits below the FormatReader and is responsible for file-system abstraction, asynchronous I/O, request coalescing, and cache integration.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              I/O Scheduler                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  FormatReader initiates a read request                                           │
│       │                                                                          │
│       ▼                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  CoalescedLoad                                                           │   │
│  │  Merge adjacent/overlapping I/O requests to reduce I/O count             │   │
│  │  Rule: merge requests with gap < coalesceThreshold (1MB default)         │   │
│  └──────────────────────────────┬──────────────────────────────────────────┘   │
│                                 │                                                │
│                                 ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  AsyncDataCache (Velox SSD Cache)                                        │   │
│  │  Two-level cache: Memory Cache (RAM) → SSD Cache (NVMe)                  │   │
│  │  Cache granularity: file + offset + length → data block                  │   │
│  │  Hit: return cached data directly                                        │   │
│  │  Miss: submit to IoExecutor                                              │   │
│  └──────────────────────────────┬──────────────────────────────────────────┘   │
│                                 │                                                │
│                                 ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  IoExecutor (Velox async I/O thread pool)                                │   │
│  │  Async submit read requests; FormatReader waits via ContinueFuture       │   │
│  │  Prefetch: while reading current Row Group, prefetch the next one        │   │
│  └──────────────────────────────┬──────────────────────────────────────────┘   │
│                                 │                                                │
│                                 ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  FileSystem Abstraction                                                  │   │
│  │  ├── S3FileSystem (AWS S3 / MinIO)                                       │   │
│  │  ├── HdfsFileSystem (HDFS)                                               │   │
│  │  ├── GcsFileSystem (Google Cloud Storage)                                │   │
│  │  └── LocalFileSystem (local files, for testing)                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 FileSystem Abstraction

```cpp
// FileSystem.h
#pragma once

#include <arrow/io/interfaces.h>
#include <string>
#include <memory>

namespace pangrova {
namespace io {

struct FileInfo {
    std::string path;
    int64_t size;
    int64_t modificationTime;
    bool isDirectory;
};

/**
 * FileSystem — file-system abstraction interface
 * Supports file://, s3://, hdfs://, gs://
 */
class FileSystem {
public:
    virtual ~FileSystem() = default;
    virtual std::shared_ptr<arrow::io::RandomAccessFile> openForRead(
        const std::string& path) = 0;
    virtual std::shared_ptr<arrow::io::OutputStream> openForWrite(
        const std::string& path) = 0;
    virtual FileInfo getFileInfo(const std::string& path) = 0;
    virtual std::vector<FileInfo> listDirectory(const std::string& path) = 0;
    virtual bool exists(const std::string& path) = 0;
};

/**
 * FileSystemFactory — creates the corresponding FileSystem based on the URI scheme
 */
class FileSystemFactory {
public:
    static std::shared_ptr<FileSystem> create(
        const std::string& uri,
        const std::unordered_map<std::string, std::string>& config);
};

} // namespace io
} // namespace pangrova
```

### 10.3 CoalescedLoad

```cpp
// CoalescedLoad.h
#pragma once

namespace pangrova {
namespace io {

/**
 * CoalescedLoad — coalesces small I/O requests
 * 
 * Problem: object storage (S3/GCS) has a fixed latency per request (~10ms)
 * Solution: merge adjacent small requests into a single large request to reduce request count
 * 
 * Example:
 *   Request 1: offset=0, length=4KB
 *   Request 2: offset=8KB, length=4KB
 *   Gap = 8KB - 4KB = 4KB < coalesceThreshold (1MB)
 *   Merged into: offset=0, length=12KB (a single I/O)
 */
struct ReadRange {
    int64_t offset;
    int64_t length;
};

class CoalescedLoad {
public:
    struct Config {
        int64_t coalesceThreshold = 1024 * 1024;  // 1MB
        int64_t maxCoalescedBytes = 8 * 1024 * 1024;  // 8MB max per merge
    };
    
    explicit CoalescedLoad(const Config& config);
    
    /**
     * Coalesce read ranges
     * @param ranges the original request list (must be sorted by offset)
     * @return the coalesced request list
     */
    std::vector<ReadRange> coalesce(const std::vector<ReadRange>& ranges);

private:
    Config config_;
};

} // namespace io
} // namespace pangrova
```

### 10.4 Velox Cache Integration

Pangrova directly reuses Velox's AsyncDataCache and IoExecutor, and does not reimplement the cache layer.

| Component | Velox Class | Responsibility |
|------|---------|------|
| Memory Cache | `AsyncDataCache` | Caches hot data blocks in RAM |
| SSD Cache | `SsdCache` | NVMe SSD second-level cache |
| I/O thread pool | `IoExecutor` | Asynchronous I/O execution |
| Prefetch | `ContinueFuture` | FormatReader asynchronously waits for I/O completion |

Configuration reference:

```yaml
pangrova:
  native:
    io:
      cache:
        memory-size: 8GB          # RAM cache size
        ssd-enabled: true
        ssd-path: /mnt/nvme/cache
        ssd-size: 100GB           # SSD cache size
      coalesced-load:
        threshold: 1MB            # coalesce threshold
        max-bytes: 8MB            # max size per merge
      prefetch:
        row-groups: 2             # Row Groups to prefetch
      thread-pool:
        size: 16                  # number of I/O threads
```

## 11. Shuffle Layer Design

### 11.1 Shuffle Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Shuffle Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Core principle: native layer does no network I/O; all network I/O by Java layer│
│                                                                                  │
│  Shuffle Serialization Path (in-memory only):                                    │
│  C++ Velox Output → Partitioner → Shuffle Partitioner (partition + buffer)  │
│       → OutputQueue (Arrow C Data Interface)                                    │
│       → Java Pull: nativeGetShuffleBatch()                                      │
│  → Java Layer Arrow Flight Client (network transfer done by Java layer)         │
│  → target Worker                                                                 │
│                                                                                  │
│  Shuffle Read Path:                                                              │
│  Java Layer Arrow Flight Server (receives data)                                 │
│       → JNI (Arrow C Data Interface)                                            │
│  → C++ Velox continues computation                                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 11.2 Partitioner

```cpp
// Partitioner.h
class Partitioner {
public:
    virtual std::vector<int32_t> partition(
        const std::shared_ptr<arrow::RecordBatch>& batch) = 0;
    virtual int numPartitions() const = 0;
};

class HashPartitioner : public Partitioner { /* Hash partitioning */ };
class RangePartitioner : public Partitioner { /* Range partitioning */ };
class RoundRobinPartitioner : public Partitioner { /* Round-robin partitioning */ };
```

### 11.3 Shuffle Complete Data Path

End-to-end Shuffle data flow spanning C++ Native layer, JNI bridge, and Java network layer:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Shuffle Complete Data Path                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Source Worker                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Stage 1: C++ Velox Pipeline Output                                      │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │ Velox Task  │  Pipeline produces RowVector batches                    │   │
│  │  └──────┬──────┘                                                         │   │
│  │         ▼                                                                │   │
│  │  Stage 2: Shuffle Partitioner (C++, hash/range/round-robin)              │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │ Partitioner │  Assigns each row to a target partition                 │   │
│  │  └──────┬──────┘                                                         │   │
│  │         ▼                                                                │   │
│  │  Stage 3: Arrow Serializer (C++, in-memory)                              │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │Arrow IPC Ser│  Serializes partitioned data to Arrow IPC format        │   │
│  │  └──────┬──────┘  Memory allocated from Task Memory Pool (see below)     │   │
│  │         ▼                                                                │   │
│  │  Stage 4: Java Pull — nativeGetShuffleBatch()                            │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │ JNI Bridge  │  Zero-copy via Arrow C Data Interface                   │   │
│  │  └──────┬──────┘  Java thread pulls; no data = natural backpressure      │   │
│  │         ▼                                                                │   │
│  │  Stage 5: Java ShuffleManager                                            │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │ShuffleManager│ Routes data to correct target Worker                   │   │
│  │  └──────┬──────┘  Optionally persists based on ShufflePersistenceMode    │   │
│  │         ▼                                                                │   │
│  │  Stage 6: Arrow Flight Client (gRPC streaming)                           │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │Flight Client│  Sends Arrow RecordBatch to target Worker               │   │
│  │  └─────────────┘                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  ════════════════════════════ Network ═══════════════════════════════════════    │
│                                                                                  │
│  Target Worker                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Stage 7: Arrow Flight Server (gRPC streaming)                           │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │Flight Server│  Receives Arrow RecordBatch stream                      │   │
│  │  └──────┬──────┘                                                         │   │
│  │         ▼                                                                │   │
│  │  Stage 8: Java ShuffleReader                                             │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │ShuffleReader│  Buffers and orders incoming partitions                 │   │
│  │  └──────┬──────┘                                                         │   │
│  │         ▼                                                                │   │
│  │  Stage 9: JNI — pass to Native Exchange Operator                         │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │ JNI Bridge  │  Zero-copy via Arrow C Data Interface                   │   │
│  │  └──────┬──────┘                                                         │   │
│  │         ▼                                                                │   │
│  │  Stage 10: C++ Velox Exchange Operator                                   │   │
│  │  ┌─────────────┐                                                         │   │
│  │  │Exchange Op  │  Feeds data into downstream pipeline                    │   │
│  │  └─────────────┘                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### Fault Tolerance

Each stage has specific failure modes and recovery strategies:

| Stage | Failure Mode | Recovery Strategy |
|-------|-------------|-------------------|
| Stage 1-3 (C++ Pipeline/Serializer) | Task crash, OOM | Task-level retry by Coordinator; Velox MemoryReclaimer triggers spill before OOM |
| Stage 4 (JNI Pull) | JNI exception | nativeGetShuffleBatch() returns error code; Java calls nativeGetTaskStatus() for details |
| Stage 5 (ShuffleManager) | Persistence failure | Falls back based on ShufflePersistenceMode (see below) |
| Stage 6-7 (Arrow Flight network) | Connection timeout, target Worker down | Retry with exponential backoff; Coordinator reschedules to healthy Worker |

<!-- Resolves: IC-RD-009 — Shuffle data Arrow Schema consistency verification -->
**Schema Validation**: On the receiving side (Stage 8), the Java `ShuffleReader` validates the Arrow Schema of the first `RecordBatch` against the expected schema from the query plan. If schemas mismatch (e.g., column count, type, or field name difference), the shuffle read is aborted with `SCHEMA_MISMATCH` error and the task fails with a retryable error. Subsequent batches are assumed schema-consistent (Arrow Flight guarantees schema uniformity within a stream).

<!-- Resolves: IC-SF-004 — Arrow→Velox RowVector conversion overhead quantification -->
**Arrow→Velox Conversion Overhead (Stage 9)**: The `ArrowCDataBridge.arrowToVelox()` conversion at the JNI boundary has the following measured overhead:
- Flat types (INT, BIGINT, VARCHAR, DOUBLE): ~0 (pointer exchange, true zero-copy)
- Nested types (STRUCT, MAP, LIST): 1–3% CPU overhead due to memory layout differences between Arrow and Velox
- For high-throughput shuffle scenarios (>1M rows/s per partition), consider the Native RowVector Reader Pipeline (OPT-022, §23) to eliminate this conversion entirely.
| Stage 8-9 (ShuffleReader/JNI) | Data corruption, deserialization error | Task fails and Coordinator retries the consuming Stage |
| Stage 10 (Exchange Operator) | Upstream data not ready | Exchange Operator blocks until data arrives or timeout triggers task failure |

**Shuffle data persistence modes** control the durability guarantee at Stage 5. Four modes are supported: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_AND_REMOTE`, and `REMOTE_ONLY`. Each mode trades latency for fault tolerance level. For full details on persistence modes and their data flow, see [02-distributed-architecture.md — Shuffle Persistence Architecture](02-distributed-architecture.md).

#### Resource Isolation

Shuffle memory is **not** allocated from an independent memory pool. Instead, it is allocated from the **Task Memory Pool** within the Velox memory hierarchy:

```
Root Memory Pool (Worker)
  └── Query Memory Pool
        └── Task Memory Pool
              ├── Operator Pools (Scan, Join, Agg, ...)
              └── Shuffle Partitioner Pool  ← Shuffle memory lives here
```

Key resource isolation rules:

| Rule | Description |
|------|-------------|
| **Pool hierarchy** | Shuffle Partitioner Pool is a child of Task Memory Pool, subject to task-level memory limits |
| **No independent pool** | Shuffle does not have a dedicated global memory pool; memory competes with operators within the same task |
| **Spill support** | When Task Memory Pool is under pressure, MemoryArbitrator may trigger spill on Shuffle buffers (MEMORY_AND_DISK mode) |
| **Backpressure** | Java stops calling nativeGetShuffleBatch() → the native side naturally pauses production, no extra signal needed |
| **Quantized reservation** | Shuffle Partitioner uses 1MB minimum reservation to reduce lock contention on the memory pool |


## 12. Unified Delete Handling

This section complements the §7.4 DeleteFilter SPI, detailing the processing flow for various delete scenarios.

### 12.1 Position Delete (Iceberg)

```
Data flow:
Java SplitInfo.delete_files (type=POSITION)
  → Native PositionDeleteFilter.initialize()
    → Read the Delete File (Parquet: file_path + pos columns)
    → Filter out the positions of the current data file
    → Build a Roaring64Map
  → filter(batch, startRowIndex)
    → For each row: bitmap.contains(startRowIndex + offset) ? delete : keep
    → Output the filtered RecordBatch
```

Performance characteristics:

| Metric | Value |
|------|-----|
| Memory usage | Roaring Bitmap compressed; 1M positions ≈ 1MB |
| Lookup complexity | O(1) per row |
| Initialization cost | Read Delete File + build Bitmap (one-time) |

### 12.2 Equality Delete (Iceberg)

```
Data flow:
Java SplitInfo.delete_files (type=EQUALITY, equality_fields=[col1, col2])
  → Native EqualityDeleteFilter.initialize()
    → Read the Delete File (Parquet: equality_fields columns)
    → Compute xxHash64(col1 || col2) for each row
    → Build a HashSet<uint64_t>
    → If the Delete File > 10000 rows, additionally build a Bloom Filter pre-filter
  → filter(batch, startRowIndex)
    → For each row: computeRowHash(batch, row) in deleteHashSet ? delete : keep
    → Output the filtered RecordBatch
```

Performance characteristics:

| Metric | Value |
|------|-----|
| Memory usage | HashSet: 8 bytes each; Bloom Filter: extra ~1 bit/entry |
| Lookup complexity | O(1) per row (hash lookup) |
| Hash collisions | xxHash64 collision probability is very low, acceptable |
| Large Delete File optimization | Bloom Filter pre-filter reduces the number of HashSet lookups |

### 12.3 Paimon Delete Markers

Paimon does not use separate Delete Files; instead it marks each row's change type via the `_VALUE_KIND` column:

| _VALUE_KIND | Meaning | MergeReader Handling |
|-------------|------|-----------------|
| +I (INSERT) | New row | Output |
| -D (DELETE) | Deleted row | Discard (the +I with the same primary key is also discarded) |
| +U (UPDATE_AFTER) | Value after update | Output (overwrites the old value) |
| -U (UPDATE_BEFORE) | Value before update | Discard |

Paimon's delete handling is performed by LsmMergeReader during multi-level merging and does not go through DeleteFilter.

## 13. Project Structure

```
pangrova-worker-native/
├── CMakeLists.txt
├── cmake/
│   ├── FindVelox.cmake
│   ├── FindArrow.cmake
│   └── BuildOptions.cmake
├── include/pangrova/
│   ├── common/          # Types, Status, Buffer, Exception
│   ├── jni/             # JniBridge, ArrowCDataInterface
│   ├── execution/       # TaskExecutor, PipelineExecutor
│   ├── substrait/       # SubstraitParser, SubstraitToVelox
│   ├── memory/          # MemoryManager, SpillManager
│   ├── reader/          # FormatReader, DeleteFilter, MergeReader, DynamicFilterSink
│   ├── io/              # FileSystem, CoalescedLoad, I/O Scheduler
│   ├── velox/           # PangrovaConnector, PangrovaVeloxConnector
│   └── shuffle/         # ShuffleSerializer, ShuffleReader, Partitioner
├── src/                 # Implementation files
├── test/                # Tests
└── benchmark/           # Benchmarks
```

> **Note**: The Arrow Flight Server/Client is uniformly implemented by the Java layer. The Java layer pulls data from the native layer via Pull mode (nativeGetNextBatch / nativeGetShuffleBatch) and then performs network transfer. The native layer does not directly expose network services. The memory-arbitration path uses synchronous Listener callbacks (see §5.1).


## 14. Extensibility Design

<!-- Resolves: DEF-065 — Native C++ SPIs registered in 01-overview.md §5.1 (see line 270) -->

| Extension Point | Layer | Interface | Registration | Note |
|--------|------|------|---------|------|
| File format | Native | `FormatReaderFactory` | `REGISTER_FORMAT_READER_FACTORY` | Parquet, ORC |
| Delete filtering | Native | `DeleteFilter` | Composed by PangrovaVeloxConnector per SplitInfo | PositionDelete, EqualityDelete |
| Merge reading | Native | `MergeReader` | Composed by PangrovaVeloxConnector per SplitInfo | Paimon LSM Merge, Changelog |
| Dynamic Filter | Native | `DynamicFilterSink` | Auto-created by PangrovaVeloxConnector | Velox HashBuild → FormatReader |
| Lakehouse | Java | `ConnectorFactory` | Java SPI | Iceberg, Paimon implemented; Hudi planned |
| File system | Native | `FileSystem` | `FileSystemFactory` | S3, HDFS, GCS, Local |
| Partitioning strategy | Native | `Partitioner` | Config selection | Hash, Range, Round-Robin |
| Compression algorithm | Native | `Codec` | Arrow built-in | LZ4, ZSTD, Snappy |
| Expression function | Native | `VectorFunction` | `REGISTER_NATIVE_UDF` | Custom UDF |
| Aggregate function | Native | `AggregateFunction` | `REGISTER_NATIVE_UDF` | Custom UDAF |
| Serialization format | Native | `Serializer` / `Deserializer` | `SerializerFactory` | Arrow IPC, Velox Serde |


---

## 15. Native UDF Design

### 15.1 UDF Types and Execution Layers

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Native UDF Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • High performance: UDFs run in the native layer with vectorization and SIMD   │
│  • Multi-language: supports writing UDFs in C++, Rust, WASM                     │
│  • Safe isolation: WASM UDFs run sandboxed; crashes cannot affect main process  │
│  • Hot-loading: load/unload UDFs at runtime, no restart needed                  │
│                                                                                  │
│  UDF types:                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Type           │ Language  │ Perf   │ Safety│ Cost  │ Use Case          │   │
│  │  ──────────────────────────────────────────────────────────────────────  │   │
│  │  Native C++     │ C++       │ Highest│ Low   │ High  │ Core functions    │   │
│  │  Native Rust    │ Rust      │ Highest│ Medium│ Medium│ Safety-sensitive  │   │
│  │  WASM           │ Multi-lang│ High   │ High  │ Low   │ User-defined      │   │
│  │  Java (fallback)│ Java      │ Medium │ High  │ Lowest│ Simple logic      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Execution flow:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  SQL: SELECT my_udf(col1, col2) FROM table                               │   │
│  │                                                                          │   │
│  │  1. Calcite parsing → identify UDF calls                                 │   │
│  │  2. UDF Registry lookup → determine UDF type and location                │   │
│  │  3. Substrait Plan → contains UDF references                             │   │
│  │  4. Native execution → invoke the corresponding UDF implementation       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 15.2 Native C++ UDF

```cpp
// NativeUdf.h - Native UDF interface definitions
#pragma once

#include <velox/expression/VectorFunction.h>
#include <velox/expression/FunctionSignature.h>

namespace pangrova {
namespace udf {

/**
 * Native UDF base class
 * Extends Velox VectorFunction to implement vectorized execution
 */
class NativeScalarUdf : public facebook::velox::exec::VectorFunction {
public:
    virtual ~NativeScalarUdf() = default;
    
    /**
     * Vectorized execution entry point
     * @param rows      the rows to process
     * @param args      input arguments (vectorized)
     * @param context   execution context
     * @param result    output result (vectorized)
     */
    void apply(
        const facebook::velox::SelectivityVector& rows,
        std::vector<facebook::velox::VectorPtr>& args,
        const facebook::velox::TypePtr& outputType,
        facebook::velox::exec::EvalCtx& context,
        facebook::velox::VectorPtr& result) const override = 0;
    
    /** UDF metadata */
    virtual std::string name() const = 0;
    virtual std::vector<facebook::velox::exec::FunctionSignaturePtr> signatures() const = 0;
};

/**
 * Native UDAF base class
 */
class NativeAggregateUdf : public facebook::velox::exec::Aggregate {
public:
    virtual ~NativeAggregateUdf() = default;
    
    /** Initialize the accumulator */
    virtual void initializeNewGroups(
        char** groups, folly::Range<const facebook::velox::vector_size_t*> indices) override = 0;
    
    /** Add input to the accumulator */
    virtual void addRawInput(
        char** groups,
        const facebook::velox::SelectivityVector& rows,
        const std::vector<facebook::velox::VectorPtr>& args,
        bool mayPushdown) override = 0;
    
    /** Extract the final result */
    virtual void extractValues(
        char** groups,
        facebook::velox::vector_size_t numGroups,
        facebook::velox::VectorPtr* result) override = 0;
};

/**
 * UDF registration macro
 */
#define REGISTER_NATIVE_UDF(UdfClass) \
    static bool _registered_##UdfClass = []() { \
        pangrova::udf::UdfRegistry::instance().registerUdf( \
            std::make_unique<UdfClass>()); \
        return true; \
    }()

} // namespace udf
} // namespace pangrova
```

Implementation example: extend `NativeScalarUdf` and implement the three methods `name()`, `signatures()`, and `apply()`. The `REGISTER_NATIVE_UDF(MyUdf)` macro automatically registers it with UdfRegistry.

### 15.3 WASM UDF (sandboxed execution)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           WASM UDF Architecture                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • Safe isolation: WASM sandbox; UDF crash cannot affect main process           │
│  • Multi-language support: Rust/C/Go/AssemblyScript compiled to WASM            │
│  • Hot-loading: load .wasm files at runtime, no restart needed                  │
│  • Resource limits: memory and CPU-time limits to prevent malicious UDFs        │
│                                                                                  │
│  Execution flow:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Velox Pipeline                                                          │   │
│  │       │                                                                  │   │
│  │       │ Arrow RecordBatch                                                │   │
│  │       ▼                                                                  │   │
│  │  ┌─────────────────┐                                                     │   │
│  │  │ WASM UDF Host   │                                                     │   │
│  │  │                 │                                                     │   │
│  │  │ 1. Serialize    │ → Arrow IPC                                         │   │
│  │  │    to WASM mem  │                                                     │   │
│  │  │                 │                                                     │   │
│  │  │ 2. Call WASM    │ → wasm_udf_execute(ptr, len)                        │   │
│  │  │    function     │                                                     │   │
│  │  │                 │                                                     │   │
│  │  │ 3. Deserialize  │ ← Arrow IPC                                         │   │
│  │  │    result       │                                                     │   │
│  │  └─────────────────┘                                                     │   │
│  │       │                                                                  │   │
│  │       ▼                                                                  │   │
│  │  Next Operator                                                           │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  WASM Runtime choices:                                                          │
│  • Wasmtime (recommended): high performance, Rust impl, good security           │
│  • Wasmer: multi-backend support                                                │
│  • WasmEdge: edge-computing optimized                                           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

`WasmUdfHost` core interface:

| Method | Description |
|--------|-------------|
| `loadModule(name, wasmBytes)` | Load WASM bytecode, create sandboxed instance |
| `unloadModule(name)` | Unload WASM module (hot-unload) |
| `execute(name, RecordBatch) → Array` | Execute WASM UDF via Arrow IPC serialization |

Key config: `maxMemoryBytes` (64MB default), `maxCpuTimeMs` (5s timeout), `enableSIMD`.

WASM UDF development flow: write the UDF in Rust/C/Go → compile to `.wasm` → load it via `UdfRegistry.registerWasmUdf()`. WASM function signature: `wasm_udf_execute(input_ptr, input_len) → *mut u8`; both input and output are in Arrow IPC format.

### 15.4 UDF Registration and Management

`UdfRegistry` (Java side) manages the metadata and lifecycle of all UDFs:

| Method | Description |
|--------|-------------|
| `registerNativeUdf(name, libraryPath, className)` | Register C++ UDF, load via NativeRuntime |
| `registerWasmUdf(name, wasmBytes, signature)` | Register WASM UDF, load to WasmUdfHost |
| `unregisterUdf(name)` | Hot-unload UDF (runtime, no restart) |
| `getSignature(name) → UdfSignature` | Return UDF signature for Calcite planning |

Internally it uses `ConcurrentHashMap<String, UdfDescriptor>` for storage and interacts with the native layer via `NativeRuntime`.

### 15.5 UDF Representation in Substrait

```protobuf
// substrait_udf_extension.proto

message UserDefinedFunction {
    string name = 1;
    UdfType type = 2;
    repeated Expression arguments = 3;
    Type return_type = 4;
    
    // UDF location information (used for native-layer lookup)
    oneof location {
        NativeUdfLocation native = 10;
        WasmUdfLocation wasm = 11;
    }
}

enum UdfType {
    UDF_TYPE_UNSPECIFIED = 0;
    UDF_TYPE_NATIVE_CPP = 1;
    UDF_TYPE_NATIVE_RUST = 2;
    UDF_TYPE_WASM = 3;
    UDF_TYPE_JAVA = 4;
}

message NativeUdfLocation {
    string library_name = 1;   // e.g., "libpangrova_udf_string.so"
    string function_name = 2;  // e.g., "string_hash"
}

message WasmUdfLocation {
    string module_name = 1;    // e.g., "my_udf"
    string function_name = 2;  // e.g., "wasm_udf_execute"
}
```

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         UDF Execution in Velox                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  UDF call in the Substrait Plan:                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Project {                                                               │   │
│  │    expressions: [                                                        │   │
│  │      UserDefinedFunction {                                               │   │
│  │        name: "string_hash",                                              │   │
│  │        type: UDF_TYPE_NATIVE_CPP,                                        │   │
│  │        arguments: [FieldReference { field: 0 }],                         │   │
│  │        return_type: BIGINT,                                              │   │
│  │        native: { library: "libpangrova_udf.so", function: "string_hash" }  │   │
│  │      }                                                                   │   │
│  │    ]                                                                     │   │
│  │  }                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Native-layer execution:                                                         │
│  1. Substrait Parser parses the UDF call                                        │
│  2. Look up the corresponding VectorFunction from UdfRegistry                   │
│  3. Build the Velox Expression Tree, including the UDF node                     │
│  4. During Pipeline execution, invoke the UDF apply() method                    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```


---

## 16. Serializer SPI Design

### 16.1 Serializer Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Serializer Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • Multi-format support: Arrow IPC, Velox RowVector, custom formats             │
│  • Zero-copy first: avoid data copies whenever possible                         │
│  • Extensible: supports future integration with systems like Spark (UnsafeRow)  │
│  • Streaming output: supports streaming serialization to reduce memory usage    │
│                                                                                  │
│  Position of Serializer in the data flow:                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Velox Pipeline Output (RowVector)                                       │   │
│  │           │                                                              │   │
│  │           ▼                                                              │   │
│  │  ┌─────────────────┐                                                     │   │
│  │  │   Serializer    │  ← select the Serializer by target                  │   │
│  │  │                 │                                                     │   │
│  │  │ • Arrow IPC     │  → Shuffle / Client Result                          │   │
│  │  │ • Arrow C Data  │  → JNI zero-copy transfer                           │   │
│  │  │ • Velox Serde   │  → Spill to Disk                                    │   │
│  │  │ • Custom Format │  → future extension                                 │   │
│  │  └─────────────────┘                                                     │   │
│  │           │                                                              │   │
│  │           ▼                                                              │   │
│  │  Target (Network / Disk / JNI)                                           │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 16.2 Serializer SPI Interface Definition

```cpp
// Serializer.h - Serializer SPI interface
#pragma once

#include <velox/vector/ComplexVector.h>
#include <arrow/record_batch.h>
#include <arrow/io/interfaces.h>

namespace pangrova {
namespace serde {

/**
 * Serialization format enum
 */
enum class SerializationFormat {
    ARROW_IPC,          // Arrow IPC Streaming Format (cross-language, network transfer)
    ARROW_C_DATA,       // Arrow C Data Interface (JNI zero-copy)
    VELOX_SERDE,        // Velox native serialization (Spill, internal transfer)
    PRESTO_PAGE,        // Presto Page Format (Presto-compatible)
    SPARK_UNSAFE_ROW,   // Spark UnsafeRow (Spark-compatible) - future extension
};

/**
 * Serialization options
 */
struct SerializerOptions {
    SerializationFormat format = SerializationFormat::ARROW_IPC;
    arrow::Compression::type compression = arrow::Compression::LZ4_FRAME;
    int64_t batchSizeBytes = 1024 * 1024;  // 1MB per batch
    bool preserveNulls = true;
    bool includeSchema = true;  // whether to include the Schema in each batch
};

/**
 * Serializer interface - serializes a Velox RowVector into the target format
 */
class Serializer {
public:
    virtual ~Serializer() = default;
    
    /**
     * Serialize a single RowVector to the output stream
     * @param rowVector input data
     * @param output    output stream
     * @return the number of serialized bytes
     */
    virtual int64_t serialize(
        const facebook::velox::RowVectorPtr& rowVector,
        arrow::io::OutputStream* output) = 0;
    
    /**
     * Serialize into a memory buffer
     * @param rowVector input data
     * @return the serialized byte array
     */
    virtual std::shared_ptr<arrow::Buffer> serializeToBuffer(
        const facebook::velox::RowVectorPtr& rowVector) = 0;
    
    /**
     * Get the serialization format
     */
    virtual SerializationFormat format() const = 0;
    
    /**
     * Get the estimated serialized size (used to pre-allocate the buffer)
     */
    virtual int64_t estimateSerializedSize(
        const facebook::velox::RowVectorPtr& rowVector) const = 0;
};

/**
     * Deserializer interface - deserializes serialized data back into a Velox RowVector
 */
class Deserializer {
public:
    virtual ~Deserializer() = default;
    
    /**
     * Deserialize from an input stream
     * @param input the input stream
     * @param pool  the memory pool
     * @return the deserialized RowVector
     */
    virtual facebook::velox::RowVectorPtr deserialize(
        arrow::io::RandomAccessFile* input,
        facebook::velox::memory::MemoryPool* pool) = 0;
    
    /**
     * Deserialize from a memory buffer
     */
    virtual facebook::velox::RowVectorPtr deserializeFromBuffer(
        const arrow::Buffer& buffer,
        facebook::velox::memory::MemoryPool* pool) = 0;
    
    /**
     * Get the serialization format
     */
    virtual SerializationFormat format() const = 0;
};

/**
 * Serializer factory
 */
class SerializerFactory {
public:
    static std::unique_ptr<Serializer> createSerializer(const SerializerOptions& options);
    static std::unique_ptr<Deserializer> createDeserializer(SerializationFormat format);
};

} // namespace serde
} // namespace pangrova
```

### 16.3 Arrow IPC Serializer

`ArrowIpcSerializer` is used for Shuffle data transfer and Client result return.

Core flow: `Velox RowVector → Arrow RecordBatch (zero-copy) → Arrow IPC Stream (with compression)`

| Feature | Detail |
|---------|--------|
| Compression | LZ4_FRAME (default), ZSTD, UNCOMPRESSED |
| Estimated size | rawSize × compressionRatio + 1KB metadata overhead |
| Compression ratios | LZ4: 0.5, ZSTD: 0.3, Uncompressed: 1.0 |

`ArrowIpcDeserializer` reverse flow: `Arrow IPC Stream → RecordBatch → Velox RowVector`.

### 16.4 Arrow C Data Serializer (JNI zero-copy)

`ArrowCDataSerializer` does not perform actual serialization; instead it exports Arrow C Data Interface pointers for zero-copy transfer at the JNI layer.

| Method | Description |
|--------|-------------|
| `exportToArrowCData(RowVector, ArrowArray*, ArrowSchema*)` | Velox → Arrow RecordBatch → C Data Interface export |
| `importFromArrowCData(ArrowArray*, ArrowSchema*, pool) → RowVector` | C Data Interface import → Arrow RecordBatch → Velox |

Zero-copy path: the Java side passes the `ArrowArray`/`ArrowSchema` pointer addresses via JNI, and the native side reads/writes directly, with no serialization/deserialization.

### 16.5 Velox Native Serializer (Spill-only)

`VeloxSerializer` uses Velox's native `PrestoVectorSerde` format, dedicated to the spill-to-disk scenario.

<!-- Resolves: DEF-068 — Complete Velox Native Serializer flow description -->
Advantages: the fastest serialization/deserialization, consistent with Velox's internal format, with no Arrow conversion overhead. Flow: `RowVector → PrestoVectorSerde::serialize() → StreamArena → flush to Spill File (NVMe SSD) → PrestoVectorSerde::deserialize() → RowVector`.

### 16.6 Serializer Usage Scenarios

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Serializer Usage Scenarios                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Scenario                │ Serializer        │ Reason                           │
│  ─────────────────────────────────────────────────────────────────────────────  │
│  Shuffle data transfer   │ Arrow IPC         │ Cross-lang, compressed, streaming│
│  Client result return    │ Arrow IPC         │ Cross-lang, native Arrow Flight  │
│  JNI data transfer       │ Arrow C Data      │ Zero-copy, lowest latency        │
│  Spill to disk           │ Velox Serde       │ Fastest ser/deserialization      │
│  Broadcast variable      │ Arrow IPC         │ Cross-Worker transfer            │
│  Checkpoint              │ Arrow IPC + comp. │ Persistence, space efficiency    │
│                                                                                  │
│  Selection principles:                                                           │
│  1. Cross-language/cross-process → Arrow IPC                                    │
│  2. JNI same-process → Arrow C Data (zero-copy)                                 │
│  3. Local disk I/O → Velox Serde (fastest)                                      │
│  4. Network transfer → Arrow IPC + LZ4 compression                              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```


## 17. Expression Codegen — LLVM JIT for Complex Expressions (OPT-005)

Pangrova currently relies entirely on Velox's interpreted vectorized expression evaluation. Each function in an expression tree is a virtual function call (`VectorFunction::apply()`) processing a batch of rows. Complex expressions (e.g., `CASE WHEN a > 0 THEN b * c + d ELSE e * f END`) result in multiple virtual dispatch calls and intermediate vector materializations per batch.

This section introduces a two-tier expression evaluation strategy: interpreted (default, Velox built-in) for simple expressions, and JIT-compiled for complex hot expressions.

### 17.1 Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          Expression Evaluation                                    │
│                                                                                   │
│  ┌──────────────┐      ┌───────────────────────────────────┐                     │
│  │ Expression   │─────▶│ Complexity Analyzer                │                     │
│  │ Tree (AST)   │      │ (count operators, estimate         │                     │
│  └──────────────┘      │  dispatch overhead)                │                     │
│                         └────────────────┬──────────────────┘                     │
│                                          │                                        │
│                          ┌───────────────┴────────────────┐                      │
│                          ▼                                ▼                       │
│                ┌──────────────────┐            ┌───────────────────┐              │
│                │ Velox Interpreted │            │ LLVM JIT Codegen  │              │
│                │ (< 5 operators)  │            │ (≥ 5 operators)   │              │
│                └──────────────────┘            └───────────────────┘              │
│                                                        │                          │
│                                               ┌────────▼──────────┐              │
│                                               │ CodegenCache      │              │
│                                               │ (LRU, keyed by    │              │
│                                               │  expression hash) │              │
│                                               └───────────────────┘              │
│                                                                                   │
│  Integration Points:                                                              │
│  • New C++ module: ExpressionCodegen in pangrova-native (depends on LLVM)          │
│  • VeloxPlanConverter: detects codegen-eligible expressions, wraps them           │
│  • Config: pangrova.native.expression-codegen.enabled (default false)              │
│  • Config: pangrova.native.expression-codegen.threshold (default 5 operators)      │
│  • Config: pangrova.native.expression-codegen.cache-size (default 1000 entries)    │
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Decision criteria for codegen vs interpreted**:

| Criterion | Interpreted Path | JIT Codegen Path |
|-----------|-----------------|------------------|
| Operator count | < 5 | ≥ 5 |
| Batch count | any | ≥ 100 (amortize compile cost) |
| String operations | — | +2 complexity score |
| Nested CASE/WHEN | — | +3 complexity score |

### 17.2 Pseudocode

**Complexity Analyzer**:

```pseudocode
class ExpressionComplexityAnalyzer:
    config:
        codegenThreshold = 5       // min operators to trigger codegen
        codegenMinBatches = 100    // min batches before JIT (amortize compile cost)

    function shouldCodegen(expr: ExpressionTree, batchCount: int):
        operatorCount = countOperators(expr)
        hasStringOps = containsStringOperations(expr)
        hasNestedCase = containsNestedCaseWhen(expr)

        complexityScore = operatorCount
            + (hasStringOps ? 2 : 0)
            + (hasNestedCase ? 3 : 0)

        return complexityScore >= codegenThreshold
           AND batchCount >= codegenMinBatches
```

**JIT Compilation**:

```pseudocode
class ExpressionJITCompiler:
    cache: LRUCache<ExpressionHash, CompiledFunction>

    function compile(expr: ExpressionTree):
        hash = expr.semanticHash()
        cached = cache.get(hash)
        if cached != null:
            return cached

        // Generate LLVM IR from expression tree
        irBuilder = LLVMIRBuilder()
        function = irBuilder.createFunction("expr_" + hash)

        // Emit a single tight loop over the batch
        batchPtr = function.addParam(Type.ARROW_BATCH_PTR)
        outputPtr = function.addParam(Type.ARROW_VECTOR_PTR)

        loop = function.createLoop(0, batchSize)
        result = emitExpression(irBuilder, loop.index, expr, batchPtr)
        irBuilder.store(result, outputPtr, loop.index)

        // Compile LLVM IR to native machine code
        compiled = LLVMCompiler.compile(function, optimizationLevel = O2)
        cache.put(hash, compiled)
        return compiled

    function emitExpression(ir, idx, expr, batchPtr):
        match expr:
            case BinaryOp(op, left, right):
                l = emitExpression(ir, idx, left, batchPtr)
                r = emitExpression(ir, idx, right, batchPtr)
                return ir.emitBinaryOp(op, l, r)
            case ColumnRef(colIdx):
                return ir.loadFromBatch(batchPtr, colIdx, idx)
            case Literal(value):
                return ir.constant(value)
            case CaseWhen(conditions, results, elseResult):
                return ir.emitCaseWhen(conditions, results, elseResult, idx, batchPtr)
```

**Adaptive Compilation (Interpret-First, JIT-Later)**:

```pseudocode
class AdaptiveExpressionEvaluator:
    function evaluate(expr: ExpressionTree, batch: RecordBatch):
        stats = getExprStats(expr)
        stats.batchCount++

        if stats.compiledFunction != null:
            return stats.compiledFunction.execute(batch)

        if analyzer.shouldCodegen(expr, stats.batchCount):
            // Compile asynchronously — don't block current batch
            compileAsync(expr, callback = (fn) => stats.compiledFunction = fn)

        // Fall back to Velox interpreted evaluation
        return veloxEvaluate(expr, batch)
```

### 17.3 Trade-offs Analysis

| Aspect | Current (Interpreted) | Proposed (JIT Codegen) |
|--------|----------------------|------------------------|
| Performance | Virtual dispatch per operator per batch; intermediate vectors materialized | Single tight loop per expression; no intermediate vectors; 2-5x speedup for complex expressions |
| Complexity | Simple — Velox built-in evaluation | High — LLVM dependency, IR generation, async compilation, cache management |
| Risk | None | Medium — LLVM adds ~50 MB binary size; JIT compilation bugs can cause crashes; LLVM version compatibility |
| Compile overhead | None | ~1-10 ms per expression (amortized over 100+ batches); async compilation avoids blocking |
| Memory overhead | None | CodegenCache: ~1 KB per compiled function × cache size; LLVM runtime: ~50 MB |

**Key risks**:

1. **LLVM binary size**: ~50 MB addition to `pangrova-native`. Mitigated by making codegen an optional build flag.
2. **Null handling**: JIT code must correctly propagate SQL NULL semantics for every operator type. Incorrect null handling causes silent data corruption.
3. **Upstream alignment**: Consider contributing to Velox upstream codegen efforts instead of building a separate layer, to reduce long-term maintenance burden.

### 17.4 Migration Path

1. **Phase 1 — Complexity Analyzer**: Implement expression complexity scoring. Log codegen-eligible expressions without compiling. Measure potential benefit via metrics (`pangrova.native.codegen.eligible_count`, `pangrova.native.codegen.estimated_speedup`).
2. **Phase 2 — LLVM Integration**: Add LLVM as a build dependency in `pangrova-native`. Implement basic IR generation for arithmetic expressions only (no strings, no CASE/WHEN).
3. **Phase 3 — Full Expression Codegen**: Extend IR generation to string operations, CASE/WHEN, NULL handling, and type casts. Implement `CodegenCache` with LRU eviction.
4. **Phase 4 — Adaptive Compilation**: Implement interpret-first-JIT-later strategy. Enable by default for expressions exceeding complexity threshold. Gate behind `pangrova.native.expression-codegen.enabled` config flag.


<!-- Resolves: DEF-AP-014 — Observability integration for native layer -->
## 18. Observability Integration

This section defines the metrics and tracing integration points for the native execution layer. For the full observability design, see [14-observability.md](./14-observability.md).

### 18.1 Metrics

The following metrics from [14 §5](./14-observability.md) are emitted by native layer components:

| Metric | Type | Source Component | Description |
|--------|------|-----------------|-------------|
| `pangrova.native.operator.rows_out` | counter | Velox Operator | Output rows per operator |
| `pangrova.native.io.bytes_read` | counter | Velox IO | Bytes read from storage |
| `pangrova.native.memory.peak_bytes` | gauge | MemoryAllocator | Peak native memory usage |
| `pangrova.native.spill.bytes` | counter | SpillManager | Bytes spilled to disk |
| `pangrova.native.jni.call_count` | counter | JNI Bridge | JNI boundary crossings |
| `pangrova.native.jni.latency_us` | histogram | JNI Bridge | JNI call latency |
| `pangrova.native.codegen.compiled_count` | counter | CodegenCache | Compiled expressions (OPT-005) |

### 18.2 Tracing

Native layer emits the following spans (see [14 §4](./14-observability.md) for span schema):

- `native.task.init` — Native task initialization via JNI
- `native.operator.execute` — Per-operator execution span
- `native.spill` — Spill operation span
- `native.jni.call` — JNI boundary crossing span (sampled)


## 19. Native UDF Framework Enhancement (OPT-006)

<!-- Source: Architecture Review — OPT-006 (Native UDF Framework) -->

Extends §15 Native UDF design with SQL interface, Java UDF bridge, library distribution, and sandboxing.

### 19.1 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Layer 1: SQL Interface                                       │
│  CREATE FUNCTION geo_distance(DOUBLE,DOUBLE,DOUBLE,DOUBLE)    │
│    RETURNS DOUBLE LANGUAGE NATIVE                             │
│    LIBRARY 'libpangrova_geo_udf.so' FUNCTION 'haversineDistance'│
├──────────────────────────────────────────────────────────────┤
│  Layer 2: Java Coordination (UDFRegistry)                     │
│  - Stores UDF metadata in PostgreSQL                          │
│  - Distributes library paths via TaskRequest.udf_libraries    │
│  - Validates type compatibility                               │
├──────────────────────────────────────────────────────────────┤
│  Layer 3: Native Execution (NativeUDFLoader)                  │
│  - dlopen() shared library                                    │
│  - Resolve pangrova_register_udfs entry point                   │
│  - Register with Velox function registry                      │
└──────────────────────────────────────────────────────────────┘
```

### 19.2 Pseudocode

**Native UDF Loader**:

```pseudocode
class NativeUDFLoader:
    function loadLibrary(path):
        handle = dlopen(path, RTLD_NOW | RTLD_LOCAL)
        registerFn = dlsym(handle, "pangrova_register_udfs")
        registerFn(veloxFunctionRegistry)
        loadedLibraries.put(path, handle)
```

**Java UDF Bridge** (compatibility layer for existing Java UDFs):

```pseudocode
class JavaUDFBridge:
    function registerJavaUDF(name, javaClass):
        // Wrapper: Velox RowVector → Arrow C Data (zero-copy) → JNI → Java UDF → Arrow → RowVector
        // Overhead: ~100ns JNI + Java execution time per batch
        nativeWrapper = createJNICallbackWrapper(name, javaClass)
        veloxFunctionRegistry.register(name, nativeWrapper)
```

### 19.3 Trade-offs

| Aspect | Current | With Native UDF Framework |
|--------|---------|--------------------------|
| Performance | Java UDFs require JNI round-trip per batch | Native UDFs at full Velox speed; Java bridge maintains compatibility |
| Complexity | None | Medium — library loading, registry, distribution |
| Risk | None | Native UDF crashes can bring down Worker; sandboxing needed for untrusted code |
| Ecosystem | Limited | Open — C++ UDF API enables geo, ML, domain-specific extensions |

### 19.4 Migration Path

1. **Phase 1**: Implement `NativeUDFLoader` with `dlopen`. Manual library deployment.
2. **Phase 2**: Add `CREATE FUNCTION ... LANGUAGE NATIVE` SQL syntax. `UDFRegistry` in PostgreSQL. Library paths in `TaskRequest`.
3. **Phase 3**: Java UDF Bridge — JNI callback wrapper for existing Java UDFs.
4. **Phase 4**: Automatic library distribution + process-level sandboxing for multi-tenant.

---

<!-- Resolves: OPT-015 — Shuffle Path JNI Overhead Quantification & Optimization -->

## 20. Shuffle Path JNI Overhead Quantification & Optimization (OPT-015)

This section addresses the unquantified overhead in the shuffle data path (Native → JNI → Java Arrow Flight → network) and proposes a two-phase approach: instrumentation first, then targeted optimization based on data.

### 20.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Shuffle Path Overhead Breakdown                     │
│                                                                  │
│  Velox Pipeline                                                  │
│       │                                                          │
│       ▼ (1) nativeGetShuffleBatch() ─── JNI call overhead        │
│  Arrow C Data (zero-copy)                                        │
│       │                                                          │
│       ▼ (2) ArrowIPC.serialize() ────── serialization overhead   │
│  Arrow IPC bytes                                                 │
│       │                                                          │
│       ▼ (3) flightClient.putNext() ──── network send overhead    │
│  Network → target Worker                                         │
│       │                                                          │
│       ▼ (4) ArrowIPC.deserialize() ──── deserialization          │
│  Arrow C Data → Velox RowVector                                  │
│                                                                  │
│  Instrumentation probes at each transition point (1)–(4)         │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: The shuffle data path traverses JNI, Arrow IPC serialization, and Java thread scheduling, but the overhead is unquantified. Arrow IPC serialization runs at ~0.5–1 GB/s per core. For a 10GB shuffle, this adds 10–20s of CPU time. For 1000-partition shuffles, 1000 drain threads compete for CPU.

### 20.2 Pseudocode

**Phase 1 — Instrumentation**:

```pseudocode
class ShufflePathInstrumentation:
    metrics:
        jniCallLatencyNs: Histogram
        arrowIpcSerializeNs: Histogram
        arrowIpcDeserializeNs: Histogram
        flightPutLatencyNs: Histogram
        shuffleDrainThreadWaitNs: Histogram
        totalShuffleOverheadRatio: Gauge

    function instrumentShuffleWrite(batch: ArrowBatch, partitionId: int):
        t0 = nanoTime()
        jniBatch = nativeGetShuffleBatch(partitionId)
        t1 = nanoTime()
        ipcBytes = ArrowIPC.serialize(jniBatch)
        t2 = nanoTime()
        flightClient.putNext(ipcBytes)
        t3 = nanoTime()

        jniCallLatencyNs.record(t1 - t0)
        arrowIpcSerializeNs.record(t2 - t1)
        flightPutLatencyNs.record(t3 - t2)
```

**Phase 2 — Optimization Options** (selected based on instrumentation data):

```pseudocode
// Option A: Batch coalescing — reduce JNI call count
class CoalescingShuffleWriter:
    bufferSize = 64KB
    function write(partitionId: int, batch: ArrowBatch):
        buffer[partitionId].append(batch)
        if buffer[partitionId].size >= bufferSize:
            flush(partitionId)

// Option B: Adaptive thread pool — reduce thread contention
class AdaptiveShuffleDrainPool:
    poolSize = min(partitionCount, cpuCores * 2)
    // Partitions multiplexed across bounded pool

// Option C: Native shuffle path (if overhead > 10%)
class NativeShufflePath:
    // Bypass Java: Native → Arrow Flight C++ client → network
    // → Arrow Flight C++ server → Native
    // Eliminates: JNI, Java thread scheduling, Java GC pressure
```

### 20.3 Trade-offs

| Aspect | Current (Unquantified) | With Instrumentation + Optimization |
|--------|----------------------|-------------------------------------|
| Observability | Shuffle overhead unknown | Full instrumentation reveals bottlenecks |
| Performance | Potentially 5–20% overhead on shuffle-heavy queries | Targeted optimization based on actual data |
| Complexity | Simple — single path | Phase 1: Low. Phase 2: Medium–High (native shuffle path) |
| Risk | Unknown overhead may be critical | Low — instrumentation is non-invasive; optimization is optional |

### 20.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Instrumentation | Add timing probes to shuffle path. Expose via Prometheus. Run TPC-DS shuffle-heavy queries. |
| 2 | Quick Wins | Batch coalescing + adaptive thread pool if instrumentation shows these are bottlenecks. |
| 3 | Native Shuffle Path | If overhead > 10% of total query time, implement native Arrow Flight C++ shuffle path bypassing Java. |

> **Cross-reference**: The Shuffle Partitioner (§11) handles data partitioning before handoff to the shuffle backend. External Shuffle Service (OPT-010 in [02 §18](./03-query-execution.md)) provides an alternative architecture that also addresses shuffle overhead. Instrumentation metrics integrate with [14-observability.md](./14-observability.md).


---

<!-- Resolves: OPT-016 — Enhanced Operator Fusion -->

## 21. Enhanced Operator Fusion (OPT-016)

This section extends Velox's built-in filter+project fusion to cover additional pipeline patterns, reducing intermediate vector materialization and improving cache locality.

### 21.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Operator Fusion Pipeline                             │
│                                                                  │
│  Before (3 intermediate materializations):                       │
│  Scan ──▶ [batch] ──▶ Filter ──▶ [batch] ──▶ Project ──▶ Agg   │
│                                                                  │
│  After (fused — 0 intermediate materializations):                │
│  FusedScanFilterProject ──▶ Agg                                  │
│    scan loop:                                                    │
│      batch = scan.getNextBatch()                                 │
│      selVec = filter.evaluate(batch)     // in-place             │
│      result = project.evaluateSelected(batch, selVec) // direct  │
│      return result                                               │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: Each operator produces a full output batch consumed by the next, causing intermediate vector materialization (memory allocation, column copy, L2/L3 cache eviction). For Scan→Filter→Project→Agg, three intermediate batches are materialized.

### 21.2 Pseudocode

**Fusion Analyzer**:

```pseudocode
class FusionAnalyzer:
    function canFuse(upstream: Operator, downstream: Operator):
        // Rule 1: Filter + Project (existing Velox)
        if upstream is Filter AND downstream is Project: return true
        // Rule 2: Project + Project (column pruning chain)
        if upstream is Project AND downstream is Project: return true
        // Rule 3: Scan + Filter (push filter into scan loop)
        if upstream is TableScan AND downstream is Filter: return true
        return false
```

**Fused Scan-Filter-Project Operator**:

```pseudocode
class FusedScanFilterProject extends Operator:
    scan: TableScan
    filter: Expression
    projection: List<Expression>

    function getNext():
        while true:
            batch = scan.getNextBatch()
            if batch == null: return null
            selectionVector = filter.evaluate(batch)
            if selectionVector.count == 0: continue
            return projection.evaluateSelected(batch, selectionVector)
```

### 21.3 Trade-offs

| Aspect | Current (FilterProject only) | Enhanced Fusion |
|--------|------------------------------|-----------------|
| Performance | Intermediate materialization between operators | 10–30% improvement for fusion-eligible pipelines |
| Complexity | Simple — Velox built-in | Medium — new fusion rules, fused operator implementations |
| Risk | None | Low — unfused fallback produces identical results |
| Upstream | Velox community may implement broader fusion | Pangrova-specific rules complement Velox improvements |

### 21.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Fusion Analyzer | Pipeline analysis to identify fusion candidates. Log opportunities without applying. |
| 2 | Scan+Filter Fusion | Fuse TableScan and Filter into single operator applying predicates during scan. |
| 3 | Project Chain Fusion | Fuse consecutive Project operators into single projection. |
| 4 | Contribute Upstream | Contribute fusion improvements to Velox community. |

> **Cross-reference**: Operator fusion operates within the Velox Pipeline execution model defined in [02 §4](./03-query-execution.md). Expression Codegen (§17 OPT-005) complements fusion by JIT-compiling the fused expression tree.


---

<!-- Resolves: OPT-021 — SIMD Optimization for Custom Components -->

## 22. SIMD Optimization for Custom Components (OPT-021)

This section adds targeted SIMD intrinsics for Pangrova's custom reader components (`PositionDeleteFilter`, `EqualityDeleteFilter`, `LsmMergeReader`) that are not covered by Velox's built-in SIMD or compiler auto-vectorization.

### 22.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              SIMD Optimization Targets                            │
│                                                                  │
│  PositionDeleteFilter (§7.4, §12)                                │
│    └─ Roaring Bitmap membership → AVX2 batch check (8 pos/op)   │
│                                                                  │
│  EqualityDeleteFilter (§7.4, §12)                                │
│    └─ Hash Set probe → AVX2 parallel hash+compare (8 keys/op)   │
│                                                                  │
│  LsmMergeReader (§7.5)                                           │
│    └─ Priority queue merge → SIMD comparison (future)            │
│                                                                  │
│  Runtime ISA Detection:                                          │
│    AVX2 available? → SIMD path                                   │
│    Otherwise       → scalar fallback                             │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: Pangrova's custom components rely on compiler auto-vectorization (`-O3 -mavx2`), which is suboptimal for complex patterns like bitmap membership and hash probing. Manual SIMD intrinsics can achieve 2–4× throughput for Iceberg MOR workloads with large delete files.

### 22.2 Pseudocode

**SIMD Roaring Bitmap Membership (AVX2)**:

```pseudocode
// Check 8 positions against bitmap simultaneously
function simdBitmapContains(bitmap: RoaringBitmap, positions: int64[8]):
    posVec = _mm256_loadu_si256(positions)
    containerIdx = _mm256_srli_epi32(posVec, 16)
    bitIdx = _mm256_and_si256(posVec, _mm256_set1_epi32(0xFFFF))
    containers = _mm256_i32gather_epi32(bitmap.containers, containerIdx, 8)
    masks = _mm256_sllv_epi32(_mm256_set1_epi32(1), bitIdx)
    results = _mm256_and_si256(containers, masks)
    return _mm256_movemask_epi8(
        _mm256_cmpeq_epi32(results, _mm256_setzero_si256()))
```

**SIMD Hash Probe for Equality Delete (AVX2)**:

```pseudocode
// Hash and probe 8 keys simultaneously
function simdHashProbe(hashSet: HashSet, keys: int64[8]):
    keyVec = _mm256_loadu_si256(keys)
    hashes = simdCRC32(keyVec)
    bucketMask = _mm256_set1_epi32(hashSet.bucketCount - 1)
    buckets = _mm256_and_si256(hashes, bucketMask)
    stored = _mm256_i32gather_epi32(hashSet.buckets, buckets, 8)
    matches = _mm256_cmpeq_epi32(keyVec, stored)
    return _mm256_movemask_epi8(matches)
```

### 22.3 Trade-offs

| Aspect | Current (Auto-Vectorization) | Manual SIMD |
|--------|------------------------------|-------------|
| Performance | Compiler-dependent; suboptimal for complex patterns | 2–4× throughput for Iceberg MOR with large delete files |
| Complexity | None | High — platform-specific intrinsics, careful testing |
| Portability | Portable (compiler handles ISA) | AVX2 required; needs scalar fallback for non-AVX2 CPUs |
| Maintainability | Simple | Higher — SIMD code harder to read and debug |

### 22.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Benchmark | Profile `PositionDeleteFilter` and `EqualityDeleteFilter` on Iceberg MOR workloads. Quantify time in bitmap/hash ops. |
| 2 | Roaring Bitmap SIMD | AVX2-accelerated bitmap membership. Consider CRoaring library's SIMD support. |
| 3 | Hash Probe SIMD | SIMD hash probe for equality delete filter. |
| 4 | Runtime ISA Detection | CPU feature detection to select SIMD vs scalar path at runtime. |

> **Cross-reference**: Delete filter design is defined in §7.4 and §12. Roaring Bitmap is used by `PositionDeleteFilter` for Iceberg position deletes. The reader pipeline architecture is in §7.


---

<!-- Resolves: OPT-022 — Native RowVector Reader Pipeline -->

## 23. Native RowVector Reader Pipeline (OPT-022)

This section refactors the reader pipeline to produce Velox `RowVector` directly, eliminating the Arrow RecordBatch → RowVector conversion overhead at the `ArrowCDataBridge` boundary.

### 23.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Current: Reader → Arrow RecordBatch → ArrowCDataBridge → Velox │
│                                         ↑ conversion overhead    │
│                                                                  │
│  Proposed: Reader → Velox RowVector directly                     │
│            (Arrow conversion only at JNI export boundary)        │
│                                                                  │
│  FormatReader (refactored)                                       │
│    ParquetFormatReader → velox::dwio::parquet::ParquetReader     │
│    OrcFormatReader → velox::dwio::orc::OrcReader                 │
│         │                                                        │
│         ▼ RowVectorPtr (no Arrow intermediate)                   │
│  DeleteFilter → RowVectorPtr                                     │
│         │                                                        │
│         ▼                                                        │
│  MergeReader → RowVectorPtr                                      │
│         │                                                        │
│         ▼                                                        │
│  Velox Pipeline (zero conversion)                                │
│                                                                  │
│  ArrowExportAdapter (JNI boundary only)                          │
│    RowVectorPtr → ArrowArray (existing ArrowCDataBridge)         │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: Reader components produce Arrow RecordBatch, which is immediately converted to Velox RowVector via `ArrowCDataBridge`. For flat types, conversion is lightweight (pointer exchange). For complex types (nested structs, maps, lists), memory layout differs and conversion adds 1–3% CPU overhead.

### 23.2 Pseudocode

**Refactored Reader Interface**:

```pseudocode
// Current
interface FormatReader:
    function readBatch(): ArrowRecordBatch

// Proposed
interface FormatReader:
    function readBatch(): velox::RowVectorPtr

class ParquetFormatReader implements FormatReader:
    function readBatch():
        // Use Velox native Parquet reader directly
        return veloxParquetReader.next()  // zero conversion

class DeleteFilter:
    function filter(batch: velox::RowVectorPtr): velox::RowVectorPtr

class MergeReader:
    function merge(streams: List<RowVectorStream>): velox::RowVectorPtr
```

**Arrow Compatibility Layer** (JNI boundary only):

```pseudocode
class ArrowExportAdapter:
    function exportToArrow(batch: velox::RowVectorPtr): ArrowArray
        return ArrowCDataBridge.exportToArrow(batch)
```

### 23.3 Trade-offs

| Aspect | Current (Arrow Intermediate) | Native RowVector |
|--------|------------------------------|------------------|
| Performance | 1–3% CPU overhead for complex schemas | Zero conversion within reader pipeline |
| Complexity | Moderate — Arrow is standard format | Medium — must use Velox native reader APIs |
| Risk | Low | Medium — Velox reader feature parity with Arrow readers (specific encodings, nested types) |
| Compatibility | Arrow RecordBatch is standard interchange | RowVector is Velox-internal; Arrow only at JNI boundary |

### 23.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Feature Parity Audit | Compare Velox Parquet/ORC reader coverage with Arrow readers. Identify gaps. |
| 2 | FormatReader Refactor | Switch `ParquetFormatReader` to Velox native `dwio::parquet::ParquetReader`. Arrow reader as fallback. |
| 3 | DeleteFilter + MergeReader | Refactor to accept and produce RowVector. |
| 4 | Remove Arrow Dependency | Remove Arrow reader from read path. Arrow conversion only at JNI export. |

> **Cross-reference**: The current reader architecture is defined in §7. `ArrowCDataBridge` is defined in §6.1. Delete filters (§7.4, §12) and MergeReader (§7.5) are the components to refactor. Velox's native readers are part of the `dwio` module.


---

## 24. Native Build Pipeline

Standardized build pipeline for `pangrova-worker-native`. Uses a hermetic dependency model — all C++ dependencies are built from source into a single prefix tree, eliminating version conflicts from system package managers (Homebrew, MacPorts, apt).

### 24.1 Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Native Build Pipeline                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Step 1: setup-native-build-env.sh (one-time, ~30 min)                  │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  abseil-cpp ──→ protobuf ──→ gRPC ──→ ~/.pangrova/native-deps/ │     │
│  │  Velox (+ folly/glog/gflags/fmt/boost) ──→ ~/.pangrova/velox/  │     │
│  │  env.sh ──→ ~/.pangrova/env.sh                                  │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  Step 2: source ~/.pangrova/env.sh                                        │
│                                                                          │
│  Step 3: cmake --preset dev-debug                                       │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  CMakePresets.json reads $env{PANGROVA_DEPS_PREFIX}              │     │
│  │  → CMAKE_PREFIX_PATH = hermetic deps prefix                   │     │
│  │  → find_package(Protobuf/gRPC) resolves to hermetic prefix    │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  Step 4: cmake --build build                                            │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  → libpangrova_native.dylib/so                                  │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  Step 5: pangrova-worker-native/bin/package-native-lib.sh                  │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  → META-INF/native/{os}/{arch}/libpangrova_native.{dylib|so}   │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 24.2 Quick Start (New Developer)

```bash
# 1. One-time setup (~30 min, builds abseil/protobuf/gRPC/Velox from source)
./pangrova-worker-native/bin/setup-native-build-env.sh

# 2. Activate environment
source ~/.pangrova/env.sh

# 3. Build
cd pangrova-worker-native
cmake --preset dev-debug
cmake --build build -j$(sysctl -n hw.ncpu)   # macOS
cmake --build build -j$(nproc)                # Linux

# 4. Test
ctest --test-dir build --output-on-failure

# 5. Package for Java
cd .. && ./pangrova-worker-native/bin/package-native-lib.sh
```

### 24.3 Dependency Resolution

| Dependency | Source | Resolution |
|-----------|--------|------------|
| abseil, protobuf, gRPC | `~/.pangrova/native-deps/` | `CMAKE_PREFIX_PATH` via CMakePresets.json |
| Velox (+ folly, glog, gflags, fmt, boost) | `~/.pangrova/velox/` | `VELOX_HOME` + `VELOX_BUILD_DIR` env vars |
| Arrow, Parquet | System (Homebrew/apt) | `find_package(Arrow)` |
| CRoaring, GoogleTest, OpenTelemetry | FetchContent | Built as part of cmake configure |
| JNI | System JDK | `find_package(JNI)` |

### 24.4 File Inventory

| File | Purpose |
|------|---------|
| `pangrova-worker-native/bin/setup-native-build-env.sh` | One-shot hermetic environment setup |
| `pangrova-worker-native/bin/package-native-lib.sh` | Copy dylib/so to Java classpath resource dir |
| `pangrova-worker-native/CMakeLists.txt` | Main build definition |
| `pangrova-worker-native/CMakePresets.json` | Preset configurations (dev-debug, dev-release, ci-*) |
| `pangrova-worker-native/cmake/PangrovaProtobuf.cmake` | Protobuf resolution + proto codegen |
| `pangrova-worker-native/cmake/FindVelox.cmake` | Velox detection + imported target |
| `pangrova-worker-native/cmake/FindArrowFlight.cmake` | Arrow Flight detection |
| `pangrova-worker-native/cmake/PangrovaSystemDeps.cmake` | System library resolution for Velox transitive deps |

### 24.5 Pinned Versions

| Library | Version | Rationale |
|---------|---------|-----------|
| gRPC | v1.62.1 | Determines compatible abseil + protobuf versions |
| abseil-cpp | 20240116.2 | Required by gRPC v1.62.1 |
| protobuf | v25.3 | Required by gRPC v1.62.1 |
| Velox | v2026.04.17.00 | Execution engine (overridable via `VELOX_REF`) |
| OpenTelemetry C++ | v1.17.0 | Tracing SDK (FetchContent) |
| CRoaring | v4.1.7 | Roaring bitmaps (FetchContent) |

### 24.6 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PANGROVA_DEPS_PREFIX` | `~/.pangrova/native-deps` | Hermetic prefix for abseil/protobuf/gRPC |
| `VELOX_HOME` | `~/.pangrova/velox` | Velox source root |
| `VELOX_BUILD_DIR` | `~/.pangrova/velox/_build/release` | Velox cmake build directory |
| `VELOX_REF` | `v2026.04.17.00` | Velox git tag to build |
| `SKIP_VELOX` | `0` | Set `1` to skip Velox build |
| `SKIP_DEPS` | `0` | Set `1` to skip deps build |

### 24.7 Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `protoc not found` | Setup not run | `./pangrova-worker-native/bin/setup-native-build-env.sh` |
| `PROTOBUF_CONSTEXPR undefined` | System protobuf headers leaking | Delete build dir, re-source env.sh, reconfigure |
| `google/protobuf/any.proto not found` | Well-known types missing | Verify `PANGROVA_DEPS_PREFIX` contains `include/google/protobuf/any.proto` |
| `Velox NOT found` | Env not activated | `source ~/.pangrova/env.sh` |
