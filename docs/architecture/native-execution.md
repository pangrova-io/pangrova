---
title: "Native Execution Layer"
sidebar_label: "Native Execution Layer"
---

# Pangrova — Native Execution Layer

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Current data-plane boundary.** Java owns task control and connector-backed
> write sinks. Velox executes relational operators in Native; Native Flight
> serves both result and shuffle streams. Worker-to-Worker shuffle carries
> serialized Velox pages and does not pull batches through Java/JNI. Historical
> interface sketches below are not executable contracts; the authoritative
> task/wire definitions are the canonical v2 proto and current JNI exports.

## 1. Overall Architecture

```text
Coordinator JVM: SQL/planning → canonical v2 task binding → admission/scheduling
                                      │
                                      ▼
Worker JVM: task lifecycle, connector registry, memory arbitration,
            Iceberg/Paimon page-sink callback, fenced status reporting
                                      │ JNI control + write callback
                                      ▼
Worker Native: Velox plan/operator execution → Native shuffle/result Flight
               ↑                                      │
               └──────── Native ExchangeSource ◀──────┘
```

The JVM Coordinator does not execute data operators or merge vector Top-K
results. The Worker JVM is not a per-batch network shuffle relay. Read-dependent
Iceberg/Paimon writes are staged through the Java connector SDK from a Native
write-drain callback; the final catalog commit remains Coordinator-owned.
Lance's Rust/C++ FFI is a separate Native storage adapter whose public SQL
write and catalog publication path is still subject to the acceptance criteria
in [distributed-writes.md](./distributed-writes.md).

For canonical v2 tasks, `StageResourceGrant.native_memory_bytes` is a hard
per-task Native growth limit, **not** an up-front resident-memory reservation.
The Worker installs its node pool before the Native allocation callback; actual
Native allocations charge both the node pool and the shared `MemoryArbitrator`.
The serialized shuffle-buffer grant is separately reserved in full before HTTP
task acceptance. This permits multiple small tasks to run concurrently without
charging each task's theoretical Native maximum as if it were already used.
Heartbeat placement advertises the smaller of the node-pool free bytes and the
shared arbitrator's available bytes; a node-specific cap cannot be hidden by a
larger global budget.
Admission and pressure handling still require bounded, fair scheduling across
Coordinators; a hard cap alone is not a memory guarantee.

The planner places a hash exchange on grouping columns **before** a grouped
Aggregate, or a singleton exchange before a global Aggregate. An unbounded
`ORDER BY` is gathered before its Native Sort; root Top-K retains task-local
pruning followed by a Native final Top-K. These are Native data-plane
operators: the Coordinator only binds stages and never combines rows in its
JVM. Pipeline consumers may start only after upstream producer task records
exist, not while that stage is still `SCHEDULING`. The Native shuffle writer's
compression codec is also installed in Velox's Exchange QueryConfig and the
task-scoped Flight result decoder so both Native exchange pages and result
OutputBuffer pages use the same codec. The singleton global Sort
remains a scale bottleneck until distributed range-sort/merge is validated.

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
│  │  Output: bound split metadata (file, offset, length, format, filters)    │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                       │                                         │
│                                       │ canonical v2 split metadata            │
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

### 5.1 Boundary Principles

- Java receives and validates task-control requests, admits the canonical v2
  resource grant, and passes the same serialized task contract to Native.
- Native owns Velox execution, Worker-to-Worker shuffle, Flight result serving,
  and the associated byte-level backpressure and resource lifetime.
- JNI commands and status are control-plane crossings. Memory arbitration may
  synchronously call the registered Java allocation listener; connector writes
  synchronously call the Java page sink with borrowed Arrow IPC bytes.
- The Coordinator never merges data rows or implements a second vector/shuffle
  wire format. It schedules and validates fenced terminal reports.
- The canonical v2 proto is the single authoritative task/report contract;
  this document contains no duplicate protobuf or JNI method declaration.

### 5.2 Current Native Data Flow

```text
Coordinator (plan, task admission, scheduling)
    → Worker Native Velox operators
    → Native serialized shuffle queue
    → Native Flight DoGet
    → downstream Native ExchangeSource
    → result Worker Native Flight
    → client
```

Java does not repeatedly pull shuffle or result batches through JNI. The
producer queue and Flight demultiplexer provide bounded handoff; producer
success, abort and cancellation have distinct terminal outcomes. A slow
consumer must propagate pressure through the Native handoff, and retained
pages must remain charged until the last reader or linger cleanup releases
them. The current queue has a known byte-accounting gap, tracked in
[ai-native-roadmap.md](../internal/ai-native-roadmap.md); count-only capacity
is not proof of bounded memory.

### 5.3 Canonical Protocol

The single authoritative task, resource-grant, operator-binding, report, and
exchange contracts live in `pangrova-protocol/src/main/proto/pangrova/protocol/v2`.
Generated Java/C++ bindings are build artifacts and must not be hand-edited.
Planner and Coordinator create a canonical `TaskRequest`; Worker admission
validates it before JNI passes the same serialized contract to Native.
Connector-specific read bindings and split metadata are carried by that task
contract, not by a second copy of a protobuf definition in this document.

### 5.4 Current JNI Boundary

`NativeRuntime` and the Native command dispatcher admit the canonical v2
`TaskRequest`, create/start/cancel/release task handles, and report fenced
status and resource usage. The authoritative method signatures are the Java
class and C++ JNI exports; this document does not define a second interface.
Result and shuffle batches are served by Native Flight, not repeatedly pulled
through Java JNI. Read-dependent table writes use a synchronous Native-to-Java
`NativeWriteExecutor` callback carrying Arrow IPC bytes to a connector
`ConnectorPageSink`; the Worker returns a staged commit fragment, and the
Coordinator performs the final connector catalog commit.

### 5.5 Native Shuffle and Result Lifetime

`VeloxTaskRunner` routes producer output through `TaskOutputRouter` into
`SerializedShuffleQueue`. `FlightShuffleServer` serves ticket-scoped
partitions; downstream `PangrovaExchangeSource` consumes those serialized
Velox pages. `FlightResultServer` serves final result batches on its separate
result path. Successful producer completion commits clean shuffle EOF;
cancellation or failure aborts it. The queue takes ownership independent of
the producer's Velox task pool so Flight readers may finish after producer
teardown. Queue and Flight retention must both be byte-accounted and bounded;
the current producer-queue gap is tracked in the
[AI Native audit](../internal/ai-native-roadmap.md).

The result reader records a Flight stream failure against the exact registered
Native executor before releasing its output buffer. Both Arrow error statuses
and C++ exceptions during page allocation or decoding prevent a successful
TaskReport; the Worker then publishes a failed, zero-completed-coverage
reconciliation with the Native failure attached. A result stream that returns
an error is not a successful producer EOF.

## 6. JNI Boundary

The JNI layer transfers task-control commands, canonical v2 task bytes and
fenced status between the Java Worker and Native runtime. Native result and
shuffle streams remain in Native Flight; no Java batch puller or per-batch
shuffle JNI bridge is part of the production data path.

Read-dependent writes are the deliberate exception: the Native drain sends
Arrow IPC bytes synchronously to Java `NativeWriteExecutor`, which decodes
them for the connector `ConnectorPageSink`. The borrowed direct buffer must
not outlive that call. The current code in `NativeRuntime`, `JniBridge` and
`NativeWriteExecutor` is the authoritative interface and lifetime contract.
The Arrow C Data bridge exists for columnar interoperability, but does not
make the network shuffle or result path zero-copy.

## 7. Native Read Architecture

Read-dependent SQL tasks arrive as canonical v2 task requests with connector
bindings and splits. `PangrovaVeloxConnector` supplies data to Velox operators.
The connector-specific split/reader/delete/merge behavior is defined by the
current connector and Native implementations, not by a duplicate split
or reader SPI sketch here. A connector capability is publishable only when its
planner binding, Native conversion, reader, failure semantics and E2E test agree.

## 8. Velox Integration

`VeloxTaskRunner` owns the Native task execution lifecycle. The current
operator plan, including scans, joins, aggregation, exchanges and result
output, is created from the bound task plan. The Worker JVM does not execute
these relational operators.

## 9. Memory Management

`VeloxMemoryGovernor` governs task-pool allocations through the Worker
memory-arbitration boundary. Native Flight demux/spill and producer-queue
retention have distinct lifetimes; a task's pool usage alone is not a complete
shuffle-memory accounting figure. The exact resource gap and acceptance
criteria are in [resource-management.md](./resource-management.md) and the
[AI Native audit](../internal/ai-native-roadmap.md).

## 10. Native I/O

Velox and connector readers perform storage I/O under task-level read limits
and connector credentials. This document does not define an independent
filesystem factory, custom async executor or a second cache protocol.
Connector-specific read semantics are documented in
[connectors.md](./connectors.md).

## 11. Shuffle Layer

Producer output is partitioned and serialized by the Native execution path.
`SerializedShuffleQueue` owns queued pages; `FlightShuffleServer` serves
ticket-scoped DoGet streams; `PangrovaExchangeSource` consumes upstream
partitions directly in Native. There is no Java per-batch shuffle relay.
`ShuffleBufferStore` bounds demux retention separately from the producer
queue. The current/default deployment must not be described as zero-copy or
producer-loss recoverable without matching storage and fault tests.

## 12. Delete and Merge Semantics

Iceberg/Paimon row-level semantics depend on the bound connector operation,
snapshot, file/delete metadata and Native reader. No generic delete/merge
pseudocode in this document overrides those connector implementations.
Incomplete connector capabilities must fail admission rather than silently
falling back to a different row set.

## 13. Project Structure

Authoritative code locations are `pangrova-worker-native/src` for Native
execution, shuffle and JNI; `pangrova-worker/src/main/java` for Worker
admission/lifecycle and connector write callbacks; and
`pangrova-protocol/src/main/proto/pangrova/protocol/v2` for the shared
contract. Generated protocol sources are never hand-edited.

## 14. Extensibility

Any new Native capability must declare its signature, version, resource
limits and execution mode in the canonical capability inventory; planner,
Worker admission and Native implementation must consume that same
declaration. SPI presence or a design sketch is not evidence of executable
support.

## 15. Native Functions

Current scalar/vector functions are registered through the implemented
Native function bootstrap and capability catalog. This document does not
claim a separate WASM UDF host, generic dynamic UDF registry or Agent
execution path. Such a capability requires its own sandbox, resource
isolation, type contract and public-entry E2E before publication.

## 16. Serialization

Worker-to-Worker shuffle uses `ShuffleBatchSerializer` and serialized Velox
pages carried in a Flight binary cell. The final result path uses
`FlightResultServer` and its own Arrow result serialization. Arrow C Data
interop is not the shuffle wire format. The canonical task/report proto
remains the sole control contract.

## 17. Expression Execution

Velox expression evaluation is the current implementation. A separate LLVM
JIT or speculative operator-fusion pipeline is not a current Pangrova
capability and is not required to interpret the task protocol.

## 18. Observability

`NativeMetricsCollector` and the canonical terminal task report expose
execution statistics. A successful task with missing or malformed required
terminal statistics must fail validation; metrics are not inferred from a
Java-side batch pull loop.

## 19. Function Lifecycle

Function registration and availability are governed by the Native capability
inventory and Worker admission. Historical examples of independent UDF
registries or WASM hosts are not executable implementation contracts.

## 20. Native Shuffle Throughput and Memory

The current shuffle data path does not cross JNI per batch. The producer's
Velox callback partitions rows, serializes bounded pages, and copies page bytes
into the independently owned `SerializedShuffleQueue`. Native Flight serves
ticket-scoped partitions; the downstream Native ExchangeSource consumes them.
The Coordinator carries control metadata only.

The page ownership copy is a correctness boundary: Flight readers may outlive
the producer's Velox task pool. The canonical page limit is 4 MiB. The producer
queue reserves both a slot and actual resident bytes before copying; its default
byte cap is 16 MiB, while production task creation binds the exact Coordinator
shuffle grant and rejects a partial Native pool reservation. This closes the
former count-only queue gap, but does **not** bound every shuffle allocation.
`ShuffleBufferStore` separately charges retained demux pages or spill files
against server-wide limits; the current Flight server starts with its own
default 64 MiB retention cap, not a reservation from the JVM node arbiter.
`PartitionDemuxer::pop()` releases that charge as
it hands a page to a Flight reader; the reader may then allocate replay,
coalesced IOBuf and Arrow binary-array copies while only the number of active
streams (default 128) is limited. This uncharged active-stream memory remains
a P1 production-scale gap. It requires byte credits covering in-flight Flight
responses through send completion, or a proved zero-copy owner transfer, plus
concurrent slow-reader/cancellation/linger stress tests. No throughput or
end-to-end memory ceiling is claimed from the current focused tests.

## 21. Future Optimization Boundary

LLVM JIT, additional operator fusion, custom SIMD reader components and a
separate RowVector reader rewrite are research ideas, not current execution
modes or alternate contracts. Any such work must demonstrate a measured
end-to-end gain on representative distributed queries, preserve identical
results and resource accounting, and replace rather than coexist with a
legacy execution path. The current Velox and connector implementations are
the production source of truth.

## 22. Build and Verification

The authoritative Native/JNI build order, mandatory Lance FFI dependency,
Release packaging, full CTest, ASan and Java/E2E validation gates are in
[the repository build pipeline](../../.pangrova/rules/build-pipeline.md).
Do not copy toolchain versions, machine-specific paths or a shortened
"quick start" into this architecture document; passing a targeted Native
test is not equivalent to the full-chain acceptance gate.
