# Architecture

Pangrova is a **hybrid C++ / Java distributed OLAP engine** using the JNI bridge pattern.
Java handles coordination and planning; C++ handles high-performance vectorized execution.

## Java Side (Maven, JDK 17)

| Module | Responsibility |
|--------|---------------|
| `pangrova-common` | Shared utilities and base types |
| `pangrova-config` | Configuration management |
| `pangrova-spi` | SPI extension interfaces |
| `pangrova-shuffle` | Shuffle data exchange abstraction (Java side) |
| `pangrova-parser` | SQL parsing (Calcite dialect extensions) |
| `pangrova-planner` | Calcite logical plan → physical plan conversion |
| `pangrova-protocol` | Network protocol layer (gRPC / Arrow Flight) |
| `pangrova-observability` | Metrics and tracing |
| `pangrova-resilience` | Fault tolerance, retry, circuit breaker |
| `pangrova-operations` | DDL/DML operation execution |
| `pangrova-resource` | Resource management and scheduling |
| `pangrova-coordinator` | Distributed coordination, query routing, metadata |
| `pangrova-worker` | Worker node process, JNI loading entry, task scheduling |
| `pangrova-metadata` | Metadata storage and Catalog management |
| `pangrova-connector-*` | Data source connectors (Iceberg / Paimon) |
| `pangrova-standalone` | Single-node standalone deployment |
| `pangrova-security` | Authentication and authorization |
| `pangrova-streaming` | Streaming query support |
| `pangrova-governance` | Data governance and lineage |
| `pangrova-indexing` | Index management and build orchestration |
| `pangrova-transport` | Transport layer abstraction |
| `pangrova-client` | Client libraries (CLI / REST / JDBC) |
| `pangrova-testing` | Test infrastructure (e2e, benchmark, chaos, smoke) |
| `pangrova-native-bundle` | Native library packaging for distribution |

## C++ Side (CMake, C++20) — `pangrova-worker-native/`

| Component | Responsibility |
|-----------|---------------|
| `src/jni/` | JNI entry — Java→C++ bridge, lifecycle management |
| `src/velox/` | Velox-based vectorized execution engine, plan conversion |
| `src/executor/` | Task execution driver, pipeline orchestration |
| `src/shuffle/` | Arrow Flight distributed data exchange |
| `src/memory/` | Velox memory allocator + JVM memory coordination |
| `src/index/` | Bloom filter, vector index native implementation |
| `src/merge/` | Merge-on-Read multi-version merge |
| `src/delete/` | Deletion vector reading and row filtering |
| `src/split/` | Split reading (Iceberg/Paimon data file readers) |
| `src/result/` | Query result streaming and serialization |
| `src/lance/` | Lance format batch reader |
| `src/vector/` | Vector distance computation |
| `src/udf/` | Native UDF registry and execution |
| `src/variant/` | Semi-structured variant column reader |
| `src/text/` | Text processing (tokenization, stemming) |
| `src/write/` | Native write path |
| `src/compat/` | Compatibility shims and template instantiations |
| `src/observability/` | OpenTelemetry C++ SDK integration |
| `lance-ffi/` | Rust→C FFI for Lance vector storage |
| `proto/` | C++ side protobuf definitions (TaskRequest, status) |

## Interaction Boundary

```
┌───────────────────────────────────────────────────┐
│  Java (JVM)                                       │
│  pangrova-worker → JNI → libpangrova_native.dylib/so │
│                                                   │
│  Data: Arrow RecordBatch (zero-copy via JNI)      │
│  Control: Protobuf-serialized TaskRequest         │
└────────────────────┬──────────────────────────────┘
                     │ System.loadLibrary / JNI
┌────────────────────▼──────────────────────────────┐
│  C++ (Native)                                     │
│  libpangrova_native — single shared library         │
│                                                   │
│  Execution: Velox vectorized engine               │
│  Exchange:  Arrow Flight shuffle                  │
│  Storage:   Parquet / Lance / ORC readers         │
└───────────────────────────────────────────────────┘
```

## Native Dependency Graph

```
pangrova-worker (Java)
  └── JNI loads → libpangrova_native.dylib (C++)
                    ├── Velox (vectorized execution)
                    ├── Arrow (columnar format + Flight)
                    ├── Lance C FFI (vector storage, Rust→C)
                    ├── Protobuf (task protocol)
                    ├── OpenTelemetry C++ SDK (tracing)
                    ├── CRoaring (bitmap indexes)
                    └── simdjson (JSON parsing)
```

## Module Dependency Matrix

| Module | Allowed dependencies |
|--------|---------------------|
| `common` | (none) |
| `spi` | common |
| `protocol` | spi, common |
| `config` | common |
| `parser` | spi, common |
| `planner` | parser, spi, common, security, shuffle |
| `metadata` | spi, common, governance, security |
| `indexing` | spi, common |
| `observability` | spi, common |
| `governance` | spi, common, security |
| `security` | spi, common |
| `resilience` | spi, common |
| `streaming` | spi, common |
| `operations` | spi, common, shuffle |
| `resource` | spi, common |
| `shuffle` | spi, common |
| `worker` | spi, protocol, common, resilience, resource, observability, operations, shuffle, native-bundle |
| `coordinator` | spi, protocol, common, metadata, planner, parser, security, observability, resilience, resource, operations, governance, streaming, indexing, worker, shuffle, client-rest, connector-* |
| `connector-*` | spi, common, protocol only |
| `client-*` | protocol, common |
| `worker-native` | protocol only (JNI/Protobuf) |
| `native-bundle` | (packaging only) |
| `testing` | all (test scope) |

### Dependency Red Lines

- `spi` and `protocol`: MUST NOT depend on any implementation module.
- `connector-*`: only `spi`, `common`, `protocol` — NO worker/coordinator imports.
- `observability`, `governance`, `security`, `resilience`: NO business execution module imports.
- `operations`, `resource`, `config`: NO coordinator/worker internal imports.
- `coordinator` → `worker`: compile-scope restricted to `org.pangrova.worker.transport.*` only.
- Java ↔ C++: only via `protocol` + JNI bridge.

## DML Execution Routing Table

The fundamental boundary between Java and C++ execution layers:

| Operation | Reads data? | Execution path | Key code |
|-----------|------------|----------------|----------|
| `SELECT` | Yes | **Native** — vectorized scan + filter + projection | `QueryDispatcher` → stage/task dispatch |
| `INSERT VALUES` | No | **Java** — direct `ConnectorPageSink` write | `DmlExecutor.executeInsert()` |
| `INSERT OVERWRITE` (static) | No | **Java** — direct `ConnectorPageSink` overwrite | `DmlExecutor.executeInsertOverwrite()` |
| `INSERT SELECT` | Yes | **Native** read → worker-direct write → coordinator atomic commit | `PlanExecutionFactory.executeInsertSelect()` |
| `DELETE` (no WHERE) | No | **Java** — `deleteByFilter(All)` metadata op | `PlanExecutionFactory.executeDeleteWithMetadataFallback()` |
| `DELETE WHERE` (simple) | No | **Java** — connector `deleteByFilter` with predicate pushdown | same method, filter-conversion branch |
| `DELETE WHERE` (complex) | Yes | **Native** distributed — NOT(filter) → surviving rows → overwrite | same method, `executeDml()` fallback |
| `UPDATE` | Yes | **Native** distributed — workers apply assignments, coordinator atomic commit | `PlanExecutionFactory.executeDml()` via `NativeDmlCompileFunction` |
| `MERGE` | Yes | **Native** distributed — workers match/transform, coordinator atomic commit | `PlanExecutionFactory.executeDml()` via `NativeDmlCompileFunction` |
| DDL | No | **Java** — catalog metadata API | `DdlExecutor` |

**Routing rule**: All read-dependent DML (SELECT, INSERT SELECT, UPDATE, MERGE, complex DELETE)
goes through Native (Velox) distributed execution — coordinator-level CoW was removed to eliminate
the unbounded coordinator-heap OOM risk. Workers produce commit-metadata fragments via
`finishWithMetadata()`; the coordinator performs the single atomic catalog commit through
`ConnectorMetadata.finishDistributedWrite()` (fenced by `WriteCommitFencer`, journaled in
`WriteCommitJournal`). Pure metadata ops (INSERT VALUES, DELETE ALL, simple DELETE WHERE, DDL)
stay in Java. Final catalog commit always happens in Java via connector SPI.

## C++ Module Dependency Rules

Within `pangrova-worker-native/src/`, C++ components follow layered dependencies:

| Component | Allowed dependencies |
|-----------|---------------------|
| `jni/` | `compat/` only — JNI layer must be THIN |
| `jni/bridge/` | `executor/`, `result/`, `shuffle/`, `write/`, `lance/`, `vector/` |
| `jni/command/` | `executor/`, `jni/bridge/`, `jni/registry/`, `compat/` |
| `velox/` | Velox SDK, Arrow, Protobuf — NO direct JNI includes |
| `executor/` | `velox/`, `memory/`, `shuffle/`, `result/`, `observability/` |
| `shuffle/` | Arrow Flight, `memory/`, `observability/` |
| `memory/` | Velox memory SDK, `observability/` |
| `merge/`, `delete/`, `index/`, `split/` | `velox/connector/`, `memory/` |
| `result/` | Arrow Flight, `memory/` |
| `observability/` | OpenTelemetry SDK only (no business logic) |
| `lance/` | Lance C FFI, Arrow |
| `vector/` | Arrow, `memory/` |
| `write/` | `velox/`, `memory/` |

### C++ Dependency Red Lines

- `jni/` headers MUST NOT include Velox headers (abstraction boundary)
- `velox/` MUST NOT include JNI headers (direction: JNI→Velox, never Velox→JNI)
- `observability/` MUST NOT include business logic headers
- `memory/` MUST NOT include executor or shuffle headers (direction: they depend on memory)
- No circular includes between C++ components

## Full-Chain Connectivity Requirements

The system ships as ONE integrated binary. These invariants must hold at all times:

### Wiring Invariants

1. **Every SPI interface has ≥1 implementation** — no dangling interfaces
2. **Every implementation is wired** — registered via constructor injection or ServiceLoader
3. **Every Java `native` declaration has a C++ implementation** — no half-bridged JNI
4. **Every C++ JNI function is in `jni_exported_symbols.map`** — no invisible exports
5. **Every proto message is used** — no orphan message definitions
6. **Every config key is read** — no orphan configuration
7. **Every public API has Javadoc AND a test** — no undocumented or untested surfaces

### Connectivity Verification

When a change can affect query execution or a boundary in this path, verify the
applicable links:
```
SQL → Parser → Planner → Protocol → JNI → Native Executor → Velox → Arrow Result → Client
```

If ANY link in this chain is broken, the system is broken. E2E tests validate this chain.

### Dead Code Policy

- **Zero tolerance for dead code**: if code is unreachable, delete it
- **Zero tolerance for stub implementations**: `throw new UnsupportedOperationException()` is acceptable ONLY with `@Experimental` annotation and tracking issue
- **Zero tolerance for orphan imports**: unused `import` (Java) or `#include` (C++) must be removed
- Run `.pangrova/scripts/detect-dead-stubs.sh` when changing SPI, wiring,
  registrations, or implementation connectivity
