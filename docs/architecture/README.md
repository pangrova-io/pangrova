---
title: Architecture
sidebar_position: 1
---

# Pangrova Architecture

Pangrova is a high-performance, horizontally scalable **distributed SQL query engine for the
lakehouse**. It pairs an Apache **Calcite** optimizer with the **Velox** native vectorized
execution engine and uses Apache **Arrow** for zero-copy columnar exchange. Its defining design
choice is **read/write separation**: scans and compute run in C++ (Velox via JNI), while
transactional DML runs in Java against the Iceberg/Paimon SDKs.

```text
SQL
 │  ANTLR4 parser (custom SQL extensions)
 ▼
Calcite optimizer (rule- + cost-based)
 │  Substrait plan (cross-language)
 ▼
Velox engine (C++ native, vectorized)
```

This section is the design reference for the engine. It is organized as a progressive path — start
with the **System Overview**, then drill into the subsystem that matters for your task.

## How to read these docs

| If you want to… | Read, in order |
|---|---|
| Understand the whole system | [System Overview](./01-overview.md) → [Distributed Architecture](./02-distributed-architecture.md) → [Query Execution](./03-query-execution.md) |
| Follow a query end to end | [Query Execution](./03-query-execution.md) → [Query Optimization](./04-query-optimization.md) → [Native Execution Layer](./05-native-layer.md) |
| Work on storage / connectors | [Connector Design](./06-connector-design.md) → [Metadata Management](./07-metadata-management.md) → [Distributed Write](./10-distributed-write-design.md) |
| Operate a cluster | [Resource Management](./11-resource-management.md) → [Resilience](./12-resilience.md) → [Observability](./14-observability.md) → [Day-2 Operations](./22-operations.md) |
| Contribute code | [Developer Guide](./24-developer-guide.md) → [Testing Framework](./23-testing-framework.md) → [JNI Bridge](./19-jni-bridge-design.md) |

## Document map

### Core Engine
| Document | Scope |
|---|---|
| [System Overview](./01-overview.md) | System topology, module design, technology stack, protocol mapping |
| [Distributed Architecture](./02-distributed-architecture.md) | Coordinator–Worker model, high availability, shuffle durability |
| [Query Execution](./03-query-execution.md) | End-to-end query flow, Calcite integration, REST API, exchange protocol, write path |
| [Query Optimization](./04-query-optimization.md) | Materialized views, result cache, admission control, smart routing |
| [Native Execution Layer](./05-native-layer.md) | C++ native layer, read architecture, Velox integration |

### Data & Storage
| Document | Scope |
|---|---|
| [Connector Design](./06-connector-design.md) | Connector SPI, Substrait abstraction, federated queries |
| [Metadata Management](./07-metadata-management.md) | Unified metadata, catalog federation, tiered cache, schema evolution |
| [Indexing](./08-indexing-architecture.md) | Index architecture, vector (ANN) indexes, three-tier pruning, distributed maintenance |
| [Streaming & CDC](./09-streaming.md) | CDC, incremental query, streaming writes, watermarks |
| [Distributed Write](./10-distributed-write-design.md) | Distributed write design, transaction atomicity, two-phase commit |

### Platform Services
| Document | Scope |
|---|---|
| [Resource Management](./11-resource-management.md) | Unified memory arbitration across JVM + native, spill, resource queues |
| [Resilience](./12-resilience.md) | Error-code taxonomy, circuit breakers, degradation, retry policy |
| [Cache Layer](./13-cache-layer.md) | Multi-tier (RAM/SSD) cache, smart prefetch, Cache SPI |
| [Observability](./14-observability.md) | OpenTelemetry + Prometheus instrumentation |
| [Security](./15-security.md) | Authentication, authorization, row/column-level access, masking, audit |
| [Multi-Tenancy](./16-multi-tenancy.md) | Tenant model, resource isolation, quota management |
| [Data Governance](./17-data-governance.md) | Data lineage, data quality, data classification |

### AI-Native
| Document | Scope |
|---|---|
| [AI-Native Architecture](./18-ai-native-architecture.md) | Vector search, AI functions, semantic cache, intelligent optimization |

### Internals & Reference
| Document | Scope |
|---|---|
| [JNI Bridge](./19-jni-bridge-design.md) | Protocol boundary contract, memory ownership, known issues |
| [Dynamic Filter Lifecycle](./20-dynamic-filter-lifecycle.md) | Dynamic filter state machine and lifecycle |
| [Configuration Reference](./21-configuration-reference.md) | Full configuration-key reference |

### Operations & Contributing

These design references are grouped under the **Operations** and **Contributing** sections of the site navigation:

| Document | Scope |
|---|---|
| [Day-2 Operations](./22-operations.md) | Rolling upgrade, online DDL, disaster recovery, autoscaling, failure-recovery SOPs |
| [Testing Framework](./23-testing-framework.md) | Test strategy, fuzzers, benchmarks, chaos testing |
| [Developer Guide](./24-developer-guide.md) | Dev environment, module development, connector development |

## Core design principles

1. **Read/write separation.** Reads run in C++/Velox (vectorized, SIMD, zero-copy) for maximum
   throughput; writes run in Java against the Iceberg/Paimon SDKs for full ACID transactions and
   upstream compatibility.
2. **Control plane vs. data plane.** The control plane (parse, optimize, schedule, metadata) speaks
   HTTP/REST; the data plane (scan, compute, shuffle, results) uses Arrow Flight (gRPC).
3. **Single source of truth for metadata.** The Java connector is the sole owner of metadata; the
   native layer never caches it. Queries observe a single consistent snapshot end to end.
4. **Write atomicity.** Distributed writes use a two-phase commit: either all fragments commit or
   none do.
5. **Extensibility via SPIs.** All extension points follow one pattern — interfaces in `pangrova-spi`,
   implementations in connector/plugin modules, discovered via `ServiceLoader`. Core SPIs include
   `ConnectorFactory`, `ConnectorMetadata`, `ConnectorSplitManager`, `ConnectorPageSourceProvider`,
   `ConnectorPageSinkProvider`, `TableWriter`, `TableMaintenanceSpi`, `CacheSpi`, `TracingProvider`,
   and `MetricsProvider`. See the [System Overview](./01-overview.md) for the full signature table.
6. **Production readiness.** Unified resource management (JVM + native memory with spill),
   multi-tier caching, fuzzer-based correctness testing, and chaos testing for fault tolerance.

## Java ↔ Native protocol

| Direction | Payload | Transport |
|---|---|---|
| Java → Native | Task request (Substrait plan + splits) | Protobuf |
| Native → Java | Data batches | Arrow C Data Interface (zero-copy); Java pull via `nativeGetNextBatch()` |
| Native → Java | Status / metrics | Protobuf; Java pull via `nativeGetTaskStats()` |

For the complete, authoritative protocol-mapping table, see the
[System Overview](./01-overview.md).

## Shared modules

| Module | Responsibility | Shared across |
|---|---|---|
| `pangrova-protocol` | Protobuf definitions, Arrow schemas | Java ↔ Native |
| `pangrova-spi` | Connector / Cache / Serializer SPIs + connector base abstractions + Flight contracts | Global |
| `pangrova-common` | Utilities, constants, exception definitions | Global |

## Platform baseline

- **Java:** 17+
- **C++ standard:** C++20
- **Core dependencies:** Calcite 1.36.0, Arrow 19.0.0, Velox (latest), Iceberg 1.11.0,
  Paimon 1.4.1, Substrait 0.23.0, Hadoop 3.3.6
