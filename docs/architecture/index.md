---
title: "Architecture"
description: "Engine ownership, design constraints, and subsystem implementation references"
sidebar_position: 1
---

# Architecture

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

Pangrova is a **distributed SQL query engine for the lakehouse**. It pairs an Apache **Calcite** optimizer with the **Velox** native vectorized
execution engine and uses Apache **Arrow** for columnar exchange. Java owns coordination,
planning, metadata, and final catalog commits; C++ owns vectorized relational execution,
including DML. Known CDC and other migration gaps are described in
[Current Capability Status](../getting-started/capability-status.md).

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

## How to interpret status

Architecture documents describe both implemented behavior and explicitly labelled remaining work.
A class, interface, parser rule, or plan node by itself is not evidence of an end-to-end feature.
For user-visible support, verify the production entry point, wiring, connector/native implementation,
and the closest integration or E2E test. The SQL reference explicitly marks recognized syntax with absent or incomplete execution
wiring. Test existence and source inspection are not a completed runtime qualification.
Roadmap material is kept outside the published site.

## How to read these docs

| If you want to… | Read, in order |
|---|---|
| Understand the whole system | [System Overview](./overview.md) → [Distributed Architecture](./distributed.md) → [Query Execution](./query-execution.md) |
| Follow a query end to end | [Query Execution](./query-execution.md) → [Query Optimization](./query-optimization.md) → [Native Execution Layer](./native-execution.md) |
| Work on storage / connectors | [Connector Design](./connectors.md) → [Metadata Management](./metadata.md) → [Distributed Write](./distributed-writes.md) |
| Operate a cluster | [Resource Management](./resource-management.md) → [Resilience](./resilience.md) → [Observability](./observability.md) → [Day-2 Operations](../operations/day-2.md) |
| Contribute code | [Developer Guide](../contributing/developer-guide.md) → [Testing Framework](../contributing/testing.md) → [JNI Bridge](./jni-bridge.md) |

## Document map

### Core Engine
| Document | Scope |
|---|---|
| [System Overview](./overview.md) | System topology, module design, technology stack, protocol mapping |
| [Distributed Architecture](./distributed.md) | Coordinator–Worker model, high availability, shuffle durability |
| [Query Execution](./query-execution.md) | End-to-end query flow, Calcite integration, REST API, exchange protocol, write path |
| [Query Optimization](./query-optimization.md) | Materialized views, result cache, admission control, smart routing |
| [Native Execution Layer](./native-execution.md) | C++ native layer, read architecture, Velox integration |

### Data & Storage
| Document | Scope |
|---|---|
| [Connector Design](./connectors.md) | Connector SPI, Substrait abstraction, federated queries |
| [Metadata Management](./metadata.md) | Unified metadata, catalog federation, tiered cache, schema evolution |
| [Indexing](./indexing.md) | Index architecture, vector (ANN) indexes, three-tier pruning, distributed maintenance |
| [Streaming & CDC](./streaming.md) | CDC, incremental query, streaming writes, watermarks |
| [Distributed Write](./distributed-writes.md) | Distributed write design, transaction atomicity, two-phase commit |

### Platform Services
| Document | Scope |
|---|---|
| [Resource Management](./resource-management.md) | Unified memory arbitration across JVM + native, spill, resource queues |
| [Resilience](./resilience.md) | Error-code taxonomy, circuit breakers, degradation, retry policy |
| [Cache Layer](./caching.md) | Multi-tier (RAM/SSD) cache, smart prefetch, Cache SPI |
| [Observability](./observability.md) | OpenTelemetry + Prometheus instrumentation |
| [Security](./security.md) | Authentication, authorization, row/column-level access, masking, audit |
| [Multi-Tenancy](./multi-tenancy.md) | Tenant model, resource isolation, quota management |
| [Data Governance](./data-governance.md) | Data lineage, data quality, data classification |

### AI-Native
| Document | Scope |
|---|---|
| [AI-Native Architecture](./ai-native.md) | Vector search, AI functions, semantic cache, intelligent optimization |

### Internals & Reference
| Document | Scope |
|---|---|
| [JNI Bridge](./jni-bridge.md) | Protocol boundary contract, memory ownership, known issues |
| [Dynamic Filter Lifecycle](./dynamic-filtering.md) | Dynamic filter state machine and lifecycle |
| [Configuration Reference](../configuration/reference.md) | Full configuration-key reference |

### Operations & Contributing

These design references are grouped under the **Operations** and **Contributing** sections of the site navigation:

| Document | Scope |
|---|---|
| [Day-2 Operations](../operations/day-2.md) | Rolling upgrade, online DDL, disaster recovery, autoscaling, failure-recovery SOPs |
| [Testing Framework](../contributing/testing.md) | Test strategy, fuzzers, benchmarks, chaos testing |
| [Developer Guide](../contributing/developer-guide.md) | Dev environment, module development, connector development |

## Core design principles

1. **Coordination/execution separation.** Relational data processing, including DML, runs in C++/Velox. Java owns
   metadata-only operations and the final Iceberg/Paimon catalog commit for distributed writes.
2. **Control plane vs. data plane.** The control plane (parse, optimize, schedule, metadata) speaks
   HTTP/REST; the data plane (scan, compute, shuffle, results) uses Arrow Flight (gRPC).
3. **Metadata authority.** Java connectors resolve table metadata; Native receives immutable
   query bindings. Snapshot and artifact validity must track the authorized table/version.
   Per-table snapshot pinning does not imply one atomic snapshot across independent catalogs.
4. **Write publication.** Workers produce fragments for one fenced connector catalog commit.
   Recovery ambiguity and incomplete paths remain explicit in [Distributed Write](distributed-writes.md);
   this design does not establish complete distributed-write qualification.
5. **Extensibility via SPIs.** Stable contracts live in `pangrova-spi` or their owning
   modules. Plugin-capable contracts may be discovered via `ServiceLoader`; not every
   SPI uses service loading. Core SPIs include
   `ConnectorFactory`, `ConnectorMetadata`, `ConnectorSplitManager`, `ConnectorPageSourceProvider`,
   `ConnectorPageSinkProvider`, `TableWriter`, `TableMaintenanceSpi`,
   `DistributedCacheStore`, `TracingProvider`,
   and `MetricsProvider`. Use source declarations and the owning subsystem document for signatures.
6. **Verifiable operation.** Resource management, caching, resilience, and observability behavior
   must be backed by production wiring and tests before the documentation calls it supported.

## Java ↔ Native protocol

| Direction | Payload | Transport |
|---|---|---|
| Java → Native | Task request (Substrait plan + splits) | Protobuf |
| Java ↔ Native | Typed data at interoperability boundaries | Arrow interfaces/IPC where applicable; not an end-to-end zero-copy guarantee |
| Native → Java | Status / metrics / terminal report | Canonical Protobuf lifecycle APIs |

For the complete, authoritative protocol-mapping table, see the
[System Overview](./overview.md).

## Shared modules

| Module | Responsibility | Shared across |
|---|---|---|
| `pangrova-protocol` | Protobuf definitions, Arrow schemas | Java ↔ Native |
| `pangrova-spi` | Connector / Cache / Serializer SPIs + connector base abstractions + Flight contracts | Global |
| `pangrova-common` | Utilities, constants, exception definitions | Global |

## Platform baseline

- **Java:** 17+
- **C++ standard:** C++20
- **Core dependencies:** Calcite 1.42.0, Arrow 19.0.0, Velox (repository-pinned revision), Iceberg 1.11.0,
  Paimon 1.4.1, Substrait 0.23.0, Hadoop 3.3.6
