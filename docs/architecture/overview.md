---
title: "System Overview"
description: "Current component ownership, execution boundaries, and protocol mapping"
sidebar_label: "System Overview"
sidebar_position: 2
---

# System Overview

Pangrova combines Java coordination and Calcite planning with C++20 / Velox
execution through JNI. Iceberg and Paimon connectors provide metadata access,
scan planning, and catalog publication. Arrow supports interoperability and data
exchange; Velox owns the Native execution representation.

Read [Current Capability Status](../getting-started/capability-status.md) before
interpreting an architectural component as a completed product feature. This page
summarizes source ownership and design constraints, not production qualification
or measured performance.

## 1. Product Direction

The intended product is a distributed SQL engine over existing lakehouse assets,
with relational processing and bounded AI/retrieval paths in a shared execution
framework. The [positioning guide](../getting-started/project-positioning.md)
explains the problems, proposed customer profiles, advantages, and evidence limits.

## 2. Planning and Execution Stack

```text
SQL / API
  -> ANTLR parser and statement classification
  -> Java metadata dispatch, or Calcite relational planning
  -> canonical v2 task request containing the execution plan and bindings
  -> Worker admission and JNI
  -> C++ / Velox execution
  -> Native exchange and Output Worker result stream
```

Parser recognition alone does not establish dispatch or execution support.
See the [SQL reference](../sql-reference/index.md) for statement-specific boundaries.

## 3. Component Topology

```text
JDBC / REST client / CLI
       | submit, status, cancel (HTTP)
       v
Coordinator
  parse / bind / authorize / plan / admit / schedule / fence
  connector metadata and final catalog commit
       | task control (HTTP)
       v
Worker JVM
  lifecycle / admission / JNI / governed provider callbacks
       |
       v
Native Worker (C++ / Velox)
  scan / relational operators / retrieval / write / shuffle
       |                           |
       | Arrow Flight exchange     | files / immutable artifacts
       v                           v
Other Workers                 Lakehouse storage
       |
Output Worker -- Arrow Flight results --> Client
```

The diagram describes the main relational execution path. The current CDC bootstrap
uses a Coordinator-local connector page-sink writer; it is a known migration gap
from the Worker/Native boundary, not evidence that distributed CDC is complete.

Coordinator leases use JDBC by default; ZooKeeper is an optional backend.
Durable metadata/journal storage and external catalogs remain availability
dependencies. Redis, automatic Worker scaling, additional connectors, and a
particular deployment topology are not prerequisites implied by this diagram.

## 4. Execution Boundaries

### 4.1 Coordination and Data Processing

Java owns parsing, planning, authentication/authorization, admission, scheduling,
metadata operations, and the final fenced connector catalog commit.
Relational DML, including `INSERT VALUES`, follows the planner → Worker → Native
path; Workers return commit fragments rather than publishing the final catalog
transaction independently. Worker write staging includes JNI callbacks into Java
connector SDK page sinks; this is not a pure C++ physical writer claim. DDL remains
a Java metadata operation.

See [Distributed Write](distributed-writes.md) for publication, recovery ambiguity,
and partial implementation status. This is coordination/execution separation,
not a division in which C++ reads and Java performs all data writes.

### 4.2 Availability

Lease, fencing, Worker registration/heartbeat, journal, and recovery components
exist. Their presence does not guarantee uninterrupted queries or eliminate all
single points of failure. Database HA, catalog availability, object storage,
networking, client result recovery, and failure tests remain part of qualification.
See [Distributed Architecture](distributed.md) and [Deployment Planning](../deployment/planning.md).

### 4.3 Extensibility

Connector factories and SPIs separate metadata and storage integration from the
engine. Connector plugins use class-loader isolation. An SPI can precede a provider;
its existence does not establish bundled implementation, live hot reload, or
production support. See [Connector Design](connectors.md).

### 4.4 Observability

Metrics, tracing, and structured-logging components are available through their
providers and runtime wiring. Configure and exercise the chosen providers before
claiming operational coverage. See [Observability](observability.md).

### 4.5 Control-Plane / Data-Plane Constraint

The architectural constraint places business-row processing, final aggregation,
retrieval fusion, and result streaming on Workers. The Coordinator validates
terminal metadata and exposes result endpoints. Worker JVM code supplies lifecycle,
policy, secrets, and governed provider networking; it is not a second relational engine.

This constraint is not a claim that every historical CDC, maintenance, or AI path
has completed migration. Known exceptions and missing wiring are tracked in the
[capability guide](../getting-started/capability-status.md) and subsystem documents.
Coordinator planning and metadata services still consume resources; neither
unlimited scaling nor automatic transparent result recovery is established here.

### 4.6 Canonical Protocol Mapping

| Communication path | Protocol / representation | Purpose |
|---|---|---|
| Client → Coordinator | HTTP REST | Submission, status, cancellation, endpoint discovery |
| Coordinator ↔ Worker | HTTP REST | Task control, registration, heartbeats, reports |
| Worker ↔ Worker | Arrow Flight | Data exchange; Native shuffle implementation |
| Output Worker → Client | Arrow Flight | Query results |
| Worker Java ↔ Native | Canonical v2 Protobuf; Arrow interoperability where applicable | Task bindings, lifecycle/status/reporting, typed data boundaries |

The authoritative execution definitions are in
[`pangrova-protocol/src/main/proto/pangrova/protocol/v2`](https://github.com/pangrova/pangrova/tree/main/pangrova-protocol/src/main/proto/pangrova/protocol/v2).
The admitted execution epoch and complete descriptor hash must match. Do not
substitute simplified documentation snippets for generated messages or invent a
Java shuffle relay from the existence of JNI interoperability methods.

Native shuffle serializes standard Arrow stream batches from Velox vectors and
serves them through Native Flight. Arrow can share buffers at specific boundaries.
IPC encoding, format conversion,
compression, Java callbacks, and network transfer still require separate accounting;
the whole query pipeline is not established as zero-copy.
See [JNI Bridge](jni-bridge.md), [Native Execution](native-execution.md), and
[Query Execution](query-execution.md).

## 5. Component Ownership

### 5.1 Core SPIs

Contracts live in the artifact that owns the domain boundary: connectors in
`pangrova-connector-spi`, observability in `pangrova-observability-api`, indexing
in `pangrova-indexing-api`, AI in `pangrova-ai-api`, and admission/placement in
`pangrova-scheduler`'s `api` packages. Production callers and bootstrap wiring
determine which providers are active. Not every contract is discovered through
`ServiceLoader`; `.pangrova/modules.yaml` records the registered providers.

Use the actual source declarations and subsystem references for signatures:
[connectors](connectors.md), [streaming and maintenance](streaming.md),
[security](security.md), and [observability](observability.md).
Conceptual lifecycle or planner interfaces are not public SPIs unless present in source.

### 5.2 Protocol and Bootstrap

`pangrova-protocol` owns generated execution contracts. Coordinator bootstrap wires
statement dispatch, planner dependencies, metadata, leases, and service providers.
Worker bootstrap wires Native loading, admission, transport, and provider callbacks.
See the [current status table](../getting-started/capability-status.md) for default
no-op providers and absent registrations.

### 5.3 Parser

`pangrova-parser` owns syntax and ASTs. Some recognized statements have no executable
default path. `COPY INTO` URI loading and clustering must not be promoted to working
features based on grammar alone; dynamic-table handlers are registered only when
durable metadata publication is configured.

### 5.4 Planner

`pangrova-planner` owns Calcite integration, transformations, and plan conversion.
Default dependency injection and enabled rules constrain which transformations
are usable. See [Query Optimization](query-optimization.md) and [AI Architecture](ai-native.md).

### 5.5 Native Execution

`pangrova-worker-native` owns Velox integration, Native operators, storage readers
and writers, artifact processing, exchange, memory, and result production.
Native registries are C++ integration points, not Java service-loader interfaces.

## 6. Project Structure

This is a selected ownership map, not a fixed count of Maven reactor modules.

| Area | Source roots |
|---|---|
| Shared contracts/configuration | `pangrova-protocol`, `pangrova-common`, `pangrova-config` |
| Parsing and planning | `pangrova-parser`, `pangrova-planner` |
| Coordination and metadata | `pangrova-coordination/pangrova-coordination-core`, `pangrova-metadata` |
| Query and durable artifact contracts/runtime | `pangrova-query-api`, `pangrova-query`, `pangrova-data-build` (artifact JDBC adapters still reside in Metadata) |
| Execution | `pangrova-worker/pangrova-worker-core`, `pangrova-worker-native`, `pangrova-shuffle`, `pangrova-transport` |
| Connectors | `pangrova-connector/pangrova-connector-iceberg`, `pangrova-connector/pangrova-connector-paimon` |
| Platform services | `pangrova-security`, `pangrova-operations`, `pangrova-governance`, `pangrova-streaming`, `pangrova-observability`, `pangrova-indexing` |
| Clients and packaging | `pangrova-client`, `pangrova-standalone`, `pangrova-native-bundle` |
| Validation | `pangrova-testing` and module-local test sources |
| Documentation | `docs/`, `website/`; proposals and historical audits in `docs/internal/` |

## 7. Module Dependencies

Use module POMs for actual compile/runtime dependencies. Architecture policy defines
permitted access across modules and the Java/Native boundary; a dependency graph
sketch is not a substitute for those rules. The Coordinator's dependency on Worker
transport types does not authorize access to Worker execution business logic.

## 8. Development Policy

### 8.1 Package Naming

Java packages follow the owning module under `org.pangrova`. Check source packages
rather than copying abbreviated names from architecture sketches.

### 8.2 Contracts

Before changing an SPI, inspect callers, implementations, bootstrap wiring, and tests.
Never hand-edit generated protocol sources.

### 8.3 Code Structure

The authoritative code and validation policy lives in
[repository rules](https://github.com/pangrova/pangrova/tree/main/.pangrova/rules).
The [Developer Guide](../contributing/developer-guide.md) provides practical entry points;
this overview does not duplicate or override repository policy.

## 9. Dependency Management

Use the [parent POM](https://github.com/pangrova/pangrova/blob/main/pom.xml)
and module POMs for pinned Java versions/dependencies, and the Native build configuration
for Velox and C++ dependencies. Baseline development uses JDK 17, Maven 3.9+, CMake,
and a C++20 compiler. A Native library or matching packaged Native bundle is required
for Worker execution; a Java-only build is not a substitute.
