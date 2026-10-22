<p align="center">
  <img src=".github/assets/logo.png" alt="Pangrova" width="400" />
</p>
<h1 align="center">Pangrova</h1>
<p align="center">
  <em>Open-source distributed SQL for open lakehouses.</em>
</p>
<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License" /></a>
  <a href="https://github.com/pangrova-project/pangrova/actions"><img src="https://img.shields.io/github/actions/workflow/status/pangrova-project/pangrova/ci.yml?branch=main" alt="Build Status" /></a>
  <img src="https://img.shields.io/badge/Java-17+-orange.svg" alt="Java 17+" />
  <img src="https://img.shields.io/badge/C++-20-blue.svg" alt="C++ 20" />
  <a href="https://github.com/pangrova-project/pangrova/releases/latest"><img src="https://img.shields.io/github/v/release/pangrova-project/pangrova?label=Latest%20Release" alt="Latest Release" /></a>
</p>

---

## What is Pangrova?

Pangrova is an **open-source distributed SQL query engine for open lakehouses**.
It queries existing lake tables for relational analytics and data processing,
with SQL primitives for vector search, full-text retrieval, and AI model invocation.

Built-in connectors currently support **Apache Iceberg and Apache Paimon**. A pluggable
**Connector SPI** provides the extension point for additional table formats and data sources.

Built with **[Apache Calcite](https://calcite.apache.org/),
[Apache Arrow](https://arrow.apache.org/), and [Velox](https://velox-lib.io/)**, Pangrova combines
SQL optimization, C++20 vectorized execution, and columnar data exchange in a Coordinator/Worker
architecture. It targets data platform engineers evaluating lakehouse analytics and AI teams
exploring retrieval alongside business data, filters, and permissions.

**Start here:** [Quickstart](docs/getting-started/quickstart.md) ·
[Architecture](docs/architecture/overview.md) ·
[Capability status](docs/getting-started/capability-status.md) ·
[Contributing](CONTRIBUTING.md)

The defining architectural boundary is between coordination and execution: Java owns SQL planning,
scheduling, metadata, and final catalog commits, while Native Workers own relational data processing.
Relational DML, including `INSERT VALUES`, uses the planner → Worker/Native write path;
Workers return commit fragments for one connector-owned atomic catalog commit. DDL is a Java
metadata operation. Model-provider calls run on the Worker side.

> **Vision:** let enterprises analyze and search their existing lakehouse data with SQL,
> while reducing data movement, application-side integration, and duplicated governance.

Read the [project positioning guide](docs/getting-started/project-positioning.md) for core
problems, advantages, target customers, competitive context, and maturity boundaries.

### Problems We Address

- **Data movement for analytics:** access existing lake tables through connectors, reducing the
  need to copy complete business datasets into another database. Indexes, caches, embeddings,
  and other derived data may still be required.
- **Fragmented analytics and retrieval:** combine supported relational operations, business
  filters, vector distances, full-text search, and reranking through SQL.
- **Separate AI processing scripts:** expose model invocation through functions such as
  `AI_EMBED` and `AI_COMPLETE`, with Worker-side provider configuration and admission.
- **Version and governance gaps:** bind snapshots, models, and index generations and apply
  authorization, tenant, and resource checks within the execution framework.

The strongest use case to validate is **business-constrained retrieval over existing lakehouse
data, combined with relational analysis**. Supported query combinations and their qualification
limits must be checked individually.

### Why Pangrova?

| Dimension | Pangrova's Approach |
|-----------|-------------------|
| **Performance** | C++ Velox vectorized execution with explicit Protobuf/Arrow IPC boundaries across JNI |
| **SQL** | ANSI-style query support plus lakehouse extensions, with support documented per statement |
| **Open Formats** | Apache Iceberg & Apache Paimon connector implementations via pluggable SPI, with per-format constraints |
| **Scalability** | Coordinator/Worker execution with HA and resource-management components |
| **Extensibility** | Trino-style Connector SPI — build your own connector without touching engine internals |
| **AI-Native** | Vector, full-text, embedding, and model-provider primitives with maturity called out in the reference docs |
| **Observability** | OpenTelemetry provider SPIs, metrics, tracing, and structured-logging components |
| **Security** | Pluggable authentication plus authorization, masking, row-filter, and audit components; see configuration docs for the verified runtime surface |

These are architectural and integration foundations. Performance leadership, cost savings,
scaling efficiency, and operational simplicity require workload-specific evidence. Arrow buffer
sharing does not make IPC, format conversion, compression, or network transport physically zero-copy.

### Who Is Pangrova For?

- **Enterprise data platform teams with Iceberg/Paimon:** evaluate interactive analytics,
  cross-table queries, and analytics/retrieval integration on existing data assets.
- **Enterprise AI and knowledge platform teams with a lakehouse:** evaluate retrieval that
  must respect business filters, permissions, and data versions.
- **Paimon operational analytics teams:** validate reads, merge semantics, and data freshness
  on continuously updated tables.
- **Software vendors and platform integrators:** explore a customizable SQL engine for
  deployments they operate, using the connector SPI and Apache 2.0 license.

These are target customer profiles, not a list of deployed customers. Initial adoption should
start with a bounded proof of concept using real data, SQL, correctness expectations, and an
existing-system baseline. Regulated production deployments require further qualification;
robotics and embodied-data platforms remain a longer-term direction.

### Maturity and Qualification

Start with [Current Capability Status](docs/getting-started/capability-status.md): it
separates wired execution paths from no-op defaults, absent registration, and target
behavior. In particular, standalone maintenance is a default no-op, server `SET` does
not persist context, dynamic-table materialization is registered only when durable
metadata publication is configured, and continuous-query refresh remains incomplete.

The repository contains substantial implementations and focused validation, but overall production
qualification remains open. A parser rule, SPI, example, or component test does not establish
end-to-end support. Full hybrid retrieval, generated embeddings, multimodal workflows, and
distributed recovery must be verified for the exact path used.

No qualified comparative performance baseline currently supports speedup multiples, cost reduction
percentages, PB-scale capacity, or semantic-cache hit-rate gains. Complete Feature Store,
training-dataset services, qualified dynamic-table refresh, and materialized-view lifecycle
support remain development or qualification work. See the [project positioning guide](docs/getting-started/project-positioning.md),
[AI architecture](docs/architecture/ai-native.md), and
[production checklist](docs/deployment/production-checklist.md).

---

## Quick Start

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| JDK | 17+ | `JAVA_HOME` must be set |
| Maven | 3.9+ | For building from source |
| Python | 3.10+ | Required for canonical function-contract generation |
| Docker | 20+ | Required for integration tests (TestContainers) |
| CMake + Ninja | 3.20+ | Required when building the Native library from source; Workers need a matching Native bundle |

### Build from Source

```bash
git clone https://github.com/pangrova-project/pangrova.git
cd pangrova
./pangrova-worker-native/bin/setup-native-build-env.sh
./pangrova-worker-native/bin/build-native.sh
mvn clean install -DskipTests
```

The native bootstrap verifies installed tools; it does not install or upgrade
OS packages. Run `./pangrova-worker-native/bin/setup-native-build-env.sh --check-tools`
for the read-only prerequisite check. Dependency builds select one complete RE2
package (pinned `2024-07-02`) for both gRPC and the execution SDK; do not repair
an archive by appending objects from another release. SDK and main builds must
also select the same Abseil package, including the instrumented package for ASan.

Pangrova ships as an integrated Java/native engine. The native library must be built and packaged
before executing queries through a Worker. See the
[Developer Guide](docs/contributing/developer-guide.md):

```bash
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
```

### Start in Development Mode

The fastest way to get started — runs Coordinator and Worker in a single JVM:

```bash
./bin/pangrova-standalone.sh
```

The engine is now listening on `http://localhost:8080`.

### Start a Distributed Cluster

```bash
# Terminal 1: Start Coordinator
./bin/pangrova-coordinator.sh

# Terminal 2: Start Worker (auto-registers with Coordinator)
./bin/pangrova-worker.sh

# Optional: check cluster health
./bin/pangrova-cluster-status.sh
./bin/pangrova-health-check.sh
```

> All scripts run in the foreground by default. Add `--daemon` to run in the
> background (PID files are written to `logs/`). A unified entry point,
> `bin/pangrova.sh`, wraps `start`, `stop`, `status`, `health-check`, `cli`,
> `catalog`, and `rolling-upgrade` subcommands.

### Run Your First Query

```bash
./bin/pangrova-cli.sh --host localhost --port 8080
```

```sql
-- Verify the engine responds
SELECT 1 + 1 AS result;

-- Inspect registered catalogs
SHOW CATALOGS;

-- Query your lakehouse after registering a catalog; see docs/getting-started/quickstart.md
SELECT count(*), region
FROM local_iceberg.sales.orders
WHERE order_date >= TIMESTAMP '2024-01-01'
GROUP BY region
ORDER BY count(*) DESC;
```

You can also submit queries over REST:

```bash
curl -X POST http://localhost:8080/v1/query/submit \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT 1 + 1 AS result"}'
```

### Stop All Processes

```bash
./bin/pangrova-stop.sh --all
```

> `pangrova-stop.sh` stops processes started with `--daemon` (via PID files in
> `logs/`). Foreground processes are stopped with `Ctrl+C`.

---

## Architecture

The target architecture spans five layers, from SQL / Python clients to lakehouse
storage, and includes the planned ML data and model lifecycle. Java coordinates work;
Workers execute it through JNI and C++ / Velox. The diagram marks **existing**
implementations, **partial** lifecycles, **planned** capabilities, and **external**
runtimes. Existing components still require workload-specific qualification; see the
[System Overview](docs/architecture/overview.md) and
[Current Capability Status](docs/getting-started/capability-status.md).

### Layers and Responsibilities

<p align="center">
  <img src=".github/assets/architecture.png" alt="Pangrova target architecture: five layers with implementation status, planned Python SDK and PyTorch data loading, feature and dataset services, and the external training and model-serving loop" width="1000" />
</p>

| Layer | Main Components | Responsibility |
|-------|-----------------|----------------|
| **1. Clients and ecosystem access** | CLI, JDBC driver, REST client; planned Python SDK and ML / Agent adapters | Submit SQL, manage requests, and consume results. Planned Python access streams Arrow batches into pandas / NumPy or a PyTorch / JAX dataset adapter. |
| **2. Coordination and data services** | Coordinator, parser, Calcite planner, metadata and scheduling; JDBC control state; target feature, dataset, refresh, semantic and model services | Bind versions, authorize, plan, schedule, execute metadata DDL and publish metadata / catalog commits. Persist leases, query ownership and commit journals. Complete feature / training datasets, CDC / dynamic-table refresh, model evaluation and self-optimization remain partial or planned. |
| **3. Worker runtime** | Worker JVM, task admission, JNI bridge | Load Native libraries, admit and manage tasks, report status and write commit metadata, and provide Java connector / model-provider callbacks. |
| **4. Native data processing** | C++ executor, Velox, Native readers, Arrow Flight; target feature / training-data operators | Execute scans, relational processing, retrieval, writes and exchange. Planned feature / training delivery adds point-in-time joins, online feature reads, dataset sharding / sampling, episode windows and media decoding. |
| **5. Lakehouse and versioned artifacts** | Iceberg / Paimon, object storage / file systems, derived artifacts; planned ML data-product manifests | Persist source data and artifacts. Planned dataset / feature manifests bind source snapshots, schema, split definitions and training shards for reproducible consumption. |

Layers 3 and 4 run within each Worker. Workers exchange data through Arrow Flight;
the output Worker streams query results directly to the client through Arrow Flight.
For writes, Workers return commit metadata to layer 2 for catalog publication.
Worker writes can call Java connector SDKs through JNI; DDL stays in layer 2.

The planned Python / PyTorch path is **SQL selection → pinned dataset version →
Worker Arrow Flight batches → Python `IterableDataset` / `DataLoader` → external
CPU / GPU training**. The adapter must partition reads by distributed rank and
DataLoader worker, preserve split / seed / epoch semantics, and handle tensor conversion
and CPU / GPU transfers. JAX requires a separate planned framework adapter.
Trained models enter the target registration / evaluation workflow, are deployed to
external serving endpoints, and can then be invoked through Worker-side AI SQL
callbacks. The Python SDK, framework adapters and complete training service are not
implemented integrations today. This design follows
[PyTorch's iterable data-loading model](https://docs.pytorch.org/docs/stable/data.html)
and [Arrow Flight batch streaming](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamReader.html).

### Protocol Matrix

| Path | Protocol | Plane | Purpose |
|------|----------|-------|---------|
| Client → Coordinator | HTTP/REST | Control | Query submission, status, cancellation |
| Coordinator → Worker | HTTP/REST | Control | Task dispatch, health check |
| Worker ↔ Worker | Arrow Flight (gRPC) | Data | Shuffle exchange |
| Output Worker → Client | Arrow Flight (gRPC) | Data | Result streaming |
| Java ↔ C++ (JNI) | Protobuf + Arrow C Data Interface / IPC | Internal | Typed control and columnar interoperability with explicit ownership |

---

## Core Features

- **Native Vectorized Execution** — C++ Velox execution with Arrow interoperability and an explicit JNI protocol
- **Coordinated Distributed Writes** — read-dependent transforms run on Workers; the Coordinator performs the final Iceberg/Paimon catalog commit
- **AI-Native Primitives** — Vector search, embedding, reranking, and model-provider building blocks with support levels documented per feature
- **Query Optimization Components** — AQE and dynamic-filtering implementations; validate applicable runtime paths and workload effects
- **Lakehouse SQL Extensions** — Time travel, DML, and maintenance syntax with current support documented in the SQL reference
- **HA Components** — Coordinator election, leases, and recovery infrastructure; deployment-specific failover requires validation
- **Connector SPI** — Pluggable data source architecture (built-in Iceberg & Paimon; extensible to custom sources)
- **Streaming Components** — CDC and watermark infrastructure; complete dynamic-table refresh and publication remain qualification work
- **Security Components** — Authentication, RBAC, column masking, and row filtering; production authentication and consumed keys are documented in [Security Configuration](docs/configuration/security.md)
- **Observability Components** — Configured tracing/metrics providers and structured key=value logging; deployment coverage requires validation

---

## AI-Native Capabilities

Pangrova exposes AI through SQL functions and retrieval primitives. Support depends on the exact
model deployment, capability binding, artifact generation, and validated execution path.

### Database WITH AI — AI as SQL Primitives

The following examples illustrate SQL forms. They require configured models, suitable table schemas,
and applicable published indexes. Their presence does not establish that every combination is
qualified end to end; see the [AI function](docs/sql-reference/functions/ai-functions.md) and
[vector search](docs/sql-reference/functions/vector-search.md) references.

```sql
-- In-database embedding generation
SELECT ai_embed('bge_m3', 'wireless headphones') AS embedding;

-- Distance ordering; actual ANN routing requires a compatible index and plan
SELECT id, l2_distance(embedding, ARRAY[0.5, 0.3, 0.2, 0.0]) AS dist
FROM products
ORDER BY l2_distance(embedding, ARRAY[0.5, 0.3, 0.2, 0.0]) ASC
LIMIT 10;

-- Hybrid search: BM25 full-text + vector similarity with RRF fusion
SELECT id, title FROM products
WHERE MATCH(description, 'headphones')
ORDER BY l2_distance(embedding, ARRAY[0.5, 0.3, 0.2, 0.0]) ASC
LIMIT 10
RERANK BY rrf(60);

-- LLM invocation as a SQL function
SELECT ai_complete('llama3', 'Summarize: ' || content) FROM articles;
```

| Capability | Description and boundary |
|-----------|--------------------------|
| **Vector Search** | Distance functions and HNSW / IVFPQ / DiskANN implementations; index routing, supported filters, and quality require per-path validation |
| **Embeddings** | `ai_embed(model, text)` invokes an exact configured model through the Worker-side provider path |
| **Hybrid Search** | `MATCH()` and vector ordering with fusion/rerank syntax; supported combinations and distributed correctness remain bounded |
| **LLM Functions** | SQL model-invocation functions; external serving, failure handling, and output contracts require configuration and validation |
| **Multimodal AI** | FILE-based model inputs; modality support depends on the exact provider dialect and governed media path |
| **Full-Text Search** | BM25/index components and `MATCH()` syntax; complete published-index retrieval must be verified |
| **Auto-Embedding DDL** | Model catalog and generated-column/materialization components; complete lifecycle and publication qualification remain open |

### Optimization Components and Roadmap

Cache, feedback, and advisor components are intended to improve workload efficiency. Their runtime
integration and benefits require separate validation; no autonomous-operation or hit-rate claim is made.

| Capability | What It Does |
|-----------|---------------|
| **Semantic Cache** | Cache components with tenant/resource constraints; workload hit-rate and end-to-end qualification remain open |
| **Cardinality Feedback** | Execution-statistics feedback components; optimizer consumption and workload gains require validation |
| **Vector Index Lifecycle** | Versioned index build/publication components; full rebuild, recovery, and maintenance authorization require validation |
| **Data Layout Advisor** | Layout recommendation components; automated application and measurable gains require validation |
| **Retrieval Quality Observability** *(planned)* | Continuously measure recall/precision of AI search results |

> **Product direction:** combine analytical queries and AI retrieval over open lakehouse assets
> through a shared SQL and governance framework. Measure the integration benefits on customer
> workloads; external model serving and derived artifacts remain part of the deployment.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| SQL Parsing | ANTLR4 (custom grammar) |
| Query Optimization | Apache Calcite |
| Plan Interchange | Substrait |
| Native Execution | Meta Velox |
| Columnar Format | Apache Arrow |
| Data Transport | Arrow Flight (gRPC) |
| Table Formats | Apache Iceberg, Apache Paimon |
| Coordination & Protocol | JDBC leases by default, optional ZooKeeper (Curator), canonical Protobuf, HTTP control, Arrow Flight |
| Observability | OpenTelemetry |
| Testing | JUnit 5, jqwik (property-based), TestContainers |

For exact dependency versions, see the root [pom.xml](pom.xml).

---

## Repository Layout

| Module | Responsibility |
|--------|----------------|
| `pangrova-parser` | ANTLR4-based SQL parsing |
| `pangrova-planner` | Calcite-based optimization & Substrait plan generation |
| `pangrova-coordinator` | Cluster coordination, query lifecycle, HA |
| `pangrova-worker` | Java-side task management, DML execution |
| `pangrova-worker-native` | C++20 Velox execution engine, JNI bridge, vector indexes |
| `pangrova-native-bundle` | Per-platform packaging of the native JNI library |
| `pangrova-spi` / `pangrova-connector` | Connector SPI and built-in Iceberg / Paimon connectors |
| `pangrova-shuffle` | Shuffle exchange contracts and in-tree storage providers |
| `pangrova-metadata` / `pangrova-indexing` | Catalog management and index services |
| `pangrova-streaming` | CDC ingestion and continuous queries |
| `pangrova-security` / `pangrova-governance` | Authentication, RBAC, masking, lineage |
| `pangrova-resource` / `pangrova-resilience` | Resource management and fault tolerance |
| `pangrova-observability` / `pangrova-operations` | Metrics, tracing, and ops tooling |
| `pangrova-protocol` | Protobuf message and REST API contract definitions |
| `pangrova-transport` | Arrow IPC / Arrow Flight decoding for clients |
| `pangrova-common` / `pangrova-config` | Shared utilities and the typed YAML configuration framework |
| `pangrova-standalone` | Single-JVM Coordinator + Worker entry point for dev/test |
| `pangrova-client` | CLI, JDBC, and REST clients |
| `pangrova-testing` | E2E, benchmark, chaos, fuzz, and smoke test suites |

---

## Documentation

| Document | Description |
|----------|-------------|
| [Project Positioning](docs/getting-started/project-positioning.md) | Core problems, advantages, target customers, competitive context, and maturity boundaries |
| [Getting Started](docs/getting-started/quickstart.md) | Installation and first-query walkthrough |
| [SQL Reference](docs/sql-reference/queries.md) | Supported SQL syntax and lakehouse extensions |
| [Vector Search](docs/sql-reference/functions/vector-search.md) | ANN indexes, similarity search, hybrid retrieval |
| [AI Functions](docs/sql-reference/functions/ai-functions.md) | `ai_embed`, `ai_complete`, and related functions |
| [Connectors](docs/getting-started/connectors/iceberg.md) | Iceberg and Paimon setup |
| [Clients](docs/getting-started/clients/jdbc.md) | CLI, JDBC, and REST client usage |
| [Architecture Overview](docs/architecture/overview.md) | System design, core principles, protocol mapping |
| [Configuration Reference](docs/configuration/reference.md) | Verified YAML keys, system properties, and environment variables |
| [Developer Guide](docs/contributing/developer-guide.md) | Development setup, code standards, testing |

The architecture index lives in [docs/architecture/](docs/architecture/index.md).

---

## Contributing

We welcome contributions of all kinds — bug reports, feature requests, documentation improvements, and code contributions.

### How to Contribute

1. **Report Issues** — Search [existing issues](https://github.com/pangrova-project/pangrova/issues) first, then file a new one with reproduction steps
2. **Submit PRs** — Fork → branch (`feature/xxx` or `fix/xxx`) → develop → `mvn clean install` → PR
3. **Follow Standards** — See [CONTRIBUTING.md](CONTRIBUTING.md) for code style, testing rules, and PR checklist

### Development Principles

- **No mock frameworks** — Use real implementations, TestContainers, and property-based testing (jqwik)
- **Constructor injection only** — No Spring DI, no `@Autowired`
- **Immutable by default** — Records, final fields, builders
- **Strict module boundaries** — SPI/protocol modules never depend on implementations

### Community

- [Code of Conduct](CODE_OF_CONDUCT.md) — Community standards
- [Security Policy](SECURITY.md) — Reporting vulnerabilities
- [Issue Templates](.github/ISSUE_TEMPLATE/) — Bug reports and feature requests

---

## Roadmap

- [ ] **RAG Pipeline Operator** — Native retrieve-augment-generate as a single SQL statement
- [ ] **Cross-Catalog Qualification** — Broaden validation of existing multi-catalog query paths and connector pushdown
- [ ] **Materialized Views** — Automatic refresh, query rewriting
- [ ] **Retrieval Quality Observability** — Recall/precision measurement for AI search (designed, see [AI-Native Architecture](docs/architecture/ai-native.md))
- [ ] **GPU Acceleration** — RAPIDS integration for AI inference and compute-intensive operations
- [ ] **Serverless Mode** — Auto-suspend/resume, pay-per-query deployment

---

## Acknowledgements

Pangrova stands on the shoulders of outstanding open-source projects:

- [Apache Calcite](https://calcite.apache.org/) — Query optimization framework
- [Meta Velox](https://velox-lib.io/) — Native vectorized execution library
- [Apache Arrow](https://arrow.apache.org/) — Columnar in-memory format and Flight RPC
- [Substrait](https://substrait.io/) — Cross-language plan specification
- [Apache Iceberg](https://iceberg.apache.org/) — Open table format for huge analytic datasets
- [Apache Paimon](https://paimon.apache.org/) — Streaming lakehouse table format

---

## License

Pangrova is licensed under the [Apache License 2.0](LICENSE).
