---
title: "Introduction"
description: "Explore Pangrova, an extensible open-source distributed SQL engine for open lakehouses, with Calcite planning, Velox execution, Arrow exchange, and AI SQL primitives."
slug: /
---

# Introduction

Pangrova is an **open-source distributed SQL query engine for open lakehouses**.
It queries existing lake tables for relational analytics and data processing,
with SQL primitives for vector search, full-text retrieval, and AI model invocation.

Built-in connectors currently support **Apache Iceberg and Apache Paimon**. The pluggable
**Connector SPI** provides the extension point for additional table formats and data sources.

The engine combines **Apache Calcite** for SQL planning and optimization, **Velox** for C++20
vectorized execution, and **Apache Arrow** for columnar interoperability and Flight transport.
Java coordinates queries and metadata; Native Workers execute relational processing through JNI.
CLI, JDBC, and REST clients provide entry points for developers and data applications.

The product direction is to reuse existing Iceberg / Paimon assets while reducing full-dataset
copies, application-side result integration, and duplicated governance. Derived indexes, caches,
and embeddings may still be needed, and actual benefits require workload measurements.

## Problems and Audience

Pangrova targets enterprise data platform teams already using Iceberg / Paimon, AI and knowledge
platform teams whose retrieval needs business filters and permissions, and software integrators
building deployments they operate. These are suggested customer profiles, not deployed-customer claims.

The strongest use case to validate is business-constrained retrieval over existing lakehouse data,
combined with relational analysis. Initial adoption should use a bounded proof of concept with real
SQL, data, correctness expectations, and a baseline from the existing system.

Read the [project positioning guide](getting-started/project-positioning.md) for the core
problems, advantages, customer priorities, example scenarios, and competitive context.

## Key Capabilities

- **Lakehouse connectors** — access Apache Iceberg and Apache Paimon through a
  multi-catalog model, subject to each connector's supported table and file features.
- **Distributed execution** — a Coordinator plans and schedules queries;
  Workers execute vectorized fragments with adaptive memory management and spilling.
- **AI SQL components** — `VECTOR` and `FILE` types, model functions, and retrieval/index
  implementations; provider modalities, ANN routing, filters, and hybrid queries require
  validation of the exact execution path.
- **Indexing components** — data-skipping, lookup, full-text, containment, spatial, and vector
  implementations with per-type constraints and publication requirements.
- **Data versioning** — time travel, branches and tags, snapshot metadata, and
  incremental reads on top of the table format's snapshot model.
- **Operational components** — high availability, resource queues, multi-tenancy,
  security, metrics, tracing, and logging, with constraints documented per subsystem.

The presence of syntax or an SPI does not by itself mean a feature is end-to-end ready.
Statement and subsystem pages call out connector requirements, runtime dependencies, and
known maturity limits where they apply. Overall production qualification remains open; component
test results do not establish complete distributed recovery or release qualification. Comparative
speedups, cost reductions, PB-scale capacity, and semantic-cache hit-rate gains are not established
by a qualified performance baseline. Complete Feature Store, training-dataset services,
qualified dynamic-table refresh, and materialized-view lifecycle support remain development
or qualification work.

## Explore the documentation

<div class="docs-card-grid">
  <a class="docs-card" href="getting-started/quickstart/"><strong>Quickstart</strong><span>Build from source, launch a standalone cluster, and run the first query.</span></a>
  <a class="docs-card" href="getting-started/capability-status/"><strong>Capability status</strong><span>Implemented paths, default wiring gaps, and qualification limits.</span></a>
  <a class="docs-card" href="architecture/overview/"><strong>Architecture</strong><span>Component ownership, execution boundaries, and protocol mapping.</span></a>
  <a class="docs-card" href="sql-reference/"><strong>SQL reference</strong><span>Supported syntax, execution constraints, and connector requirements.</span></a>
  <a class="docs-card" href="data-types/numeric/boolean/"><strong>Data types</strong><span>Numeric, string, temporal, complex, AI-native, spatial, and binary types.</span></a>
  <a class="docs-card" href="indexing/index-ddl/"><strong>Indexing</strong><span>Index DDL plus skipping, lookup, full-text, containment, spatial, and vector guides.</span></a>
  <a class="docs-card" href="configuration/"><strong>Configuration</strong><span>Typed configuration model and the per-component key reference.</span></a>
  <a class="docs-card" href="deployment/planning/"><strong>Deployment</strong><span>Docker, Helm/Kubernetes, distributed deployment, and the production checklist.</span></a>
  <a class="docs-card" href="operations/"><strong>Operations</strong><span>Day-2 operations, game days, and failure-recovery runbooks.</span></a>
  <a class="docs-card" href="contributing/developer-guide/"><strong>Developer guide</strong><span>Local setup, module development, and connector development.</span></a>
  <a class="docs-card" href="getting-started/roadmap/"><strong>Roadmap</strong><span>Development priorities and open engineering items.</span></a>
  <a class="docs-card" href="getting-started/project-positioning/"><strong>Positioning</strong><span>Core problems, target customers, advantages, and maturity boundaries.</span></a>
</div>

## Clients

Connect from Java with the [JDBC driver](getting-started/clients/jdbc.md), from any
language through the [REST API](getting-started/clients/rest-api.md), integrate
catalog tooling through the [REST Catalog API](getting-started/clients/rest-catalog-api.md),
or work interactively with the [CLI](getting-started/clients/cli.md).
