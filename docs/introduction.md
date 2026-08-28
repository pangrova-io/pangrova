---
title: "Introduction"
description: "Pangrova — a distributed SQL query engine for lakehouse architecture"
slug: /
---

# Introduction

Pangrova is a distributed SQL query engine built for lakehouse architecture. It combines
Java-based coordination and Apache Calcite planning with C++20 vectorized execution
connected through JNI, delivering fast interactive analytics over open table formats.

## Key Capabilities

- **Lakehouse-native** — query Apache Iceberg and Apache Paimon tables through a
  multi-catalog model with unified SQL across warehouses.
- **Distributed execution** — a Coordinator plans and schedules queries; stateless
  Workers execute vectorized fragments with adaptive memory management and spilling.
- **AI-native SQL** — `VECTOR` and `FILE` data types, AI functions
  (`AI_COMPLETE`, `AI_EMBED`, multimodal inference), and ANN vector search with
  HNSW / IVFPQ indexes, all expressible in standard SQL.
- **Rich indexing** — data-skipping indexes (bloom filter, bitmap, sparse), lookup
  indexes (B-Tree, hash, prefix), full-text (inverted), containment (GIN), spatial
  (R-Tree), and vector indexes.
- **Data versioning** — time travel, branches and tags, snapshot metadata, and
  incremental reads on top of the table format's snapshot model.
- **Operational components** — high availability, resource queues, multi-tenancy,
  security, metrics, tracing, and logging, with constraints documented per subsystem.

The presence of syntax or an SPI does not by itself mean a feature is end-to-end ready.
Statement and subsystem pages call out connector requirements, runtime dependencies, and
known maturity limits where they apply.

## Where to Go Next

| Section | Description |
|---------|-------------|
| [Quickstart](getting-started/quickstart.md) | Build Pangrova, launch a standalone cluster, and run your first query in 5 minutes |
| [SQL Reference](sql-reference/queries.md) | Supported SQL syntax, execution constraints, and connector requirements |
| [Data Types](data-types/numeric/boolean.md) | All supported types: numeric, string, date/time, complex, AI-native, semi-structured, spatial, binary |
| [Indexing](indexing/index-ddl.md) | Index DDL and per-type guides for skipping, lookup, full-text, containment, spatial, and vector indexes |
| [Configuration](configuration/index.md) | Configuration model and per-component key reference |
| [Deployment](deployment/planning.md) | Docker, Helm/Kubernetes, distributed bare-metal deployment, and production checklist |
| [Architecture](architecture/index.md) | In-depth design documentation for every subsystem |
| [Operations](operations/index.md) | Day-2 operations, game days, and runbooks |

## Clients

Connect from Java with the [JDBC driver](getting-started/clients/jdbc.md), from any
language through the [REST API](getting-started/clients/rest-api.md), integrate
catalog tooling through the [REST Catalog API](getting-started/clients/rest-catalog-api.md),
or work interactively with the [CLI](getting-started/clients/cli.md).
