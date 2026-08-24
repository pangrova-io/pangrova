---
title: User Guide
sidebar_position: 1
---

# Pangrova User Guide

Essential user-facing guides for working with Pangrova. For architecture design details, see [docs/architecture/](../architecture/README.md). For deployment and operations, see [docs/develop/](../develop/README.md).

---

## Quick Start

| Guide | Description |
|-------|-------------|
| [Getting Started](getting-started.md) | 5-minute quick experience: prerequisites, build, standalone launch, first query |

## SQL Language

| Guide | Description |
|-------|-------------|
| [SQL Reference](sql-reference.md) | Complete SQL syntax reference organized by category |
| [Data Types](data-types.md) | All supported data types: numeric, string, date/time, complex, AI-native, spatial |
| [Functions Reference](functions/index.md) | Built-in functions: aggregate, window, string, math, datetime, conditional |

## AI & Vector Search

| Guide | Description |
|-------|-------------|
| [AI Functions](ai-functions.md) | 15 AI functions: inference, multimodal, embedding, model DDL, backfill |
| [Vector Search](vector-search.md) | Vector similarity search: distance functions, HNSW/IVFPQ indexes, hybrid search, RERANK BY |

## Connect & Integrate

| Guide | Description |
|-------|-------------|
| [Clients](clients.md) | JDBC, REST API, and CLI client usage |
| [Connectors](connectors.md) | Iceberg & Paimon connectors, storage backends, Catalog management |

---

## Cross-References to Architecture Docs

For detailed design documentation on these topics, see the architecture docs:

| Topic | Architecture Doc |
|-------|-----------------|
| Security & Auth | [Security](../architecture/15-security.md) |
| Governance & Lineage | [Data Governance](../architecture/17-data-governance.md) |
| Observability | [Observability](../architecture/14-observability.md) |
| Resource Management | [Resource Management](../architecture/11-resource-management.md) |
| Streaming & CDC | [Streaming & CDC](../architecture/09-streaming.md) |
| Indexing | [Indexing](../architecture/08-indexing-architecture.md) |
| Query Optimization | [Query Optimization](../architecture/04-query-optimization.md) |
| Operations & Deployment | [Day-2 Operations](../architecture/22-operations.md) |
| Configuration Reference | [Configuration Reference](../architecture/21-configuration-reference.md) |
