---
title: Introducing Pangrova — A Distributed SQL Engine for the Lakehouse Era
authors: [pangrova-team]
tags: [announcement, architecture]
description: We are excited to introduce Pangrova, a high-performance distributed SQL query engine designed from the ground up for lakehouse architecture.
---

# Introducing Pangrova

We are excited to introduce **Pangrova**, a high-performance distributed SQL query engine that brings native performance to the lakehouse.

## Why Another Query Engine?

The lakehouse architecture promises to unify data warehousing and data lake workloads on open table formats. But existing engines force a trade-off: either you get SQL compatibility with JVM overhead, or bare-metal performance with a proprietary stack.

Pangrova eliminates this trade-off.

<!-- truncate -->

## The Key Design Bets

### 1. Read-Write Separation

Pangrova makes a clear architectural split:
- **Reads** (scans, filters, aggregations) run in **C++ via Meta Velox** — no JVM overhead, bare-metal vectorized execution.
- **Writes** (INSERT, UPDATE, DELETE, MERGE) run in **Java** against the Iceberg/Paimon SDK — correct transactional semantics guaranteed by the table format.

### 2. Apache Arrow Zero-Copy

Data flows between Java and C++ through Apache Arrow columnar buffers with zero serialization cost. The JNI bridge transfers memory pointers, not data.

### 3. Calcite-Powered Optimization

The full power of Apache Calcite's cost-based optimizer, extended with Pangrova-specific rules for partition pruning, predicate pushdown, and dynamic filtering.

### 4. AI-Native SQL

Vector search, hybrid retrieval, in-database embeddings, and LLM UDFs are first-class SQL primitives — not external service calls.

## What's Next

We're actively developing Pangrova in the open. Check out our [Getting Started guide](/docs/getting-started/quickstart) to try it out, or dive into the [Architecture docs](/docs/architecture/overview) to understand the design.

Star us on [GitHub](https://github.com/pangrova/pangrova) and join the conversation!
