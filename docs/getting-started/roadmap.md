---
title: "Roadmap"
description: "Remaining implementation gaps, proposed extensions, and qualification goals"
---

# Roadmap

This roadmap lists remaining work, not an inventory of implemented features.
These goals are not release commitments. Read the
[current capability status](capability-status.md) for support boundaries.

## In progress and open items

- **Materialization extensions:** implement CDC refresh and incrementally refreshed
  continuous queries beyond the existing full and incremental refresh paths.
- **Embedding-model deletion:** add durable dependency reservations and resumable
  CASCADE processing before enabling `DROP EMBEDDING MODEL`.
- **Training-dataset delivery:** add SQL entry points, durable multi-source export,
  Native generation publication, and streaming readers.
- **Evaluation execution:** implement Native run submission, fenced terminal
  metric reports, and RBAC subject resolution.
- **System-table coverage:** extend Paimon `$files` type/filesystem coverage and
  support non-local `$branches`. Carry quoted table names containing a literal
  `$` through typed parser, planner, and connector identities.
- **Directory queries:** execute SQL projections over published Stage directory
  indexes through the Native data plane.

See the [AI architecture](../architecture/ai-native.md) for the remaining model,
training, and evaluation designs.

## Proposed directions

| Direction | Remaining scope |
|---|---|
| Retrieval quality | Build reproducible recall and precision benchmarks for supported search paths |
| RAG workflows | Add workflow orchestration around retrieval, context assembly, and generation |
| Python access | Develop a Python client and Arrow-based dataset delivery integrations |
| GPU acceleration | Evaluate GPU-backed engine operators for suitable workloads |
| Serverless deployment | Connect idle-scaling decisions to provisioning and demand-driven resumption |

## Qualification goals

Billion-entry directory publication, 100 GiB transfers, PB-scale workloads, and
Linux production acceptance require workload-specific evidence. These are
qualification goals, not missing base features or claims of supported scale.
Follow the [production checklist](../deployment/production-checklist.md) for
release acceptance.

Propose priorities or concrete work through GitHub issues.
