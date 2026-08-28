---
title: "Planning"
description: "Deployment topology selection, sizing guidance, and prerequisites"
---

# Deployment Planning

Choose a deployment topology before installing Pangrova. This guide covers the
decision points, sizing guidance, and prerequisites shared by all deployment methods.

## Deployment Options

| Method | Use Case | Guide |
|--------|----------|-------|
| Standalone (single JVM) | Local development, feature evaluation | [Quickstart](../getting-started/quickstart.md) |
| Docker Compose | Development and testing clusters | [Docker](docker.md) |
| Helm / Kubernetes | Cloud-native production deployment | [Helm / Kubernetes](helm-kubernetes.md) |
| Bare-metal / VM | Production with S3/OSS/MinIO | [Distributed](distributed.md) |

## Topology

A Pangrova cluster has two roles:

- **Coordinator** — parses and plans SQL, schedules fragments, aggregates results,
  and serves the REST / catalog APIs. Run 2+ coordinators with HA enabled for
  production.
- **Worker** — executes query fragments with vectorized operators. Workers are
  stateless and register with the coordinator on startup; scale them horizontally
  with the workload.

Query data lives in the object store (S3, OSS, MinIO, HDFS, or local filesystem);
workers need local disk only for spilling and caches.

## Sizing Guidance

| Component | Starting point | Notes |
|-----------|---------------|-------|
| Coordinator | 4 CPU / 8 GiB, JVM heap up to 16 GiB | Scale with concurrent query count, not data volume |
| Worker | 8–16 CPU / 32–64 GiB | Keep the JVM heap small (`-Xmx` 8–16 GiB); give the rest to off-heap execution memory (`worker.memory.budget-bytes`) and direct memory |
| Spill disk | SSD, sized to the largest expected spill | Configure `worker.spill.directory` |
| Object storage | Bandwidth-bound | Prefer same-region buckets; enable checksums / versioning per your storage policy |

See [Worker Configuration](../configuration/worker.md) for the memory
model and [Resource Configuration](../configuration/resources.md) for
query admission and rate limiting.

## Prerequisites

- **JDK 17** on every node (bundled images include a JRE)
- **Object storage bucket** with credentials reachable from all workers
- **HA state store** (JDBC database) for multi-coordinator deployments
- **Metadata store** (JDBC database) if using the built-in catalog service and the
  [REST Catalog API](../getting-started/clients/rest-catalog-api.md)
- Network access: coordinator HTTP port (default `8080`), worker HTTP port
  (default `8081`), and the worker Arrow Flight transport port
  (`worker.transport.port`; production templates pin it to `9090`) between nodes

## Pre-Go-Live

Before promoting a cluster to production, walk through the
[Production Checklist](production-checklist.md).

**See Also:** [Configuration Model](../configuration/index.md), [Operations](../operations/index.md)
