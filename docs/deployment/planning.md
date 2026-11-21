---
title: "Deployment Planning"
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
| Helm / Kubernetes | Cloud cluster setup; production use requires qualification | [Helm / Kubernetes](helm-kubernetes.md) |
| Bare-metal / VM | Distributed cluster setup with storage dependencies | [Distributed](distributed.md) |

## Topology

A Pangrova cluster has two roles:

- **Coordinator** — parses and plans SQL, authorizes and admits queries, schedules fragments,
  serves control/catalog APIs, and performs final catalog publication. Final query
  aggregation and result streaming belong to an Output Worker. Multiple Coordinators
  need configured leases/fencing and an available durable state store.
- **Worker** — executes fragments with Native vectorized operators and serves results.
  Workers register on startup and retain task, shuffle, spill, cache, and artifact state.
  Replacement and scaling require validating the workload's recovery/storage behavior.

Query data lives in the object store (S3, OSS, MinIO, HDFS, or local filesystem);
Workers may also use local disk for shuffle and artifact staging; size it for the enabled paths.
See [Current Capability Status](../getting-started/capability-status.md).

## Sizing Guidance

These are planning starting points, not benchmark-derived capacity guarantees.
Measure concurrency, Native/JVM memory, spill, exchange, and storage on the target workload.

| Component | Starting point | Notes |
|-----------|---------------|-------|
| Coordinator | 4 CPU / 16 GiB, JVM heap 4–8 GiB | Scale with concurrent query count, not data volume |
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
