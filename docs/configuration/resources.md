---
title: "Resources"
description: "Admission control, rate limiting, and worker memory management configuration"
---

# Resources

Pangrova does not expose a declarative resource-group / queue configuration
file. Resource governance is achieved by a small set of verified configuration
keys covering query admission, HTTP rate limiting, and worker memory
management.


## Query Admission

The coordinator limits how many queries execute concurrently. Queries beyond
this limit are rejected at submission.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.query.max-concurrent` | int | `100` | Maximum number of concurrently executing queries |
| `coordinator.query.timeout` | duration | `"5m"` | Maximum query execution time before timeout |

See [Coordinator](coordinator.md) for the full coordinator key reference.

## HTTP Rate Limiting

The coordinator HTTP server applies a per-minute request rate limit through
its request-limit filter.

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.http.max-requests-per-minute` | `100` | Maximum HTTP requests per minute accepted by the coordinator |

## Worker Memory Management

Workers govern memory through the `worker.memory.*` keys (see
[Worker](worker.md) for the full table):

| Key | Default | Description |
|-----|---------|-------------|
| `worker.memory.budget-bytes` | 512 MB | Memory budget for the native execution engine |
| `worker.memory.system-reserved-bytes` | 64 MB | Memory reserved for system overhead |
| `worker.memory.node-bytes` | 1 GB | Total node memory pool capacity |
| `worker.memory.pressure-threshold-low` | `50.0` | Usage percentage triggering LOW pressure |
| `worker.memory.pressure-threshold-medium` | `70.0` | Usage percentage triggering MEDIUM pressure |
| `worker.memory.pressure-threshold-high` | `85.0` | Usage percentage triggering HIGH pressure |
| `worker.memory.pressure-threshold-critical` | `95.0` | Usage percentage triggering CRITICAL pressure |

## Spill and Shuffle Backpressure

When memory pressure rises, intermediate data spills to disk:

| Key | Default | Description |
|-----|---------|-------------|
| `worker.spill.directory` | — | Spill directory; leaving it unset disables spill |
| `worker.shuffle.spill-threshold` | `0.8` | Memory usage ratio (0.0–1.0) triggering shuffle spill |
| `worker.shuffle.max-memory-bytes` | 512 MB | Total memory budget for all shuffle partitions |

## Concurrency

Worker-side parallelism is bounded by thread pool sizing:

| Key | Default | Description |
|-----|---------|-------------|
| `worker.threads.query` | `4` | Query execution threads |
| `worker.threads.io` | `2` | I/O operation threads |
| `worker.threads.scheduler` | `1` | Task scheduler threads |

**See Also:** [Coordinator](coordinator.md), [Worker](worker.md),
[Quick Reference](reference.md)
