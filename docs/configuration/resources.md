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

### Tenant Reservations

Tenant concurrency limits come from the authenticated tenant's stored quota, not
from a second resource-group configuration file. With the metadata datasource
wired, the security bootstrap uses the shared PostgreSQL admission authority.
Without that authority, local counters enforce only process-local concurrency.

Each shared live reservation identifies both the query and its tenant. An exact
repeated reservation consumes one slot; an unknown, duplicate, or wrong-tenant
release cannot return another query's slot. Backend failures surface to the
reservation owner and must be retried; unavailable usage is never reported as
zero. Query identifiers must not be reused after their physical lifetime closes.
Capacity may be returned only after the issuing query authority confirms that
all entered execution has physically drained, not merely after a logical final
state, an HTTP acknowledgement, or lease expiry.

An existing anonymous nonzero counter has no query-owner evidence. Activation
rejects that state rather than clearing or relabeling it. Reconciliation requires
proof that its original executions have drained. An admission `QUEUE` verdict
alone is not a durable queue or a guarantee that the query will be retried.

Memory estimates checked against heartbeat usage are observational checks, not
atomic cluster-wide memory reservations. They do not install Worker CPU, scan,
or daily-usage enforcement. Independent-process failover, physical-drain wiring,
and runtime quota enforcement require separate deployment acceptance tests.

### Process-Local Query Throttle

The coordinator also owns a process-local global concurrency semaphore plus
tenant and user token-bucket rate limits. A slot is tied to the exact issued
`ThrottlePermit` object. Duplicate returns, copied values, and permits issued by
another service cannot increase its capacity. Rejection or an exception before
the permit is published returns the acquired global slot. Token-bucket tokens
are not refundable.

Rates must be finite and positive; invalid updates leave both configured rates
unchanged. This throttle is not a distributed reservation authority. Returning
its slot still depends on the issuing query's physical-drain lifecycle.

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
