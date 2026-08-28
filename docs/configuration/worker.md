---
title: "Worker"
description: "Worker core, memory, shuffle, native cache, and JVM configuration"
---

# Worker

All worker-side configuration keys, as defined by `WorkerOptions` in the
`pangrova-config` module. The bundled defaults ship in
`pangrova-config/src/main/resources/worker.yml`.

> **Protocol mapping.** The Worker receives control commands from the
> Coordinator over HTTP (REST) (control plane) and exchanges shuffle data /
> streams results over Arrow Flight (gRPC) (data plane). For the full protocol
> mapping, see [System Overview §4.6](../architecture/overview.md#46-canonical-protocol-mapping).


## worker (root)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.host` | string | `"0.0.0.0"` | Bind address for the worker HTTP server |
| `worker.port` | int | `8081` | Listen port for the worker HTTP server |
| `worker.advertised-host` | string | — | Hostname advertised to the coordinator for result fetching. Set explicitly when auto-detection fails (e.g. behind NAT) |
| `worker.id` | string | `"worker-1"` | Unique identifier for this worker instance |
| `worker.coordinator-url` | string | — | URL of the coordinator to register with. Required for registration |
| `worker.heartbeat-interval` | duration | `"10s"` | Interval between heartbeat reports to the coordinator. Must be smaller than `coordinator.worker.heartbeat-timeout` |

## worker.transport

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.transport.port` | int | `0` | Listen port for the Arrow Flight transport server (`0` = auto-assign). Set a fixed port in production for firewall rules |

## worker.task

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.task.result-retention-seconds` | int | `300` | Seconds to retain query results before expiration. Clients must fetch results before this window expires |

## worker.shuffle

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.shuffle.buffer-capacity` | int | `100` | Maximum number of batches per shuffle partition buffer |
| `worker.shuffle.max-memory-bytes` | long | `536870912` (512 MB) | Total memory budget in bytes for all shuffle partitions combined |
| `worker.shuffle.spill-threshold` | double | `0.8` | Memory usage ratio (0.0–1.0) that triggers shuffle spill to disk |

## worker.memory

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.memory.budget-bytes` | long | `536870912` (512 MB) | Total memory budget in bytes for the native execution engine |
| `worker.memory.system-reserved-bytes` | long | `67108864` (64 MB) | Memory reserved for system overhead in bytes |
| `worker.memory.node-bytes` | long | `1073741824` (1 GB) | Total node memory pool capacity in bytes |
| `worker.memory.pressure-threshold-low` | double | `50.0` | Memory usage percentage that triggers LOW pressure level |
| `worker.memory.pressure-threshold-medium` | double | `70.0` | Memory usage percentage that triggers MEDIUM pressure level |
| `worker.memory.pressure-threshold-high` | double | `85.0` | Memory usage percentage that triggers HIGH pressure level |
| `worker.memory.pressure-threshold-critical` | double | `95.0` | Memory usage percentage that triggers CRITICAL pressure level |

## worker.threads

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.threads.query` | int | `4` | Number of threads for query execution |
| `worker.threads.io` | int | `2` | Number of threads for I/O operations |
| `worker.threads.scheduler` | int | `1` | Number of threads for the task scheduler |

## worker.spill

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.spill.directory` | string | — | Directory for spilling intermediate data to disk. Leaving it unset disables spill |

## worker.native

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.native.path` | string | — | Explicit path to the native library file. If unset, the library is extracted from the classpath bundle |
| `worker.native.workdir` | string | — | Working directory for native library extraction |
| `worker.native.delete-after-load` | bool | `true` | Delete the extracted native library temp file after loading |

### worker.native.cache

The Velox async data cache absorbs repeated scans of hot data. The RAM cache
is in-process; the SSD cache persists across restarts.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.native.cache.enabled` | bool | `false` | Enable the native async data cache (in-memory + optional SSD) |
| `worker.native.cache.memory-bytes` | long | `8589934592` (8 GB) | RAM budget in bytes for the in-memory data cache |
| `worker.native.cache.ssd-path` | string | `""` | Local filesystem path for SSD-backed cache |
| `worker.native.cache.ssd-bytes` | long | `107374182400` (100 GB) | SSD cache capacity in bytes |
| `worker.native.cache.ssd-shards` | int | `8` | Number of SSD cache shards for parallel I/O |
| `worker.native.cache.checkpoint-interval-bytes` | long | `1073741824` (1 GB) | SSD cache checkpoint interval in bytes. The native engine persists its SSD cache index every N bytes so the cache survives process restart. Set to `0` to disable checkpointing |

## worker.registration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.registration.max-attempts` | int | `5` | Maximum number of registration attempts to the coordinator |
| `worker.registration.initial-backoff` | duration | `"1s"` | Initial backoff duration for registration retry |
| `worker.registration.max-backoff` | duration | `"30s"` | Maximum backoff duration for registration retry |

## worker.cdc

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.cdc.enabled` | bool | `true` | Enable CDC (Change Data Capture) processing on this worker |

## worker.internal-auth-secret

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.internal-auth-secret` | string | — | Shared secret for the `X-Internal-Auth` header between Coordinator and Worker. Must be identical on all nodes |

---

## Worker JVM System Properties

| System property | Default | Description |
|-----------------|---------|-------------|
| `pangrova.ai.inference.endpoint` | `http://localhost:11434/v1` | OpenAI-compatible inference endpoint used by the AI_* functions |
| `pangrova.ai.inference.api-key` | _(empty)_ | API key sent as a Bearer token to the inference endpoint |
| `pangrova.ai.inference.model` | `llama3` | Default model name for AI_* functions |

See [AI Functions](../sql-reference/functions/ai-functions.md) for details.

---

## JVM Sizing Guide

Workers allocate memory across three pools:

| Component | Setting | Notes |
|-----------|---------|-------|
| JVM heap | `-Xmx` | Task metadata, result buffers, sort tables |
| Arrow off-heap | `-XX:MaxDirectMemorySize` | Arrow columnar buffers |
| Velox RAM cache | `worker.native.cache.memory-bytes` | Velox data cache |

Rule of thumb: `Xmx + MaxDirectMemorySize + native.cache.memory-bytes ≤ 0.85 × total node RAM`

| Node RAM | `-Xmx` | `MaxDirectMemorySize` | Velox RAM cache |
|----------|--------|-----------------------|-----------------|
| 64 GB | `24g` | `8g` | `16g` |
| 128 GB | `32g` | `16g` | `32g` |
| 256 GB | `48g` | `32g` | `128g` |

Recommended JVM flags: `-XX:+UseG1GC`, `-XX:+ExitOnOutOfMemoryError`,
`-XX:+HeapDumpOnOutOfMemoryError`, `-Dfile.encoding=UTF-8`,
`-Duser.timezone=UTC`, `-Djava.net.preferIPv4Stack=true`.

**See Also:** [Configuration model](index.md), [Coordinator](coordinator.md), [Quick Reference](reference.md)
